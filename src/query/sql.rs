//! ClickHouse SQL generation for the hybrid serving layout.
//!
//! ClickHouse does not enforce the typed-value contract: a missing Map key
//! reads as the type default and `sum(Int64)` wraps. Every statement therefore
//! guards on Map key membership and accumulates integers in `Int128`; the
//! result checks that need the aggregated values live in `result`.
//!
//! Request values are never interpolated. They travel as ClickHouse query
//! parameters (`{p0:String}`), including attribute keys, so a key or string
//! literal cannot change the statement.

use super::model::{Aggregate, AttrRef, CmpOp, Namespace, Predicate, Query, Signal};
use super::schema::{
    self, DATASET_COLUMN, SPAN_DURATION_COLUMN, SPAN_TIMING_STATUS_COLUMN, ServingTables,
    TENANT_COLUMN, TIMESTAMP_COLUMN, TIMING_END_BEFORE_START, TYPED_SCALARS,
};
use super::value::{Literal, ValueType};

/// Result column names shared with `result`.
pub mod columns {
    pub const EVENTS: &str = "events";
    pub const CONTRIBUTING: &str = "contributing";
    pub const SUM: &str = "sum_value";
    pub const NON_FINITE: &str = "non_finite";
    pub const INCOMPATIBLE_TYPES: &str = "incompatible_types";
    pub const TIMING_ERRORS: &str = "timing_errors";
    pub const NON_FINITE_EXCLUDED: &str = "non_finite_excluded";

    pub fn group_type(index: usize) -> String {
        format!("g{index}_type")
    }

    pub fn group_value(index: usize) -> String {
        format!("g{index}_value")
    }
}

/// Group-key type of an attribute that is not present, distinct from every
/// received type including explicit empty.
pub const MISSING_TYPE: &str = "missing";

/// A bound ClickHouse query parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Param {
    pub name: String,
    pub ch_type: &'static str,
    pub value: String,
}

impl Param {
    /// The value as ClickHouse parses a `param_<name>` in the escaped text
    /// format, for the HTTP interface and `clickhouse-client`.
    pub fn wire_value(&self) -> String {
        let mut out = String::with_capacity(self.value.len());
        for c in self.value.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '\t' => out.push_str("\\t"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\0' => out.push_str("\\0"),
                c => out.push(c),
            }
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlStatement {
    /// A `SELECT ... FORMAT JSONEachRow` with `{name:Type}` placeholders.
    pub sql: String,
    pub params: Vec<Param>,
}

#[derive(Default)]
struct Binder {
    params: Vec<Param>,
}

impl Binder {
    fn bind(&mut self, ch_type: &'static str, value: String) -> String {
        let name = match self
            .params
            .iter()
            .find(|p| p.ch_type == ch_type && p.value == value)
        {
            Some(existing) => existing.name.clone(),
            None => {
                let name = format!("p{}", self.params.len());
                self.params.push(Param {
                    name: name.clone(),
                    ch_type,
                    value,
                });
                name
            }
        };
        format!("{{{name}:{ch_type}}}")
    }

    fn string(&mut self, value: &str) -> String {
        self.bind("String", value.to_owned())
    }
}

/// Generate the statement for a request that passed `validate`.
pub fn generate(query: &Query, tables: &ServingTables) -> SqlStatement {
    let mut binder = Binder::default();
    let mut select: Vec<String> = Vec::new();
    let mut group: Vec<String> = Vec::new();

    for (index, attr) in query.group_by.iter().enumerate() {
        let key = binder.string(&attr.key);
        let (type_col, value_col) = (columns::group_type(index), columns::group_value(index));
        select.push(format!(
            "{} AS {type_col}",
            type_expr(query.signal, attr, &key)
        ));
        select.push(format!(
            "{} AS {value_col}",
            value_expr(query.signal, attr, &key)
        ));
        group.push(type_col);
        group.push(value_col);
    }
    select.push(format!("count() AS {}", columns::EVENTS));
    select.extend(aggregate_columns(query, &mut binder));

    let conditions = conditions(query, &mut binder, |_| true);

    let mut sql = format!(
        "SELECT {} FROM {} WHERE {}",
        select.join(", "),
        tables.table(query.signal),
        conditions.join(" AND ")
    );
    if !group.is_empty() {
        let keys = group.join(", ");
        sql.push_str(&format!(" GROUP BY {keys} ORDER BY {keys}"));
    }
    sql.push_str(" FORMAT JSONEachRow");
    SqlStatement {
        sql,
        params: binder.params,
    }
}

/// Scope, time range, and the filters selected by `keep`.
fn conditions(
    query: &Query,
    binder: &mut Binder,
    keep: impl Fn(&Predicate) -> bool,
) -> Vec<String> {
    let mut conditions = vec![
        format!(
            "{TENANT_COLUMN} = {}",
            binder.string(&query.scope.workspace_id)
        ),
        format!("{DATASET_COLUMN} = {}", binder.string(&query.scope.dataset)),
        format!(
            "{TIMESTAMP_COLUMN} >= fromUnixTimestamp64Nano({})",
            binder.bind("Int64", query.time.start_ns.to_string())
        ),
        format!(
            "{TIMESTAMP_COLUMN} < fromUnixTimestamp64Nano({})",
            binder.bind("Int64", query.time.end_ns.to_string())
        ),
    ];
    conditions.extend(
        query
            .filters
            .iter()
            .filter(|p| keep(p))
            .map(|p| predicate(query.signal, p, binder)),
    );
    conditions
}

/// Count the events a double comparison silently skips: events that satisfy
/// every other filter but hold a non-finite float (NaN, +Inf, -Inf) at a
/// compared attribute. Comparisons never match such values, and the response
/// reports how many were left out instead of failing the query. `None` when
/// the request has no double comparison.
pub fn generate_non_finite_probe(query: &Query, tables: &ServingTables) -> Option<SqlStatement> {
    if !query.filters.iter().any(|filter| {
        matches!(
            filter,
            Predicate::Compare {
                value: Literal::Double(_),
                ..
            }
        )
    }) {
        return None;
    }
    let mut binder = Binder::default();
    let map = typed_map(query.signal, ValueType::Double);
    let mut conditions = conditions(query, &mut binder, |filter| {
        !matches!(
            filter,
            Predicate::Compare {
                value: Literal::Double(_),
                ..
            }
        )
    });
    let mut seen: Vec<&AttrRef> = Vec::new();
    let mut held = Vec::new();
    for filter in &query.filters {
        if let Predicate::Compare {
            attr,
            op,
            value: Literal::Double(value),
        } = filter
        {
            let key = binder.string(&attr.key);
            let bound = binder.bind("Float64", value.to_string());
            // Preserve every double predicate, but let a present non-finite
            // value through so the final condition can count it as excluded.
            conditions.push(format!(
                "(mapContains({map}, {key}) AND (NOT isFinite({map}[{key}]) OR {map}[{key}] {} {bound}))",
                sql_op(*op)
            ));
            if !seen.contains(&attr) {
                seen.push(attr);
                held.push(format!(
                    "(mapContains({map}, {key}) AND NOT isFinite({map}[{key}]))"
                ));
            }
        }
    }
    conditions.push(format!("({})", held.join(" OR ")));
    Some(SqlStatement {
        sql: format!(
            "SELECT count() AS {} FROM {} WHERE {} FORMAT JSONEachRow",
            columns::NON_FINITE_EXCLUDED,
            tables.table(query.signal),
            conditions.join(" AND ")
        ),
        params: binder.params,
    })
}

fn typed_map(signal: Signal, ty: ValueType) -> &'static str {
    schema::typed_map(signal, ty).expect("a typed Map exists for every scalar type")
}

/// Whether the key is present in a record Map of type `ty`. The key-only Map
/// carries the type name, so bytes, array, map, and empty test its value.
fn record_has(signal: Signal, ty: ValueType, key: &str) -> String {
    match schema::typed_map(signal, ty) {
        Some(map) => format!("mapContains({map}, {key})"),
        None => {
            let other = schema::other_map(signal);
            format!(
                "(mapContains({other}, {key}) AND {other}[{key}] = '{}')",
                ty.as_str()
            )
        }
    }
}

/// Key membership across every record Map: presence is never inferred from a
/// Map's default value for a missing key.
fn record_present(signal: Signal, key: &str) -> String {
    let mut parts: Vec<String> = TYPED_SCALARS
        .iter()
        .map(|ty| format!("mapContains({}, {key})", typed_map(signal, *ty)))
        .collect();
    parts.push(format!("mapContains({}, {key})", schema::other_map(signal)));
    format!("({})", parts.join(" OR "))
}

/// Present with a type other than `ty` that is not explicit empty.
fn record_incompatible(signal: Signal, ty: ValueType, key: &str) -> String {
    let other = schema::other_map(signal);
    let mut parts: Vec<String> = TYPED_SCALARS
        .iter()
        .filter(|t| **t != ty)
        .map(|t| format!("mapContains({}, {key})", typed_map(signal, *t)))
        .collect();
    parts.push(format!(
        "(mapContains({other}, {key}) AND {other}[{key}] != 'empty')"
    ));
    format!("({})", parts.join(" OR "))
}

fn text_maps(signal: Signal, namespace: Namespace) -> (&'static str, &'static str) {
    schema::text_maps(signal, namespace).expect("resource and scope are stored as text Maps")
}

fn type_expr(signal: Signal, attr: &AttrRef, key: &str) -> String {
    if attr.namespace != Namespace::Record {
        let (_, types) = text_maps(signal, attr.namespace);
        return format!("if(mapContains({types}, {key}), {types}[{key}], '{MISSING_TYPE}')");
    }
    let other = schema::other_map(signal);
    let mut arms: Vec<String> = TYPED_SCALARS
        .iter()
        .map(|ty| {
            format!(
                "mapContains({}, {key}), '{}'",
                typed_map(signal, *ty),
                ty.as_str()
            )
        })
        .collect();
    arms.push(format!("mapContains({other}, {key}), {other}[{key}]"));
    format!("multiIf({}, '{MISSING_TYPE}')", arms.join(", "))
}

/// The group-key value as text. Only scalar values are read; bytes, arrays,
/// and maps stay key-only, so their group value is empty and the type alone
/// identifies them (the result check rejects them).
fn value_expr(signal: Signal, attr: &AttrRef, key: &str) -> String {
    if attr.namespace != Namespace::Record {
        let (values, types) = text_maps(signal, attr.namespace);
        return format!(
            "if(mapContains({types}, {key}) AND {types}[{key}] IN ('string', 'int', 'double', 'bool'), {values}[{key}], '')"
        );
    }
    let arms: Vec<String> = TYPED_SCALARS
        .iter()
        .map(|ty| {
            let map = typed_map(signal, *ty);
            let read = match ty {
                ValueType::String => format!("{map}[{key}]"),
                _ => format!("toString({map}[{key}])"),
            };
            format!("mapContains({map}, {key}), {read}")
        })
        .collect();
    format!("multiIf({}, '')", arms.join(", "))
}

fn sql_op(op: CmpOp) -> &'static str {
    match op {
        CmpOp::Eq => "=",
        CmpOp::Ne => "!=",
        CmpOp::Lt => "<",
        CmpOp::Le => "<=",
        CmpOp::Gt => ">",
        CmpOp::Ge => ">=",
    }
}

fn predicate(signal: Signal, predicate: &Predicate, binder: &mut Binder) -> String {
    match predicate {
        Predicate::Exists(attr) | Predicate::Absent(attr) => {
            let key = binder.string(&attr.key);
            let present = if attr.namespace == Namespace::Record {
                record_present(signal, &key)
            } else {
                let (values, _) = text_maps(signal, attr.namespace);
                format!("mapContains({values}, {key})")
            };
            if matches!(predicate, Predicate::Absent(_)) {
                format!("NOT {present}")
            } else {
                present
            }
        }
        Predicate::HasType(attr, ty) => {
            let key = binder.string(&attr.key);
            if attr.namespace == Namespace::Record {
                record_has(signal, *ty, &key)
            } else {
                let (_, types) = text_maps(signal, attr.namespace);
                format!(
                    "(mapContains({types}, {key}) AND {types}[{key}] = '{}')",
                    ty.as_str()
                )
            }
        }
        Predicate::Compare { attr, op, value } => {
            let key = binder.string(&attr.key);
            let ty = value.value_type();
            if attr.namespace == Namespace::Record {
                let map = typed_map(signal, ty);
                let bound = match value {
                    Literal::Int(v) => binder.bind("Int64", v.to_string()),
                    Literal::Double(v) => binder.bind("Float64", v.to_string()),
                    Literal::String(v) => binder.string(v),
                    Literal::Bool(v) => binder.bind("Bool", v.to_string()),
                    other => unreachable!("validated literal is a scalar: {other:?}"),
                };
                // A non-finite stored float must not satisfy an inequality
                // (`+inf > 5`) or an equality it was never compared to.
                let finite = if ty == ValueType::Double {
                    format!(" AND isFinite({map}[{key}])")
                } else {
                    String::new()
                };
                format!(
                    "(mapContains({map}, {key}){finite} AND {map}[{key}] {} {bound})",
                    sql_op(*op)
                )
            } else {
                let (values, types) = text_maps(signal, attr.namespace);
                let text = match value {
                    Literal::Int(v) => v.to_string(),
                    Literal::String(v) => v.clone(),
                    Literal::Bool(v) => v.to_string(),
                    other => unreachable!("validated literal is text-comparable: {other:?}"),
                };
                let bound = binder.string(&text);
                format!(
                    "(mapContains({types}, {key}) AND {types}[{key}] = '{}' AND {values}[{key}] {} {bound})",
                    ty.as_str(),
                    sql_op(*op)
                )
            }
        }
    }
}

fn aggregate_columns(query: &Query, binder: &mut Binder) -> Vec<String> {
    let signal = query.signal;
    match &query.aggregate {
        Aggregate::Count => vec![],
        Aggregate::CountValues { attr, value_type } => {
            let key = binder.string(&attr.key);
            vec![format!(
                "countIf({}) AS {}",
                record_has(signal, *value_type, &key),
                columns::CONTRIBUTING
            )]
        }
        Aggregate::Sum { attr, value_type } => {
            let key = binder.string(&attr.key);
            let map = typed_map(signal, *value_type);
            let has = record_has(signal, *value_type, &key);
            let mut out = vec![format!("countIf({has}) AS {}", columns::CONTRIBUTING)];
            if *value_type == ValueType::Int {
                // Int64 `sum` wraps; Int128 holds the true total of any
                // realistic row count so the result check can range-check it.
                out.push(format!(
                    "toString(sumIf(toInt128({map}[{key}]), {has})) AS {}",
                    columns::SUM
                ));
            } else {
                out.push(format!(
                    "toString(sumIf({map}[{key}], {has} AND isFinite({map}[{key}]))) AS {}",
                    columns::SUM
                ));
                out.push(format!(
                    "countIf({has} AND NOT isFinite({map}[{key}])) AS {}",
                    columns::NON_FINITE
                ));
            }
            // An explicit type filter already restricts the scan to the declared
            // type, so the other Maps need not be read.
            if !super::plan::has_type_filter(&query.filters, attr, *value_type) {
                out.push(format!(
                    "arraySort(groupUniqArrayIf({}, {})) AS {}",
                    type_expr(signal, attr, &key),
                    record_incompatible(signal, *value_type, &key),
                    columns::INCOMPATIBLE_TYPES
                ));
            }
            out
        }
        Aggregate::SpanDuration => {
            let observable = format!("isNotNull({SPAN_DURATION_COLUMN})");
            vec![
                format!("countIf({observable}) AS {}", columns::CONTRIBUTING),
                format!(
                    "countIf({SPAN_TIMING_STATUS_COLUMN} = '{TIMING_END_BEFORE_START}') AS {}",
                    columns::TIMING_ERRORS
                ),
                format!(
                    "toString(sumIf(toInt128(assumeNotNull({SPAN_DURATION_COLUMN})), {observable})) AS {}",
                    columns::SUM
                ),
            ]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::model::{Scope, TimeRange};

    #[test]
    fn values_are_bound_never_interpolated() {
        let hostile = "x' OR 1=1; --";
        let query = Query {
            scope: Scope {
                workspace_id: "ws".into(),
                dataset: "ds".into(),
            },
            signal: Signal::Logs,
            time: TimeRange {
                start_ns: 1,
                end_ns: 2,
            },
            filters: vec![
                Predicate::Exists(AttrRef::record(hostile)),
                Predicate::Compare {
                    attr: AttrRef::record(hostile),
                    op: CmpOp::Eq,
                    value: Literal::String(hostile.into()),
                },
            ],
            group_by: vec![],
            aggregate: Aggregate::Count,
        };
        let statement = generate(&query, &ServingTables::new("lh").unwrap());
        assert!(!statement.sql.contains(hostile));
        assert!(!statement.sql.contains("ws"), "scope is bound");
        // The key and the equal string literal share one binding.
        let bound: Vec<_> = statement
            .params
            .iter()
            .filter(|p| p.value == hostile)
            .collect();
        assert_eq!(bound.len(), 1);
        assert!(
            statement
                .sql
                .contains(&format!("{{{}:String}}", bound[0].name))
        );
    }

    #[test]
    fn wire_values_escape_the_text_format() {
        let cases = [
            ("plain", "plain"),
            ("a\\b", "a\\\\b"),
            ("tab\there", "tab\\there"),
            ("two\nlines\r", "two\\nlines\\r"),
        ];
        for (value, wire) in cases {
            let param = Param {
                name: "p0".into(),
                ch_type: "String",
                value: value.into(),
            };
            assert_eq!(param.wire_value(), wire);
        }
    }

    #[test]
    fn database_names_are_plain_identifiers() {
        for (name, ok) in [
            ("lh021", true),
            ("_x", true),
            ("", false),
            ("1db", false),
            ("a.b", false),
            ("a; DROP", false),
            ("a`b", false),
        ] {
            assert_eq!(ServingTables::new(name).is_ok(), ok, "{name:?}");
        }
    }
}
