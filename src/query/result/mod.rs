//! Enforce the typed-value contract on ClickHouse result rows.
//!
//! The statement computes exact values (`Int128` totals, per-type counts); the
//! checks that turn them into errors live here, because ClickHouse would
//! otherwise return a wrapped, coerced, or silently partial answer.

use serde_json::{Value, json};

use super::error::{Operation, QueryError, UnsupportedValue};
use super::model::{Aggregate, AttrRef};
use super::plan::{Plan, PlanDiagnostics, has_type_filter};
use super::sql::{MISSING_TYPE, columns};
use super::value::{TypedValue, ValueType};

#[derive(Debug, Clone, PartialEq)]
pub enum GroupValue {
    /// The group key was not present. Distinct from explicit empty, the empty
    /// string, false, and zero.
    Missing,
    Value(TypedValue),
}

impl GroupValue {
    pub fn to_json(&self) -> Value {
        match self {
            GroupValue::Missing => json!({ "type": MISSING_TYPE }),
            GroupValue::Value(v) => v.to_json(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Sum {
    Int(i64),
    Double(f64),
}

/// Value counts reported next to, never instead of, the event count.
#[derive(Debug, Clone, PartialEq)]
pub struct ValueStats {
    /// Events that contributed a value of the declared type (or an observable
    /// span duration). Missing, explicit empty, and unavailable values do not
    /// contribute and are not zero.
    pub contributing: u64,
    /// Spans whose end precedes their start; span durations only.
    pub timing_errors: Option<u64>,
    /// `None` when nothing contributed.
    pub sum: Option<Sum>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultRow {
    pub groups: Vec<GroupValue>,
    pub events: u64,
    pub values: Option<ValueStats>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub rows: Vec<ResultRow>,
    pub diagnostics: PlanDiagnostics,
    /// Events a double comparison skipped because they hold a non-finite float
    /// at a compared attribute. `None` when the request has no double
    /// comparison. The query succeeds; the count keeps the gap visible.
    pub non_finite_excluded: Option<u64>,
}

impl QueryResult {
    /// Lossless response: integers are decimal strings with explicit types,
    /// counts are plain numbers.
    pub fn to_json(&self) -> Value {
        let rows: Vec<Value> = self
            .rows
            .iter()
            .map(|row| {
                let mut out = json!({
                    "groups": row.groups.iter().map(GroupValue::to_json).collect::<Vec<_>>(),
                    "events": row.events,
                });
                if let Some(stats) = &row.values {
                    out["contributing"] = json!(stats.contributing);
                    if let Some(errors) = stats.timing_errors {
                        out["timing_errors"] = json!(errors);
                    }
                    out["sum"] = match stats.sum {
                        None => Value::Null,
                        Some(Sum::Int(v)) => TypedValue::Int(v).to_json(),
                        Some(Sum::Double(v)) => TypedValue::Double(v).to_json(),
                    };
                }
                out
            })
            .collect();
        let mut diagnostics = self.diagnostics.to_json();
        if let Some(excluded) = self.non_finite_excluded {
            diagnostics["non_finite_excluded"] = json!(excluded);
        }
        json!({ "rows": rows, "diagnostics": diagnostics })
    }
}

/// Apply the contract checks to the `JSONEachRow` rows of `plan.statement`;
/// `probe_rows` are the rows of `plan.non_finite_probe` (empty when the plan
/// has none). The first violation fails the whole query.
pub fn interpret(
    plan: &Plan,
    rows: &[Value],
    probe_rows: &[Value],
) -> Result<QueryResult, QueryError> {
    let rows = rows
        .iter()
        .map(|row| interpret_row(plan, row))
        .collect::<Result<Vec<_>, _>>()?;
    let non_finite_excluded = match (&plan.non_finite_probe, probe_rows) {
        (None, _) => None,
        (Some(_), [row]) => Some(count(row, columns::NON_FINITE_EXCLUDED)?),
        (Some(_), _) => return Err(malformed("the non-finite probe returned no single row")),
    };
    Ok(QueryResult {
        rows,
        diagnostics: plan.diagnostics.clone(),
        non_finite_excluded,
    })
}

fn malformed(detail: impl Into<String>) -> QueryError {
    QueryError::MalformedResult {
        detail: detail.into(),
    }
}

fn field<'a>(row: &'a Value, name: &str) -> Result<&'a Value, QueryError> {
    row.get(name)
        .ok_or_else(|| malformed(format!("missing column {name}")))
}

fn text<'a>(row: &'a Value, name: &str) -> Result<&'a str, QueryError> {
    field(row, name)?
        .as_str()
        .ok_or_else(|| malformed(format!("column {name} is not text")))
}

/// ClickHouse quotes 64-bit integers in JSON by default; accept both forms.
fn count(row: &Value, name: &str) -> Result<u64, QueryError> {
    match field(row, name)? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
    .ok_or_else(|| malformed(format!("column {name} is not a count")))
}

fn interpret_row(plan: &Plan, row: &Value) -> Result<ResultRow, QueryError> {
    let groups = (0..plan.query.group_by.len())
        .map(|i| {
            group_value(
                text(row, &columns::group_type(i))?,
                text(row, &columns::group_value(i))?,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ResultRow {
        groups,
        events: count(row, columns::EVENTS)?,
        values: value_stats(plan, row)?,
    })
}

fn group_value(type_name: &str, value: &str) -> Result<GroupValue, QueryError> {
    if type_name == MISSING_TYPE {
        return Ok(GroupValue::Missing);
    }
    let ty = ValueType::parse(type_name)
        .ok_or_else(|| malformed(format!("unknown group type {type_name:?}")))?;
    if let Some(value) = UnsupportedValue::from_type(ty) {
        return Err(QueryError::UnsupportedOperation {
            operation: Operation::Group,
            value,
        });
    }
    let bad = |what: &str| malformed(format!("{value:?} is not a valid {what}"));
    Ok(GroupValue::Value(match ty {
        ValueType::String => TypedValue::String(value.to_owned()),
        ValueType::Bool => TypedValue::Bool(match value {
            "true" => true,
            "false" => false,
            _ => return Err(bad("bool")),
        }),
        ValueType::Int => TypedValue::Int(value.parse().map_err(|_| bad("int"))?),
        ValueType::Double => {
            let v: f64 = value.parse().map_err(|_| bad("double"))?;
            if !v.is_finite() {
                return Err(QueryError::UnsupportedOperation {
                    operation: Operation::Group,
                    value: UnsupportedValue::NonFiniteFloat,
                });
            }
            TypedValue::Double(v)
        }
        ValueType::Empty => TypedValue::Empty,
        ValueType::Bytes | ValueType::Array | ValueType::Map => unreachable!("rejected above"),
    }))
}

fn value_stats(plan: &Plan, row: &Value) -> Result<Option<ValueStats>, QueryError> {
    match &plan.query.aggregate {
        Aggregate::Count => Ok(None),
        Aggregate::CountValues { .. } => Ok(Some(ValueStats {
            contributing: count(row, columns::CONTRIBUTING)?,
            timing_errors: None,
            sum: None,
        })),
        Aggregate::SpanDuration => {
            let contributing = count(row, columns::CONTRIBUTING)?;
            Ok(Some(ValueStats {
                contributing,
                timing_errors: Some(count(row, columns::TIMING_ERRORS)?),
                sum: int_sum(row, contributing)?,
            }))
        }
        Aggregate::Sum { attr, value_type } => {
            check_incompatible(plan, row, attr, *value_type)?;
            let contributing = count(row, columns::CONTRIBUTING)?;
            let sum = if *value_type == ValueType::Int {
                int_sum(row, contributing)?
            } else {
                float_sum(row, contributing)?
            };
            Ok(Some(ValueStats {
                contributing,
                timing_errors: None,
                sum,
            }))
        }
    }
}

/// Fail on non-empty values of another type. Bytes, arrays, and maps are
/// unsupported operands; any other type is a plain type error.
fn check_incompatible(
    plan: &Plan,
    row: &Value,
    attr: &AttrRef,
    expected: ValueType,
) -> Result<(), QueryError> {
    // The statement omits the check only when the validated request itself
    // restricts the aggregate input to its declared type.
    if has_type_filter(&plan.query.filters, attr, expected) {
        return Ok(());
    }
    let found: Vec<ValueType> = field(row, columns::INCOMPATIBLE_TYPES)?
        .as_array()
        .ok_or_else(|| malformed("incompatible_types is not an array"))?
        .iter()
        .map(|v| {
            v.as_str()
                .and_then(ValueType::parse)
                .ok_or_else(|| malformed(format!("unknown incompatible type {v}")))
        })
        .collect::<Result<_, _>>()?;
    if let Some(value) = found.iter().find_map(|t| UnsupportedValue::from_type(*t)) {
        return Err(QueryError::UnsupportedOperation {
            operation: Operation::Aggregate,
            value,
        });
    }
    if !found.is_empty() {
        return Err(QueryError::IncompatibleType {
            attr: attr.key.clone(),
            expected,
            found,
        });
    }
    Ok(())
}

/// The `Int128` total, range-checked into `Int64`.
fn int_sum(row: &Value, contributing: u64) -> Result<Option<Sum>, QueryError> {
    let total: i128 = text(row, columns::SUM)?
        .parse()
        .map_err(|_| malformed("sum is not an integer"))?;
    if contributing == 0 {
        return Ok(None);
    }
    let total = i64::try_from(total).map_err(|_| QueryError::IntegerOverflow { total })?;
    Ok(Some(Sum::Int(total)))
}

fn float_sum(row: &Value, contributing: u64) -> Result<Option<Sum>, QueryError> {
    let non_finite = || QueryError::UnsupportedOperation {
        operation: Operation::Aggregate,
        value: UnsupportedValue::NonFiniteFloat,
    };
    if count(row, columns::NON_FINITE)? > 0 {
        return Err(non_finite());
    }
    let total: f64 = text(row, columns::SUM)?
        .parse()
        .map_err(|_| malformed("sum is not a float"))?;
    if contributing == 0 {
        return Ok(None);
    }
    // A finite series can still overflow to infinity.
    if !total.is_finite() {
        return Err(non_finite());
    }
    Ok(Some(Sum::Double(total)))
}

#[cfg(test)]
mod tests;
