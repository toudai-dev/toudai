//! Contract validation and plan diagnostics.

use std::collections::BTreeSet;

use serde_json::{Value, json};

use super::error::{Operation, QueryError, UnsupportedValue};
use super::model::{Aggregate, AttrRef, CmpOp, Namespace, Predicate, Query, Signal};
use super::schema::{
    self, DATASET_COLUMN, SPAN_DURATION_COLUMN, SPAN_TIMING_STATUS_COLUMN, ServingTables,
    TS_COLUMN, TYPED_SCALARS, WORKSPACE_COLUMN,
};
use super::sql::{self, SqlStatement};
use super::value::{Literal, ValueType};

/// The storage path a plan reads. The PoC has only the raw serving tables;
/// projection, metadata, and aggregate paths arrive with the planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoragePath {
    Raw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApproximationMode {
    Exact,
}

/// A contract rule the plan enforces, so a client can see why a query returned
/// what it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Guard {
    /// Comparisons match only values received with the literal's type.
    TypeMatchedPredicates,
    /// Non-finite floats never match a numeric comparison; the response
    /// reports how many events that left out.
    FiniteFloatComparison,
    /// Presence is Map key membership, never a default value.
    KeyMembershipPresence,
    /// Group keys are `(type, value)`.
    GroupKeyIncludesType,
    /// Integer sums accumulate in `Int128` and are range-checked into `Int64`.
    Int128Sum,
    /// Contributing-value counts are reported apart from event counts.
    ContributingCounts,
    /// Incompatible non-empty values fail the aggregate.
    IncompatibleTypeCheck,
    /// Non-finite floats fail float aggregation and grouping.
    NonFiniteFloatCheck,
}

impl Guard {
    pub fn as_str(self) -> &'static str {
        match self {
            Guard::TypeMatchedPredicates => "type_matched_predicates",
            Guard::FiniteFloatComparison => "finite_float_comparison",
            Guard::KeyMembershipPresence => "key_membership_presence",
            Guard::GroupKeyIncludesType => "group_key_includes_type",
            Guard::Int128Sum => "int128_sum",
            Guard::ContributingCounts => "contributing_counts",
            Guard::IncompatibleTypeCheck => "incompatible_type_check",
            Guard::NonFiniteFloatCheck => "non_finite_float_check",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanDiagnostics {
    pub storage_path: StoragePath,
    pub approximation: ApproximationMode,
    pub table: &'static str,
    /// Every serving column the statement reads.
    pub columns: Vec<&'static str>,
    pub guards: Vec<Guard>,
}

impl PlanDiagnostics {
    pub fn to_json(&self) -> Value {
        json!({
            "storage_path": "raw",
            "approximation": "exact",
            "table": self.table,
            "columns": self.columns,
            "guards": self.guards.iter().map(|g| g.as_str()).collect::<Vec<_>>(),
        })
    }
}

/// Whether the filters restrict `attr` to `ty`: an explicit type predicate, or
/// a comparison against a literal of that type (which only matches values
/// received with it). Only such a filter excludes incompatible values from an
/// aggregate instead of leaving them to fail it.
pub(crate) fn has_type_filter(filters: &[Predicate], attr: &AttrRef, ty: ValueType) -> bool {
    filters.iter().any(|p| match p {
        Predicate::HasType(a, t) => a == attr && *t == ty,
        Predicate::Compare { attr: a, value, .. } => a == attr && value.value_type() == ty,
        _ => false,
    })
}

/// Check a request against the PoC typed-value contract.
pub fn validate(query: &Query) -> Result<(), QueryError> {
    if query.scope.workspace_id.is_empty() {
        return Err(QueryError::MissingScope {
            field: "workspace_id",
        });
    }
    if query.scope.dataset.is_empty() {
        return Err(QueryError::MissingScope { field: "dataset" });
    }
    if query.time.start_ns >= query.time.end_ns {
        return Err(QueryError::InvalidTimeRange {
            start_ns: query.time.start_ns,
            end_ns: query.time.end_ns,
        });
    }
    for predicate in &query.filters {
        match predicate {
            Predicate::Exists(attr) | Predicate::Absent(attr) | Predicate::HasType(attr, _) => {
                validate_key(attr)?
            }
            Predicate::Compare { attr, op, value } => {
                validate_key(attr)?;
                validate_compare(attr, *op, value)?;
            }
        }
    }
    for attr in &query.group_by {
        validate_key(attr)?;
    }
    validate_aggregate(query)
}

fn validate_key(attr: &AttrRef) -> Result<(), QueryError> {
    if attr.key.is_empty() {
        return Err(QueryError::EmptyAttributeKey);
    }
    Ok(())
}

fn validate_compare(attr: &AttrRef, op: CmpOp, value: &Literal) -> Result<(), QueryError> {
    let operation = if op.is_ordering() {
        Operation::Order
    } else {
        Operation::Compare
    };
    value.ensure_comparable(operation)?;
    let ty = value.value_type();
    if op.is_ordering() && ty == ValueType::Bool {
        return Err(QueryError::OrderingNotDefined { value_type: ty });
    }
    // Resource and scope values are text; exact equality is the only operation
    // that cannot change the answer (a float's text would not compare as a float).
    if attr.namespace != Namespace::Record
        && (op.is_ordering() || !matches!(ty, ValueType::String | ValueType::Bool | ValueType::Int))
    {
        return Err(QueryError::UnsupportedForNamespace {
            namespace: attr.namespace,
            operation,
            value_type: ty,
        });
    }
    Ok(())
}

fn validate_aggregate(query: &Query) -> Result<(), QueryError> {
    match &query.aggregate {
        Aggregate::Count => Ok(()),
        Aggregate::SpanDuration => {
            if query.signal != Signal::Spans {
                return Err(QueryError::AggregateRequiresSpans);
            }
            Ok(())
        }
        Aggregate::CountValues { attr, value_type } => {
            validate_key(attr)?;
            validate_record_aggregate(attr, *value_type)?;
            if !value_type.is_scalar() {
                return Err(QueryError::NotScalar {
                    value_type: *value_type,
                });
            }
            Ok(())
        }
        Aggregate::Sum { attr, value_type } => {
            validate_key(attr)?;
            validate_record_aggregate(attr, *value_type)?;
            if !matches!(value_type, ValueType::Int | ValueType::Double) {
                return Err(QueryError::NotNumeric {
                    value_type: *value_type,
                });
            }
            Ok(())
        }
    }
}

fn validate_record_aggregate(attr: &AttrRef, ty: ValueType) -> Result<(), QueryError> {
    if let Some(value) = UnsupportedValue::from_type(ty) {
        return Err(QueryError::UnsupportedOperation {
            operation: Operation::Aggregate,
            value,
        });
    }
    if attr.namespace != Namespace::Record {
        return Err(QueryError::UnsupportedForNamespace {
            namespace: attr.namespace,
            operation: Operation::Aggregate,
            value_type: ty,
        });
    }
    Ok(())
}

/// Serving columns a query reads, derived from the same schema helpers the SQL
/// generator uses.
pub(crate) fn columns_read(query: &Query) -> Vec<&'static str> {
    let signal = query.signal;
    let mut columns: BTreeSet<&'static str> = BTreeSet::new();
    columns.extend([WORKSPACE_COLUMN, DATASET_COLUMN, TS_COLUMN]);

    // Every typed Map plus the key-only Map: what presence and grouping need
    // when the type of a key is not known in advance.
    let all_record_maps = |columns: &mut BTreeSet<&'static str>| {
        columns.extend(
            TYPED_SCALARS
                .iter()
                .filter_map(|t| schema::typed_map(signal, *t)),
        );
        columns.insert(schema::other_map(signal));
    };
    let text_maps = |columns: &mut BTreeSet<&'static str>, attr: &AttrRef, values: bool| {
        if let Some((value_map, type_map)) = schema::text_maps(signal, attr.namespace) {
            columns.insert(type_map);
            if values {
                columns.insert(value_map);
            }
        }
    };

    for predicate in &query.filters {
        match predicate {
            Predicate::Exists(attr) | Predicate::Absent(attr) => {
                if attr.namespace == Namespace::Record {
                    all_record_maps(&mut columns);
                } else if let Some((value_map, _)) = schema::text_maps(signal, attr.namespace) {
                    columns.insert(value_map);
                }
            }
            Predicate::HasType(attr, ty) => {
                if attr.namespace == Namespace::Record {
                    columns.insert(
                        schema::typed_map(signal, *ty).unwrap_or(schema::other_map(signal)),
                    );
                } else {
                    text_maps(&mut columns, attr, false);
                }
            }
            Predicate::Compare { attr, value, .. } => {
                if attr.namespace == Namespace::Record {
                    columns.extend(schema::typed_map(signal, value.value_type()));
                } else {
                    text_maps(&mut columns, attr, true);
                }
            }
        }
    }
    for attr in &query.group_by {
        if attr.namespace == Namespace::Record {
            all_record_maps(&mut columns);
        } else {
            text_maps(&mut columns, attr, true);
        }
    }
    match &query.aggregate {
        Aggregate::Count => {}
        Aggregate::CountValues { value_type, .. } => {
            columns.extend(schema::typed_map(signal, *value_type));
        }
        Aggregate::Sum { attr, value_type } => {
            columns.extend(schema::typed_map(signal, *value_type));
            if !has_type_filter(&query.filters, attr, *value_type) {
                all_record_maps(&mut columns);
            }
        }
        Aggregate::SpanDuration => {
            columns.extend([SPAN_DURATION_COLUMN, SPAN_TIMING_STATUS_COLUMN]);
        }
    }
    columns.into_iter().collect()
}

pub(crate) fn guards(query: &Query) -> Vec<Guard> {
    let mut guards: BTreeSet<Guard> = BTreeSet::new();
    let mut references_attr = !query.group_by.is_empty();
    for predicate in &query.filters {
        references_attr = true;
        if let Predicate::Compare { value, .. } = predicate {
            guards.insert(Guard::TypeMatchedPredicates);
            if value.value_type() == ValueType::Double {
                guards.insert(Guard::FiniteFloatComparison);
            }
        }
    }
    if references_attr {
        guards.insert(Guard::KeyMembershipPresence);
    }
    if !query.group_by.is_empty() {
        guards.insert(Guard::GroupKeyIncludesType);
        guards.insert(Guard::NonFiniteFloatCheck);
    }
    match &query.aggregate {
        Aggregate::Count => {}
        Aggregate::CountValues { .. } => {
            guards.insert(Guard::ContributingCounts);
        }
        Aggregate::Sum { attr, value_type } => {
            guards.insert(Guard::ContributingCounts);
            guards.insert(Guard::KeyMembershipPresence);
            if *value_type == ValueType::Int {
                guards.insert(Guard::Int128Sum);
            } else {
                guards.insert(Guard::NonFiniteFloatCheck);
            }
            if !has_type_filter(&query.filters, attr, *value_type) {
                guards.insert(Guard::IncompatibleTypeCheck);
            }
        }
        Aggregate::SpanDuration => {
            guards.insert(Guard::ContributingCounts);
            guards.insert(Guard::Int128Sum);
        }
    }
    guards.into_iter().collect()
}

pub fn diagnostics(query: &Query) -> PlanDiagnostics {
    PlanDiagnostics {
        storage_path: StoragePath::Raw,
        approximation: ApproximationMode::Exact,
        table: schema::table_name(query.signal),
        columns: columns_read(query),
        guards: guards(query),
    }
}

/// A validated request with its statement and diagnostics.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub query: Query,
    pub statement: SqlStatement,
    /// Counts the events double comparisons skipped because they hold a
    /// non-finite float; present only when the request has such a comparison.
    pub non_finite_probe: Option<SqlStatement>,
    pub diagnostics: PlanDiagnostics,
}

/// Validate a request and build its ClickHouse statement.
pub fn plan(query: Query, tables: &ServingTables) -> Result<Plan, QueryError> {
    validate(&query)?;
    Ok(Plan {
        statement: sql::generate(&query, tables),
        non_finite_probe: sql::generate_non_finite_probe(&query, tables),
        diagnostics: diagnostics(&query),
        query,
    })
}

#[cfg(test)]
mod tests;
