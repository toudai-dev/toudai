//! Enforce the typed-value contract on ClickHouse result rows.
//!
//! The statement computes exact values (`Int128` totals, per-type counts); the
//! checks that turn them into errors live here, because ClickHouse would
//! otherwise return a wrapped, coerced, or silently partial answer.

use serde_json::{Value, json};

use super::error::{Operation, QueryError, UnsupportedValue};
use super::model::{Aggregate, AttrRef, Percentile};
use super::plan::{Plan, PlanDiagnostics, has_type_filter};
use super::schema::{EVENT_ID_COLUMN, ListingType, listing_columns};
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

/// A listed field value. Unlike a group key, a listing reports every received
/// value: the query surface cannot operate on bytes, arrays, maps, or
/// non-finite floats, but it can say they are there.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Missing,
    Value(TypedValue),
    /// Bytes, an array, or a map: the type only; the value is in the
    /// canonical payload.
    CanonicalOnly(ValueType),
    NonFinite(f64),
}

impl FieldValue {
    /// Non-finite floats use the request literals' spellings.
    pub fn to_json(&self) -> Value {
        match self {
            FieldValue::Missing => json!({ "type": MISSING_TYPE }),
            FieldValue::Value(v) => v.to_json(),
            FieldValue::CanonicalOnly(ty) => json!({ "type": ty.as_str() }),
            FieldValue::NonFinite(v) => {
                let text = if v.is_nan() {
                    "NaN"
                } else if *v > 0.0 {
                    "Infinity"
                } else {
                    "-Infinity"
                };
                json!({ "type": "double", "value": text })
            }
        }
    }
}

/// A serving envelope column of a listed event.
#[derive(Debug, Clone, PartialEq)]
pub enum EnvelopeValue {
    Text(String),
    /// `None` for an unavailable value, such as a span's unobservable duration.
    Int(Option<i64>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventRow {
    pub event_id: String,
    pub timestamp_ns: i64,
    /// `(name, value)` in [`listing_columns`] order; `DateTime64` columns are
    /// named `<column>_ns`.
    pub envelope: Vec<(String, EnvelopeValue)>,
    /// One per requested field, in request order.
    pub fields: Vec<FieldValue>,
}

/// One page of an event listing.
#[derive(Debug, Clone, PartialEq)]
pub struct EventPage {
    /// Events that matched, counted before the page limit.
    pub matched: u64,
    pub rows: Vec<EventRow>,
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
    /// Percentile aggregates only: each requested percentile with its value,
    /// `None` when nothing contributed. A percentile result has no sum.
    pub percentiles: Option<Vec<(Percentile, Option<TypedValue>)>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultRow {
    /// Start of the row's time-series bucket; `None` unless the request set a
    /// bucket width.
    pub bucket_start_ns: Option<i64>,
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
    /// The page of an event listing; `rows` is empty then.
    pub events: Option<EventPage>,
}

impl QueryResult {
    /// Lossless response: integers and unsigned counts are decimal strings
    /// with explicit types.
    pub fn to_json(&self) -> Value {
        let rows: Vec<Value> = self
            .rows
            .iter()
            .map(|row| {
                let mut out = json!({
                    "groups": row.groups.iter().map(GroupValue::to_json).collect::<Vec<_>>(),
                    "events": count_json(row.events),
                });
                if let Some(start) = row.bucket_start_ns {
                    out["bucket_start_ns"] = TypedValue::Int(start).to_json();
                }
                if let Some(stats) = &row.values {
                    out["contributing"] = count_json(stats.contributing);
                    if let Some(errors) = stats.timing_errors {
                        out["timing_errors"] = count_json(errors);
                    }
                    if let Some(percentiles) = &stats.percentiles {
                        out["percentiles"] = percentiles
                            .iter()
                            .map(|(p, v)| {
                                json!({
                                    "percentile": p.to_string(),
                                    "value": v.as_ref().map_or(Value::Null, TypedValue::to_json),
                                })
                            })
                            .collect();
                    } else {
                        out["sum"] = match stats.sum {
                            None => Value::Null,
                            Some(Sum::Int(v)) => TypedValue::Int(v).to_json(),
                            Some(Sum::Double(v)) => TypedValue::Double(v).to_json(),
                        };
                    }
                }
                out
            })
            .collect();
        let mut diagnostics = self.diagnostics.to_json();
        if let Some(excluded) = self.non_finite_excluded {
            diagnostics["non_finite_excluded"] = count_json(excluded);
        }
        match &self.events {
            None => json!({ "rows": rows, "diagnostics": diagnostics }),
            Some(page) => json!({
                "rows": page.rows.iter().map(event_json).collect::<Vec<_>>(),
                "matched": count_json(page.matched),
                "diagnostics": diagnostics,
            }),
        }
    }
}

fn event_json(row: &EventRow) -> Value {
    let mut out = json!({
        "event_id": row.event_id,
        "timestamp_ns": TypedValue::Int(row.timestamp_ns).to_json(),
    });
    for (name, value) in &row.envelope {
        out[name.as_str()] = match value {
            EnvelopeValue::Text(text) => json!(text),
            EnvelopeValue::Int(Some(v)) => TypedValue::Int(*v).to_json(),
            EnvelopeValue::Int(None) => Value::Null,
        };
    }
    out["fields"] = row.fields.iter().map(FieldValue::to_json).collect();
    out
}

fn count_json(value: u64) -> Value {
    json!({ "type": "uint64", "value": value.to_string() })
}

/// Apply the contract checks to the `JSONEachRow` rows of `plan.statement`;
/// `probe_rows` are the rows of `plan.non_finite_probe` (empty when the plan
/// has none). The first violation fails the whole query.
pub fn interpret(
    plan: &Plan,
    rows: &[Value],
    probe_rows: &[Value],
) -> Result<QueryResult, QueryError> {
    let events = match &plan.query.aggregate {
        Aggregate::Events { fields, .. } => Some(event_page(plan, fields.len(), rows)?),
        _ => None,
    };
    let rows = match events {
        Some(_) => Vec::new(),
        None => rows
            .iter()
            .map(|row| interpret_row(plan, row))
            .collect::<Result<Vec<_>, _>>()?,
    };
    let non_finite_excluded = match (&plan.non_finite_probe, probe_rows) {
        (None, _) => None,
        (Some(_), [row]) => Some(count(row, columns::NON_FINITE_EXCLUDED)?),
        (Some(_), _) => return Err(malformed("the non-finite probe returned no single row")),
    };
    Ok(QueryResult {
        rows,
        diagnostics: plan.diagnostics.clone(),
        non_finite_excluded,
        events,
    })
}

fn event_page(plan: &Plan, fields: usize, rows: &[Value]) -> Result<EventPage, QueryError> {
    let matched = match rows.first() {
        Some(row) => count(row, columns::MATCHED)?,
        None => 0,
    };
    let rows = rows
        .iter()
        .map(|row| {
            let mut envelope = Vec::new();
            for (name, ty) in listing_columns(plan.query.signal) {
                let (name, value) = match ty {
                    ListingType::Text => (
                        name.to_string(),
                        EnvelopeValue::Text(text(row, &columns::listed(name))?.to_owned()),
                    ),
                    ListingType::TextProjection => (
                        format!("{name}_text"),
                        EnvelopeValue::Text(text(row, &columns::listed(name))?.to_owned()),
                    ),
                    ListingType::Int => {
                        let column = columns::listed(name);
                        (
                            name.to_string(),
                            EnvelopeValue::Int(nullable_int(row, &column)?),
                        )
                    }
                    ListingType::Nanos => {
                        let name = format!("{name}_ns");
                        let column = columns::listed(&name);
                        (name, EnvelopeValue::Int(Some(int_text(row, &column)?)))
                    }
                };
                envelope.push((name, value));
            }
            Ok(EventRow {
                event_id: text(row, &columns::listed(EVENT_ID_COLUMN))?.to_owned(),
                timestamp_ns: int_text(row, &columns::listed("timestamp_ns"))?,
                envelope,
                fields: (0..fields)
                    .map(|i| {
                        field_value(
                            text(row, &columns::field_type(i))?,
                            text(row, &columns::field_value(i))?,
                        )
                    })
                    .collect::<Result<_, _>>()?,
            })
        })
        .collect::<Result<Vec<_>, QueryError>>()?;
    if rows.len() as u64 > matched {
        return Err(malformed(
            "an event listing returned more rows than matched",
        ));
    }
    Ok(EventPage { matched, rows })
}

fn int_text(row: &Value, name: &str) -> Result<i64, QueryError> {
    text(row, name)?
        .parse()
        .map_err(|_| malformed(format!("column {name} is not an integer")))
}

fn nullable_int(row: &Value, name: &str) -> Result<Option<i64>, QueryError> {
    match field(row, name)? {
        Value::Null => Ok(None),
        _ => int_text(row, name).map(Some),
    }
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
        bucket_start_ns: bucket_start(plan, row)?,
        groups,
        events: count(row, columns::EVENTS)?,
        values: value_stats(plan, row)?,
    })
}

/// The bucket's start from its index: `start + index * width`, which must lie
/// inside the requested range.
fn bucket_start(plan: &Plan, row: &Value) -> Result<Option<i64>, QueryError> {
    let Some(width) = plan.query.bucket_ns else {
        return Ok(None);
    };
    let index = count(row, columns::BUCKET)?;
    let time = plan.query.time;
    let start = i128::from(time.start_ns) + i128::from(index) * i128::from(width);
    if start >= i128::from(time.end_ns) {
        return Err(malformed(format!(
            "bucket {index} is outside the time range"
        )));
    }
    Ok(Some(start as i64))
}

/// A group key: a received scalar or missing. Values the query surface cannot
/// operate on fail the grouping.
fn group_value(type_name: &str, value: &str) -> Result<GroupValue, QueryError> {
    let unsupported = |value| QueryError::UnsupportedOperation {
        operation: Operation::Group,
        value,
    };
    match field_value(type_name, value)? {
        FieldValue::Missing => Ok(GroupValue::Missing),
        FieldValue::Value(v) => Ok(GroupValue::Value(v)),
        FieldValue::CanonicalOnly(ty) => Err(unsupported(
            UnsupportedValue::from_type(ty).expect("a canonical-only type"),
        )),
        FieldValue::NonFinite(_) => Err(unsupported(UnsupportedValue::NonFiniteFloat)),
    }
}

/// The `(type, text)` pair a group or field expression returns.
fn field_value(type_name: &str, value: &str) -> Result<FieldValue, QueryError> {
    if type_name == MISSING_TYPE {
        return Ok(FieldValue::Missing);
    }
    let ty = ValueType::parse(type_name)
        .ok_or_else(|| malformed(format!("unknown value type {type_name:?}")))?;
    if ty.is_canonical_only() {
        return Ok(FieldValue::CanonicalOnly(ty));
    }
    let bad = |what: &str| malformed(format!("{value:?} is not a valid {what}"));
    Ok(FieldValue::Value(match ty {
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
                return Ok(FieldValue::NonFinite(v));
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
        Aggregate::Events { .. } => unreachable!("a listing has no aggregate rows"),
        Aggregate::CountValues { .. } => Ok(Some(ValueStats {
            contributing: count(row, columns::CONTRIBUTING)?,
            timing_errors: None,
            sum: None,
            percentiles: None,
        })),
        Aggregate::SpanDuration => {
            let contributing = count(row, columns::CONTRIBUTING)?;
            Ok(Some(ValueStats {
                contributing,
                timing_errors: Some(count(row, columns::TIMING_ERRORS)?),
                sum: int_sum(row, contributing)?,
                percentiles: None,
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
                percentiles: None,
            }))
        }
        Aggregate::Percentiles {
            attr,
            value_type,
            percentiles,
        } => {
            check_incompatible(plan, row, attr, *value_type)?;
            if *value_type == ValueType::Double && count(row, columns::NON_FINITE)? > 0 {
                return Err(QueryError::UnsupportedOperation {
                    operation: Operation::Aggregate,
                    value: UnsupportedValue::NonFiniteFloat,
                });
            }
            let contributing = count(row, columns::CONTRIBUTING)?;
            Ok(Some(ValueStats {
                contributing,
                timing_errors: None,
                sum: None,
                percentiles: Some(percentile_values(
                    row,
                    percentiles,
                    *value_type,
                    contributing,
                )?),
            }))
        }
        Aggregate::SpanDurationPercentiles { percentiles } => {
            let contributing = count(row, columns::CONTRIBUTING)?;
            Ok(Some(ValueStats {
                contributing,
                timing_errors: Some(count(row, columns::TIMING_ERRORS)?),
                sum: None,
                percentiles: Some(percentile_values(
                    row,
                    percentiles,
                    ValueType::Int,
                    contributing,
                )?),
            }))
        }
    }
}

/// Pair each requested percentile with its value read as `ty`.
fn percentile_values(
    row: &Value,
    percentiles: &[Percentile],
    ty: ValueType,
    contributing: u64,
) -> Result<Vec<(Percentile, Option<TypedValue>)>, QueryError> {
    let values = field(row, columns::PERCENTILES)?
        .as_array()
        .filter(|values| values.len() == percentiles.len())
        .ok_or_else(|| malformed("percentile_values does not match the request"))?;
    percentiles
        .iter()
        .zip(values)
        .map(|(p, value)| {
            if contributing == 0 {
                return Ok((*p, None));
            }
            let text = value
                .as_str()
                .ok_or_else(|| malformed("a percentile value is not text"))?;
            let bad = || {
                malformed(format!(
                    "percentile value {text:?} is not a valid {}",
                    ty.as_str()
                ))
            };
            let value = match ty {
                ValueType::Int => TypedValue::Int(text.parse().map_err(|_| bad())?),
                _ => {
                    let v: f64 = text.parse().map_err(|_| bad())?;
                    if !v.is_finite() {
                        return Err(bad());
                    }
                    TypedValue::Double(v)
                }
            };
            Ok((*p, Some(value)))
        })
        .collect()
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
