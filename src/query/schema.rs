//! The serving layout the query service is written against.
//!
//! Names follow the product DDL in `clickhouse/schema/003-serving-logs.sql` and
//! `004-serving-spans.sql` (Decision 0001 D1/D2/D4), so every column name lives
//! here and nowhere else.

use super::error::QueryError;
use super::model::{Namespace, Signal};
use super::value::ValueType;

pub const DATASET_COLUMN: &str = "dataset";
pub const TIMESTAMP_COLUMN: &str = "timestamp";
pub const SPAN_DURATION_COLUMN: &str = "duration_ns";
pub const SPAN_TIMING_STATUS_COLUMN: &str = "span_timing_status";
/// `span_timing_status` of a span whose end precedes its start.
pub const TIMING_END_BEFORE_START: &str = "end_before_start";

/// The ClickHouse database holding the serving tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServingTables {
    database: String,
}

impl ServingTables {
    /// The name is interpolated into SQL, so it is restricted to a plain identifier.
    pub fn new(database: &str) -> Result<ServingTables, QueryError> {
        let mut chars = database.chars();
        let valid = chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !valid {
            return Err(QueryError::InvalidIdentifier {
                name: database.to_owned(),
            });
        }
        Ok(ServingTables {
            database: database.to_owned(),
        })
    }

    pub fn table(&self, signal: Signal) -> String {
        format!("{}.{}", self.database, table_name(signal))
    }

    /// The canonical payload table (Decision 0001 D3), looked up by
    /// `(dataset, event_id)` (Decision 0002 D2).
    pub fn canonical_events(&self) -> String {
        format!("{}.{CANONICAL_EVENTS_TABLE}", self.database)
    }
}

pub const CANONICAL_EVENTS_TABLE: &str = "canonical_events";

pub fn table_name(signal: Signal) -> &'static str {
    match signal {
        Signal::Logs => "serving_logs",
        Signal::Spans => "serving_spans",
    }
}

/// Native typed value Map of the record namespace (`record_int`, `span_int`, ...).
pub fn typed_map(signal: Signal, ty: ValueType) -> Option<&'static str> {
    Some(match (signal, ty) {
        (Signal::Logs, ValueType::Int) => "record_int",
        (Signal::Logs, ValueType::Double) => "record_double",
        (Signal::Logs, ValueType::String) => "record_string",
        (Signal::Logs, ValueType::Bool) => "record_bool",
        (Signal::Spans, ValueType::Int) => "span_int",
        (Signal::Spans, ValueType::Double) => "span_double",
        (Signal::Spans, ValueType::String) => "span_string",
        (Signal::Spans, ValueType::Bool) => "span_bool",
        _ => return None,
    })
}

/// Key-only Map holding the type name of bytes, array, map, and explicit empty
/// record attributes.
pub fn other_map(signal: Signal) -> &'static str {
    match signal {
        Signal::Logs => "record_other",
        Signal::Spans => "span_other",
    }
}

/// `(values, types)` Maps of a resource or scope namespace. Values are stored
/// as text with a companion type Map. Spans keep their instrumentation scope in
/// `span_scope`.
pub fn text_maps(signal: Signal, namespace: Namespace) -> Option<(&'static str, &'static str)> {
    Some(match (signal, namespace) {
        (_, Namespace::Resource) => ("resource", "resource_types"),
        (Signal::Logs, Namespace::Scope) => ("scope", "scope_types"),
        (Signal::Spans, Namespace::Scope) => ("span_scope", "span_scope_types"),
        (_, Namespace::Record) => return None,
    })
}

/// The scalar types a record attribute can be stored as in a typed Map.
pub const TYPED_SCALARS: [ValueType; 4] = [
    ValueType::Int,
    ValueType::Double,
    ValueType::String,
    ValueType::Bool,
];

/// How an event listing reads and reports a serving column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListingType {
    Text,
    /// A lossy text projection, reported under `<column>_text` so it is not
    /// read as the typed value: the serving `body` is the body as text, so an
    /// explicit-empty body and an empty string both read `""`. The canonical
    /// payload (`GET /v1/events`) holds the typed value.
    TextProjection,
    /// An integer column, possibly Nullable; reported as a typed int or null.
    Int,
    /// A `DateTime64(9)` column, reported as Unix nanoseconds under
    /// `<column>_ns`.
    Nanos,
}

/// The envelope columns an event listing reports for each event, besides
/// `event_id` and `timestamp`: what an operator needs to recognise an event
/// and follow it to its trace, not its attributes (those are `fields`).
pub fn listing_columns(signal: Signal) -> &'static [(&'static str, ListingType)] {
    use ListingType::*;
    match signal {
        Signal::Logs => &[
            ("kind", Text),
            ("service_name", Text),
            ("severity_text", Text),
            ("severity_number", Int),
            ("event_name", Text),
            ("body", TextProjection),
            ("trace_id", Text),
            ("span_id", Text),
        ],
        Signal::Spans => &[
            ("kind", Text),
            ("service_name", Text),
            ("span_name", Text),
            ("span_kind", Text),
            ("status_code", Int),
            (SPAN_DURATION_COLUMN, Int),
            (SPAN_TIMING_STATUS_COLUMN, Text),
            ("end_ts", Nanos),
            ("trace_id", Text),
            ("span_id", Text),
            ("parent_span_id", Text),
        ],
    }
}

pub const EVENT_ID_COLUMN: &str = "event_id";
