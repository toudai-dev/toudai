//! The serving layout the query service is written against.
//!
//! Names follow `rfd/0001/experiments/021-hybrid-layout-collector-integration/
//! harness/serving-tables.sql`, the layout Decision 0001 D1 selects. Decision
//! 0001 D2 names the tenant column `tenant_id` and adds `routing_epoch`; those
//! belong to the DDL the Decision's implementation lands, so every column name
//! lives here and nowhere else.

use super::error::QueryError;
use super::model::{Namespace, Signal};
use super::value::ValueType;

pub const WORKSPACE_COLUMN: &str = "workspace_id";
pub const DATASET_COLUMN: &str = "dataset";
pub const TS_COLUMN: &str = "ts";
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
}

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
