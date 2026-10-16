//! The query request model.

use super::value::{Literal, ValueType};

/// The authenticated scope every query runs under. Both fields are mandatory
/// and are bound as parameters; a request cannot widen them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub workspace_id: String,
    pub dataset: String,
}

/// Half-open event-time range `[start_ns, end_ns)` in Unix nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub start_ns: i64,
    pub end_ns: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Logs,
    Spans,
}

/// Field identity includes its namespace: the same key in different namespaces
/// is a different field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Namespace {
    Record,
    Resource,
    Scope,
}

impl Namespace {
    pub fn as_str(self) -> &'static str {
        match self {
            Namespace::Record => "record",
            Namespace::Resource => "resource",
            Namespace::Scope => "scope",
        }
    }
}

/// A literal attribute key within one namespace. A dot in the key does not
/// define nesting and case is preserved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttrRef {
    pub namespace: Namespace,
    pub key: String,
}

impl AttrRef {
    pub fn record(key: &str) -> AttrRef {
        AttrRef {
            namespace: Namespace::Record,
            key: key.to_owned(),
        }
    }

    pub fn resource(key: &str) -> AttrRef {
        AttrRef {
            namespace: Namespace::Resource,
            key: key.to_owned(),
        }
    }

    pub fn scope(key: &str) -> AttrRef {
        AttrRef {
            namespace: Namespace::Scope,
            key: key.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    pub fn is_ordering(self) -> bool {
        matches!(self, CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge)
    }
}

/// Filters combine with AND.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// The key is present, whatever its type or value (explicit empty, empty
    /// string, false, and zero are all present).
    Exists(AttrRef),
    /// The key is not present.
    Absent(AttrRef),
    /// The key was received with this type.
    HasType(AttrRef, ValueType),
    /// The key was received with the literal's type and the value compares true.
    /// Values of any other type never match, including `Ne`.
    Compare {
        attr: AttrRef,
        op: CmpOp,
        value: Literal,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregate {
    /// Event count only.
    Count,
    /// Event count plus the number of events whose attribute has `value_type`.
    CountValues {
        attr: AttrRef,
        value_type: ValueType,
    },
    /// Event count, contributing-value count, and the sum of an attribute of
    /// the declared numeric type. Incompatible non-empty values are an error
    /// unless an explicit type filter excludes them.
    Sum {
        attr: AttrRef,
        value_type: ValueType,
    },
    /// Span duration (nanoseconds): contributing, timing-error, and sum.
    SpanDuration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub scope: Scope,
    pub signal: Signal,
    pub time: TimeRange,
    pub filters: Vec<Predicate>,
    /// Group keys include the value type, so string `"1"`, integer `1`, and
    /// float `1.0` are distinct groups, as are missing and explicit empty.
    pub group_by: Vec<AttrRef>,
    pub aggregate: Aggregate,
}
