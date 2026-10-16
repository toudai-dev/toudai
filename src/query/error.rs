//! Explicit, machine-readable query errors.

use std::fmt;

use super::value::ValueType;

/// The query operation a value was rejected for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Compare,
    Group,
    Aggregate,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Operation::Compare => "compare",
            Operation::Group => "group",
            Operation::Aggregate => "aggregate",
        }
    }
}

/// Values preserved for canonical retrieval that the PoC query surface does not
/// operate on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedValue {
    Bytes,
    Array,
    Map,
    NonFiniteFloat,
}

impl UnsupportedValue {
    pub fn as_str(self) -> &'static str {
        match self {
            UnsupportedValue::Bytes => "bytes",
            UnsupportedValue::Array => "array",
            UnsupportedValue::Map => "map",
            UnsupportedValue::NonFiniteFloat => "non_finite_float",
        }
    }

    /// The unsupported class of a value type, if any. Non-finite floats are a
    /// property of a value, not of the `double` type, so they are never
    /// returned here.
    pub fn from_type(ty: ValueType) -> Option<UnsupportedValue> {
        match ty {
            ValueType::Bytes => Some(UnsupportedValue::Bytes),
            ValueType::Array => Some(UnsupportedValue::Array),
            ValueType::Map => Some(UnsupportedValue::Map),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryError {
    /// The operation is not defined for the value on the PoC query surface.
    UnsupportedOperation {
        operation: Operation,
        value: UnsupportedValue,
    },
}

impl QueryError {
    /// Stable identifier for clients and diagnostics.
    pub fn code(&self) -> &'static str {
        match self {
            QueryError::UnsupportedOperation { .. } => "unsupported_operation",
        }
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::UnsupportedOperation { operation, value } => write!(
                f,
                "cannot {} {} values: they are preserved for canonical retrieval only",
                operation.as_str(),
                value.as_str()
            ),
        }
    }
}

impl std::error::Error for QueryError {}
