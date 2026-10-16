//! Explicit, machine-readable query errors.

use std::fmt;

use super::model::Namespace;
use super::value::ValueType;

/// The query operation a value was rejected for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Compare,
    Order,
    Group,
    Aggregate,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Operation::Compare => "compare",
            Operation::Order => "order",
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
    /// The request has no workspace or dataset scope.
    MissingScope {
        field: &'static str,
    },
    /// The half-open time range is empty or inverted.
    InvalidTimeRange {
        start_ns: i64,
        end_ns: i64,
    },
    EmptyAttributeKey,
    InvalidIdentifier {
        name: String,
    },
    /// Ordering is defined for integers, finite floats, and strings only.
    OrderingNotDefined {
        value_type: ValueType,
    },
    /// Resource and scope attributes are stored as text with a companion type
    /// Map, so only exact string, bool, and int equality is defined for them.
    UnsupportedForNamespace {
        namespace: Namespace,
        operation: Operation,
        value_type: ValueType,
    },
    /// A numeric aggregate declared a non-numeric input type.
    NotNumeric {
        value_type: ValueType,
    },
    /// A value count was declared for a type without a scalar value.
    NotScalar {
        value_type: ValueType,
    },
    /// The aggregate reads a span column.
    AggregateRequiresSpans,
    /// A numeric aggregate met non-empty values of another type and no
    /// explicit type filter excluded them. Casts and mixed aggregation are not
    /// part of the PoC surface.
    IncompatibleType {
        attr: String,
        expected: ValueType,
        found: Vec<ValueType>,
    },
    /// The exact integer total does not fit `Int64`; it is rejected, never wrapped.
    IntegerOverflow {
        total: i128,
    },
    /// ClickHouse returned a row the statement cannot have produced.
    MalformedResult {
        detail: String,
    },
}

impl QueryError {
    /// Stable identifier for clients and diagnostics.
    pub fn code(&self) -> &'static str {
        match self {
            QueryError::UnsupportedOperation { .. } => "unsupported_operation",
            QueryError::MissingScope { .. } => "missing_scope",
            QueryError::InvalidTimeRange { .. } => "invalid_time_range",
            QueryError::EmptyAttributeKey => "empty_attribute_key",
            QueryError::InvalidIdentifier { .. } => "invalid_identifier",
            QueryError::OrderingNotDefined { .. } => "ordering_not_defined",
            QueryError::UnsupportedForNamespace { .. } => "unsupported_for_namespace",
            QueryError::NotNumeric { .. } => "not_numeric",
            QueryError::NotScalar { .. } => "not_scalar",
            QueryError::AggregateRequiresSpans => "aggregate_requires_spans",
            QueryError::IncompatibleType { .. } => "incompatible_type",
            QueryError::IntegerOverflow { .. } => "integer_overflow",
            QueryError::MalformedResult { .. } => "malformed_result",
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
            QueryError::MissingScope { field } => write!(f, "query scope requires {field}"),
            QueryError::InvalidTimeRange { start_ns, end_ns } => {
                write!(f, "time range [{start_ns}, {end_ns}) is empty")
            }
            QueryError::EmptyAttributeKey => write!(f, "attribute key is empty"),
            QueryError::InvalidIdentifier { name } => {
                write!(f, "{name:?} is not a plain SQL identifier")
            }
            QueryError::OrderingNotDefined { value_type } => write!(
                f,
                "ordering is not defined for {} values",
                value_type.as_str()
            ),
            QueryError::UnsupportedForNamespace {
                namespace,
                operation,
                value_type,
            } => write!(
                f,
                "cannot {} {} values in the {} namespace: they are stored as text",
                operation.as_str(),
                value_type.as_str(),
                namespace.as_str()
            ),
            QueryError::NotNumeric { value_type } => {
                write!(
                    f,
                    "{} is not a numeric aggregate input",
                    value_type.as_str()
                )
            }
            QueryError::NotScalar { value_type } => {
                write!(f, "{} has no scalar value to count", value_type.as_str())
            }
            QueryError::AggregateRequiresSpans => {
                write!(f, "span duration aggregates need the spans signal")
            }
            QueryError::IncompatibleType {
                attr,
                expected,
                found,
            } => {
                let found: Vec<_> = found.iter().map(|t| t.as_str()).collect();
                write!(
                    f,
                    "{attr:?} holds {} values but the aggregate declares {}; \
                     add a type filter to exclude them",
                    found.join(", "),
                    expected.as_str()
                )
            }
            QueryError::IntegerOverflow { total } => {
                write!(f, "integer total {total} does not fit Int64")
            }
            QueryError::MalformedResult { detail } => {
                write!(f, "unexpected ClickHouse result: {detail}")
            }
        }
    }
}

impl std::error::Error for QueryError {}
