//! The OTLP `AnyValue` type system as the query layer sees it.

use serde_json::{Value, json};

use super::error::{Operation, QueryError, UnsupportedValue};

/// Received value type. The names match the serving layout: they are the
/// values of the `record_other`, `*_types` Maps and the query-visible type names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ValueType {
    String,
    Bool,
    Int,
    Double,
    Bytes,
    Array,
    Map,
    Empty,
}

impl ValueType {
    pub fn as_str(self) -> &'static str {
        match self {
            ValueType::String => "string",
            ValueType::Bool => "bool",
            ValueType::Int => "int",
            ValueType::Double => "double",
            ValueType::Bytes => "bytes",
            ValueType::Array => "array",
            ValueType::Map => "map",
            ValueType::Empty => "empty",
        }
    }

    pub fn parse(name: &str) -> Option<ValueType> {
        Some(match name {
            "string" => ValueType::String,
            "bool" => ValueType::Bool,
            "int" => ValueType::Int,
            "double" => ValueType::Double,
            "bytes" => ValueType::Bytes,
            "array" => ValueType::Array,
            "map" => ValueType::Map,
            "empty" => ValueType::Empty,
            _ => return None,
        })
    }

    /// Types with a value the PoC query surface can compare, group, or sum.
    pub fn is_scalar(self) -> bool {
        matches!(
            self,
            ValueType::String | ValueType::Bool | ValueType::Int | ValueType::Double
        )
    }

    /// Types preserved for canonical retrieval only; query operations on their
    /// values are rejected.
    pub fn is_canonical_only(self) -> bool {
        matches!(self, ValueType::Bytes | ValueType::Array | ValueType::Map)
    }
}

/// A literal in a request. Every OTLP value type is representable so that a
/// request using an unsupported one is rejected explicitly instead of being
/// unrepresentable or silently coerced.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Bool(bool),
    Int(i64),
    Double(f64),
    Bytes(Vec<u8>),
    Array(Vec<Literal>),
    Map(Vec<(String, Literal)>),
}

impl Literal {
    pub fn value_type(&self) -> ValueType {
        match self {
            Literal::String(_) => ValueType::String,
            Literal::Bool(_) => ValueType::Bool,
            Literal::Int(_) => ValueType::Int,
            Literal::Double(_) => ValueType::Double,
            Literal::Bytes(_) => ValueType::Bytes,
            Literal::Array(_) => ValueType::Array,
            Literal::Map(_) => ValueType::Map,
        }
    }

    /// Reject literals the PoC query surface cannot compare: bytes, arrays,
    /// maps, and non-finite floats.
    pub fn ensure_comparable(&self, operation: Operation) -> Result<(), QueryError> {
        match self {
            Literal::Double(v) if !v.is_finite() => Err(QueryError::UnsupportedOperation {
                operation,
                value: UnsupportedValue::NonFiniteFloat,
            }),
            other => match UnsupportedValue::from_type(other.value_type()) {
                Some(value) => Err(QueryError::UnsupportedOperation { operation, value }),
                None => Ok(()),
            },
        }
    }
}

/// A scalar value read back from the serving layout.
#[derive(Debug, Clone, PartialEq)]
pub enum TypedValue {
    String(String),
    Bool(bool),
    Int(i64),
    Double(f64),
    /// The value was received as an explicit empty `AnyValue`.
    Empty,
}

impl TypedValue {
    pub fn value_type(&self) -> ValueType {
        match self {
            TypedValue::String(_) => ValueType::String,
            TypedValue::Bool(_) => ValueType::Bool,
            TypedValue::Int(_) => ValueType::Int,
            TypedValue::Double(_) => ValueType::Double,
            TypedValue::Empty => ValueType::Empty,
        }
    }

    /// Lossless JSON with explicit type information. Integers are decimal
    /// strings so a client with IEEE-754 numbers cannot round them (`2^53 + 1`
    /// is a valid `Int64`).
    pub fn to_json(&self) -> Value {
        let ty = self.value_type().as_str();
        match self {
            TypedValue::String(v) => json!({ "type": ty, "value": v }),
            TypedValue::Bool(v) => json!({ "type": ty, "value": v }),
            TypedValue::Int(v) => json!({ "type": ty, "value": v.to_string() }),
            TypedValue::Double(v) => json!({ "type": ty, "value": v }),
            TypedValue::Empty => json!({ "type": ty }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_type_names_round_trip() {
        for ty in [
            ValueType::String,
            ValueType::Bool,
            ValueType::Int,
            ValueType::Double,
            ValueType::Bytes,
            ValueType::Array,
            ValueType::Map,
            ValueType::Empty,
        ] {
            assert_eq!(ValueType::parse(ty.as_str()), Some(ty));
        }
        assert_eq!(
            ValueType::parse("Int"),
            None,
            "type names are case-sensitive"
        );
        assert_eq!(
            ValueType::parse("missing"),
            None,
            "missing is not a value type"
        );
    }

    #[test]
    fn comparable_literals() {
        use Literal::*;
        // (literal, rejected value). The first rows are the fixture's
        // `metric.value` inventory (Experiment 019 fixture, via Experiment 021).
        let cases: Vec<(Literal, Option<UnsupportedValue>)> = vec![
            (Int(1), None),
            (Int(i64::MIN), None),
            (Double(1.0), None),
            (Double(-0.0), None),
            (String("1".into()), None),
            (String("".into()), None),
            (Bool(false), None),
            (Double(f64::NAN), Some(UnsupportedValue::NonFiniteFloat)),
            (
                Double(f64::INFINITY),
                Some(UnsupportedValue::NonFiniteFloat),
            ),
            (
                Double(f64::NEG_INFINITY),
                Some(UnsupportedValue::NonFiniteFloat),
            ),
            (Bytes(vec![0, 1, 0xfe, 0xff]), Some(UnsupportedValue::Bytes)),
            (
                Array(vec![String("a".into()), Int(1), Bool(true)]),
                Some(UnsupportedValue::Array),
            ),
            (
                Map(vec![("nested.int".into(), Int(1))]),
                Some(UnsupportedValue::Map),
            ),
        ];
        for (literal, rejected) in cases {
            let got = literal.ensure_comparable(Operation::Compare);
            match rejected {
                None => assert_eq!(got, Ok(()), "{literal:?}"),
                Some(value) => assert_eq!(
                    got,
                    Err(QueryError::UnsupportedOperation {
                        operation: Operation::Compare,
                        value
                    }),
                    "{literal:?}"
                ),
            }
        }
    }

    #[test]
    fn json_encoding_is_lossless_and_typed() {
        let cases = [
            // 2^53 + 1 is exact in Int64 but not in a double: must stay a string.
            (
                TypedValue::Int(9_007_199_254_740_993),
                json!({"type": "int", "value": "9007199254740993"}),
            ),
            (
                TypedValue::Int(i64::MIN),
                json!({"type": "int", "value": "-9223372036854775808"}),
            ),
            (
                TypedValue::Double(1.0),
                json!({"type": "double", "value": 1.0}),
            ),
            (
                TypedValue::String("1".into()),
                json!({"type": "string", "value": "1"}),
            ),
            (
                TypedValue::String(String::new()),
                json!({"type": "string", "value": ""}),
            ),
            (
                TypedValue::Bool(false),
                json!({"type": "bool", "value": false}),
            ),
            (TypedValue::Empty, json!({"type": "empty"})),
        ];
        for (value, expected) in cases {
            assert_eq!(value.to_json(), expected, "{value:?}");
        }
        // Distinct types must stay distinct in the response even when the
        // text is the same.
        assert_ne!(
            TypedValue::Int(1).to_json(),
            TypedValue::Double(1.0).to_json()
        );
        assert_ne!(
            TypedValue::Int(1).to_json(),
            TypedValue::String("1".into()).to_json()
        );
    }
}
