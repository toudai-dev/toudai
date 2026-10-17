//! JSON wire form of a query request.
//!
//! The wire types mirror [`crate::query::Query`] and carry no tenant: the scope
//! a query runs under comes from the credential, and `deny_unknown_fields` turns
//! a body that tries to name a tenant (or anything else unrecognised) into a
//! rejected request instead of an ignored field.
//!
//! Literals use the typed form of the response encoding, `{"type": ..., "value":
//! ...}`, so the type is never inferred from JSON syntax: `{"type":"int"}` and
//! `{"type":"double"}` stay distinct. Integers may be JSON integers or decimal
//! strings (clients without 64-bit integers must use the string form); the same
//! holds for the nanosecond time range.

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::auth::TenantId;
use crate::query::{
    Aggregate, AttrRef, CmpOp, Literal, Namespace, Predicate, Query, Scope, Signal, TimeRange,
    ValueType,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub dataset: String,
    signal: SignalWire,
    time: TimeWire,
    #[serde(default)]
    filters: Vec<PredicateWire>,
    #[serde(default)]
    group_by: Vec<AttrWire>,
    aggregate: AggregateWire,
}

impl QueryRequest {
    /// The request under the authenticated tenant's scope.
    pub fn into_query(self, tenant: &TenantId) -> Query {
        Query {
            scope: Scope {
                workspace_id: tenant.as_str().to_owned(),
                dataset: self.dataset,
            },
            signal: match self.signal {
                SignalWire::Logs => Signal::Logs,
                SignalWire::Spans => Signal::Spans,
            },
            time: TimeRange {
                start_ns: self.time.start_ns.0,
                end_ns: self.time.end_ns.0,
            },
            filters: self.filters.into_iter().map(Predicate::from).collect(),
            group_by: self.group_by.into_iter().map(AttrRef::from).collect(),
            aggregate: self.aggregate.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SignalWire {
    Logs,
    Spans,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimeWire {
    start_ns: Nanos,
    end_ns: Nanos,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(try_from = "Value")]
struct Nanos(i64);

impl TryFrom<Value> for Nanos {
    type Error = String;

    fn try_from(value: Value) -> Result<Nanos, String> {
        int64(&value).map(Nanos)
    }
}

/// An `Int64` from a JSON integer or a decimal string.
fn int64(value: &Value) -> Result<i64, String> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| format!("{n} is not a 64-bit integer")),
        Value::String(s) => s
            .parse()
            .map_err(|_| format!("{s:?} is not a 64-bit integer")),
        other => Err(format!("{other} is not a 64-bit integer")),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttrWire {
    namespace: NamespaceWire,
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NamespaceWire {
    Record,
    Resource,
    Scope,
}

impl From<AttrWire> for AttrRef {
    fn from(attr: AttrWire) -> AttrRef {
        AttrRef {
            namespace: match attr.namespace {
                NamespaceWire::Record => Namespace::Record,
                NamespaceWire::Resource => Namespace::Resource,
                NamespaceWire::Scope => Namespace::Scope,
            },
            key: attr.key,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(try_from = "String")]
struct ValueTypeWire(ValueType);

impl TryFrom<String> for ValueTypeWire {
    type Error = String;

    fn try_from(name: String) -> Result<ValueTypeWire, String> {
        ValueType::parse(&name)
            .map(ValueTypeWire)
            .ok_or_else(|| format!("unknown value type {name:?}"))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CmpOpWire {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PredicateWire {
    Exists {
        attr: AttrWire,
    },
    Absent {
        attr: AttrWire,
    },
    HasType {
        attr: AttrWire,
        value_type: ValueTypeWire,
    },
    Compare {
        attr: AttrWire,
        op: CmpOpWire,
        value: LiteralWire,
    },
}

impl From<PredicateWire> for Predicate {
    fn from(predicate: PredicateWire) -> Predicate {
        match predicate {
            PredicateWire::Exists { attr } => Predicate::Exists(attr.into()),
            PredicateWire::Absent { attr } => Predicate::Absent(attr.into()),
            PredicateWire::HasType { attr, value_type } => {
                Predicate::HasType(attr.into(), value_type.0)
            }
            PredicateWire::Compare { attr, op, value } => Predicate::Compare {
                attr: attr.into(),
                op: match op {
                    CmpOpWire::Eq => CmpOp::Eq,
                    CmpOpWire::Ne => CmpOp::Ne,
                    CmpOpWire::Lt => CmpOp::Lt,
                    CmpOpWire::Le => CmpOp::Le,
                    CmpOpWire::Gt => CmpOp::Gt,
                    CmpOpWire::Ge => CmpOp::Ge,
                },
                value: value.0,
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum AggregateWire {
    // Empty struct variants, not unit variants: serde ignores unknown fields on
    // a unit variant of an internally tagged enum, which would let
    // `{"kind": "count", "tenant_id": ...}` through.
    Count {},
    CountValues {
        attr: AttrWire,
        value_type: ValueTypeWire,
    },
    Sum {
        attr: AttrWire,
        value_type: ValueTypeWire,
    },
    SpanDuration {},
}

impl From<AggregateWire> for Aggregate {
    fn from(aggregate: AggregateWire) -> Aggregate {
        match aggregate {
            AggregateWire::Count {} => Aggregate::Count,
            AggregateWire::CountValues { attr, value_type } => Aggregate::CountValues {
                attr: attr.into(),
                value_type: value_type.0,
            },
            AggregateWire::Sum { attr, value_type } => Aggregate::Sum {
                attr: attr.into(),
                value_type: value_type.0,
            },
            AggregateWire::SpanDuration {} => Aggregate::SpanDuration,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "Value")]
struct LiteralWire(Literal);

impl TryFrom<Value> for LiteralWire {
    type Error = String;

    fn try_from(value: Value) -> Result<LiteralWire, String> {
        literal(&value).map(LiteralWire)
    }
}

fn literal(value: &Value) -> Result<Literal, String> {
    let object = value
        .as_object()
        .ok_or("a literal is an object {\"type\": ..., \"value\": ...}")?;
    if let Some(extra) = object.keys().find(|k| *k != "type" && *k != "value") {
        return Err(format!("unknown literal field {extra:?}"));
    }
    let ty = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or("a literal needs a text \"type\"")?;
    let inner = object
        .get("value")
        .ok_or_else(|| format!("a {ty} literal needs a \"value\""))?;
    let wrong = |expected: &str| format!("a {ty} literal's value must be {expected}");
    Ok(match ty {
        "string" => Literal::String(inner.as_str().ok_or_else(|| wrong("text"))?.to_owned()),
        "bool" => Literal::Bool(inner.as_bool().ok_or_else(|| wrong("true or false"))?),
        "int" => Literal::Int(int64(inner)?),
        "double" => Literal::Double(
            double(inner)
                .ok_or_else(|| wrong("a number, or \"NaN\", \"Infinity\", or \"-Infinity\""))?,
        ),
        "bytes" => Literal::Bytes(
            hex_decode(inner.as_str().ok_or_else(|| wrong("a hex string"))?)
                .ok_or_else(|| wrong("a hex string"))?,
        ),
        "array" => Literal::Array(
            inner
                .as_array()
                .ok_or_else(|| wrong("an array of literals"))?
                .iter()
                .map(literal)
                .collect::<Result<_, _>>()?,
        ),
        "map" => Literal::Map(
            inner
                .as_object()
                .ok_or_else(|| wrong("an object of literals"))
                .and_then(map_entries)?,
        ),
        other => return Err(format!("unknown literal type {other:?}")),
    })
}

fn map_entries(object: &Map<String, Value>) -> Result<Vec<(String, Literal)>, String> {
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), literal(value)?)))
        .collect()
}

/// JSON cannot carry NaN or the infinities, so they are spelled as text; the
/// query layer rejects them with an explicit error rather than the request
/// failing to parse.
fn double(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => match s.as_str() {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            _ => None,
        },
        _ => None,
    }
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const TENANT: &str = "tenant-from-credential";

    fn parse(request: Value) -> Result<Query, String> {
        serde_json::from_value::<QueryRequest>(request)
            .map(|r| r.into_query(&TenantId::new(TENANT)))
            .map_err(|e| e.to_string())
    }

    fn base() -> Value {
        json!({
            "dataset": "fixture",
            "signal": "logs",
            "time": {"start_ns": "1788246000123456789", "end_ns": 1788246000123456790_i64},
            "aggregate": {"kind": "count"},
        })
    }

    fn with(field: &str, value: Value) -> Value {
        let mut request = base();
        request[field] = value;
        request
    }

    #[test]
    fn full_request_maps_to_the_query_model() {
        let request = json!({
            "dataset": "fixture",
            "signal": "spans",
            "time": {"start_ns": 5, "end_ns": "9007199254740993"},
            "filters": [
                {"kind": "exists", "attr": {"namespace": "record", "key": "a.b"}},
                {"kind": "absent", "attr": {"namespace": "resource", "key": "r"}},
                {"kind": "has_type", "attr": {"namespace": "scope", "key": "s"}, "value_type": "int"},
                {"kind": "compare", "attr": {"namespace": "record", "key": "k"}, "op": "ge",
                 "value": {"type": "int", "value": "9007199254740993"}},
            ],
            "group_by": [{"namespace": "record", "key": "g"}],
            "aggregate": {"kind": "sum", "attr": {"namespace": "record", "key": "k"}, "value_type": "int"},
        });
        let query = parse(request).unwrap();
        assert_eq!(
            query,
            Query {
                scope: Scope {
                    workspace_id: TENANT.into(),
                    dataset: "fixture".into()
                },
                signal: Signal::Spans,
                time: TimeRange {
                    start_ns: 5,
                    end_ns: 9_007_199_254_740_993
                },
                filters: vec![
                    Predicate::Exists(AttrRef::record("a.b")),
                    Predicate::Absent(AttrRef::resource("r")),
                    Predicate::HasType(AttrRef::scope("s"), ValueType::Int),
                    Predicate::Compare {
                        attr: AttrRef::record("k"),
                        op: CmpOp::Ge,
                        value: Literal::Int(9_007_199_254_740_993),
                    },
                ],
                group_by: vec![AttrRef::record("g")],
                aggregate: Aggregate::Sum {
                    attr: AttrRef::record("k"),
                    value_type: ValueType::Int
                },
            }
        );
    }

    #[test]
    fn the_body_cannot_name_a_tenant_or_anything_unknown() {
        let cases = [
            with("tenant_id", json!("someone-else")),
            with("workspace_id", json!("someone-else")),
            with(
                "scope",
                json!({"workspace_id": "someone-else", "dataset": "fixture"}),
            ),
            with("surprise", json!(true)),
            with(
                "time",
                json!({"start_ns": 1, "end_ns": 2, "tenant_id": "x"}),
            ),
            with("aggregate", json!({"kind": "count", "tenant_id": "x"})),
            with(
                "filters",
                json!([{"kind": "exists", "tenant_id": "x",
                        "attr": {"namespace": "record", "key": "k"}}]),
            ),
        ];
        for request in cases {
            let err = parse(request.clone()).expect_err(&request.to_string());
            assert!(err.contains("unknown field"), "{request}: {err}");
        }
    }

    #[test]
    fn literals() {
        use Literal::*;
        let ok = [
            (json!({"type": "string", "value": ""}), String("".into())),
            (json!({"type": "string", "value": "1"}), String("1".into())),
            (json!({"type": "bool", "value": false}), Bool(false)),
            (json!({"type": "int", "value": 1}), Int(1)),
            (
                json!({"type": "int", "value": "-9223372036854775808"}),
                Int(i64::MIN),
            ),
            (json!({"type": "double", "value": 1}), Double(1.0)),
            (json!({"type": "double", "value": 1.5}), Double(1.5)),
            (
                json!({"type": "double", "value": "-Infinity"}),
                Double(f64::NEG_INFINITY),
            ),
            (
                json!({"type": "bytes", "value": "00fe"}),
                Bytes(vec![0, 0xfe]),
            ),
            (
                json!({"type": "array", "value": [{"type": "int", "value": 1}]}),
                Array(vec![Int(1)]),
            ),
            (
                json!({"type": "map", "value": {"k": {"type": "bool", "value": true}}}),
                Map(vec![("k".into(), Bool(true))]),
            ),
        ];
        for (wire, expected) in ok {
            assert_eq!(literal(&wire), Ok(expected), "{wire}");
        }
        assert!(
            matches!(literal(&json!({"type": "double", "value": "NaN"})), Ok(Double(v)) if v.is_nan())
        );

        let bad = [
            (json!(1), "an object"),
            (json!({"value": 1}), "\"type\""),
            (json!({"type": "int"}), "needs a \"value\""),
            (json!({"type": "int", "value": 1.5}), "64-bit integer"),
            (
                json!({"type": "int", "value": "9223372036854775808"}),
                "64-bit integer",
            ),
            (json!({"type": "int", "value": true}), "64-bit integer"),
            (json!({"type": "string", "value": 1}), "must be text"),
            (json!({"type": "bool", "value": "true"}), "true or false"),
            (
                json!({"type": "double", "value": "1.5"}),
                "must be a number",
            ),
            (json!({"type": "bytes", "value": "0"}), "hex string"),
            (json!({"type": "bytes", "value": "zz"}), "hex string"),
            (json!({"type": "empty"}), "needs a \"value\""),
            (
                json!({"type": "empty", "value": null}),
                "unknown literal type",
            ),
            (
                json!({"type": "int", "value": 1, "extra": 1}),
                "unknown literal field",
            ),
        ];
        for (wire, detail) in bad {
            let err = literal(&wire).unwrap_err();
            assert!(err.contains(detail), "{wire}: {err}");
        }
    }

    /// The request is parsed from text, as on the wire: `from_value` would skip
    /// the number parser. Without `float_roundtrip`, serde_json reads
    /// `900719925474099.5` (exact in binary64) as `900719925474099.625`.
    #[test]
    fn double_literals_parse_correctly_rounded() {
        let cases = [
            "900719925474099.5",
            "0.1",
            "1e23",
            "5e-324",
            "-2.2250738585072014e-308",
        ];
        for text in cases {
            let request = format!(
                r#"{{"dataset":"d","signal":"logs","time":{{"start_ns":1,"end_ns":2}},
                    "filters":[{{"kind":"compare","op":"eq",
                      "attr":{{"namespace":"record","key":"k"}},
                      "value":{{"type":"double","value":{text}}}}}],
                    "aggregate":{{"kind":"count"}}}}"#
            );
            let parsed: QueryRequest = serde_json::from_str(&request).unwrap();
            let query = parsed.into_query(&TenantId::new(TENANT));
            let Predicate::Compare {
                value: Literal::Double(got),
                ..
            } = &query.filters[0]
            else {
                panic!("not a double comparison")
            };
            let expected: f64 = text.parse().unwrap();
            assert_eq!(got.to_bits(), expected.to_bits(), "{text}");
        }
    }

    #[test]
    fn malformed_requests() {
        let cases = [
            (with("signal", json!("metrics")), "unknown variant"),
            (with("time", json!({"start_ns": 1})), "missing field"),
            (
                with("time", json!({"start_ns": 1.5, "end_ns": 2})),
                "64-bit integer",
            ),
            (
                with("filters", json!([{"kind": "regex"}])),
                "unknown variant",
            ),
            (
                with("group_by", json!([{"namespace": "span", "key": "k"}])),
                "unknown variant",
            ),
            (
                with(
                    "aggregate",
                    json!({"kind": "sum", "value_type": "number",
                           "attr": {"namespace": "record", "key": "k"}}),
                ),
                "unknown value type",
            ),
        ];
        for (request, detail) in cases {
            let err = parse(request.clone()).unwrap_err();
            assert!(err.contains(detail), "{request}: {err}");
        }
        let mut missing = base();
        missing.as_object_mut().unwrap().remove("aggregate");
        assert!(parse(missing).unwrap_err().contains("missing field"));
    }
}
