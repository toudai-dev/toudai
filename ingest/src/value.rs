//! Decoded OTLP attribute values and the text forms the stored rows use.
//!
//! Two text forms exist, and both reproduce the Go collector at `f780574`
//! byte for byte, because rows written by either path must compare equal
//! (Decision 0003 D5, Experiment 025):
//!
//! * the serving string of a resource or scope attribute and of a log body,
//!   which is pdata's `Value.AsString` ([`Value::as_serving_string`]);
//! * the typed canonical node `{t, v}` of the canonical payload
//!   ([`Value::canonical_node`]), with doubles as [`double_text`].

use std::collections::BTreeMap;
use std::fmt::Write;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use serde_json::{Value as Json, json};

/// One decoded OTLP `AnyValue`. Integers and doubles stay distinct, bytes stay
/// raw, and maps hold unique keys in sorted order.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Empty,
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Map(BTreeMap<String, Value>),
}

/// A map value repeats a key. The record, scope, or resource holding it is
/// rejected rather than silently keeping one copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuplicateKey;

impl Value {
    /// Decode an `AnyValue`. An absent value is [`Value::Empty`], and so is the
    /// profiling-only string-table reference, which the OTLP spec says other
    /// signals treat as absent (pdata reads it as empty too).
    pub fn decode(value: Option<&AnyValue>) -> Result<Value, DuplicateKey> {
        use any_value::Value as V;
        Ok(match value.and_then(|v| v.value.as_ref()) {
            None | Some(V::StringValueStrindex(_)) => Value::Empty,
            Some(V::StringValue(s)) => Value::Str(s.clone()),
            Some(V::IntValue(i)) => Value::Int(*i),
            Some(V::DoubleValue(d)) => Value::Double(*d),
            Some(V::BoolValue(b)) => Value::Bool(*b),
            Some(V::BytesValue(b)) => Value::Bytes(b.clone()),
            Some(V::ArrayValue(a)) => Value::Array(
                a.values
                    .iter()
                    .map(|v| Value::decode(Some(v)))
                    .collect::<Result<_, _>>()?,
            ),
            Some(V::KvlistValue(kvs)) => {
                let mut map = BTreeMap::new();
                for kv in &kvs.values {
                    if map
                        .insert(kv.key.clone(), Value::decode(kv.value.as_ref())?)
                        .is_some()
                    {
                        return Err(DuplicateKey);
                    }
                }
                Value::Map(map)
            }
        })
    }

    /// The type name stored in the companion type Maps and `*_other`.
    pub fn kind(&self) -> &'static str {
        match self {
            Value::Empty => "empty",
            Value::Str(_) => "string",
            Value::Int(_) => "int",
            Value::Double(_) => "double",
            Value::Bool(_) => "bool",
            Value::Bytes(_) => "bytes",
            Value::Array(_) => "array",
            Value::Map(_) => "map",
        }
    }

    /// pdata's `Value.AsString`: ES6 doubles with `NaN` and `Infinity`, base64
    /// bytes, and maps and arrays as JSON without HTML escaping. Go's encoder
    /// fails on a non-finite double inside a map or array and pdata ignores the
    /// error, so such a value serves as the empty string.
    pub fn as_serving_string(&self) -> String {
        match self {
            Value::Empty => String::new(),
            Value::Str(s) => s.clone(),
            Value::Int(i) => i.to_string(),
            Value::Double(d) if d.is_nan() => "NaN".into(),
            Value::Double(d) if d.is_infinite() => {
                if *d > 0.0 { "Infinity" } else { "-Infinity" }.into()
            }
            Value::Double(d) => es6_number(*d),
            Value::Bool(b) => b.to_string(),
            Value::Bytes(b) => BASE64.encode(b),
            Value::Array(_) | Value::Map(_) => {
                let mut out = String::new();
                match self.write_go_json(&mut out) {
                    Ok(()) => out,
                    Err(NonFinite) => String::new(),
                }
            }
        }
    }

    /// Go `encoding/json` of pdata's `AsRaw` form, with HTML escaping off.
    fn write_go_json(&self, out: &mut String) -> Result<(), NonFinite> {
        match self {
            Value::Empty => out.push_str("null"),
            Value::Str(s) => write_go_json_string(s, out),
            Value::Int(i) => write!(out, "{i}").expect("write to String"),
            Value::Double(d) if !d.is_finite() => return Err(NonFinite),
            Value::Double(d) => out.push_str(&es6_number(*d)),
            Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            // pdata's `AsRaw` turns empty bytes into a nil slice, which Go's
            // encoder writes as `null` (only inside a map or array: alone,
            // empty bytes serve as the empty base64 string).
            Value::Bytes(b) if b.is_empty() => out.push_str("null"),
            Value::Bytes(b) => {
                out.push('"');
                out.push_str(&BASE64.encode(b));
                out.push('"');
            }
            Value::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write_go_json(out)?;
                }
                out.push(']');
            }
            Value::Map(map) => {
                out.push('{');
                for (i, (key, item)) in map.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_go_json_string(key, out);
                    out.push(':');
                    item.write_go_json(out)?;
                }
                out.push('}');
            }
        }
        Ok(())
    }

    /// The typed canonical node (Experiment 019's encoding): an explicit type
    /// tag, integers as decimal strings, doubles as [`double_text`], bytes as
    /// base64.
    pub fn canonical_node(&self) -> Json {
        match self {
            Value::Empty => json!({"t": "empty"}),
            Value::Str(s) => json!({"t": "string", "v": s}),
            Value::Int(i) => json!({"t": "int", "v": i.to_string()}),
            Value::Double(d) => json!({"t": "double", "v": double_text(*d)}),
            Value::Bool(b) => json!({"t": "bool", "v": b}),
            Value::Bytes(b) => json!({"t": "bytes", "v": BASE64.encode(b)}),
            Value::Array(items) => {
                json!({"t": "array", "v": items.iter().map(Value::canonical_node).collect::<Vec<_>>()})
            }
            Value::Map(map) => json!({"t": "map", "v": canonical_nodes(map.iter())}),
        }
    }
}

struct NonFinite;

/// A canonical namespace: attribute name to typed node.
pub fn canonical_nodes<'a>(values: impl Iterator<Item = (&'a String, &'a Value)>) -> Json {
    Json::Object(
        values
            .map(|(k, v)| (k.clone(), v.canonical_node()))
            .collect(),
    )
}

/// One decoded attribute collection with unique keys.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Attributes {
    pub values: BTreeMap<String, Value>,
    /// Producer keys in the reserved `toudai.` namespace that were dropped.
    pub dropped_reserved: usize,
}

/// Decode an attribute collection. With `drop_reserved`, `toudai.*` keys are
/// dropped (and counted) before the duplicate check, as the Go exporter does,
/// so two copies of a reserved key do not reject the record.
pub fn decode_attributes(
    attributes: &[KeyValue],
    drop_reserved: bool,
) -> Result<Attributes, DuplicateKey> {
    let mut out = Attributes::default();
    for kv in attributes {
        if drop_reserved && kv.key.starts_with(crate::admit::RESERVED_PREFIX) {
            out.dropped_reserved += 1;
            continue;
        }
        let value = Value::decode(kv.value.as_ref())?;
        if out.values.insert(kv.key.clone(), value).is_some() {
            return Err(DuplicateKey);
        }
    }
    Ok(out)
}

/// Go `encoding/json` string escaping with HTML escaping off: `"` and `\`,
/// the short control escapes, other control bytes as `\u00xx`, and U+2028 and
/// U+2029, which Go always escapes. Everything else, DEL included, is raw.
fn write_go_json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\u{2028}' | '\u{2029}' => write!(out, "\\u{:04x}", c as u32).expect("write"),
            c if (c as u32) < 0x20 => write!(out, "\\u{:04x}", c as u32).expect("write"),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// The shortest round-trip decimal digits of a finite, nonzero `|f|`, and the
/// position of the decimal point relative to them (`0.0123` is `("123", -1)`).
///
/// The digits come from Ryu, which breaks a tie at the last digit to even as
/// Go's `strconv` does. Rust's own `{:e}` rounds such a tie up, so a double
/// like `1731196800000000.25` would print `...0003e+15` where Go prints
/// `...0002e+15`, and the canonical payload and its checksum would differ.
fn shortest_digits(f: f64) -> (String, i32) {
    let mut buffer = ryu::Buffer::new();
    let text = buffer.format_finite(f.abs());
    let (mantissa, exponent) = text.split_once('e').unwrap_or((text, "0"));
    let exponent: i32 = exponent.parse().expect("ryu exponent");
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all = format!("{int}{frac}");
    let leading = all.len() - all.trim_start_matches('0').len();
    let point = int.len() as i32 + exponent - leading as i32;
    (all.trim_matches('0').to_owned(), point)
}

fn sign(f: f64) -> &'static str {
    if f.is_sign_negative() { "-" } else { "" }
}

/// Go's `%f` layout of shortest digits.
fn fixed(digits: &str, point: i32) -> String {
    let n = digits.len() as i32;
    if point <= 0 {
        format!("0.{}{digits}", "0".repeat((-point) as usize))
    } else if point >= n {
        format!("{digits}{}", "0".repeat((point - n) as usize))
    } else {
        let (int, frac) = digits.split_at(point as usize);
        format!("{int}.{frac}")
    }
}

/// Go's `%e` layout of shortest digits, with the exponent padded to `width`.
fn scientific(digits: &str, point: i32, width: usize) -> String {
    let (first, rest) = digits.split_at(1);
    let exponent = point - 1;
    let dot = if rest.is_empty() { "" } else { "." };
    let exp_sign = if exponent < 0 { '-' } else { '+' };
    format!(
        "{first}{dot}{rest}e{exp_sign}{:0width$}",
        exponent.unsigned_abs()
    )
}

/// A finite double as ES6 number-to-string conversion, the form Go's JSON
/// encoder and pdata's `AsString` use: plain decimals unless the magnitude is
/// below 1e-6 or at least 1e21, then exponent form without padding.
pub fn es6_number(f: f64) -> String {
    if f == 0.0 {
        return format!("{}0", sign(f));
    }
    let (digits, point) = shortest_digits(f);
    let abs = f.abs();
    let body = if !(1e-6..1e21).contains(&abs) {
        scientific(&digits, point, 1)
    } else {
        fixed(&digits, point)
    };
    format!("{}{body}", sign(f))
}

/// The canonical double text (Experiment 019): Go's shortest `%g`, `nan`,
/// `inf`, and `-inf`, an explicit `-0.0`, and `.0` appended to an integral
/// value so it never reads as an integer.
pub fn double_text(f: f64) -> String {
    if f.is_nan() {
        return "nan".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf" } else { "-inf" }.into();
    }
    if f == 0.0 {
        return format!("{}0.0", sign(f));
    }
    let (digits, point) = shortest_digits(f);
    // Go's shortest %g switches to exponent form below 1e-4 and from 1e6 up,
    // whatever the digit count, and pads the exponent to two digits.
    let exponent = point - 1;
    let body = if !(-4..6).contains(&exponent) {
        scientific(&digits, point, 2)
    } else {
        fixed(&digits, point)
    };
    let mut text = format!("{}{body}", sign(f));
    if !text.contains(['.', 'e']) {
        text.push_str(".0");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected values are Go's: `strconv.FormatFloat(f, 'g', -1, 64)` with the
    /// exporter's `.0` rule, and `encoding/json` float output.
    #[test]
    // The tie values are exact doubles (between 2^50 and 2^51 doubles are 0.25
    // apart); their full digits are what the test is about.
    #[allow(clippy::excessive_precision)]
    fn double_forms_match_go() {
        // (value, double_text, es6)
        let cases = [
            (0.0, "0.0", "0"),
            (-0.0, "-0.0", "-0"),
            (1.0, "1.0", "1"),
            (-1.5, "-1.5", "-1.5"),
            (0.1, "0.1", "0.1"),
            (1e-4, "0.0001", "0.0001"),
            (1.5e-5, "1.5e-05", "0.000015"),
            (1e-6, "1e-06", "0.000001"),
            (1e-7, "1e-07", "1e-7"),
            (123456.0, "123456.0", "123456"),
            (999999.0, "999999.0", "999999"),
            (1e6, "1e+06", "1000000"),
            (1234567.5, "1.2345675e+06", "1234567.5"),
            (1e20, "1e+20", "100000000000000000000"),
            (1e21, "1e+21", "1e+21"),
            (-1.25e21, "-1.25e+21", "-1.25e+21"),
            (1e300, "1e+300", "1e+300"),
            (5e-324, "5e-324", "5e-324"),
            (
                f64::MAX,
                "1.7976931348623157e+308",
                "1.7976931348623157e+308",
            ),
            (
                9007199254740993.0,
                "9.007199254740992e+15",
                "9007199254740992",
            ),
            // Exactly halfway at the 17th digit: Go rounds to even.
            (
                1731196800000000.25,
                "1.7311968000000002e+15",
                "1731196800000000.2",
            ),
            (
                1533979136307259.25,
                "1.5339791363072592e+15",
                "1533979136307259.2",
            ),
            (0.3, "0.3", "0.3"),
            (100.0, "100.0", "100"),
            (0.000123, "0.000123", "0.000123"),
        ];
        for (value, text, es6) in cases {
            assert_eq!(double_text(value), text, "{value:e}");
            assert_eq!(es6_number(value), es6, "{value:e}");
        }
        assert_eq!(double_text(f64::NAN), "nan");
        assert_eq!(double_text(f64::INFINITY), "inf");
        assert_eq!(double_text(f64::NEG_INFINITY), "-inf");
    }

    #[test]
    fn serving_strings_match_pdata() {
        let map = |pairs: Vec<(&str, Value)>| {
            Value::Map(pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
        };
        let array = Value::Array(vec![
            Value::Str("<&>\u{2028}\"\\\u{1}\u{7f}\t".into()),
            Value::Int(9007199254740993),
            Value::Double(-0.0),
            Value::Double(1e-7),
            Value::Bytes(vec![0, 1]),
            Value::Empty,
        ]);
        // Go escapes U+2028 and control bytes but not `<`, `&`, `>`, or DEL.
        let array_json = r#"["<&>\u2028\"\\\u0001"#.to_owned()
            + "\u{7f}"
            + r#"\t",9007199254740993,-0,1e-7,"AAE=",null]"#;
        let cases: Vec<(Value, String)> = vec![
            (Value::Empty, "".into()),
            (Value::Str("<a&b>\u{2028}".into()), "<a&b>\u{2028}".into()),
            (Value::Int(i64::MIN), "-9223372036854775808".into()),
            (Value::Double(f64::NAN), "NaN".into()),
            (Value::Double(f64::NEG_INFINITY), "-Infinity".into()),
            (Value::Double(1e21), "1e+21".into()),
            (Value::Bool(false), "false".into()),
            (Value::Bytes(vec![0, 1, 0xfe, 0xff]), "AAH+/w==".into()),
            (Value::Array(vec![]), "[]".into()),
            (Value::Bytes(vec![]), "".into()),
            (
                Value::Array(vec![
                    Value::Bytes(vec![]),
                    map(vec![("b", Value::Bytes(vec![]))]),
                ]),
                r#"[null,{"b":null}]"#.into(),
            ),
            (map(vec![]), "{}".into()),
            (array, array_json),
            (
                map(vec![
                    ("z", Value::Int(1)),
                    ("a", map(vec![("d", Value::Double(0.1))])),
                ]),
                r#"{"a":{"d":0.1},"z":1}"#.into(),
            ),
            // Go's encoder fails on a non-finite double and pdata ignores it.
            (map(vec![("n", Value::Double(f64::NAN))]), "".into()),
            (Value::Array(vec![Value::Double(f64::INFINITY)]), "".into()),
        ];
        for (value, want) in cases {
            assert_eq!(value.as_serving_string(), want, "{value:?}");
        }
    }

    #[test]
    fn nested_duplicate_keys_are_rejected() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};
        let kv = |k: &str, v: i64| KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(v)),
            }),
            ..Default::default()
        };
        let list = |kvs: Vec<KeyValue>| AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList { values: kvs })),
        };
        let in_array = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![list(vec![kv("k", 1), kv("k", 2)])],
            })),
        };
        assert_eq!(
            Value::decode(Some(&list(vec![kv("k", 1), kv("k", 2)]))),
            Err(DuplicateKey)
        );
        assert_eq!(Value::decode(Some(&in_array)), Err(DuplicateKey));
        assert!(Value::decode(Some(&list(vec![kv("a", 1), kv("b", 2)]))).is_ok());
    }
}
