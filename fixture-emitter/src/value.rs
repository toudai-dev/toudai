//! Typed attribute values and the canonical encoding used for payload checksums.
//!
//! The node encoding is the one Experiments 019 and 021 use for the canonical
//! payload: an explicit type tag per value, decimal strings for numbers, base64
//! for bytes, and `nan`/`inf`/`-inf`/`-0.0` for the special doubles. Nothing is
//! inferred from string content, so `"1"`, `1`, and `1.0` stay distinct.

use base64::Engine;
use serde_json::{Map, Value as Json, json};

/// An OTLP `AnyValue`, plus the explicit empty value (an `AnyValue` with no
/// value set). The Rust SDK cannot produce `Empty`; the observer can still
/// receive it from other producers.
#[derive(Clone, Debug)]
pub enum Val {
    Empty,
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Array(Vec<Val>),
    Map(Vec<(String, Val)>),
}

/// An ordered attribute collection. Order and duplicates are preserved so that
/// duplicate keys stay detectable.
pub type Attrs = Vec<(String, Val)>;

impl Val {
    pub fn kind(&self) -> &'static str {
        match self {
            Val::Empty => "empty",
            Val::Str(_) => "string",
            Val::Int(_) => "int",
            Val::Double(_) => "double",
            Val::Bool(_) => "bool",
            Val::Bytes(_) => "bytes",
            Val::Array(_) => "array",
            Val::Map(_) => "map",
        }
    }

    /// True for values that are preserved canonically but that the query
    /// surface rejects: bytes, arrays, maps, and non-finite doubles.
    pub fn unsupported_for_query(&self) -> bool {
        match self {
            Val::Bytes(_) | Val::Array(_) | Val::Map(_) => true,
            Val::Double(d) => !d.is_finite(),
            _ => false,
        }
    }

    /// The type-tagged canonical node. A map with a repeated nested key is an
    /// error, matching the rule for top-level attribute collections.
    pub fn node(&self) -> Result<Json, String> {
        Ok(match self {
            Val::Empty => json!({"t": "empty"}),
            Val::Str(s) => json!({"t": "string", "v": s}),
            Val::Bool(b) => json!({"t": "bool", "v": b}),
            Val::Int(i) => json!({"t": "int", "v": i.to_string()}),
            Val::Double(d) => json!({"t": "double", "v": double_text(*d)}),
            Val::Bytes(y) => {
                json!({"t": "bytes", "v": base64::engine::general_purpose::STANDARD.encode(y)})
            }
            Val::Array(items) => {
                let nodes: Result<Vec<_>, _> = items.iter().map(Val::node).collect();
                json!({"t": "array", "v": nodes?})
            }
            Val::Map(items) => json!({"t": "map", "v": nodes(items)?}),
        })
    }

    /// The string form stored for resource and scope attributes, which the
    /// layout keeps as one string Map plus a companion type Map. Only scalar
    /// kinds have a defined text here.
    pub fn serving_text(&self) -> Result<String, String> {
        match self {
            Val::Str(s) => Ok(s.clone()),
            Val::Int(i) => Ok(i.to_string()),
            Val::Bool(b) => Ok(b.to_string()),
            other => Err(format!(
                "{} values have no serving text in resource or scope attributes",
                other.kind()
            )),
        }
    }
}

/// Canonical nodes for one attribute collection, rejecting a repeated key.
pub fn nodes(attrs: &[(String, Val)]) -> Result<Map<String, Json>, String> {
    let mut out = Map::new();
    for (key, value) in attrs {
        if out.insert(key.clone(), value.node()?).is_some() {
            return Err(format!("duplicate attribute key {key:?}"));
        }
    }
    Ok(out)
}

/// The canonical double text: shortest round-trip digits laid out like Go's
/// `strconv.FormatFloat(f, 'g', -1, 64)` (exponent form when the decimal
/// exponent is below -4 or at least 6, at least two exponent digits), a `.0`
/// suffix when the result has no point or exponent, `nan`/`inf`/`-inf` for
/// non-finite values, and an explicit sign for negative zero.
pub fn double_text(f: f64) -> String {
    if f.is_nan() {
        return "nan".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf" } else { "-inf" }.into();
    }
    if f == 0.0 {
        return if f.is_sign_negative() { "-0.0" } else { "0.0" }.into();
    }
    let sci = format!("{:e}", f.abs());
    let (mantissa, exp) = sci
        .split_once('e')
        .expect("LowerExp always has an exponent");
    let exp: i32 = exp.parse().expect("exponent is an integer");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let nd = digits.len() as i32;
    let dp = exp + 1;
    let mut out = String::new();
    if f < 0.0 {
        out.push('-');
    }
    if !(-4..6).contains(&exp) {
        out.push_str(&digits[..1]);
        if nd > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        out.push_str(&format!("{:02}", exp.abs()));
    } else if dp <= 0 {
        out.push_str("0.");
        out.push_str(&"0".repeat((-dp) as usize));
        out.push_str(&digits);
    } else if dp >= nd {
        out.push_str(&digits);
        out.push_str(&"0".repeat((dp - nd) as usize));
    } else {
        out.push_str(&digits[..dp as usize]);
        out.push('.');
        out.push_str(&digits[dp as usize..]);
    }
    if !out.contains(['.', 'e', 'E']) {
        out.push_str(".0");
    }
    out
}

/// `YYYY-MM-DD HH:MM:SS.nnnnnnnnn` (UTC), the timestamp text of the serving
/// fixtures.
pub fn timestamp_text(ns: u64) -> String {
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = ns % 1_000_000_000;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Civil-from-days (proleptic Gregorian), after H. Hinnant.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{nanos:09}",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn double_text_table() {
        let cases: &[(f64, &str)] = &[
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (1.5, "1.5"),
            (-2.25, "-2.25"),
            (129.95, "129.95"),
            (123456.0, "123456.0"),
            (1234567.0, "1.234567e+06"),
            (1_000_000.0, "1e+06"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (9007199254740992.0, "9.007199254740992e+15"),
            (1e21, "1e+21"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::NAN, "nan"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ];
        for (input, want) in cases {
            assert_eq!(double_text(*input), *want, "{input:?}");
        }
    }

    #[test]
    fn timestamp_text_table() {
        let cases: &[(u64, &str)] = &[
            (0, "1970-01-01 00:00:00.000000000"),
            (1_788_246_000_123_456_789, "2026-09-01 07:00:00.123456789"),
            (951_782_400_000_000_001, "2000-02-29 00:00:00.000000001"),
            (1_709_251_199_999_999_999, "2024-02-29 23:59:59.999999999"),
        ];
        for (ns, want) in cases {
            assert_eq!(timestamp_text(*ns), *want, "{ns}");
        }
    }

    #[test]
    fn node_keeps_types_distinct() {
        let cases: Vec<(Val, &str)> = vec![
            (Val::Str("1".into()), r#"{"t":"string","v":"1"}"#),
            (Val::Int(1), r#"{"t":"int","v":"1"}"#),
            (Val::Double(1.0), r#"{"t":"double","v":"1.0"}"#),
            (
                Val::Int(9007199254740993),
                r#"{"t":"int","v":"9007199254740993"}"#,
            ),
            (Val::Bool(false), r#"{"t":"bool","v":false}"#),
            (Val::Str(String::new()), r#"{"t":"string","v":""}"#),
            (Val::Empty, r#"{"t":"empty"}"#),
            (
                Val::Bytes(vec![0, 1, 254, 255]),
                r#"{"t":"bytes","v":"AAH+/w=="}"#,
            ),
            (
                Val::Array(vec![Val::Str("a".into()), Val::Int(1), Val::Bool(true)]),
                r#"{"t":"array","v":[{"t":"string","v":"a"},{"t":"int","v":"1"},{"t":"bool","v":true}]}"#,
            ),
            (
                Val::Map(vec![
                    ("b".into(), Val::Int(2)),
                    ("a".into(), Val::Bool(false)),
                ]),
                r#"{"t":"map","v":{"a":{"t":"bool","v":false},"b":{"t":"int","v":"2"}}}"#,
            ),
        ];
        for (value, want) in cases {
            assert_eq!(value.node().unwrap().to_string(), want);
        }
    }

    #[test]
    fn duplicate_keys_are_errors() {
        let dup = vec![
            ("k".to_string(), Val::Int(1)),
            ("k".to_string(), Val::Int(2)),
        ];
        assert!(nodes(&dup).is_err());
        assert!(Val::Map(dup).node().is_err());
    }
}
