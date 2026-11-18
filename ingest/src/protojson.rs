//! OTLP/JSON beyond what the `opentelemetry-proto` serde types accept. A
//! Collector `otlphttp` exporter with `encoding: json` writes JSON with
//! pdata, which writes NaN and the infinities as the strings `"NaN"`,
//! `"Infinity"`, and `"-Infinity"`; the serde types refuse a string double,
//! so such a request failed whole with a 400. Protobuf JSON (protojson) also
//! allows a double as a decimal string, such as `"1.5"`, and `null` for any
//! field as its default; those are accepted too.
//!
//! Preserve direct decoding's compatibility behavior. On failure, normalize
//! borrowed JSON slices into bytes, not a full Value tree. The fallback's
//! intermediate storage is bounded by input bytes and live object keys,
//! rather than a second decoded object graph. The fallback refuses serde's
//! private first-key raw-JSON escape, which could reset those depth limits.

use std::collections::HashSet;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use serde::de::DeserializeOwned;

#[path = "protojson_tokens.rs"]
mod tokens;

/// An export request whose attribute and body values can be visited, so
/// stand-ins can be put back.
pub trait AnyValues {
    fn for_each_value(&mut self, f: &mut dyn FnMut(&mut AnyValue));
}

/// Decode an OTLP/JSON export request, accepting protojson's string doubles
/// and `null` fields. `None` for a body that is not a valid request.
pub fn decode<M: DeserializeOwned + AnyValues>(body: &[u8]) -> Option<M> {
    if let Ok(message) = serde_json::from_slice(body) {
        return Some(message);
    }
    let (bytes, stand_ins) = tokens::normalize(body)?;
    let mut message: M = serde_json::from_slice(&bytes).ok()?;
    message.for_each_value(&mut |value| stand_ins.restore(value));
    Some(message)
}

const SPECIALS: [(&str, f64); 3] = [
    ("NaN", f64::NAN),
    ("Infinity", f64::INFINITY),
    ("-Infinity", f64::NEG_INFINITY),
];

/// One finite stand-in per special double, none of them a double the
/// request holds, so putting them back changes no real value.
struct StandIns([f64; 3]);

impl StandIns {
    fn choose(taken: &HashSet<u64>) -> StandIns {
        // Counting down from the largest finite double; a request would need
        // every candidate to push the choice further.
        let mut bits = f64::MAX.to_bits();
        let mut next = || loop {
            let candidate = bits;
            bits -= 1;
            if !taken.contains(&candidate) {
                return f64::from_bits(candidate);
            }
        };
        StandIns([next(), next(), next()])
    }

    fn restore(&self, value: &mut AnyValue) {
        if let Some(any_value::Value::DoubleValue(d)) = &mut value.value
            && let Some(i) = self.0.iter().position(|s| s.to_bits() == d.to_bits())
        {
            *d = SPECIALS[i].1;
        }
    }
}

fn attributes(values: &mut [KeyValue], f: &mut dyn FnMut(&mut AnyValue)) {
    for kv in values {
        if let Some(value) = &mut kv.value {
            any(value, f);
        }
    }
}

fn any(value: &mut AnyValue, f: &mut dyn FnMut(&mut AnyValue)) {
    f(value);
    match &mut value.value {
        Some(any_value::Value::ArrayValue(array)) => {
            array.values.iter_mut().for_each(|v| any(v, f));
        }
        Some(any_value::Value::KvlistValue(list)) => attributes(&mut list.values, f),
        _ => {}
    }
}

impl AnyValues for ExportLogsServiceRequest {
    fn for_each_value(&mut self, f: &mut dyn FnMut(&mut AnyValue)) {
        for resource in &mut self.resource_logs {
            if let Some(r) = &mut resource.resource {
                attributes(&mut r.attributes, f);
            }
            for scope in &mut resource.scope_logs {
                if let Some(s) = &mut scope.scope {
                    attributes(&mut s.attributes, f);
                }
                for record in &mut scope.log_records {
                    attributes(&mut record.attributes, f);
                    if let Some(body) = &mut record.body {
                        any(body, f);
                    }
                }
            }
        }
    }
}

impl AnyValues for ExportTraceServiceRequest {
    fn for_each_value(&mut self, f: &mut dyn FnMut(&mut AnyValue)) {
        for resource in &mut self.resource_spans {
            if let Some(r) = &mut resource.resource {
                attributes(&mut r.attributes, f);
            }
            for scope in &mut resource.scope_spans {
                if let Some(s) = &mut scope.scope {
                    attributes(&mut s.attributes, f);
                }
                for span in &mut scope.spans {
                    attributes(&mut span.attributes, f);
                    for event in &mut span.events {
                        attributes(&mut event.attributes, f);
                    }
                    for link in &mut span.links {
                        attributes(&mut link.attributes, f);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn double(value: &AnyValue) -> Option<f64> {
        match value.value {
            Some(any_value::Value::DoubleValue(d)) => Some(d),
            _ => None,
        }
    }

    /// Record attribute `a` of the first record, as a double, from a logs
    /// request whose record attributes are `attributes`.
    fn attribute(attributes: Value) -> Option<Option<f64>> {
        let body = json!({"resourceLogs": [{"scopeLogs": [{"logRecords": [
            {"attributes": attributes}
        ]}]}]});
        let request: ExportLogsServiceRequest = decode(body.to_string().as_bytes())?;
        let record = &request.resource_logs[0].scope_logs[0].log_records[0];
        Some(
            record
                .attributes
                .first()
                .and_then(|kv| double(kv.value.as_ref()?)),
        )
    }

    fn d(value: Value) -> Value {
        json!([{"key": "a", "value": {"doubleValue": value}}])
    }

    #[test]
    fn string_doubles_decode_as_protojson_writes_them() {
        let nan = attribute(d(json!("NaN"))).unwrap().unwrap();
        assert!(nan.is_nan());
        let cases = [
            (json!("Infinity"), Some(Some(f64::INFINITY))),
            (json!("-Infinity"), Some(Some(f64::NEG_INFINITY))),
            (json!("1.5"), Some(Some(1.5))),
            (json!("-2e3"), Some(Some(-2000.0))),
            (json!(0.25), Some(Some(0.25))),
            // Rust spellings protojson refuses, and an overflow.
            (json!("nan"), None),
            (json!("inf"), None),
            (json!("infinity"), None),
            (json!("1e999"), None),
            (json!("x"), None),
        ];
        for (value, expected) in cases {
            assert_eq!(attribute(d(value.clone())), expected, "{value}");
        }
    }

    /// A real double equal to the first stand-in keeps its value, while a
    /// special double elsewhere in the same request is still put back.
    #[test]
    fn stand_ins_never_collide_with_real_values() {
        let first_stand_in = f64::MAX;
        let body = json!({"resourceLogs": [{"scopeLogs": [{"logRecords": [{
            "attributes": [
                {"key": "real", "value": {"doubleValue": first_stand_in}},
                {"key": "special", "value": {"doubleValue": "Infinity"}},
            ],
        }]}]}]});
        let request: ExportLogsServiceRequest = decode(body.to_string().as_bytes()).unwrap();
        let attributes = &request.resource_logs[0].scope_logs[0].log_records[0].attributes;
        let values: Vec<f64> = attributes
            .iter()
            .map(|kv| double(kv.value.as_ref().unwrap()).unwrap())
            .collect();
        assert_eq!(values, [first_stand_in, f64::INFINITY]);
    }

    /// Special doubles are found wherever an attribute value can be: nested
    /// in arrays and maps, in bodies, and in span events and links.
    #[test]
    fn special_doubles_are_restored_everywhere() {
        let inf = json!({"doubleValue": "-Infinity"});
        let logs = json!({"resourceLogs": [{
            "resource": {"attributes": [{"key": "r", "value": {"arrayValue": {"values": [inf]}}}]},
            "scopeLogs": [{
                "scope": {"attributes": [{"key": "s", "value": {"kvlistValue": {"values": [
                    {"key": "k", "value": inf}
                ]}}}]},
                "logRecords": [{"body": inf}],
            }],
        }]});
        let mut request: ExportLogsServiceRequest = decode(logs.to_string().as_bytes()).unwrap();
        let mut found = Vec::new();
        request.for_each_value(&mut |v| found.extend(double(v)));
        assert_eq!(found, [f64::NEG_INFINITY; 3]);

        let traces = json!({"resourceSpans": [{"scopeSpans": [{"spans": [{
            "attributes": [{"key": "a", "value": inf}],
            "events": [{"attributes": [{"key": "e", "value": inf}]}],
            "links": [{"attributes": [{"key": "l", "value": inf}]}],
        }]}]}]});
        let mut request: ExportTraceServiceRequest = decode(traces.to_string().as_bytes()).unwrap();
        let mut found = Vec::new();
        request.for_each_value(&mut |v| found.extend(double(v)));
        assert_eq!(found, [f64::NEG_INFINITY; 3]);
    }

    /// protojson reads `null` as a field's default, for repeated and message
    /// fields alike.
    #[test]
    fn null_fields_are_defaults() {
        let body = json!({"resourceLogs": [{
            "resource": null,
            "scopeLogs": [{"scope": null, "logRecords": [
                {"attributes": null, "body": null, "timeUnixNano": null}
            ]}],
        }]});
        let request: ExportLogsServiceRequest = decode(body.to_string().as_bytes()).unwrap();
        let record = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert!(record.attributes.is_empty() && record.body.is_none());
        assert!(decode::<ExportLogsServiceRequest>(b"{not json").is_none());
    }

    #[test]
    fn unknown_fields_and_last_duplicate_keep_their_previous_meaning() {
        for body in [
            r#"{"resourceLogs":[],"future":{"doubleValue":"text"}}"#,
            r#"{"future":1e400}"#,
            r#"{"resourceLogs":[],"future":1e400}"#,
            r#"{"resourceLogs":[{}],"resourceLogs":[]}"#,
            r#"{"resourceLogs":[{}],"resourceLogs":null}"#,
        ] {
            assert_eq!(
                decode::<ExportLogsServiceRequest>(body.as_bytes()),
                Some(ExportLogsServiceRequest::default()),
                "{body}"
            );
        }
        assert!(
            decode::<ExportLogsServiceRequest>(
                br#"{"resourceLogs":null,"future":{"doubleValue":"text"}}"#
            )
            .is_none()
        );
        for body in [
            r#"{"stringValue":"kept","stringValue":null}"#,
            r#"{"stringValue":"kept","doubleValue":"1.5"}"#,
        ] {
            let body = format!(
                r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[{{"body":{body}}}]}}]}}]}}"#
            );
            let request: ExportLogsServiceRequest = decode(body.as_bytes()).unwrap();
            assert_eq!(
                request.resource_logs[0].scope_logs[0].log_records[0]
                    .body
                    .as_ref()
                    .unwrap()
                    .value,
                Some(any_value::Value::StringValue("kept".into()))
            );
        }
        let body = br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"doubleValue":"NaN","doubleValue":1.5}}]}]}]}"#;
        let request: ExportLogsServiceRequest = decode(body).unwrap();
        assert_eq!(
            double(
                request.resource_logs[0].scope_logs[0].log_records[0]
                    .body
                    .as_ref()
                    .unwrap()
            ),
            Some(1.5)
        );
    }

    #[test]
    fn fallback_refuses_private_embedded_json_without_changing_direct_decode() {
        for body in [
            r#"{"resourceLogs":{"$serde_json::private::RawValue":"[]"}}"#,
            r#"{"resourceLogs":null,"future":{"$serde_json::private::RawValue":"[]"}}"#,
            r#"{"resourceLogs":null,"future":{"$serde_json::private::RawValue":0}}"#,
        ] {
            assert!(
                decode::<ExportLogsServiceRequest>(body.as_bytes()).is_none(),
                "{body}"
            );
        }
        // This is an ordinary ignored extension; the direct path does not
        // interpret embedded JSON, so there is no reason to refuse it.
        assert_eq!(
            decode::<ExportLogsServiceRequest>(
                br#"{"resourceLogs":[],"future":{"$serde_json::private::RawValue":0}}"#
            ),
            Some(ExportLogsServiceRequest::default())
        );
    }
}
