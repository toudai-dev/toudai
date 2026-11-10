//! Checks on input a client controls, shared by the property tests below and
//! the cargo-fuzz targets in `ingest/fuzz`. Each panics when ingest breaks an
//! invariant, so a panic is a bug whichever driver finds it. Built only for
//! tests and with the `fuzzing` feature, which CI's `--all-features` lint
//! also builds.

use std::collections::HashSet;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use axum::body::Bytes;
use http::HeaderValue;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use prost::Message;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value as Json;
use sha2::{Digest, Sha256};
use toudai::auth::DatasetAccess;

use crate::admit::{DATASET_ATTR, RESERVED_PREFIX, admit_logs, admit_traces};
use crate::auth::Scope;
use crate::grpc::unsnappy;
use crate::http::{Format, MAX_BODY_BYTES, decompress};
use crate::mapping::{CanonicalRow, map_logs, map_traces};
use crate::protojson::{self, AnyValues};

/// Every `Content-Encoding` the HTTP receiver accepts, and one it refuses.
const ENCODINGS: [&str; 9] = [
    "",
    "gzip",
    "zlib",
    "deflate",
    "zstd",
    "lz4",
    "snappy",
    "x-snappy-framed",
    "br",
];

/// The dataset every check resolves, the one the test fixtures name; keys
/// may also permit another.
const DATASET: &str = "fixture";

/// An OTLP/HTTP body taken as the receiver and pipeline take it: decompressed,
/// decoded, admitted, and mapped. `selector` picks the encoding (low nibble),
/// the format (bit 4), the signal (bit 5), and the key's datasets (bits 6-7).
pub fn http_body(selector: u8, body: &[u8]) {
    let encoding = ENCODINGS[usize::from(selector & 0x0f) % ENCODINGS.len()];
    let format = if selector & 0x10 == 0 {
        Format::Protobuf
    } else {
        Format::Json
    };
    let header = (!encoding.is_empty()).then(|| HeaderValue::from_static(encoding));
    let Ok(body) = decompress(header.as_ref(), Bytes::copy_from_slice(body)) else {
        return;
    };
    if !encoding.is_empty() {
        assert!(body.len() <= MAX_BODY_BYTES, "decompression is capped");
    }
    let scope = scope(selector >> 6);
    if selector & 0x20 == 0 {
        if let Some(request) = format.decode::<ExportLogsServiceRequest>(&body) {
            check_logs(&request, &scope);
        }
    } else if let Some(request) = format.decode::<ExportTraceServiceRequest>(&body) {
        check_traces(&request, &scope);
    }
}

/// A key's scope: always [`DATASET`], which `authorize` guarantees the key
/// permits, with all datasets, only it, or it and one other.
pub fn scope(bits: u8) -> Scope {
    let access = match bits % 3 {
        0 => DatasetAccess::All,
        1 => DatasetAccess::Only(vec![DATASET.into()]),
        _ => DatasetAccess::Only(vec![DATASET.into(), "other".into()]),
    };
    Scope {
        dataset: DATASET.into(),
        access,
    }
}

/// A gRPC request body through the snappy layer. No message reaches tonic
/// still compressed, and a body with no compressed message passes through
/// unchanged, for tonic to decode or refuse.
pub fn grpc_body(body: &[u8]) {
    let Ok(out) = read_grpc(body) else {
        return;
    };
    assert!(
        !has_compressed_message(&out),
        "every message is decompressed"
    );
    if !has_compressed_message(body) {
        assert_eq!(&out[..], body, "uncompressed framing passes through");
    }
}

fn read_grpc(body: &[u8]) -> Result<Bytes, tonic::Status> {
    let body = tonic::body::Body::new(axum::body::Body::from(Bytes::copy_from_slice(body)));
    // An in-memory body is never pending, so one poll finishes the read and
    // no runtime is needed.
    let Poll::Ready(result) = pin!(unsnappy(body)).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("an in-memory body is read without waiting");
    };
    result
}

/// Whether any complete gRPC message in `body` is flagged compressed.
fn has_compressed_message(mut body: &[u8]) -> bool {
    while body.len() >= 5 {
        let length = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
        if body.len() - 5 < length {
            return false;
        }
        if body[0] == 1 {
            return true;
        }
        body = &body[5 + length..];
    }
    false
}

/// [`round_trip`] for a protobuf request. `selector` picks the signal (bit 0)
/// and whether doubles are written as decimal strings (bit 1).
pub fn json_round_trip(selector: u8, protobuf: &[u8]) {
    let decimal_strings = selector & 2 != 0;
    if selector & 1 == 0 {
        if let Ok(request) = ExportLogsServiceRequest::decode(protobuf) {
            round_trip(request, decimal_strings);
        }
    } else if let Ok(request) = ExportTraceServiceRequest::decode(protobuf) {
        round_trip(request, decimal_strings);
    }
}

/// Prefix of the strings standing in for doubles while the JSON is written.
const MARKER: &str = "\u{1}toudai-fuzzing-double:";

/// A request, written as OTLP/JSON with NaN and the infinities as strings, as
/// pdata spells them, decodes to the same request. With `decimal_strings`,
/// finite doubles are written as decimal strings, as protojson allows.
pub fn round_trip<M>(request: M, decimal_strings: bool)
where
    M: Message + Default + Clone + Serialize + DeserializeOwned + AnyValues,
{
    let Some(json) = otlp_json(&request, decimal_strings) else {
        return;
    };
    // serde_json refuses JSON nested over 128 levels. A request prost
    // accepts can need more, since each nested value costs three JSON levels
    // for two protobuf ones, so OTLP/JSON nested about 40 values deep gets a
    // 400 where protobuf is stored. That is the parser's limit, not a round
    // trip that changed the request.
    if serde_json::from_str::<Json>(&json).is_err() {
        return;
    }
    let decoded: M = protojson::decode(json.as_bytes())
        .unwrap_or_else(|| panic!("the JSON for a valid request decodes: {json}"));
    // Bytes rather than `==`, so -0.0 and 0.0 differ and NaN equals NaN once
    // both sides use the same NaN.
    assert!(
        stored_alike(decoded).encode_to_vec() == stored_alike(request).encode_to_vec(),
        "the JSON round trip changed the request: {json}"
    );
}

/// The request as the serde types write OTLP/JSON, except NaN and the
/// infinities as strings, as pdata spells them, and optionally every double
/// as a decimal string. `None` for a request holding a string that looks like
/// a marker.
fn otlp_json<M: Serialize + Clone + AnyValues>(
    request: &M,
    decimal_strings: bool,
) -> Option<String> {
    // serde writes a non-finite double as `null`, so each double that must
    // be a string is swapped for a marked string, put back in the tree.
    let mut marked = request.clone();
    let mut collides = false;
    marked.for_each_value(&mut |value| match &value.value {
        Some(any_value::Value::StringValue(s)) if s.starts_with(MARKER) => collides = true,
        Some(any_value::Value::DoubleValue(d)) if decimal_strings || !d.is_finite() => {
            let text = match *d {
                d if d.is_nan() => "NaN".to_owned(),
                f64::INFINITY => "Infinity".to_owned(),
                f64::NEG_INFINITY => "-Infinity".to_owned(),
                d => d.to_string(),
            };
            value.value = Some(any_value::Value::StringValue(format!("{MARKER}{text}")));
        }
        _ => {}
    });
    if collides {
        return None;
    }
    let mut tree = serde_json::to_value(&marked).expect("a request serializes");
    unmark(&mut tree);
    Some(tree.to_string())
}

fn unmark(value: &mut Json) {
    match value {
        Json::Object(fields) => {
            let text = match fields.get("stringValue") {
                Some(Json::String(s)) => s.strip_prefix(MARKER).map(str::to_owned),
                _ => None,
            };
            if let Some(text) = text {
                fields.clear();
                fields.insert("doubleValue".into(), Json::String(text));
            }
            fields.values_mut().for_each(unmark);
        }
        Json::Array(items) => items.iter_mut().for_each(unmark),
        _ => {}
    }
}

/// The request with values ingest stores alike made equal: every NaN the
/// same NaN, and the profiling-only string-table reference absent. The serde
/// types write that reference in JSON but ignore it when reading, and
/// [`crate::value::Value::decode`] stores it as empty either way.
fn stored_alike<M: AnyValues>(mut request: M) -> M {
    request.for_each_value(&mut |value: &mut AnyValue| match &mut value.value {
        Some(any_value::Value::DoubleValue(d)) if d.is_nan() => *d = f64::NAN,
        Some(any_value::Value::StringValueStrindex(_)) => value.value = None,
        _ => {}
    });
    request
}

/// Admit and map a logs request, and check what would be stored.
pub fn check_logs(request: &ExportLogsServiceRequest, scope: &Scope) {
    let Ok(ids) = admit_logs(request, scope) else {
        return; // Refused whole: nothing is mapped or stored.
    };
    let records = request
        .resource_logs
        .iter()
        .flat_map(|r| &r.scope_logs)
        .map(|s| s.log_records.len())
        .sum();
    // Which records storing keeps, worked out apart from the mapping: those
    // whose resource, scope, attributes, and body all have unique keys.
    let expected = request
        .resource_logs
        .iter()
        .filter(|r| unique_keys(r.resource.as_ref().map_or(&[], |r| &r.attributes)))
        .flat_map(|r| &r.scope_logs)
        .filter(|s| unique_keys(s.scope.as_ref().map_or(&[], |s| &s.attributes)))
        .flat_map(|s| &s.log_records)
        .filter(|r| unique_keys(&r.attributes) && nested_unique(r.body.as_ref()))
        .count();
    let mapped = map_logs(request, &scope.dataset, &ids, 0);
    let stored = mapped.serving.iter().map(|r| Stored {
        dataset: &r.dataset,
        event_id: &r.event_id,
        payload_sha256: &r.payload_sha256,
        resource: r
            .resource
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect(),
        keys: [&r.scope, &r.scope_types, &r.record_string, &r.record_other]
            .into_iter()
            .flatten()
            .map(|(k, _)| k.as_str())
            .chain(r.record_int.iter().map(|(k, _)| k.as_str()))
            .chain(r.record_double.iter().map(|(k, _)| k.as_str()))
            .chain(r.record_bool.iter().map(|(k, _)| k.as_str()))
            .collect(),
    });
    let stored: Vec<_> = stored.collect();
    check_stored(
        scope,
        &ids,
        records,
        expected,
        mapped.rejected_records(),
        &mapped.canonical,
        &stored,
    );
}

/// Admit and map a traces request, and check what would be stored.
pub fn check_traces(request: &ExportTraceServiceRequest, scope: &Scope) {
    let Ok(ids) = admit_traces(request, scope) else {
        return;
    };
    let records = request
        .resource_spans
        .iter()
        .flat_map(|r| &r.scope_spans)
        .map(|s| s.spans.len())
        .sum();
    let expected = request
        .resource_spans
        .iter()
        .filter(|r| unique_keys(r.resource.as_ref().map_or(&[], |r| &r.attributes)))
        .flat_map(|r| &r.scope_spans)
        .filter(|s| unique_keys(s.scope.as_ref().map_or(&[], |s| &s.attributes)))
        .flat_map(|s| &s.spans)
        .filter(|s| unique_keys(&s.attributes))
        .count();
    let mapped = map_traces(request, &scope.dataset, &ids, 0);
    let stored: Vec<_> = mapped
        .serving
        .iter()
        .map(|r| Stored {
            dataset: &r.dataset,
            event_id: &r.event_id,
            payload_sha256: &r.payload_sha256,
            resource: r
                .resource
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect(),
            keys: [
                &r.span_scope,
                &r.span_scope_types,
                &r.span_string,
                &r.span_other,
            ]
            .into_iter()
            .flatten()
            .map(|(k, _)| k.as_str())
            .chain(r.span_int.iter().map(|(k, _)| k.as_str()))
            .chain(r.span_double.iter().map(|(k, _)| k.as_str()))
            .chain(r.span_bool.iter().map(|(k, _)| k.as_str()))
            .collect(),
        })
        .collect();
    check_stored(
        scope,
        &ids,
        records,
        expected,
        mapped.rejected_records(),
        &mapped.canonical,
        &stored,
    );
}

/// Whether attribute keys are unique as storing requires: at the top level,
/// where producer `toudai.*` keys are dropped unread, and within every nested
/// map.
fn unique_keys(attributes: &[KeyValue]) -> bool {
    let mut seen = HashSet::new();
    attributes.iter().all(|kv| {
        kv.key.starts_with(RESERVED_PREFIX)
            || (seen.insert(kv.key.as_str()) && nested_unique(kv.value.as_ref()))
    })
}

fn nested_unique(value: Option<&AnyValue>) -> bool {
    match value.and_then(|v| v.value.as_ref()) {
        Some(any_value::Value::ArrayValue(array)) => {
            array.values.iter().all(|v| nested_unique(Some(v)))
        }
        Some(any_value::Value::KvlistValue(list)) => {
            let mut seen = HashSet::new();
            list.values
                .iter()
                .all(|kv| seen.insert(kv.key.as_str()) && nested_unique(kv.value.as_ref()))
        }
        _ => true,
    }
}

/// What a serving row stores that the checks read.
struct Stored<'a> {
    dataset: &'a str,
    event_id: &'a str,
    payload_sha256: &'a str,
    resource: Vec<(&'a str, &'a str)>,
    /// Scope and record keys.
    keys: Vec<&'a str>,
}

fn check_stored(
    scope: &Scope,
    ids: &[uuid::Uuid],
    records: usize,
    expected: usize,
    rejected: usize,
    canonical: &[CanonicalRow],
    stored: &[Stored<'_>],
) {
    assert_eq!(ids.len(), records, "one event_id per record");
    let ids: HashSet<String> = ids.iter().map(ToString::to_string).collect();
    assert_eq!(ids.len(), records, "event_ids are distinct");
    assert_eq!(
        stored.len() + rejected,
        records,
        "every record is stored or rejected"
    );
    assert_eq!(
        stored.len(),
        expected,
        "records are rejected for repeated keys only"
    );
    assert_eq!(
        canonical.len(),
        stored.len(),
        "one canonical row per serving row"
    );
    let mut seen = HashSet::new();
    for (row, c) in stored.iter().zip(canonical) {
        assert!(ids.contains(row.event_id), "a stored event_id was assigned");
        assert!(seen.insert(row.event_id), "no event_id is stored twice");
        assert_eq!(
            c.event_id, row.event_id,
            "serving and canonical rows pair up"
        );
        assert_eq!(row.dataset, scope.dataset, "rows use the trusted dataset");
        assert_eq!(c.dataset, scope.dataset, "rows use the trusted dataset");
        let sha: String = Sha256::digest(c.payload.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(c.payload_sha256, sha, "the canonical hash is the payload's");
        assert_eq!(row.payload_sha256, sha, "the serving hash is the payload's");

        // The only reserved key stored is the trusted dataset, on the
        // resource, in the serving row and the canonical payload alike.
        let reserved: Vec<_> = row
            .resource
            .iter()
            .filter(|(k, _)| k.starts_with(RESERVED_PREFIX))
            .collect();
        assert_eq!(reserved, [&(DATASET_ATTR, scope.dataset.as_str())]);
        assert!(
            !row.keys.iter().any(|k| k.starts_with(RESERVED_PREFIX)),
            "no producer reserved key is stored on a scope or record"
        );
        let payload: Json = serde_json::from_str(&c.payload).expect("the payload is JSON");
        assert_eq!(payload["identity"]["dataset"], scope.dataset.as_str());
        assert_eq!(payload["identity"]["event_id"], row.event_id);
        assert_eq!(
            payload["resource"][DATASET_ATTR]["v"],
            scope.dataset.as_str()
        );
        for namespace in ["resource", "scope", "record"] {
            let keys = payload[namespace]
                .as_object()
                .expect("namespaces are objects");
            let reserved = keys
                .keys()
                .filter(|k| k.starts_with(RESERVED_PREFIX))
                .any(|k| namespace != "resource" || k != DATASET_ATTR);
            assert!(
                !reserved,
                "no producer reserved key in the payload's {namespace}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use opentelemetry_proto::tonic::common::v1::{
        ArrayValue as Array, InstrumentationScope, KeyValue, KeyValueList,
    };
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
    use proptest::collection::vec;
    use proptest::option;
    use proptest::prelude::*;

    use super::*;

    /// Doubles whose encodings differ: the specials, signed zero, the
    /// extremes, integers, and the largest doubles, which protojson's
    /// stand-ins are chosen from.
    fn double() -> impl Strategy<Value = f64> {
        let near_max = |n| f64::from_bits(f64::MAX.to_bits() - n);
        prop_oneof![
            prop::sample::select(vec![
                f64::NAN,
                -f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                0.0,
                -0.0,
                3.0,
                f64::MIN_POSITIVE,
                5e-324,
                f64::MAX,
                near_max(1),
                near_max(2),
                f64::MIN,
            ]),
            prop::num::f64::ANY,
        ]
    }

    /// Attribute keys, so that some repeat, some are reserved, and rarely one
    /// is the producer `event_id` that refuses a whole request.
    fn key(reserved: u32, event_id: u32) -> impl Strategy<Value = String> {
        prop_oneof![
            40 => prop::sample::select(vec!["a", "b", "c", "d", "e", "f", "service.name"])
                .prop_map(String::from),
            4 => "\\PC{0,6}",
            reserved => prop::sample::select(vec!["toudai.dataset", "toudai.x"]).prop_map(String::from),
            event_id => Just("toudai.event_id".to_owned()),
        ]
    }

    fn kv(key: String, value: AnyValue) -> KeyValue {
        KeyValue {
            key,
            value: Some(value),
            ..Default::default()
        }
    }

    fn any_value() -> impl Strategy<Value = AnyValue> {
        use any_value::Value::*;
        let leaf = prop_oneof![
            Just(None),
            "\\PC{0,8}".prop_map(|s| Some(StringValue(s))),
            any::<bool>().prop_map(|b| Some(BoolValue(b))),
            any::<i64>().prop_map(|i| Some(IntValue(i))),
            double().prop_map(|d| Some(DoubleValue(d))),
            vec(any::<u8>(), 0..4).prop_map(|b| Some(BytesValue(b))),
            any::<i32>().prop_map(|i| Some(StringValueStrindex(i))),
        ]
        .prop_map(|value| AnyValue { value });
        leaf.prop_recursive(3, 16, 4, |inner| {
            prop_oneof![
                vec(inner.clone(), 0..4).prop_map(|values| AnyValue {
                    value: Some(ArrayValue(Array { values })),
                }),
                vec((key(1, 0), inner), 0..4).prop_map(|pairs| AnyValue {
                    value: Some(KvlistValue(KeyValueList {
                        values: pairs.into_iter().map(|(k, v)| kv(k, v)).collect(),
                    })),
                }),
            ]
        })
    }

    fn attributes(reserved: u32, event_id: u32) -> impl Strategy<Value = Vec<KeyValue>> {
        vec((key(reserved, event_id), any_value()), 0..4)
            .prop_map(|pairs| pairs.into_iter().map(|(k, v)| kv(k, v)).collect())
    }

    /// Mostly valid OTLP IDs, sometimes another length.
    fn id(length: usize) -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            20 => Just(Vec::new()),
            20 => vec(any::<u8>(), length),
            1 => vec(any::<u8>(), 0..20),
        ]
    }

    /// A resource with ordinary keys and, sometimes, a `toudai.dataset`
    /// naming the resolved dataset or another.
    fn resource() -> impl Strategy<Value = Option<Resource>> {
        let dataset = option::weighted(
            0.3,
            prop::sample::select(vec![DATASET, DATASET, "other", "x"]),
        );
        option::of(
            (attributes(0, 0), dataset).prop_map(|(mut attributes, dataset)| {
                if let Some(d) = dataset {
                    let value = AnyValue {
                        value: Some(any_value::Value::StringValue(d.into())),
                    };
                    attributes.push(kv(DATASET_ATTR.into(), value));
                }
                Resource {
                    attributes,
                    ..Default::default()
                }
            }),
        )
    }

    fn scope_attributes() -> impl Strategy<Value = Option<InstrumentationScope>> {
        option::of(
            attributes(2, 0).prop_map(|attributes| InstrumentationScope {
                attributes,
                ..Default::default()
            }),
        )
    }

    fn logs() -> impl Strategy<Value = ExportLogsServiceRequest> {
        let record = (
            (any::<u64>(), any::<u64>(), any::<i32>(), "\\PC{0,4}"),
            option::of(any_value()),
            attributes(2, 1),
            id(16),
            id(8),
        )
            .prop_map(
                |((time, observed, severity, text), body, attributes, trace, span)| LogRecord {
                    time_unix_nano: time,
                    observed_time_unix_nano: observed,
                    severity_number: severity,
                    severity_text: text,
                    body,
                    attributes,
                    trace_id: trace,
                    span_id: span,
                    ..Default::default()
                },
            );
        let scope =
            (scope_attributes(), vec(record, 0..5)).prop_map(|(scope, log_records)| ScopeLogs {
                scope,
                log_records,
                ..Default::default()
            });
        let resource =
            (resource(), vec(scope, 1..3)).prop_map(|(resource, scope_logs)| ResourceLogs {
                resource,
                scope_logs,
                ..Default::default()
            });
        vec(resource, 1..3).prop_map(|resource_logs| ExportLogsServiceRequest { resource_logs })
    }

    fn traces() -> impl Strategy<Value = ExportTraceServiceRequest> {
        let event = (any::<u64>(), "\\PC{0,4}", attributes(2, 0)).prop_map(
            |(time_unix_nano, name, attributes)| Event {
                time_unix_nano,
                name,
                attributes,
                ..Default::default()
            },
        );
        let link =
            (id(16), id(8), attributes(2, 0)).prop_map(|(trace_id, span_id, attributes)| Link {
                trace_id,
                span_id,
                attributes,
                ..Default::default()
            });
        let span = (
            (id(16), id(8), id(8), "\\PC{0,4}", any::<i32>()),
            (any::<u64>(), any::<u64>()),
            attributes(2, 1),
            (vec(event, 0..2), vec(link, 0..2)),
            option::of((any::<i32>(), "\\PC{0,4}")),
        )
            .prop_map(
                |(
                    (trace, span, parent, name, kind),
                    (start, end),
                    attributes,
                    (events, links),
                    status,
                )| {
                    Span {
                        trace_id: trace,
                        span_id: span,
                        parent_span_id: parent,
                        name,
                        kind,
                        start_time_unix_nano: start,
                        end_time_unix_nano: end,
                        attributes,
                        events,
                        links,
                        status: status.map(|(code, message)| Status { code, message }),
                        ..Default::default()
                    }
                },
            );
        let scope = (scope_attributes(), vec(span, 0..5)).prop_map(|(scope, spans)| ScopeSpans {
            scope,
            spans,
            ..Default::default()
        });
        let resource =
            (resource(), vec(scope, 1..3)).prop_map(|(resource, scope_spans)| ResourceSpans {
                resource,
                scope_spans,
                ..Default::default()
            });
        vec(resource, 1..3).prop_map(|resource_spans| ExportTraceServiceRequest { resource_spans })
    }

    /// A message of up to 200 KB, so snappy writes several chunks: a
    /// repeated pattern, which it compresses, or noise, which it stores.
    fn message() -> impl Strategy<Value = Vec<u8>> {
        (vec(any::<u8>(), 1..16), 0usize..200_000, any::<bool>()).prop_map(
            |(pattern, length, noise)| {
                let mut state = u64::from_le_bytes([pattern[0]; 8]) | 1;
                (0..length)
                    .map(|i| {
                        if noise {
                            // xorshift: cheap bytes snappy cannot compress
                            state ^= state << 13;
                            state ^= state >> 7;
                            state ^= state << 17;
                            state as u8
                        } else {
                            pattern[i % pattern.len()]
                        }
                    })
                    .collect()
            },
        )
    }

    /// A body nested 40 arrays deep is within prost's limit but past
    /// serde_json's once written as JSON, so it is skipped rather than
    /// reported as a round trip that changed the request.
    #[test]
    fn json_too_deep_to_parse_is_not_a_round_trip_failure() {
        let mut body = AnyValue {
            value: Some(any_value::Value::IntValue(1)),
        };
        for _ in 0..40 {
            body = AnyValue {
                value: Some(any_value::Value::ArrayValue(Array { values: vec![body] })),
            };
        }
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(body),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let decoded = ExportLogsServiceRequest::decode(&request.encode_to_vec()[..]);
        assert_eq!(decoded.as_ref(), Ok(&request), "prost accepts the depth");
        round_trip(request, false);
    }

    proptest! {
        /// Whatever a request holds, every record is stored once or counted
        /// as rejected, under the trusted dataset, with a distinct event_id,
        /// a payload matching its hash, and no producer reserved key.
        #[test]
        fn stored_logs_keep_their_invariants(request in logs(), key in any::<u8>()) {
            check_logs(&request, &scope(key));
        }

        #[test]
        fn stored_spans_keep_their_invariants(request in traces(), key in any::<u8>()) {
            check_traces(&request, &scope(key));
        }

        /// OTLP/JSON, with NaN and the infinities as strings and, as
        /// protojson also allows, doubles as decimal strings, decodes to the
        /// request it was written from, bit for bit.
        #[test]
        fn otlp_json_logs_decode_unchanged(request in logs(), decimal in any::<bool>()) {
            round_trip(request, decimal);
        }

        #[test]
        fn otlp_json_spans_decode_unchanged(request in traces(), decimal in any::<bool>()) {
            round_trip(request, decimal);
        }

        /// Any mix of snappy-compressed and plain gRPC messages reaches tonic
        /// as the same messages, uncompressed and framed in order.
        #[test]
        fn snappy_messages_reach_tonic_uncompressed(
            messages in vec((message(), any::<bool>()), 0..4),
        ) {
            let frame = |flag: u8, message: &[u8]| {
                let mut out = vec![flag];
                out.extend_from_slice(&(message.len() as u32).to_be_bytes());
                out.extend_from_slice(message);
                out
            };
            let (mut sent, mut expected) = (Vec::new(), Vec::new());
            for (message, compress) in &messages {
                expected.extend(frame(0, message));
                if *compress {
                    let mut encoder = snap::write::FrameEncoder::new(Vec::new());
                    encoder.write_all(message).unwrap();
                    sent.extend(frame(1, &encoder.into_inner().unwrap()));
                } else {
                    sent.extend(frame(0, message));
                }
            }
            prop_assert_eq!(read_grpc(&sent).unwrap(), expected);
        }
    }
}
