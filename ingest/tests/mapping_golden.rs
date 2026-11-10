//! Rows from `toudai-ingest` against rows from the Go exporter at collector
//! `f780574`, for the same OTLP requests.
//!
//! `tests/fixtures/mapping/*.otlp.json` are the producer requests. The Go
//! exporter's own `prepareLogs` and `prepareSpans` produced `*.expected.jsonl`
//! and the protobuf form `*.otlp.pb` (see `fixtures/mapping/regenerate.sh`).
//! Both sides use the event_ids `0198a0c4-7b3e-7c11-8a52-<n as 12 hex digits>`
//! for the n-th record, so canonical payloads and their SHA-256 must match
//! byte for byte, not only as JSON values.
//!
//! `toudai-ingest` differs from the Go exporter on purpose, and the test
//! applies each difference to the Go rows before comparing rather than
//! regenerating them, so the files stay the Go exporter's output for
//! Experiment 025. The mapping module's doc lists every difference; these are
//! the ones these requests exercise:
//!
//! - A span's canonical payload keeps its events (`span.events`).

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use toudai::auth::DatasetAccess;
use toudai_ingest::admit::{admit_logs, admit_traces};
use toudai_ingest::auth::Scope;
use toudai_ingest::mapping::{CanonicalRow, LogRow, Mapped, SpanRow, map_logs, map_traces};
use toudai_ingest::value::double_text;
use uuid::Uuid;

const DATASET: &str = "fixture";

fn golden_ids(n: usize) -> Vec<Uuid> {
    (0..n)
        .map(|i| Uuid::parse_str(&format!("0198a0c4-7b3e-7c11-8a52-{i:012x}")).unwrap())
        .collect()
}

fn scope() -> Scope {
    Scope {
        dataset: DATASET.into(),
        access: DatasetAccess::All,
    }
}

fn expected(text: &str) -> (Vec<Value>, Vec<(String, String)>) {
    let mut rows = Vec::new();
    let mut rejections = Vec::new();
    for line in text.lines() {
        let value: Value = serde_json::from_str(line).unwrap();
        match value.get("rejected") {
            Some(reason) => rejections.push((
                reason.as_str().unwrap().to_owned(),
                value["namespace"].as_str().unwrap().to_owned(),
            )),
            None => rows.push(value),
        }
    }
    (rows, rejections)
}

fn object<V: Into<Value> + Clone>(pairs: &[(String, V)]) -> Value {
    Value::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().into()))
            .collect::<Map<_, _>>(),
    )
}

fn doubles(pairs: &[(String, f64)]) -> Value {
    Value::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.clone(), double_text(*v).into()))
            .collect(),
    )
}

fn canonical(row: &CanonicalRow) -> Value {
    json!({
        "dataset": row.dataset, "signal": row.signal, "event_id": row.event_id,
        "timestamp": row.timestamp, "schema_version": row.schema_version,
        "payload": row.payload, "payload_sha256": row.payload_sha256,
    })
}

fn log_row(row: &LogRow, canonical_row: &CanonicalRow) -> Value {
    json!({
        "dataset": row.dataset, "event_id": row.event_id, "routing_epoch": row.routing_epoch,
        "timestamp": row.timestamp, "observed_ts": row.observed_ts, "kind": row.kind,
        "service_name": row.service_name, "severity_text": row.severity_text,
        "severity_number": row.severity_number, "body": row.body, "trace_id": row.trace_id,
        "span_id": row.span_id, "event_name": row.event_name,
        "resource": object(&row.resource), "scope": object(&row.scope),
        "resource_types": object(&row.resource_types), "scope_types": object(&row.scope_types),
        "record_int": object(&row.record_int), "record_double": doubles(&row.record_double),
        "record_string": object(&row.record_string), "record_bool": object(&row.record_bool),
        "record_other": object(&row.record_other), "payload_sha256": row.payload_sha256,
        "canonical": canonical(canonical_row),
    })
}

fn span_row(row: &SpanRow, canonical_row: &CanonicalRow) -> Value {
    json!({
        "dataset": row.dataset, "event_id": row.event_id, "routing_epoch": row.routing_epoch,
        "timestamp": row.timestamp, "end_ts": row.end_ts, "kind": row.kind,
        "service_name": row.service_name, "trace_id": row.trace_id, "span_id": row.span_id,
        "parent_span_id": row.parent_span_id, "span_name": row.span_name,
        "span_kind": row.span_kind, "status_code": row.status_code,
        "duration_ns": row.duration_ns, "span_timing_status": row.span_timing_status,
        "resource": object(&row.resource), "resource_types": object(&row.resource_types),
        "span_scope": object(&row.span_scope), "span_scope_types": object(&row.span_scope_types),
        "span_int": object(&row.span_int), "span_double": doubles(&row.span_double),
        "span_string": object(&row.span_string), "span_bool": object(&row.span_bool),
        "span_other": object(&row.span_other), "payload_sha256": row.payload_sha256,
        "canonical": canonical(canonical_row),
    })
}

/// Replace the Go row's payload with `edit(payload)`, and its hashes with the
/// new payload's. Edits are textual so the rest of the payload stays the Go
/// exporter's bytes.
fn edit_payload(row: &mut Value, edit: impl FnOnce(&str) -> String) {
    let payload = edit(row["canonical"]["payload"].as_str().unwrap());
    let sha: String = Sha256::digest(payload.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    row["canonical"]["payload"] = payload.into();
    row["canonical"]["payload_sha256"] = sha.clone().into();
    row["payload_sha256"] = sha.into();
}

/// Insert `events` (compact JSON) into the Go span envelope. Payload keys are
/// sorted, so `events` comes right after `end_ns`, which every span has.
fn with_span_events(row: &mut Value, events: &str) {
    edit_payload(row, |payload| {
        let span = payload.find("\"span\":{").unwrap();
        let end_ns = span + payload[span..].find("\"end_ns\":\"").unwrap();
        let value_end = end_ns + "\"end_ns\":\"".len();
        let at = value_end + payload[value_end..].find('"').unwrap() + 1;
        format!("{},\"events\":{events}{}", &payload[..at], &payload[at..])
    });
}

/// Compare row by row, so a failure names the record and field.
fn assert_rows<R>(
    signal: &str,
    mapped: &Mapped<R>,
    to_json: fn(&R, &CanonicalRow) -> Value,
    expected_text: &str,
    differences: fn(&mut Value),
    rejected_records: &[usize],
) {
    let (mut want_rows, want_rejections) = expected(expected_text);
    want_rows.iter_mut().for_each(differences);
    assert_eq!(mapped.serving.len(), want_rows.len(), "{signal} row count");
    assert_eq!(mapped.canonical.len(), mapped.serving.len());
    for ((row, canonical_row), want) in mapped.serving.iter().zip(&mapped.canonical).zip(&want_rows)
    {
        let got = to_json(row, canonical_row);
        for (field, want_value) in want.as_object().unwrap() {
            assert_eq!(
                &got[field], want_value,
                "{signal} {} field {field}",
                want["event_id"]
            );
        }
        assert_eq!(
            got.as_object().unwrap().len(),
            want.as_object().unwrap().len()
        );
    }
    let got_rejections: Vec<(String, String)> = mapped
        .rejections
        .iter()
        .map(|r| (r.reason.to_owned(), r.namespace.to_owned()))
        .collect();
    assert_eq!(got_rejections, want_rejections, "{signal} rejections");
    let got_counts: Vec<usize> = mapped.rejections.iter().map(|r| r.records).collect();
    assert_eq!(
        got_counts, rejected_records,
        "{signal} rejected record counts"
    );
}

#[test]
fn logs_match_the_go_exporter() {
    let request =
        ExportLogsServiceRequest::decode(&include_bytes!("fixtures/mapping/logs.otlp.pb")[..])
            .unwrap();
    let assigned = admit_logs(&request, &scope()).unwrap();
    let ids = golden_ids(assigned.len());
    let mapped = map_logs(&request, DATASET, &ids, 1);
    // Records with duplicate attribute, nested, and body keys; then a scope of
    // two and a resource of two with duplicate keys.
    assert_rows(
        "logs",
        &mapped,
        log_row,
        include_str!("fixtures/mapping/logs.expected.jsonl"),
        |_| {},
        &[1, 1, 1, 2, 2],
    );
    assert_eq!(mapped.serving.len() + mapped.rejected_records(), ids.len());
    // `toudai.dataset` and `toudai.anything` on the first record, two copies of
    // `toudai.x` on another, and `toudai.dataset` on the first scope.
    assert_eq!(mapped.dropped_reserved, 5);
}

#[test]
fn spans_match_the_go_exporter() {
    let request =
        ExportTraceServiceRequest::decode(&include_bytes!("fixtures/mapping/traces.otlp.pb")[..])
            .unwrap();
    let assigned = admit_traces(&request, &scope()).unwrap();
    let ids = golden_ids(assigned.len());
    let mapped = map_traces(&request, DATASET, &ids, 1);
    assert_rows(
        "spans",
        &mapped,
        span_row,
        include_str!("fixtures/mapping/traces.expected.jsonl"),
        |row| {
            // The request's one span event, on `GET /cart`; its reserved-key
            // and rejection rules are table-tested in the mapping module.
            let events = match row["span_name"].as_str().unwrap() {
                "GET /cart" => {
                    r#"[{"attributes":{"k":{"t":"string","v":"v"}},"name":"cache.miss","time_ns":"1788246000100000000"}]"#
                }
                _ => "[]",
            };
            with_span_events(row, events);
        },
        &[1, 1, 1],
    );
    assert_eq!(mapped.serving.len() + mapped.rejected_records(), ids.len());
    // One on the first scope and one on the first span.
    assert_eq!(mapped.dropped_reserved, 2);
}

/// Same content, different assigned IDs: only the identity differs, so the
/// payload hashes must differ and the IDs must be the ones assigned.
#[test]
fn identity_is_the_assigned_event_id() {
    let request =
        ExportLogsServiceRequest::decode(&include_bytes!("fixtures/mapping/logs.otlp.pb")[..])
            .unwrap();
    let count = admit_logs(&request, &scope()).unwrap().len();
    let first = golden_ids(count);
    let second: Vec<Uuid> = (0..count).map(|_| Uuid::now_v7()).collect();
    let a = map_logs(&request, DATASET, &first, 1);
    let b = map_logs(&request, DATASET, &second, 1);
    for ((ra, rb), (ca, cb)) in a
        .serving
        .iter()
        .zip(&b.serving)
        .zip(a.canonical.iter().zip(&b.canonical))
    {
        assert_ne!(ra.payload_sha256, rb.payload_sha256);
        assert_eq!(ra.event_id, ca.event_id);
        assert_eq!(rb.event_id, cb.event_id);
        assert!(second.iter().any(|id| id.to_string() == rb.event_id));
        assert!(!cb.payload.contains("toudai.event_id"));
    }
}
