//! OTLP log records and spans to the Decision 0001 hybrid layout (as amended by
//! Decision 0002 D2): one serving row and one canonical row per stored record.
//!
//! The mapping reproduces the Go exporter at collector `f780574`, so rows
//! written by either path compare equal apart from `event_id` and the
//! differences listed below (Experiment 025). The dataset and the `event_id`s arrive as typed values from
//! [`crate::admit`] rather than as attributes stamped into the request, so the
//! exporter's "missing trusted scope" and "missing event_id" failures have no
//! counterpart here.
//!
//! It differs from the Go exporter on purpose where the Experiment 023 fixture
//! expects otherwise:
//!
//! - A span's events are kept in its canonical payload (`span.events`); the Go
//!   exporter drops them. So a span whose events repeat a key is rejected, and
//!   producer `toudai.*` keys on events are dropped and counted, where the Go
//!   exporter stores the span and ignores its events.
//! - A log record with `event_name` set is an application event, `kind`
//!   `event`; the Go exporter stores `log`.
//! - The payload schema is `toudai.canonical.event/2`; the Go exporter writes
//!   `/1`.
//!
//! A record is rejected on its own when it, its scope, its resource, or one of
//! its span events repeats an attribute key or nested map key; the rest of the
//! request is still stored. Producer `toudai.*` keys on records, spans, span
//! events, and scopes are dropped and counted. On the resource,
//! [`crate::admit`] has already refused every `toudai.*` key except a
//! `toudai.dataset` equal to the resolved one, which is replaced by the trusted
//! value.

use std::collections::BTreeMap;
use std::io;

use clickhouse::Row;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::Event;
use serde::Serialize;
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::admit::DATASET_ATTR;
use crate::value::{Attributes, DuplicateKey, Value, canonical_nodes, decode_attributes};

/// The `schema` field of every canonical payload and the `schema_version`
/// column of `canonical_events`. `/2` because the payload differs from the Go
/// exporter's `/1`: a span payload always has `events` (a `/1` span's events
/// were not captured, not absent), and `kind` may be `event`.
pub const CANONICAL_SCHEMA: &str = "toudai.canonical.event/2";

/// A `canonical_events` row. Timestamps are nanoseconds since the epoch, the
/// RowBinary form of `DateTime64(9)`.
#[derive(Debug, Clone, PartialEq, Row, Serialize)]
pub struct CanonicalRow {
    pub dataset: String,
    pub signal: String,
    pub event_id: String,
    pub timestamp: i64,
    pub schema_version: String,
    pub payload: String,
    pub payload_sha256: String,
}

/// A `serving_logs` row. Maps are key-sorted `(key, value)` lists.
#[derive(Debug, Clone, PartialEq, Row, Serialize)]
pub struct LogRow {
    pub dataset: String,
    pub event_id: String,
    pub routing_epoch: u16,
    pub timestamp: i64,
    pub observed_ts: i64,
    pub kind: String,
    pub service_name: String,
    pub severity_text: String,
    pub severity_number: i32,
    pub body: String,
    pub trace_id: String,
    pub span_id: String,
    pub event_name: String,
    pub resource: Vec<(String, String)>,
    pub scope: Vec<(String, String)>,
    pub resource_types: Vec<(String, String)>,
    pub scope_types: Vec<(String, String)>,
    pub record_int: Vec<(String, i64)>,
    pub record_double: Vec<(String, f64)>,
    pub record_string: Vec<(String, String)>,
    pub record_bool: Vec<(String, bool)>,
    pub record_other: Vec<(String, String)>,
    pub payload_sha256: String,
}

/// A `serving_spans` row.
#[derive(Debug, Clone, PartialEq, Row, Serialize)]
pub struct SpanRow {
    pub dataset: String,
    pub event_id: String,
    pub routing_epoch: u16,
    pub timestamp: i64,
    pub end_ts: i64,
    pub kind: String,
    pub service_name: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub span_name: String,
    pub span_kind: String,
    pub status_code: i32,
    pub duration_ns: Option<i64>,
    pub span_timing_status: String,
    pub resource: Vec<(String, String)>,
    pub resource_types: Vec<(String, String)>,
    pub span_scope: Vec<(String, String)>,
    pub span_scope_types: Vec<(String, String)>,
    pub span_int: Vec<(String, i64)>,
    pub span_double: Vec<(String, f64)>,
    pub span_string: Vec<(String, String)>,
    pub span_bool: Vec<(String, bool)>,
    pub span_other: Vec<(String, String)>,
    pub payload_sha256: String,
}

/// Records not stored, with the counter labels the Go exporter used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordRejection {
    pub reason: &'static str,
    pub namespace: &'static str,
    /// How many log records or spans it covers: a rejected resource or scope
    /// rejects every record under it.
    pub records: usize,
}

/// The rows of one request, and what was left out of them.
#[derive(Debug, Clone, PartialEq)]
pub struct Mapped<R> {
    pub serving: Vec<R>,
    pub canonical: Vec<CanonicalRow>,
    pub rejections: Vec<RecordRejection>,
    /// Producer `toudai.*` keys dropped from records, spans, span events, and
    /// scopes.
    pub dropped_reserved: usize,
}

impl<R> Mapped<R> {
    fn new() -> Self {
        Mapped {
            serving: Vec::new(),
            canonical: Vec::new(),
            rejections: Vec::new(),
            dropped_reserved: 0,
        }
    }

    fn reject(&mut self, reason: &'static str, namespace: &'static str, records: usize) {
        self.rejections.push(RecordRejection {
            reason,
            namespace,
            records,
        });
    }

    pub fn rejected_records(&self) -> usize {
        self.rejections.iter().map(|r| r.records).sum()
    }
}

/// Map a logs request. `event_ids` are [`crate::admit::admit_logs`]'s, one per
/// log record in request order; a rejected record's ID is simply unused.
pub fn map_logs(
    request: &ExportLogsServiceRequest,
    dataset: &str,
    event_ids: &[Uuid],
    routing_epoch: u16,
) -> Mapped<LogRow> {
    let mut out = Mapped::new();
    let mut ids = event_ids.iter();
    for rl in &request.resource_logs {
        let records = rl.scope_logs.iter().map(|s| s.log_records.len()).sum();
        let Some(resource) = stored_resource(rl.resource.as_ref(), dataset) else {
            skip(&mut ids, records);
            out.reject("invalid_resource_attributes", "resource", records);
            continue;
        };
        let (resource_strings, resource_types) = string_maps(&resource);
        let service_name = service_name(&resource);
        for sl in &rl.scope_logs {
            let Some(scope) = scope_attributes(sl.scope.as_ref()) else {
                skip(&mut ids, sl.log_records.len());
                out.reject("invalid_scope_attributes", "scope", sl.log_records.len());
                continue;
            };
            out.dropped_reserved += scope.dropped_reserved;
            let (scope_strings, scope_types) = string_maps(&scope.values);
            for lr in &sl.log_records {
                let event_id = next_id(&mut ids);
                let Ok(record) = decode_attributes(&lr.attributes, true) else {
                    out.reject("duplicate_or_invalid_record_attributes", "record", 1);
                    continue;
                };
                let Ok(body) = Value::decode(lr.body.as_ref()) else {
                    out.reject("invalid_body", "body", 1);
                    continue;
                };
                out.dropped_reserved += record.dropped_reserved;
                // A record without its own time is served at its observed
                // time; the canonical payload keeps both raw values.
                let served = if lr.time_unix_nano == 0 {
                    lr.observed_time_unix_nano
                } else {
                    lr.time_unix_nano
                };
                let trace_id = hex_id(&lr.trace_id);
                let span_id = hex_id(&lr.span_id);
                // The application-event encoding: standard OTLP, a log record
                // that names its event. It keeps the `log` envelope.
                let kind = if lr.event_name.is_empty() {
                    "log"
                } else {
                    "event"
                };
                let (payload, payload_sha256) = canonical_payload(Canonical {
                    kind,
                    dataset,
                    event_id: &event_id,
                    timestamp_ns: lr.time_unix_nano,
                    observed_ns: lr.observed_time_unix_nano,
                    trace_id: &trace_id,
                    span_id: &span_id,
                    body: &body,
                    resource: &resource,
                    scope: &scope.values,
                    record: &record.values,
                    envelope: (
                        "log",
                        json!({
                            "severity_text": lr.severity_text,
                            "severity_number": i64::from(lr.severity_number),
                            "event_name": lr.event_name,
                        }),
                    ),
                });
                let typed = TypedMaps::new(&record.values);
                out.canonical.push(CanonicalRow {
                    dataset: dataset.to_owned(),
                    signal: "logs".into(),
                    event_id: event_id.clone(),
                    timestamp: served as i64,
                    schema_version: CANONICAL_SCHEMA.into(),
                    payload,
                    payload_sha256: payload_sha256.clone(),
                });
                out.serving.push(LogRow {
                    dataset: dataset.to_owned(),
                    event_id,
                    routing_epoch,
                    timestamp: served as i64,
                    observed_ts: lr.observed_time_unix_nano as i64,
                    kind: kind.into(),
                    service_name: service_name.clone(),
                    severity_text: lr.severity_text.clone(),
                    severity_number: lr.severity_number,
                    body: body.as_serving_string(),
                    trace_id,
                    span_id,
                    event_name: lr.event_name.clone(),
                    resource: resource_strings.clone(),
                    scope: scope_strings.clone(),
                    resource_types: resource_types.clone(),
                    scope_types: scope_types.clone(),
                    record_int: typed.ints,
                    record_double: typed.doubles,
                    record_string: typed.strings,
                    record_bool: typed.bools,
                    record_other: typed.others,
                    payload_sha256,
                });
            }
        }
    }
    assert!(
        ids.next().is_none(),
        "admit assigns one event_id per record"
    );
    out
}

/// Map a traces request. `event_ids` are [`crate::admit::admit_traces`]'s, one
/// per span in request order.
pub fn map_traces(
    request: &ExportTraceServiceRequest,
    dataset: &str,
    event_ids: &[Uuid],
    routing_epoch: u16,
) -> Mapped<SpanRow> {
    let mut out = Mapped::new();
    let mut ids = event_ids.iter();
    for rs in &request.resource_spans {
        let records = rs.scope_spans.iter().map(|s| s.spans.len()).sum();
        let Some(resource) = stored_resource(rs.resource.as_ref(), dataset) else {
            skip(&mut ids, records);
            out.reject("invalid_resource_attributes", "resource", records);
            continue;
        };
        let (resource_strings, resource_types) = string_maps(&resource);
        let service_name = service_name(&resource);
        for ss in &rs.scope_spans {
            let Some(scope) = scope_attributes(ss.scope.as_ref()) else {
                skip(&mut ids, ss.spans.len());
                out.reject("invalid_scope_attributes", "scope", ss.spans.len());
                continue;
            };
            out.dropped_reserved += scope.dropped_reserved;
            let (scope_strings, scope_types) = string_maps(&scope.values);
            for span in &ss.spans {
                let event_id = next_id(&mut ids);
                let Ok(record) = decode_attributes(&span.attributes, true) else {
                    out.reject("duplicate_or_invalid_span_attributes", "span", 1);
                    continue;
                };
                let Ok((events, events_dropped)) = span_events(&span.events) else {
                    out.reject(
                        "duplicate_or_invalid_span_event_attributes",
                        "span_event",
                        1,
                    );
                    continue;
                };
                out.dropped_reserved += record.dropped_reserved + events_dropped;
                let (start, end) = (span.start_time_unix_nano, span.end_time_unix_nano);
                // OTLP has no presence for timestamps, so 0 is unset. Missing
                // timing wins over reversed timing: a missing start must not
                // yield a duration from epoch 0, and a missing end must not
                // read as end < start. Neither case is ever an underflow.
                let (duration_ns, timing) = if start == 0 || end == 0 {
                    (None, "missing")
                } else if end < start {
                    (None, "end_before_start")
                } else {
                    (Some((end - start) as i64), "ok")
                };
                let trace_id = hex_id(&span.trace_id);
                let span_id = hex_id(&span.span_id);
                let parent_span_id = hex_id(&span.parent_span_id);
                let span_kind = span_kind_name(span.kind);
                let status_code = span.status.as_ref().map_or(0, |s| s.code);
                let mut envelope = json!({
                    "name": span.name,
                    "parent_span_id": parent_span_id,
                    "kind": span_kind,
                    "status_code": status_code.to_string(),
                    "start_ns": start.to_string(),
                    "end_ns": end.to_string(),
                    "events": events,
                });
                if let Some(duration) = duration_ns {
                    envelope["duration_ns"] = duration.to_string().into();
                }
                let (payload, payload_sha256) = canonical_payload(Canonical {
                    kind: "span",
                    dataset,
                    event_id: &event_id,
                    timestamp_ns: start,
                    observed_ns: start,
                    trace_id: &trace_id,
                    span_id: &span_id,
                    body: &Value::Empty,
                    resource: &resource,
                    scope: &scope.values,
                    record: &record.values,
                    envelope: ("span", envelope),
                });
                let typed = TypedMaps::new(&record.values);
                out.canonical.push(CanonicalRow {
                    dataset: dataset.to_owned(),
                    signal: "spans".into(),
                    event_id: event_id.clone(),
                    timestamp: start as i64,
                    schema_version: CANONICAL_SCHEMA.into(),
                    payload,
                    payload_sha256: payload_sha256.clone(),
                });
                out.serving.push(SpanRow {
                    dataset: dataset.to_owned(),
                    event_id,
                    routing_epoch,
                    timestamp: start as i64,
                    end_ts: end as i64,
                    kind: "span".into(),
                    service_name: service_name.clone(),
                    trace_id,
                    span_id,
                    parent_span_id,
                    span_name: span.name.clone(),
                    span_kind: span_kind.into(),
                    status_code,
                    duration_ns,
                    span_timing_status: timing.into(),
                    resource: resource_strings.clone(),
                    resource_types: resource_types.clone(),
                    span_scope: scope_strings.clone(),
                    span_scope_types: scope_types.clone(),
                    span_int: typed.ints,
                    span_double: typed.doubles,
                    span_string: typed.strings,
                    span_bool: typed.bools,
                    span_other: typed.others,
                    payload_sha256,
                });
            }
        }
    }
    assert!(ids.next().is_none(), "admit assigns one event_id per span");
    out
}

fn next_id(ids: &mut std::slice::Iter<'_, Uuid>) -> String {
    ids.next()
        .expect("admit assigns one event_id per record")
        .to_string()
}

fn skip(ids: &mut std::slice::Iter<'_, Uuid>, records: usize) {
    for _ in 0..records {
        next_id(ids);
    }
}

/// A span's events as stored in its canonical envelope, in producer order, and
/// the producer `toudai.*` keys dropped from them. Their attributes follow the
/// record rules, so an event that repeats a key rejects its span instead of
/// being stored partly. Only the canonical payload keeps events; the serving
/// row has no column for them.
fn span_events(events: &[Event]) -> Result<(Json, usize), DuplicateKey> {
    let mut dropped = 0;
    let mut out = Vec::with_capacity(events.len());
    for event in events {
        let attributes = decode_attributes(&event.attributes, true)?;
        dropped += attributes.dropped_reserved;
        out.push(json!({
            "name": event.name,
            "time_ns": event.time_unix_nano.to_string(),
            "attributes": canonical_nodes(attributes.values.iter()),
        }));
    }
    Ok((Json::Array(out), dropped))
}

/// The resource namespace as stored: the producer's attributes without the
/// reserved namespace, plus the trusted `toudai.dataset`. `None` when an
/// attribute key or nested map key repeats.
fn stored_resource(resource: Option<&Resource>, dataset: &str) -> Option<BTreeMap<String, Value>> {
    let attributes: &[KeyValue] = resource.map_or(&[], |r| &r.attributes);
    let mut values = decode_attributes(attributes, true).ok()?.values;
    values.insert(DATASET_ATTR.into(), Value::Str(dataset.into()));
    Some(values)
}

/// A scope's attributes without the reserved namespace. Its name and version
/// are not stored, as in the Go exporter.
fn scope_attributes(scope: Option<&InstrumentationScope>) -> Option<Attributes> {
    let attributes: &[KeyValue] = scope.map_or(&[], |s| &s.attributes);
    decode_attributes(attributes, true).ok()
}

fn service_name(resource: &BTreeMap<String, Value>) -> String {
    resource
        .get("service.name")
        .map(Value::as_serving_string)
        .unwrap_or_default()
}

/// A string-valued Map column as key-sorted `(key, value)` pairs.
type StringMap = Vec<(String, String)>;

/// The string Map and companion type Map of a resource or scope namespace.
fn string_maps(values: &BTreeMap<String, Value>) -> (StringMap, StringMap) {
    values
        .iter()
        .map(|(k, v)| {
            (
                (k.clone(), v.as_serving_string()),
                (k.clone(), v.kind().into()),
            )
        })
        .unzip()
}

/// Record attributes routed to exactly one typed Map each. The kinds only the
/// canonical payload can hold, and explicit empty, keep their type name in
/// `*_other`.
struct TypedMaps {
    ints: Vec<(String, i64)>,
    doubles: Vec<(String, f64)>,
    strings: Vec<(String, String)>,
    bools: Vec<(String, bool)>,
    others: Vec<(String, String)>,
}

impl TypedMaps {
    fn new(values: &BTreeMap<String, Value>) -> TypedMaps {
        let mut maps = TypedMaps {
            ints: Vec::new(),
            doubles: Vec::new(),
            strings: Vec::new(),
            bools: Vec::new(),
            others: Vec::new(),
        };
        for (key, value) in values {
            let key = key.clone();
            match value {
                Value::Int(i) => maps.ints.push((key, *i)),
                Value::Double(d) => maps.doubles.push((key, *d)),
                Value::Str(s) => maps.strings.push((key, s.clone())),
                Value::Bool(b) => maps.bools.push((key, *b)),
                other => maps.others.push((key, other.kind().into())),
            }
        }
        maps
    }
}

/// Lowercase hex, or empty for an absent or all-zero ID.
fn hex_id(bytes: &[u8]) -> String {
    if bytes.iter().all(|b| *b == 0) {
        return String::new();
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// pdata's span kind names; an unknown kind is empty.
fn span_kind_name(kind: i32) -> &'static str {
    match kind {
        0 => "Unspecified",
        1 => "Internal",
        2 => "Server",
        3 => "Client",
        4 => "Producer",
        5 => "Consumer",
        _ => "",
    }
}

struct Canonical<'a> {
    /// `log`, `event`, or `span`.
    kind: &'static str,
    dataset: &'a str,
    event_id: &'a str,
    timestamp_ns: u64,
    observed_ns: u64,
    trace_id: &'a str,
    span_id: &'a str,
    body: &'a Value,
    resource: &'a BTreeMap<String, Value>,
    scope: &'a BTreeMap<String, Value>,
    record: &'a BTreeMap<String, Value>,
    /// `log` or `span`, and the envelope stored under it.
    envelope: (&'static str, Json),
}

/// The canonical payload ([`CANONICAL_SCHEMA`]) and its SHA-256 hex.
/// Identity is `{dataset, event_id}`, times are nanosecond strings, and keys
/// are sorted.
fn canonical_payload(c: Canonical<'_>) -> (String, String) {
    let mut doc = json!({
        "schema": CANONICAL_SCHEMA,
        "kind": c.kind,
        "identity": {"dataset": c.dataset, "event_id": c.event_id},
        "time": {
            "timestamp_ns": c.timestamp_ns.to_string(),
            "observed_ns": c.observed_ns.to_string(),
        },
        "body": c.body.canonical_node(),
        "resource": canonical_nodes(c.resource.iter()),
        "scope": canonical_nodes(c.scope.iter()),
        "record": canonical_nodes(c.record.iter()),
    });
    if !c.trace_id.is_empty() {
        doc["trace"] = json!({"trace_id": c.trace_id, "span_id": c.span_id});
    }
    doc[c.envelope.0] = c.envelope.1;
    let mut payload = Vec::new();
    let mut serializer = serde_json::Serializer::with_formatter(&mut payload, GoJson);
    doc.serialize(&mut serializer)
        .expect("a JSON value serializes");
    let payload = String::from_utf8(payload).expect("JSON output is UTF-8");
    let sha = Sha256::digest(payload.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    (payload, sha)
}

/// serde_json's compact output with Go `encoding/json`'s string escaping (HTML
/// escaping off): serde_json already escapes what Go does except U+2028 and
/// U+2029, which Go always escapes. Matching it keeps payload bytes, and so
/// `payload_sha256`, identical to the Go exporter's for the same content.
struct GoJson;

impl serde_json::ser::Formatter for GoJson {
    fn write_string_fragment<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> io::Result<()> {
        let mut rest = fragment;
        while let Some(at) = rest.find(['\u{2028}', '\u{2029}']) {
            writer.write_all(&rest.as_bytes()[..at])?;
            let c = rest[at..].chars().next().expect("found a character");
            write!(writer, "\\u{:04x}", c as u32)?;
            rest = &rest[at + c.len_utf8()..];
        }
        writer.write_all(rest.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each row struct's columns, in order, are the DDL's, so an insert names
    /// every column the table has and no other. The live-schema check in the
    /// ClickHouse e2e test also compares types.
    #[test]
    fn row_columns_match_the_ddl() {
        let cases = [
            ("002-canonical-events.sql", CanonicalRow::COLUMN_NAMES),
            ("003-serving-logs.sql", LogRow::COLUMN_NAMES),
            ("004-serving-spans.sql", SpanRow::COLUMN_NAMES),
        ];
        for (file, columns) in cases {
            let path = format!("{}/../clickhouse/schema/{file}", env!("CARGO_MANIFEST_DIR"));
            let ddl = std::fs::read_to_string(&path).unwrap();
            assert_eq!(ddl_columns(&ddl), columns, "{file}");
        }
    }

    /// The column names of the one `CREATE TABLE` in `ddl`: the first word of
    /// each line inside its column list, skipping comments.
    fn ddl_columns(ddl: &str) -> Vec<&str> {
        let body = &ddl[ddl.find("CREATE TABLE").expect("CREATE TABLE")..];
        let start = body.find("\n(").expect("column list") + 2;
        let end = body.find("\n)").expect("end of column list");
        body[start..end]
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with("--"))
            .map(|line| line.split_whitespace().next().unwrap())
            .collect()
    }

    #[test]
    fn go_json_escapes_line_separators_only() {
        let doc = json!({"b": "<&>\u{2028}x\u{2029}\u{1}\u{7f}\"\\é", "a": 1});
        let mut out = Vec::new();
        doc.serialize(&mut serde_json::Serializer::with_formatter(
            &mut out, GoJson,
        ))
        .unwrap();
        // Keys sorted, U+2028/U+2029 and control bytes escaped, `<&>`, DEL,
        // and non-ASCII raw.
        let want = "{\"a\":1,\"b\":\"<&>\\u2028x\\u2029\\u0001\u{7f}\\\"\\\\é\"}";
        assert_eq!(String::from_utf8(out).unwrap(), want);
    }

    /// Span events are kept in producer order in the canonical envelope, with
    /// the record rules for their attributes.
    #[test]
    fn span_events_are_kept_in_the_canonical_payload() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        fn kv(key: &str, v: i64) -> KeyValue {
            KeyValue {
                key: key.into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(v)),
                }),
                ..Default::default()
            }
        }
        fn event(name: &str, time: u64, attributes: Vec<KeyValue>) -> Event {
            Event {
                name: name.into(),
                time_unix_nano: time,
                attributes,
                ..Default::default()
            }
        }
        let int = |v: &str| json!({"t": "int", "v": v});
        // (events, stored `span.events` or None when the span is rejected,
        // reserved keys dropped)
        let cases = [
            (vec![], Some(json!([])), 0),
            (
                // Producer order, not time order, and a repeated name.
                vec![
                    event("b", 20, vec![kv("x", 1), kv("toudai.x", 2)]),
                    event("a", 10, vec![]),
                    event("b", 5, vec![kv("toudai.y", 3), kv("toudai.y", 3)]),
                ],
                Some(json!([
                    {"name": "b", "time_ns": "20", "attributes": {"x": int("1")}},
                    {"name": "a", "time_ns": "10", "attributes": {}},
                    {"name": "b", "time_ns": "5", "attributes": {}},
                ])),
                3,
            ),
            (
                vec![
                    event("ok", 1, vec![]),
                    event("dup", 2, vec![kv("x", 1), kv("x", 2)]),
                ],
                None,
                0,
            ),
        ];
        for (events, want, dropped) in cases {
            let request = ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    scope_spans: vec![ScopeSpans {
                        spans: vec![Span {
                            name: "s".into(),
                            events,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            let mapped = map_traces(&request, "d", &[Uuid::now_v7()], 0);
            match &want {
                Some(want) => {
                    let payload: Json = serde_json::from_str(&mapped.canonical[0].payload).unwrap();
                    assert_eq!(&payload["span"]["events"], want);
                    assert!(mapped.rejections.is_empty());
                }
                None => assert_eq!(
                    mapped.rejections,
                    [RecordRejection {
                        reason: "duplicate_or_invalid_span_event_attributes",
                        namespace: "span_event",
                        records: 1,
                    }]
                ),
            }
            assert_eq!(mapped.dropped_reserved, dropped, "{want:?}");
        }
    }

    #[test]
    fn ids_are_lowercase_hex_or_empty_when_zero() {
        let cases: [(&[u8], &str); 4] = [
            (&[], ""),
            (&[0; 8], ""),
            (&[0xab, 0, 0, 0, 0, 0, 0, 0x0f], "ab0000000000000f"),
            (&[0; 16], ""),
        ];
        for (bytes, want) in cases {
            assert_eq!(hex_id(bytes), want, "{bytes:?}");
        }
    }
}
