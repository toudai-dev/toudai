//! Expected contents: for every fixture event, the serving values the storage
//! layout is expected to hold and the checksum of its canonical payload.
//!
//! Rows are computed from the emitter's own model (or, in `selfcheck`, from the
//! OTLP the receiver decoded), never from a downstream store. A row has the
//! shape of the Decision 0001 serving tables minus the two columns the gateway
//! owns (`event_id`, `routing_epoch`); numbers that JSON readers may round
//! (integers, doubles) are decimal strings in canonical form.

use std::collections::BTreeMap;

use serde_json::{Map, Value as Json, json};
use sha2::{Digest, Sha256};

use crate::model::{
    DATASET_ATTR, KEY_ATTR, LogRec, MetricRec, RESERVED_PREFIX, SpanRec, WORKSPACE_ATTR,
};
use crate::value::{Attrs, Val, double_text, nodes, timestamp_text};

pub const ROW_SCHEMA: &str = "toudai.fixture.expected/1";
/// The canonical payload is the Experiment 021 node encoding with the event
/// identity blanked (the gateway assigns `event_id`), extended with span events.
pub const PAYLOAD_SCHEMA: &str = "toudai.fixture.payload/1";

/// A decoded fixture event, from the model or from the wire.
pub enum Rec<'a> {
    Log(&'a LogRec),
    Span(&'a SpanRec),
    Metric(&'a MetricRec),
}

/// One expected row plus the join identity.
pub struct Row {
    pub tenant_id: String,
    pub dataset: String,
    pub signal: &'static str,
    pub fixture_key: String,
    pub json: Json,
}

impl Row {
    pub fn join_key(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.tenant_id, self.dataset, self.signal, self.fixture_key
        )
    }
}

pub fn row(rec: &Rec) -> Result<Row, String> {
    match rec {
        Rec::Log(r) => log_row(r),
        Rec::Span(r) => span_row(r),
        Rec::Metric(r) => metric_row(r),
    }
}

fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn find_str(attrs: &Attrs, key: &str) -> Result<String, String> {
    match attrs.iter().find(|(k, _)| k == key) {
        Some((_, Val::Str(s))) if !s.is_empty() => Ok(s.clone()),
        _ => Err(format!("missing trusted scope attribute {key}")),
    }
}

/// The post-policy view of one attribute collection.
struct Namespace {
    /// Typed values that survive policy (reserved keys removed).
    values: Attrs,
    dropped: Vec<String>,
}

fn apply_policy(attrs: &Attrs, keep_scope_keys: bool) -> Namespace {
    let mut values = Vec::new();
    let mut dropped = Vec::new();
    for (k, v) in attrs {
        let reserved = k.starts_with(RESERVED_PREFIX);
        // The trusted workspace and dataset stay addressable as resource
        // attributes; every other reserved key is dropped.
        let trusted = keep_scope_keys && (k == WORKSPACE_ATTR || k == DATASET_ATTR);
        if reserved && !trusted {
            dropped.push(k.clone());
        } else {
            values.push((k.clone(), v.clone()));
        }
    }
    dropped.sort();
    Namespace { values, dropped }
}

/// A string Map plus a companion type Map, the layout for resource and scope.
fn text_maps(attrs: &Attrs) -> Result<(Json, Json), String> {
    let mut text = Map::new();
    let mut types = Map::new();
    for (k, v) in attrs {
        if text.insert(k.clone(), json!(v.serving_text()?)).is_some() {
            return Err(format!("duplicate attribute key {k:?}"));
        }
        types.insert(k.clone(), json!(v.kind()));
    }
    Ok((Json::Object(text), Json::Object(types)))
}

/// Typed value Maps for record (or span) attributes: one native Map per kind,
/// and a key-only Map for the kinds the serving row cannot hold.
struct Typed {
    int: Map<String, Json>,
    double: Map<String, Json>,
    string: Map<String, Json>,
    bool_: Map<String, Json>,
    other: Map<String, Json>,
}

fn typed_maps(attrs: &Attrs) -> Typed {
    let mut t = Typed {
        int: Map::new(),
        double: Map::new(),
        string: Map::new(),
        bool_: Map::new(),
        other: Map::new(),
    };
    for (k, v) in attrs {
        match v {
            Val::Str(s) => t.string.insert(k.clone(), json!(s)),
            Val::Int(i) => t.int.insert(k.clone(), json!(i.to_string())),
            Val::Double(d) => t.double.insert(k.clone(), json!(double_text(*d))),
            Val::Bool(b) => t.bool_.insert(k.clone(), json!(b)),
            Val::Bytes(_) | Val::Array(_) | Val::Map(_) | Val::Empty => {
                t.other.insert(k.clone(), json!(v.kind()))
            }
        };
    }
    t
}

fn unsupported_keys(attrs: &Attrs) -> Vec<String> {
    let mut keys: Vec<String> = attrs
        .iter()
        .filter(|(_, v)| v.unsupported_for_query())
        .map(|(k, _)| k.clone())
        .collect();
    keys.sort();
    keys
}

/// Everything a row reports besides its identity.
struct Parts<'a> {
    class: &'a str,
    disposition: &'a str,
    reject_reason: Option<String>,
    dropped: Vec<String>,
    unsupported: Vec<String>,
    absent: &'a [String],
    event_names: Vec<String>,
    table: Option<&'a str>,
    serving: Option<Json>,
    payload: Option<String>,
}

type Ident<'a> = (&'a str, &'a str, &'static str, String);

fn finish(ident: Ident, parts: Parts) -> Row {
    let (tenant_id, dataset, signal, fixture_key) = ident;
    let Parts {
        class,
        disposition,
        reject_reason,
        dropped,
        unsupported,
        absent,
        event_names,
        table,
        serving,
        payload,
    } = parts;
    let checksum = payload.as_deref().map(sha256_hex);
    let mut serving = serving;
    if let (Some(Json::Object(map)), Some(sum)) = (serving.as_mut(), checksum.as_ref()) {
        map.insert("payload_sha256".into(), json!(sum));
    }
    let json = json!({
        "schema": ROW_SCHEMA,
        "tenant_id": tenant_id,
        "dataset": dataset,
        "signal": signal,
        "fixture_key": fixture_key,
        "class": class,
        "disposition": disposition,
        "reject_reason": reject_reason,
        "dropped_keys": dropped,
        "unsupported_keys": unsupported,
        "absent_keys": absent,
        "span_event_names": event_names,
        "table": table,
        "serving": serving,
        "payload": payload,
        "payload_sha256": checksum,
    });
    Row {
        tenant_id: tenant_id.to_string(),
        dataset: dataset.to_string(),
        signal,
        fixture_key,
        json,
    }
}

fn payload_text(doc: Json) -> String {
    // serde_json objects are sorted maps, so the text is canonical.
    doc.to_string()
}

fn scope_json(attrs: &Attrs) -> Result<Json, String> {
    Ok(Json::Object(nodes(attrs)?))
}

fn rejected(ident: Ident, class: &str, reason: String, absent: &[String], table: &str) -> Row {
    finish(
        ident,
        Parts {
            class,
            disposition: "reject",
            reject_reason: Some(reason),
            dropped: Vec::new(),
            unsupported: Vec::new(),
            absent,
            event_names: Vec::new(),
            table: Some(table),
            serving: None,
            payload: None,
        },
    )
}

fn log_row(r: &LogRec) -> Result<Row, String> {
    let tenant_id = find_str(&r.ctx.resource, WORKSPACE_ATTR)?;
    let dataset = find_str(&r.ctx.resource, DATASET_ATTR)?;
    let fixture_key = find_str(&r.attrs, KEY_ATTR)?;
    let ctx = (
        tenant_id.as_str(),
        dataset.as_str(),
        "logs",
        fixture_key.clone(),
    );

    let resource = apply_policy(&r.ctx.resource, true);
    let scope = apply_policy(&r.ctx.scope_attrs, false);
    let record = apply_policy(&r.attrs, false);
    let dropped: Vec<String> = record
        .dropped
        .iter()
        .chain(&scope.dropped)
        .cloned()
        .collect();
    let kind = if r.event_name.is_empty() {
        "log"
    } else {
        "event"
    };

    let canonical = (|| -> Result<(Json, String), String> {
        let (res_text, res_types) = text_maps(&resource.values)?;
        let (scope_text, scope_types) = text_maps(&scope.values)?;
        let body = r.body.clone().unwrap_or(Val::Empty);
        let served_ns = if r.timestamp_ns == 0 {
            r.observed_ns
        } else {
            r.timestamp_ns
        };
        let typed = typed_maps(&record.values);
        let mut doc = json!({
            "schema": PAYLOAD_SCHEMA,
            "kind": kind,
            "identity": {"tenant_id": tenant_id, "dataset": dataset, "event_id": ""},
            "time": {
                "timestamp_ns": r.timestamp_ns.to_string(),
                "observed_ns": r.observed_ns.to_string(),
            },
            "body": body.node()?,
            "resource": nodes(&resource.values)?,
            "scope": scope_json(&scope.values)?,
            "record": nodes(&record.values)?,
            "log": {
                "severity_text": r.severity_text,
                "severity_number": r.severity_number,
                "event_name": r.event_name,
            },
        });
        if let Some((trace_id, span_id)) = &r.trace {
            doc["trace"] = json!({"trace_id": hex(trace_id), "span_id": hex(span_id)});
        }
        let serving = json!({
            "tenant_id": tenant_id,
            "dataset": dataset,
            "kind": kind,
            "timestamp": timestamp_text(served_ns),
            "observed_ts": timestamp_text(r.observed_ns),
            "service_name": service_name(&resource.values),
            "severity_text": r.severity_text,
            "severity_number": r.severity_number,
            "body": body_text(&r.body)?,
            "trace_id": r.trace.map(|t| hex(&t.0)).unwrap_or_default(),
            "span_id": r.trace.map(|t| hex(&t.1)).unwrap_or_default(),
            "event_name": r.event_name,
            "resource": res_text,
            "resource_types": res_types,
            "scope": scope_text,
            "scope_types": scope_types,
            "record_int": typed.int,
            "record_double": typed.double,
            "record_string": typed.string,
            "record_bool": typed.bool_,
            "record_other": typed.other,
        });
        Ok((serving, payload_text(doc)))
    })();

    match canonical {
        Ok((serving, payload)) => Ok(finish(
            ctx,
            Parts {
                class: r.notes.class,
                disposition: "serve",
                reject_reason: None,
                dropped,
                unsupported: unsupported_keys(&record.values),
                absent: &r.notes.absent_keys,
                event_names: Vec::new(),
                table: Some("serving_logs"),
                serving: Some(serving),
                payload: Some(payload),
            },
        )),
        Err(reason) => Ok(rejected(
            ctx,
            r.notes.class,
            reason,
            &r.notes.absent_keys,
            "serving_logs",
        )),
    }
}

fn service_name(resource: &Attrs) -> String {
    resource
        .iter()
        .find(|(k, _)| k == "service.name")
        .and_then(|(_, v)| v.serving_text().ok())
        .unwrap_or_default()
}

fn body_text(body: &Option<Val>) -> Result<String, String> {
    match body {
        None => Ok(String::new()),
        Some(v) => v.serving_text(),
    }
}

/// Derived span timing: `(duration_ns, status)`. OTLP has no timestamp
/// presence, so 0 is "unset"; missing timing takes precedence over reversed,
/// and an unavailable duration is NULL, never zero or an unsigned underflow.
pub fn span_timing(start: u64, end: u64) -> (Option<i64>, &'static str) {
    if start == 0 || end == 0 {
        (None, "missing")
    } else if end < start {
        (None, "end_before_start")
    } else {
        (Some((end - start) as i64), "ok")
    }
}

fn span_row(r: &SpanRec) -> Result<Row, String> {
    let tenant_id = find_str(&r.ctx.resource, WORKSPACE_ATTR)?;
    let dataset = find_str(&r.ctx.resource, DATASET_ATTR)?;
    let fixture_key = find_str(&r.attrs, KEY_ATTR)?;
    let ctx = (
        tenant_id.as_str(),
        dataset.as_str(),
        "spans",
        fixture_key.clone(),
    );

    let resource = apply_policy(&r.ctx.resource, true);
    let scope = apply_policy(&r.ctx.scope_attrs, false);
    let record = apply_policy(&r.attrs, false);
    let dropped: Vec<String> = record
        .dropped
        .iter()
        .chain(&scope.dropped)
        .cloned()
        .collect();
    let (duration, timing) = span_timing(r.start_ns, r.end_ns);
    let parent = r.parent_span_id.map(|p| hex(&p)).unwrap_or_default();

    let canonical = (|| -> Result<(Json, String), String> {
        let (res_text, res_types) = text_maps(&resource.values)?;
        let (scope_text, scope_types) = text_maps(&scope.values)?;
        let typed = typed_maps(&record.values);
        let mut events = Vec::new();
        for e in &r.events {
            events.push(json!({
                "name": e.name,
                "time_ns": e.time_ns.to_string(),
                "attributes": nodes(&e.attrs)?,
            }));
        }
        let mut span = json!({
            "name": r.name,
            "parent_span_id": parent,
            "kind": r.kind.name(),
            "status_code": r.status.code().to_string(),
            "start_ns": r.start_ns.to_string(),
            "end_ns": r.end_ns.to_string(),
            "events": events,
        });
        if let Some(d) = duration {
            span["duration_ns"] = json!(d.to_string());
        }
        let doc = json!({
            "schema": PAYLOAD_SCHEMA,
            "kind": "span",
            "identity": {"tenant_id": tenant_id, "dataset": dataset, "event_id": ""},
            "time": {
                "timestamp_ns": r.start_ns.to_string(),
                "observed_ns": r.start_ns.to_string(),
            },
            "body": Val::Empty.node()?,
            "resource": nodes(&resource.values)?,
            "scope": scope_json(&scope.values)?,
            "record": nodes(&record.values)?,
            "trace": {"trace_id": hex(&r.trace_id), "span_id": hex(&r.span_id)},
            "span": span,
        });
        let serving = json!({
            "tenant_id": tenant_id,
            "dataset": dataset,
            "kind": "span",
            "timestamp": timestamp_text(r.start_ns),
            "end_ts": timestamp_text(r.end_ns),
            "service_name": service_name(&resource.values),
            "trace_id": hex(&r.trace_id),
            "span_id": hex(&r.span_id),
            "parent_span_id": parent,
            "span_name": r.name,
            "span_kind": r.kind.name(),
            "status_code": r.status.code(),
            "duration_ns": duration.map(|d| d.to_string()),
            "span_timing_status": timing,
            "resource": res_text,
            "resource_types": res_types,
            "span_scope": scope_text,
            "span_scope_types": scope_types,
            "span_int": typed.int,
            "span_double": typed.double,
            "span_string": typed.string,
            "span_bool": typed.bool_,
            "span_other": typed.other,
        });
        Ok((serving, payload_text(doc)))
    })();

    match canonical {
        Ok((serving, payload)) => Ok(finish(
            ctx,
            Parts {
                class: r.notes.class,
                disposition: "serve",
                reject_reason: None,
                dropped,
                unsupported: unsupported_keys(&record.values),
                absent: &r.notes.absent_keys,
                event_names: r.events.iter().map(|e| e.name.clone()).collect(),
                table: Some("serving_spans"),
                serving: Some(serving),
                payload: Some(payload),
            },
        )),
        Err(reason) => Ok(rejected(
            ctx,
            r.notes.class,
            reason,
            &r.notes.absent_keys,
            "serving_spans",
        )),
    }
}

/// Native metrics keep their own signal semantics: there is no events serving
/// row, only the canonical sample. Timestamps are assigned by the SDK at
/// collection time, so they are not part of the expected contents.
fn metric_row(r: &MetricRec) -> Result<Row, String> {
    let tenant_id = find_str(&r.ctx.resource, WORKSPACE_ATTR)?;
    let dataset = find_str(&r.ctx.resource, DATASET_ATTR)?;
    let fixture_key = find_str(&r.attrs, KEY_ATTR)?;
    let ctx = (
        tenant_id.as_str(),
        dataset.as_str(),
        "metrics",
        fixture_key.clone(),
    );
    let resource = apply_policy(&r.ctx.resource, true);
    let doc = json!({
        "schema": PAYLOAD_SCHEMA,
        "kind": "metric",
        "identity": {"tenant_id": tenant_id, "dataset": dataset},
        "resource": nodes(&resource.values)?,
        "scope": scope_json(&r.ctx.scope_attrs)?,
        "metric": {
            "name": r.name,
            "unit": r.unit,
            "description": r.description,
            "instrument": "counter",
            "monotonic": true,
            "value": Val::Int(r.value as i64).node()?,
            "attributes": nodes(&r.attrs)?,
        },
    });
    Ok(finish(
        ctx,
        Parts {
            class: "native-metric",
            disposition: "native-metric",
            reject_reason: None,
            dropped: Vec::new(),
            unsupported: Vec::new(),
            absent: &[],
            event_names: Vec::new(),
            table: None,
            serving: None,
            payload: Some(payload_text(doc)),
        },
    ))
}

/// All rows of a fixture, ordered by tenant (model order), then logs, spans,
/// metrics, each in definition order. Two rows with one join key are a fixture
/// bug and an error.
pub fn rows(logs: &[LogRec], spans: &[SpanRec], metrics: &[MetricRec]) -> Result<Vec<Row>, String> {
    let mut out = Vec::new();
    for r in logs {
        out.push(row(&Rec::Log(r))?);
    }
    for r in spans {
        out.push(row(&Rec::Span(r))?);
    }
    for r in metrics {
        out.push(row(&Rec::Metric(r))?);
    }
    let mut seen = BTreeMap::new();
    for row in &out {
        if seen.insert(row.join_key(), ()).is_some() {
            return Err(format!("duplicate fixture key {}", row.join_key()));
        }
    }
    Ok(out)
}

pub fn to_jsonl(rows: &[Row]) -> String {
    let mut text = String::new();
    for row in rows {
        text.push_str(&row.json.to_string());
        text.push('\n');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model;

    fn fixture_rows() -> Vec<Row> {
        let f = model::build(1);
        rows(&f.logs, &f.spans, &f.metrics).unwrap()
    }

    fn find<'a>(rows: &'a [Row], tenant_prefix: &str, key: &str) -> &'a Row {
        rows.iter()
            .find(|r| r.tenant_id.starts_with(tenant_prefix) && r.fixture_key == key)
            .unwrap_or_else(|| panic!("no row {key}"))
    }

    #[test]
    fn span_timing_table() {
        let cases: &[(u64, u64, Option<i64>, &str)] = &[
            (100, 250, Some(150), "ok"),
            (100, 100, Some(0), "ok"),
            (0, 250, None, "missing"),
            (100, 0, None, "missing"),
            (0, 0, None, "missing"),
            (250, 100, None, "end_before_start"),
            (1, u64::MAX / 2, Some((u64::MAX / 2 - 1) as i64), "ok"),
        ];
        for (start, end, duration, status) in cases {
            assert_eq!(
                span_timing(*start, *end),
                (*duration, *status),
                "{start} {end}"
            );
        }
    }

    /// (tenant prefix, fixture key, disposition, JSON pointer checks)
    type Case<'a> = (&'a str, &'a str, &'a str, &'a [(&'a str, &'a str)]);

    #[test]
    fn expected_rows_per_class() {
        let rows = fixture_rows();
        // (tenant prefix, key, expected disposition, serving checks as JSON pointers)
        let cases: &[Case] = &[
            (
                "1111",
                "log.info",
                "serve",
                &[
                    ("/serving/kind", "log"),
                    ("/serving/record_int/cart.items", "3"),
                    ("/serving/record_double/cart.total", "129.95"),
                    ("/serving/record_string/http.route", "/checkout"),
                    ("/serving/resource_types/fixture.replica", "int"),
                    (
                        "/serving/resource/toudai.workspace_id",
                        "11111111-1111-4111-8111-111111111111",
                    ),
                ],
            ),
            (
                "2222",
                "log.info",
                "serve",
                &[
                    ("/serving/record_double/cart.total", "59.5"),
                    ("/serving/resource/toudai.dataset", "checkout"),
                ],
            ),
            (
                "1111",
                "event.checkout.completed",
                "serve",
                &[
                    ("/serving/kind", "event"),
                    ("/serving/event_name", "checkout.completed"),
                    ("/serving/body", ""),
                ],
            ),
            (
                "1111",
                "log.spoofed-scope",
                "serve",
                &[("/serving/tenant_id", "11111111-1111-4111-8111-111111111111")],
            ),
            (
                "1111",
                "invalid.bytes",
                "serve",
                &[("/serving/record_other/payload.raw", "bytes")],
            ),
            (
                "1111",
                "invalid.array",
                "serve",
                &[("/serving/record_other/payload.list", "array")],
            ),
            (
                "1111",
                "invalid.map",
                "serve",
                &[("/serving/record_other/payload.map", "map")],
            ),
            (
                "1111",
                "invalid.nonfinite",
                "serve",
                &[
                    ("/serving/record_double/double.nan", "nan"),
                    ("/serving/record_double/double.posinf", "inf"),
                    ("/serving/record_double/double.neginf", "-inf"),
                    ("/serving/record_double/double.negzero", "-0.0"),
                ],
            ),
            (
                "1111",
                "invalid.bigint",
                "serve",
                &[
                    ("/serving/record_int/int.2p53plus1", "9007199254740993"),
                    ("/serving/record_int/int.max", "9223372036854775807"),
                    ("/serving/record_int/int.min", "-9223372036854775808"),
                    (
                        "/serving/record_double/double.2p53",
                        "9.007199254740992e+15",
                    ),
                ],
            ),
            (
                "1111",
                "invalid.empty-vs-missing",
                "serve",
                &[
                    ("/serving/record_string/empty.string", ""),
                    ("/serving/record_other/empty.bytes", "bytes"),
                    ("/serving/record_other/empty.array", "array"),
                    ("/serving/record_other/empty.map", "map"),
                    ("/serving/record_int/zero.int", "0"),
                    ("/serving/record_double/zero.float", "0.0"),
                ],
            ),
            ("1111", "invalid.duplicate-key", "reject", &[]),
            (
                "1111",
                "span.root",
                "serve",
                &[
                    ("/serving/duration_ns", "1500000"),
                    ("/serving/span_timing_status", "ok"),
                    ("/serving/status_code", "1"),
                    ("/serving/span_kind", "Server"),
                ],
            ),
            (
                "2222",
                "span.root",
                "serve",
                &[("/serving/status_code", "2")],
            ),
            (
                "1111",
                "span.zero-duration",
                "serve",
                &[
                    ("/serving/duration_ns", "0"),
                    ("/serving/span_timing_status", "ok"),
                ],
            ),
            (
                "1111",
                "span.missing-start",
                "serve",
                &[
                    ("/serving/span_timing_status", "missing"),
                    ("/serving/timestamp", "1970-01-01 00:00:00.000000000"),
                ],
            ),
            (
                "1111",
                "span.missing-end",
                "serve",
                &[("/serving/span_timing_status", "missing")],
            ),
            (
                "1111",
                "span.missing-both",
                "serve",
                &[("/serving/span_timing_status", "missing")],
            ),
            (
                "1111",
                "span.reversed",
                "serve",
                &[("/serving/span_timing_status", "end_before_start")],
            ),
        ];
        for (tenant, key, disposition, checks) in cases {
            let row = find(&rows, tenant, key);
            assert_eq!(row.json["disposition"], *disposition, "{tenant} {key}");
            for (pointer, want) in *checks {
                let got = row
                    .json
                    .pointer(pointer)
                    .unwrap_or_else(|| panic!("{key} {pointer}"));
                let got = match got {
                    Json::String(s) => s.clone(),
                    other => other.to_string(),
                };
                assert_eq!(got, *want, "{tenant} {key} {pointer}");
            }
        }
    }

    #[test]
    fn null_duration_and_rejected_rows_have_no_values() {
        let rows = fixture_rows();
        let reversed = find(&rows, "1111", "span.reversed");
        assert!(reversed.json["serving"]["duration_ns"].is_null());
        assert!(
            !reversed.json["payload"]
                .as_str()
                .unwrap()
                .contains("duration_ns")
        );
        let dup = find(&rows, "1111", "invalid.duplicate-key");
        assert!(dup.json["serving"].is_null());
        assert!(dup.json["payload"].is_null());
        assert!(dup.json["payload_sha256"].is_null());
        assert!(
            dup.json["reject_reason"]
                .as_str()
                .unwrap()
                .contains("dup.key")
        );
    }

    #[test]
    fn spoofed_reserved_keys_are_dropped_everywhere() {
        let rows = fixture_rows();
        let spoof = find(&rows, "1111", "log.spoofed-scope");
        assert_eq!(
            spoof.json["dropped_keys"],
            json!(["toudai.dataset", "toudai.workspace_id"])
        );
        let payload = spoof.json["payload"].as_str().unwrap();
        // The only toudai.* keys left are the trusted resource ones.
        assert_eq!(payload.matches("2222").count(), 0);
        assert_eq!(payload.matches("other-dataset").count(), 0);
        assert!(
            spoof.json["serving"]["record_string"]
                .get("toudai.workspace_id")
                .is_none()
        );
    }

    #[test]
    fn colliding_dataset_and_identical_event_differ_only_by_scope() {
        let rows = fixture_rows();
        let a = find(&rows, "1111", "log.collision");
        let b = find(&rows, "2222", "log.collision");
        assert_eq!(a.dataset, b.dataset);
        assert_ne!(a.tenant_id, b.tenant_id);
        let strip = |r: &Row| {
            let mut v = r.json["serving"].clone();
            for key in ["tenant_id", "resource", "payload_sha256"] {
                v.as_object_mut().unwrap().remove(key);
            }
            v
        };
        assert_eq!(strip(a), strip(b));
        assert_ne!(a.json["payload_sha256"], b.json["payload_sha256"]);
    }

    #[test]
    fn payload_checksum_is_sha256_of_payload_text() {
        for row in fixture_rows() {
            if let Some(payload) = row.json["payload"].as_str() {
                assert_eq!(
                    row.json["payload_sha256"],
                    sha256_hex(payload),
                    "{}",
                    row.fixture_key
                );
            }
        }
        // Known SHA-256 vector.
        assert_eq!(
            sha256_hex("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn span_events_are_in_the_payload() {
        let rows = fixture_rows();
        let root = find(&rows, "1111", "span.root");
        assert_eq!(
            root.json["span_event_names"],
            json!(["cart.loaded", "exception"])
        );
        let payload: Json = serde_json::from_str(root.json["payload"].as_str().unwrap()).unwrap();
        let events = payload["span"]["events"].as_array().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[1]["attributes"]["exception.type"]["v"],
            "TimeoutError"
        );
    }

    #[test]
    fn metric_row_has_no_serving_row_and_no_timestamps() {
        let rows = fixture_rows();
        let metrics: Vec<_> = rows.iter().filter(|r| r.signal == "metrics").collect();
        assert_eq!(metrics.len(), 1);
        assert!(metrics[0].json["serving"].is_null());
        assert!(metrics[0].json["table"].is_null());
        assert!(!metrics[0].json["payload"].as_str().unwrap().contains("_ns"));
    }
}
