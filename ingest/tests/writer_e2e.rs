//! The writer against a real ClickHouse with the product DDL applied verbatim
//! to a database that is not the default (Decision 0002 D1).
//!
//! The server comes from the query service's harness (`tests/e2e/server.rs`):
//! Docker when its daemon answers, else the native `clickhouse` binary, and a
//! skip unless `TOUDAI_REQUIRE_CLICKHOUSE` is set. The `clickhouse` crate checks
//! every row struct against the live table schema before inserting, so a DDL
//! change the writer does not follow fails here.

#[allow(dead_code)]
#[path = "../../tests/e2e/server.rs"]
mod server;

use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use serde_json::{Map, Value, json};
use tokio::time::Instant;
use toudai::auth::DatasetAccess;
use toudai_ingest::admit::{admit_logs, admit_traces};
use toudai_ingest::auth::Scope;
use toudai_ingest::mapping::{map_logs, map_traces};
use toudai_ingest::metrics::Counters;
use toudai_ingest::writer::{Writer, WriterConfig};

use server::{ClickHouseServer, apply_ddl, execute, product_ddl};

const DATABASE: &str = "toudai_ingest_e2e";
/// Not the default 1, so the stamp is observable.
const EPOCH: u16 = 7;

fn logs_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest::decode(&include_bytes!("fixtures/mapping/logs.otlp.pb")[..]).unwrap()
}

fn traces_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest::decode(&include_bytes!("fixtures/mapping/traces.otlp.pb")[..])
        .unwrap()
}

fn scope(dataset: &str) -> Scope {
    Scope {
        dataset: dataset.into(),
        access: DatasetAccess::All,
    }
}

fn writer(server: &ClickHouseServer, counters: Arc<Counters>) -> Writer {
    let config = WriterConfig {
        url: server.url.clone(),
        user: Some(server::USER.into()),
        password: Some(server::PASSWORD.into()),
        database: DATABASE.into(),
    };
    Writer::new(config, counters).unwrap()
}

/// `JSONEachRow` rows of `sql`, with 64-bit integers as numbers and non-finite
/// doubles as strings.
async fn rows(server: &ClickHouseServer, sql: &str) -> Vec<Value> {
    let sql = format!(
        "{sql} SETTINGS output_format_json_quote_64bit_integers = 0, \
         output_format_json_quote_denormals = 1 FORMAT JSONEachRow"
    );
    execute(server, &sql, None)
        .await
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn count(server: &ClickHouseServer, sql: &str) -> u64 {
    rows(server, &format!("SELECT count() AS n FROM {sql}")).await[0]["n"]
        .as_u64()
        .unwrap()
}

fn object<V: Into<Value> + Clone>(pairs: &[(String, V)]) -> Value {
    Value::Object(
        pairs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().into()))
            .collect::<Map<_, _>>(),
    )
}

/// Doubles read back equal to those written, NaN and infinities included.
fn assert_doubles(what: &str, got: &Value, want: &[(String, f64)]) {
    let got = got.as_object().unwrap();
    assert_eq!(got.len(), want.len(), "{what}");
    for (key, value) in want {
        let read = match &got[key] {
            Value::String(text) => text.parse::<f64>().unwrap(),
            number => number.as_f64().unwrap(),
        };
        assert!(
            read == *value || (read.is_nan() && value.is_nan()),
            "{what} {key}: read {read:e}, wrote {value:e}"
        );
    }
}

#[tokio::test]
async fn writes_both_destinations_to_a_real_clickhouse() {
    let Some(server) = server::start().await else {
        return;
    };
    for (_, sql) in product_ddl() {
        apply_ddl(&server, &sql, DATABASE).await;
    }
    stores_every_column_as_mapped(&server).await;
    retries_only_the_destination_that_failed(&server).await;
    nothing_commits_behind_a_deadline_error(&server).await;
}

/// Every stored row reads back as mapped, and every serving row joins its
/// canonical row with an intact payload checksum.
async fn stores_every_column_as_mapped(server: &ClickHouseServer) {
    let counters = Arc::new(Counters::default());
    let writer = writer(server, counters.clone());
    let deadline = Instant::now() + Duration::from_secs(30);

    let request = logs_request();
    let ids = admit_logs(&request, &scope("fixture")).unwrap();
    let logs = map_logs(&request, "fixture", &ids, EPOCH);
    writer.write_logs(&logs, deadline).await.unwrap();
    let request = traces_request();
    let ids = admit_traces(&request, &scope("fixture")).unwrap();
    let spans = map_traces(&request, "fixture", &ids, EPOCH);
    writer.write_spans(&spans, deadline).await.unwrap();

    // UUIDv7s are assigned in request order, so event_id order is row order.
    let got = rows(
        server,
        &format!(
            "SELECT dataset, event_id, routing_epoch, kind, toUnixTimestamp64Nano(timestamp) AS ts, \
             toUnixTimestamp64Nano(observed_ts) AS observed, service_name, severity_text, \
             severity_number, body, trace_id, span_id, event_name, resource, resource_types, \
             scope, scope_types, record_int, record_double, record_string, record_bool, \
             record_other, payload_sha256 FROM {DATABASE}.serving_logs ORDER BY event_id"
        ),
    )
    .await;
    assert_eq!(got.len(), logs.serving.len());
    for (got, r) in got.iter().zip(&logs.serving) {
        let want = json!({
            "dataset": r.dataset, "event_id": r.event_id, "routing_epoch": EPOCH, "kind": "log",
            "ts": r.timestamp, "observed": r.observed_ts, "service_name": r.service_name,
            "severity_text": r.severity_text, "severity_number": r.severity_number,
            "body": r.body, "trace_id": r.trace_id, "span_id": r.span_id,
            "event_name": r.event_name, "resource": object(&r.resource),
            "resource_types": object(&r.resource_types), "scope": object(&r.scope),
            "scope_types": object(&r.scope_types), "record_int": object(&r.record_int),
            "record_string": object(&r.record_string), "record_bool": object(&r.record_bool),
            "record_other": object(&r.record_other), "payload_sha256": r.payload_sha256,
        });
        for (field, value) in want.as_object().unwrap() {
            assert_eq!(&got[field], value, "serving_logs {} {field}", r.event_id);
        }
        assert_doubles(&r.event_id, &got["record_double"], &r.record_double);
    }

    let got = rows(
        server,
        &format!(
            "SELECT event_id, routing_epoch, kind, toUnixTimestamp64Nano(timestamp) AS ts, \
             toUnixTimestamp64Nano(end_ts) AS end, service_name, trace_id, span_id, \
             parent_span_id, span_name, span_kind, status_code, duration_ns, \
             span_timing_status, resource, resource_types, span_scope, span_scope_types, \
             span_int, span_double, span_string, span_bool, span_other, payload_sha256 \
             FROM {DATABASE}.serving_spans ORDER BY event_id"
        ),
    )
    .await;
    assert_eq!(got.len(), spans.serving.len());
    for (got, r) in got.iter().zip(&spans.serving) {
        let want = json!({
            "event_id": r.event_id, "routing_epoch": EPOCH, "kind": "span", "ts": r.timestamp,
            "end": r.end_ts, "service_name": r.service_name, "trace_id": r.trace_id,
            "span_id": r.span_id, "parent_span_id": r.parent_span_id, "span_name": r.span_name,
            "span_kind": r.span_kind, "status_code": r.status_code,
            "duration_ns": r.duration_ns, "span_timing_status": r.span_timing_status,
            "resource": object(&r.resource), "resource_types": object(&r.resource_types),
            "span_scope": object(&r.span_scope), "span_scope_types": object(&r.span_scope_types),
            "span_int": object(&r.span_int), "span_string": object(&r.span_string),
            "span_bool": object(&r.span_bool), "span_other": object(&r.span_other),
            "payload_sha256": r.payload_sha256,
        });
        for (field, value) in want.as_object().unwrap() {
            assert_eq!(&got[field], value, "serving_spans {} {field}", r.event_id);
        }
        assert_doubles(&r.event_id, &got["span_double"], &r.span_double);
    }

    // Payload bytes are stored exactly, and both tables carry their checksum.
    let got = rows(
        server,
        &format!(
            "SELECT dataset, signal, event_id, toUnixTimestamp64Nano(timestamp) AS ts, \
             schema_version, payload, payload_sha256, \
             lower(hex(SHA256(payload))) = payload_sha256 AS intact \
             FROM {DATABASE}.canonical_events ORDER BY signal, event_id"
        ),
    )
    .await;
    let canonical: Vec<_> = logs.canonical.iter().chain(&spans.canonical).collect();
    assert_eq!(got.len(), canonical.len());
    for (got, c) in got.iter().zip(canonical) {
        let want = json!({
            "dataset": c.dataset, "signal": c.signal, "event_id": c.event_id, "ts": c.timestamp,
            "schema_version": c.schema_version, "payload": c.payload,
            "payload_sha256": c.payload_sha256, "intact": 1,
        });
        assert_eq!(got, &want, "canonical_events {}", c.event_id);
    }
    for (table, signal) in [("serving_logs", "logs"), ("serving_spans", "spans")] {
        let joined = count(
            server,
            &format!(
                "{DATABASE}.{table} AS s INNER JOIN {DATABASE}.canonical_events AS c \
                 ON s.dataset = c.dataset AND s.event_id = c.event_id \
                 WHERE c.signal = '{signal}' AND s.payload_sha256 = c.payload_sha256"
            ),
        )
        .await;
        let total = count(server, &format!("{DATABASE}.{table}")).await;
        assert_eq!(joined, total, "every {table} row joins its canonical row");
    }

    let labels = |destination| [("signal", "logs"), ("destination", destination)];
    assert_eq!(counters.get("records", &labels("canonical")), 11);
    assert_eq!(counters.get("records", &labels("serving")), 11);
    assert_eq!(counters.get("insert_failures", &labels("serving")), 0);
    let spans_serving = [("signal", "spans"), ("destination", "serving")];
    assert_eq!(counters.get("records", &spans_serving), 8);
}

/// With the serving table gone, the canonical insert commits once and only
/// the serving insert is retried; when the table returns, the write completes
/// with one row per record in each destination. The Go exporter's whole-batch
/// retry wrote the canonical rows again (Experiment 021, `ambiguous-serving`).
async fn retries_only_the_destination_that_failed(server: &ClickHouseServer) {
    let counters = Arc::new(Counters::default());
    let writer = writer(server, counters.clone());
    let request = logs_request();
    let ids = admit_logs(&request, &scope("fixture")).unwrap();
    let mapped = map_logs(&request, "fixture", &ids, EPOCH);
    let records = mapped.serving.len() as u64;
    // UUIDv7s are monotonic in the process, so this write's rows are exactly
    // those at or after its first event_id.
    let this_write = format!("event_id >= '{}'", ids[0]);

    let serving = format!("{DATABASE}.serving_logs");
    let away = format!("{DATABASE}.serving_logs_away");
    execute(server, &format!("RENAME TABLE {serving} TO {away}"), None).await;
    let write = tokio::spawn(async move {
        let deadline = Instant::now() + Duration::from_secs(30);
        writer.write_logs(&mapped, deadline).await
    });

    let serving_labels = [("signal", "logs"), ("destination", "serving")];
    let waited = Instant::now();
    while counters.get("insert_failures", &serving_labels) < 2 {
        assert!(
            waited.elapsed() < Duration::from_secs(20),
            "the serving insert was not retried"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let canonical_rows =
        format!("{DATABASE}.canonical_events WHERE signal = 'logs' AND {this_write}");
    assert_eq!(count(server, &canonical_rows).await, records);
    execute(server, &format!("RENAME TABLE {away} TO {serving}"), None).await;

    write.await.unwrap().unwrap();
    assert_eq!(
        count(server, &canonical_rows).await,
        records,
        "no canonical duplicates"
    );
    let serving_rows = format!("{serving} WHERE {this_write}");
    assert_eq!(count(server, &serving_rows).await, records);
    let canonical_labels = [("signal", "logs"), ("destination", "canonical")];
    assert_eq!(counters.get("insert_failures", &canonical_labels), 0);
    assert_eq!(counters.get("records", &canonical_labels), records);
    assert_eq!(counters.get("records", &serving_labels), records);
    assert_eq!(
        counters.get("partial_write_batches", &[("signal", "logs")]),
        0
    );
}

/// With the schema cached, an insert reaches ClickHouse in its first poll, so
/// one started at the deadline would commit after the caller was told it
/// failed. The writer must refuse to start it.
async fn nothing_commits_behind_a_deadline_error(server: &ClickHouseServer) {
    let writer = writer(server, Arc::new(Counters::default()));
    let request = logs_request();
    let warm_ids = admit_logs(&request, &scope("fixture")).unwrap();
    let warm = map_logs(&request, "fixture", &warm_ids, EPOCH);
    writer
        .write_logs(&warm, Instant::now() + Duration::from_secs(30))
        .await
        .unwrap();

    let ids = admit_logs(&request, &scope("fixture")).unwrap();
    let late = map_logs(&request, "fixture", &ids, EPOCH);
    let error = writer.write_logs(&late, Instant::now()).await.unwrap_err();
    assert!(error.retryable(), "{error}");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let this_write = format!("event_id >= '{}'", ids[0]);
    for table in ["canonical_events", "serving_logs"] {
        let rows = count(server, &format!("{DATABASE}.{table} WHERE {this_write}")).await;
        assert_eq!(
            rows, 0,
            "{table} holds rows from a write reported as failed"
        );
    }
}
