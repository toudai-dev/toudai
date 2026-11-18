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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
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
    recovers_from_a_column_type_change(&server).await;
    closes_the_connection_of_an_unanswered_insert(&server).await;
    closes_after_a_slow_final_chunk(&server).await;
    accepts_a_response_in_the_second_half(&server).await;
    cancels_a_connect_after_schema_lookup(&server).await;
}

static LAST_SERIALIZED: AtomicBool = AtomicBool::new(false);
static FINAL_PAUSED: AtomicBool = AtomicBool::new(false);
static PERMANENT_PAUSE: AtomicBool = AtomicBool::new(false);
static STALLED_PORT: AtomicUsize = AtomicUsize::new(0);
static RESPONSE_DELAY_MS: AtomicUsize = AtomicUsize::new(0);

#[derive(clickhouse::Row, serde::Serialize)]
struct ObservedRow {
    #[serde(serialize_with = "observe_last")]
    payload: String,
}

fn observe_last<S: serde::Serializer>(text: &String, serializer: S) -> Result<S::Ok, S::Error> {
    if text == "final" {
        LAST_SERIALIZED.store(true, Ordering::SeqCst);
    }
    serializer.serialize_str(text)
}

impl toudai_ingest::writer::ServingRow for ObservedRow {
    const LABEL: &'static str = "logs";
    const SIGNAL: toudai::query::Signal = toudai::query::Signal::Logs;
}

/// Two incompressible chunks larger than a TCP send buffer leave the final
/// short row waiting in end(), not write(). Stop reading at that exact phase,
/// then resume within the old send timeout but too late for its response
/// timeout to beat the outer backstop. This used to detach the response task.
async fn closes_after_a_slow_final_chunk(server: &ClickHouseServer) {
    let database = "toudai_ingest_slow_final";
    for (_, sql) in product_ddl() {
        apply_ddl(server, &sql, database).await;
    }
    execute(server, &format!("DROP TABLE {database}.serving_logs"), None).await;
    execute(
        server,
        &format!("CREATE TABLE {database}.serving_logs (payload String) ENGINE = Memory"),
        None,
    )
    .await;
    let proxy = WithholdingProxy::start(&server.url, true).await;
    let writer = Writer::new(
        WriterConfig {
            url: proxy.url.clone(),
            user: Some(server::USER.into()),
            password: Some(server::PASSWORD.into()),
            database: database.into(),
        },
        Arc::default(),
    )
    .unwrap();
    let batch = |serving| toudai_ingest::mapping::Mapped {
        serving,
        canonical: vec![],
        rejections: vec![],
        dropped_reserved: 0,
    };
    writer
        .write(
            &batch(vec![ObservedRow {
                payload: "warm".into(),
            }]),
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    let mut seed = 0x456789abcdef1234u64;
    let mut random = || {
        (0..8 * 1024 * 1024)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                (32 + seed % 95) as u8 as char
            })
            .collect::<String>()
    };
    let mapped = batch(vec![
        ObservedRow { payload: random() },
        ObservedRow { payload: random() },
        ObservedRow {
            payload: "final".into(),
        },
    ]);
    // On Linux verify that the client releases its descriptor while the proxy
    // NEVER resumes reading. EOF alone would require draining its data. Other
    // platforms fall back to checking EOF after the proxy resumes reading.
    {
        let permanent = cfg!(target_os = "linux");
        PERMANENT_PAUSE.store(permanent, Ordering::SeqCst);
        STALLED_PORT.store(0, Ordering::SeqCst);
        LAST_SERIALIZED.store(false, Ordering::SeqCst);
        FINAL_PAUSED.store(false, Ordering::SeqCst);
        proxy.withhold.store(true, Ordering::SeqCst);
        let started = Instant::now();
        let error = writer
            .write(&mapped, started + Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(error.retryable(), "{error}");
        assert!(started.elapsed() < Duration::from_millis(2500));
        assert!(
            FINAL_PAUSED.load(Ordering::SeqCst),
            "the final chunk never stalled"
        );
        let waited = Instant::now();
        if permanent {
            let port = STALLED_PORT.load(Ordering::SeqCst);
            assert_ne!(port, 0);
            while owns_tcp_socket(port) {
                assert!(
                    waited.elapsed() < Duration::from_secs(2),
                    "permanently stalled INSERT still owns its socket"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        } else {
            while proxy.open_withheld.load(Ordering::SeqCst) > 0 {
                assert!(
                    waited.elapsed() < Duration::from_secs(3),
                    "slow final chunk left its connection open"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    PERMANENT_PAUSE.store(false, Ordering::SeqCst);
}

fn owns_tcp_socket(port: usize) -> bool {
    let tcp = std::fs::read_to_string("/proc/self/net/tcp").unwrap();
    let port = format!("{port:04X}");
    let inodes: Vec<_> = tcp
        .lines()
        .skip(1)
        .filter_map(|line| {
            let columns: Vec<_> = line.split_whitespace().collect();
            (columns[1].rsplit(':').next() == Some(&port) && columns[9] != "0")
                .then(|| format!("socket:[{}]", columns[9]))
        })
        .collect();
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .flatten()
        .any(|fd| {
            std::fs::read_link(fd.path()).ok().is_some_and(|path| {
                inodes
                    .iter()
                    .any(|inode| path.as_os_str() == inode.as_str())
            })
        })
}

async fn accepts_a_response_in_the_second_half(server: &ClickHouseServer) {
    let proxy = WithholdingProxy::start(&server.url, false).await;
    let writer = Writer::new(
        WriterConfig {
            url: proxy.url.clone(),
            user: Some(server::USER.into()),
            password: Some(server::PASSWORD.into()),
            database: DATABASE.into(),
        },
        Arc::default(),
    )
    .unwrap();
    let request = logs_request();
    let ids = admit_logs(&request, &scope("fixture")).unwrap();
    let mapped = map_logs(&request, "late-response", &ids, EPOCH);
    RESPONSE_DELAY_MS.store(1500, Ordering::SeqCst);
    writer
        .write_logs(&mapped, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    for table in ["canonical_events", "serving_logs"] {
        assert_eq!(
            count(
                server,
                &format!("{DATABASE}.{table} WHERE dataset = 'late-response'")
            )
            .await,
            mapped.serving.len() as u64,
            "{table}: late response must not cause a retry"
        );
    }
}

/// Fill a listener's accept queue after forwarding the real DESCRIBE. The
/// subsequent INSERT then owns a SYN_SENT socket, but no dispatcher task yet.
/// This exercises the gap BEFORE Hyper calls the connection executor.
#[cfg(target_os = "linux")]
async fn cancels_a_connect_after_schema_lookup(server: &ClickHouseServer) {
    for cancel_early in [false, true] {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(1).unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = server.url.trim_start_matches("http://").to_owned();
        let proxy = tokio::spawn(async move {
            let (mut client, _) = listener.accept().await.unwrap();
            // Linux admits backlog + 1 established sockets. Leave them in the
            // queue so the writer's next TCP handshake cannot complete.
            let fillers = [
                TcpStream::connect(address).await.unwrap(),
                TcpStream::connect(address).await.unwrap(),
            ];
            let mut server = TcpStream::connect(upstream).await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(client.read_u8().await.unwrap());
            }
            assert!(
                !String::from_utf8_lossy(&header).contains("query=INSERT"),
                "first request must validate the schema"
            );
            // Force the old pooled client to reconnect too: the same test must
            // expose its leak, not bypass the stall through its schema socket.
            header.splice(
                header.len() - 2..header.len() - 2,
                b"Connection: close\r\n".iter().copied(),
            );
            server.write_all(&header).await.unwrap();
            let mut relays = tokio::task::JoinSet::new();
            relays.spawn(async move {
                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
            });
            std::future::pending::<()>().await;
            drop((listener, fillers, relays));
        });
        // Abort the listener and all relay/filler sockets even if an assertion
        // fails. The same guard is used by the other real ClickHouse proxies.
        struct Proxy(tokio::task::JoinHandle<()>);
        impl Drop for Proxy {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _proxy = Proxy(proxy);
        let writer = Writer::new(
            WriterConfig {
                url: format!("http://{address}"),
                user: Some(server::USER.into()),
                password: Some(server::PASSWORD.into()),
                database: DATABASE.into(),
            },
            Arc::default(),
        )
        .unwrap();
        let request = logs_request();
        let ids = admit_logs(&request, &scope("fixture")).unwrap();
        let mapped = map_logs(&request, "pending-connect", &ids, EPOCH);
        let started = Instant::now();
        let write = tokio::spawn(async move {
            writer
                .write_logs(&mapped, started + Duration::from_secs(2))
                .await
        });
        while pending_connections(address.port()).is_empty() {
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "no pending INSERT connect after schema lookup"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if cancel_early {
            write.abort();
            assert!(write.await.unwrap_err().is_cancelled());
        } else {
            assert!(write.await.unwrap().unwrap_err().retryable());
            assert!(started.elapsed() < Duration::from_millis(2500));
        }
        let stopped = Instant::now();
        while !pending_connections(address.port()).is_empty() {
            assert!(
                stopped.elapsed() < Duration::from_millis(500),
                "pending INSERT connect retained its socket after cancellation (early={cancel_early})"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn cancels_a_connect_after_schema_lookup(_: &ClickHouseServer) {}

#[cfg(target_os = "linux")]
fn pending_connections(port: u16) -> Vec<usize> {
    let tcp = std::fs::read_to_string("/proc/self/net/tcp").unwrap();
    let port = format!("{port:04X}");
    tcp.lines()
        .skip(1)
        .filter_map(|line| {
            let columns: Vec<_> = line.split_whitespace().collect();
            let local = usize::from_str_radix(columns[1].rsplit(':').next().unwrap(), 16).unwrap();
            (columns[3] == "02"
                && columns[2].rsplit(':').next() == Some(&port)
                && owns_tcp_socket(local))
            .then_some(local)
        })
        .collect()
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
            "dataset": r.dataset, "event_id": r.event_id, "routing_epoch": EPOCH, "kind": r.kind,
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
    // Warm the client's column cache, so the serving insert itself fails
    // once the table is gone, not only the column lookup before it.
    write_fixture_logs(&writer).await.unwrap();
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
    // The warm-up write stored one set of records too.
    assert_eq!(counters.get("records", &canonical_labels), 2 * records);
    assert_eq!(counters.get("records", &serving_labels), 2 * records);
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

/// The client caches each table's columns and sends them with every insert.
/// After a column changes type, the cached header no longer matches the table,
/// so the writer must fetch the columns again rather than fail every insert
/// until it restarts.
async fn recovers_from_a_column_type_change(server: &ClickHouseServer) {
    let writer = writer(server, Arc::new(Counters::default()));
    write_fixture_logs(&writer).await.unwrap();
    let serving = format!("{DATABASE}.serving_logs");
    let alter = |to: &str| format!("ALTER TABLE {serving} MODIFY COLUMN severity_text {to}");
    execute(server, &alter("String"), None).await;
    let changed = write_fixture_logs(&writer).await;
    execute(server, &alter("LowCardinality(String)"), None).await;
    changed.unwrap();
}

/// When ClickHouse commits an insert but never answers, the attempt fails at
/// the deadline and its connection is closed, rather than held by the
/// crate's response task until the process exits (Experiment 025).
async fn closes_the_connection_of_an_unanswered_insert(server: &ClickHouseServer) {
    let proxy = WithholdingProxy::start(&server.url, false).await;
    let config = WriterConfig {
        url: proxy.url.clone(),
        user: Some(server::USER.into()),
        password: Some(server::PASSWORD.into()),
        database: DATABASE.into(),
    };
    let writer = Writer::new(config, Arc::new(Counters::default())).unwrap();
    // Warm the column cache, so the next attempt's only request is the insert.
    write_fixture_logs(&writer).await.unwrap();
    proxy.withhold.store(true, Ordering::SeqCst);

    let request = logs_request();
    let ids = admit_logs(&request, &scope("fixture")).unwrap();
    let mapped = map_logs(&request, "fixture", &ids, EPOCH);
    let started = Instant::now();
    let error = writer
        .write_logs(&mapped, started + Duration::from_secs(1))
        .await
        .unwrap_err();
    assert!(
        matches!(error.source, clickhouse::error::Error::TimedOut),
        "{error}"
    );
    assert!(started.elapsed() < Duration::from_millis(1500), "{error}");
    assert!(
        proxy.withheld.load(Ordering::SeqCst) >= 1,
        "no response was withheld"
    );
    let waited = Instant::now();
    while proxy.open_withheld.load(Ordering::SeqCst) > 0 {
        assert!(
            waited.elapsed() < Duration::from_secs(2),
            "the connection of the unanswered insert is still open"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A TCP proxy in front of ClickHouse. Once `withhold` is set it still
/// forwards requests, so inserts commit, but drops ClickHouse's responses.
/// It counts connections that had a response withheld, and those of them the
/// client has not closed yet.
struct WithholdingProxy {
    url: String,
    withhold: Arc<AtomicBool>,
    withheld: Arc<AtomicUsize>,
    open_withheld: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for WithholdingProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl WithholdingProxy {
    async fn start(upstream_url: &str, slow_final: bool) -> WithholdingProxy {
        let upstream = upstream_url.trim_start_matches("http://").to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let withhold = Arc::default();
        let withheld = Arc::default();
        let open_withheld = Arc::default();
        let counters = (
            Arc::clone(&withhold),
            Arc::clone(&withheld),
            Arc::clone(&open_withheld),
        );
        let task = tokio::spawn(async move {
            let mut relays = tokio::task::JoinSet::new();
            while let Ok((client, _)) = listener.accept().await {
                let server = TcpStream::connect(&upstream).await.unwrap();
                let (withhold, withheld, open) = counters.clone();
                relays.spawn(relay(client, server, withhold, withheld, open, slow_final));
            }
        });
        WithholdingProxy {
            url,
            withhold,
            withheld,
            open_withheld,
            task,
        }
    }
}

async fn relay(
    mut client: TcpStream,
    mut server: TcpStream,
    withhold: Arc<AtomicBool>,
    withheld: Arc<AtomicUsize>,
    open_withheld: Arc<AtomicUsize>,
    slow_final: bool,
) {
    // The isolated client revalidates its schema. Never fault DESCRIBE:
    // the response under test must belong to an actual INSERT.
    let mut line = Vec::new();
    while !line.ends_with(b"\r\n") {
        let Ok(byte) = client.read_u8().await else {
            return;
        };
        line.push(byte);
    }
    let is_insert = String::from_utf8_lossy(&line).contains("query=INSERT");
    let is_insert = Arc::new(AtomicBool::new(is_insert));
    let client_port = client.peer_addr().unwrap().port();
    if server.write_all(&line).await.is_err() {
        return;
    }
    let (mut client_read, mut client_write) = client.into_split();
    let (mut server_read, mut server_write) = server.into_split();
    let this_withheld = Arc::new(AtomicBool::new(false));
    let mut tasks = tokio::task::JoinSet::new();
    {
        let (this_withheld, open_withheld) = (this_withheld.clone(), open_withheld.clone());
        let is_insert = is_insert.clone();
        tasks.spawn(async move {
            let mut buf = [0u8; 8192];
            loop {
                let n = match server_read.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                if is_insert.load(Ordering::SeqCst) {
                    let delay = RESPONSE_DELAY_MS.swap(0, Ordering::SeqCst);
                    if delay > 0 {
                        tokio::time::sleep(Duration::from_millis(delay as u64)).await;
                    }
                }
                if is_insert.load(Ordering::SeqCst) && withhold.load(Ordering::SeqCst) {
                    if !this_withheld.swap(true, Ordering::SeqCst) {
                        withheld.fetch_add(1, Ordering::SeqCst);
                        open_withheld.fetch_add(1, Ordering::SeqCst);
                    }
                } else if client_write.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
        })
    };
    // Requests flow until the client closes its side.
    let mut buf = [0; 8192];
    let mut paused = false;
    loop {
        if is_insert.load(Ordering::SeqCst)
            && slow_final
            && !paused
            && LAST_SERIALIZED.load(Ordering::SeqCst)
        {
            paused = true;
            FINAL_PAUSED.store(true, Ordering::SeqCst);
            STALLED_PORT.store(client_port as usize, Ordering::SeqCst);
            if !this_withheld.swap(true, Ordering::SeqCst) {
                open_withheld.fetch_add(1, Ordering::SeqCst);
            }
            let delay = if PERMANENT_PAUSE.load(Ordering::SeqCst) {
                60_000
            } else {
                1500
            };
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let n = match client_read.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if buf[..n]
            .windows(b"query=INSERT".len())
            .any(|w| w == b"query=INSERT")
        {
            is_insert.store(true, Ordering::SeqCst);
        }
        if server_write.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    if this_withheld.load(Ordering::SeqCst) {
        open_withheld.fetch_sub(1, Ordering::SeqCst);
    }
    tasks.abort_all();
}

async fn write_fixture_logs(writer: &Writer) -> Result<(), toudai_ingest::writer::WriteError> {
    let request = logs_request();
    let ids = admit_logs(&request, &scope("fixture")).unwrap();
    let mapped = map_logs(&request, "fixture", &ids, EPOCH);
    writer
        .write_logs(&mapped, Instant::now() + Duration::from_secs(5))
        .await
}
