//! End to end: the `toudai-ingest` binary over OTLP/HTTP and OTLP/gRPC
//! against a real ClickHouse, with the product DDL applied verbatim to a
//! database that is not the default (Decision 0002 D1).
//!
//! The cases port the Go collector's receiver suite (`main_test.go` at
//! collector `f780574`). Where Go read a file exporter, this reads the tables:
//! success means stored (Decision 0003 D2), so an accepted request's rows must
//! be there the moment it is answered, and a refused request must have written
//! nothing to either table. Each case's log body is a unique marker that finds
//! its rows.

#[allow(dead_code)]
#[path = "../../tests/e2e/server.rs"]
mod server;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tonic::Code;
use uuid::Uuid;

use server::{ClickHouseServer, apply_ddl, execute, product_ddl};

const DATABASE: &str = "toudai_ingest_receivers_e2e";

// The Go suite's keys (`config/keys.example.yaml`), in the shared format.
const KEY_PROD: &str = "tk_example_prod_0123456789abcdef";
const KEY_MULTI: &str = "tk_example_multi_0123456789abcdef";
const KEY_STAGING: &str = "tk_example_staging_0123456789abcdef";
const KEY_ANY: &str = "tk_example_any_0123456789abcdef";
/// A read key: it is in the query service's file, not this one.
const KEY_READONLY: &str = "tk_example_readonly_0123456789abcd";
const KEYS: &str = "tk_example_prod_0123456789abcdef prod
tk_example_multi_0123456789abcdef prod staging
tk_example_staging_0123456789abcdef staging
tk_example_any_0123456789abcdef
";

struct Service {
    child: Child,
    http: String,
    grpc: String,
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `flush_interval` is short in most steps, so each request is not held for
/// the default second waiting for others to share its insert.
fn spawn(clickhouse: &ClickHouseServer, default_dataset: &str, flush_interval: &str) -> Service {
    let keys = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("ingest-write-keys.txt");
    std::fs::write(&keys, KEYS).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_toudai-ingest"))
        .env("TOUDAI_WRITE_KEYS_FILE", &keys)
        .env("TOUDAI_OTLP_GRPC_LISTEN", "127.0.0.1:0")
        .env("TOUDAI_OTLP_HTTP_LISTEN", "127.0.0.1:0")
        .env("TOUDAI_CLICKHOUSE_URL", &clickhouse.url)
        .env("TOUDAI_CLICKHOUSE_USER", server::USER)
        .env("TOUDAI_CLICKHOUSE_PASSWORD", server::PASSWORD)
        .env("TOUDAI_CLICKHOUSE_DATABASE", DATABASE)
        .env("TOUDAI_DEFAULT_DATASET", default_dataset)
        .env("TOUDAI_INGEST_FLUSH_INTERVAL", flush_interval)
        .stderr(Stdio::piped())
        .spawn()
        .expect("start toudai-ingest");
    let stderr = child.stderr.take().unwrap();
    // Guard the child before waiting on it, so a hang or early exit below
    // still reaps it.
    let mut service = Service {
        child,
        http: String::new(),
        grpc: String::new(),
    };
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            eprintln!("[toudai-ingest] {line}");
            if let Some(rest) = line.strip_prefix("toudai-ingest listening on grpc://")
                && let Some((grpc, http)) = rest.split_once(" and ")
            {
                let _ = sender.send((format!("http://{grpc}"), http.to_owned()));
            }
        }
    });
    (service.grpc, service.http) = receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("toudai-ingest did not report its addresses");
    service
}

/// The Go suite's request: one record under a `service.name` resource, with
/// extra resource and record attributes, and `marker` as its body.
fn log_body(marker: &str, resource: &[Value], record: &[Value]) -> Value {
    let mut resource_attributes =
        vec![json!({"key": "service.name", "value": {"stringValue": "svc"}})];
    resource_attributes.extend_from_slice(resource);
    json!({"resourceLogs": [{
        "resource": {"attributes": resource_attributes},
        "scopeLogs": [{"logRecords": [{
            "timeUnixNano": "1788246000123456789",
            "body": {"stringValue": marker},
            "attributes": record,
        }]}],
    }]})
}

fn s(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

async fn post(
    base: &str,
    path: &str,
    body: &Value,
    headers: &[(&str, &str)],
) -> (StatusCode, String) {
    let mut request = reqwest::Client::new()
        .post(format!("{base}{path}"))
        .json(body);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = request.send().await.expect("OTLP/HTTP request");
    (response.status(), response.text().await.unwrap())
}

async fn rows(server: &ClickHouseServer, sql: &str) -> Vec<Value> {
    execute(server, &format!("{sql} FORMAT JSONEachRow"), None)
        .await
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// The stored log rows with body `marker`, each with its canonical row's
/// dataset (null when it has none).
async fn stored(server: &ClickHouseServer, marker: &str) -> Vec<Value> {
    rows(
        server,
        &format!(
            "SELECT l.dataset AS dataset, l.event_id AS event_id, l.resource AS resource, \
             c.dataset AS canonical_dataset \
             FROM {DATABASE}.serving_logs AS l \
             LEFT JOIN {DATABASE}.canonical_events AS c ON c.event_id = l.event_id \
             WHERE l.body = '{marker}' ORDER BY l.event_id \
             SETTINGS join_use_nulls = 1"
        ),
    )
    .await
}

/// Canonical rows whose payload holds `marker`, so a refused request that
/// wrote only canonical rows is caught too.
async fn canonical_count(server: &ClickHouseServer, marker: &str) -> usize {
    rows(
        server,
        &format!("SELECT event_id FROM {DATABASE}.canonical_events WHERE position(payload, '{marker}') > 0"),
    )
    .await
    .len()
}

fn assert_uuid_v7(id: &Value, what: &str) {
    let id = Uuid::parse_str(id.as_str().unwrap()).unwrap();
    assert_eq!(id.get_version_num(), 7, "{what}: {id}");
}

#[tokio::test]
async fn ingests_over_http_and_grpc_into_a_real_clickhouse() {
    let Some(server) = server::start().await else {
        return;
    };
    for (_, sql) in product_ddl() {
        apply_ddl(&server, &sql, DATABASE).await;
    }
    {
        let service = spawn(&server, "", "50ms");
        http_requests_are_stored_or_refused(&server, &service).await;
        identical_events_get_distinct_ids(&server, &service).await;
        traces_are_stored(&server, &service).await;
        grpc_requests_are_stored_or_refused(&server, &service).await;
    }
    default_dataset_resolves_unnamed_requests(&server).await;
    concurrent_requests_share_one_insert(&server).await;
    no_key_reaches_storage(&server).await;
}

/// (name, headers, extra resource attributes, record attributes, status,
/// stored dataset)
type HttpCase<'a> = (
    &'a str,
    Vec<(&'a str, &'a str)>,
    Vec<Value>,
    Vec<Value>,
    u16,
    &'a str,
);

/// (name, metadata, code)
type GrpcCase<'a> = (&'a str, Vec<(&'a str, &'a str)>, Code);

/// `TestGatewayIngestion`: what is accepted is stored in a key-checked
/// dataset with a fresh UUIDv7, and everything else is refused with the right
/// status and stores nothing.
async fn http_requests_are_stored_or_refused(server: &ClickHouseServer, service: &Service) {
    let key = |k| ("X-Toudai-Key", k);
    let ds = |d| ("X-Toudai-Dataset", d);
    let bearer = format!("Bearer {KEY_PROD}");
    let cases: Vec<HttpCase> = vec![
        (
            "single-dataset key",
            vec![key(KEY_PROD)],
            vec![],
            vec![],
            200,
            "prod",
        ),
        (
            "other single-dataset key",
            vec![key(KEY_STAGING)],
            vec![],
            vec![],
            200,
            "staging",
        ),
        (
            "multi-dataset key names its dataset",
            vec![key(KEY_MULTI), ds("staging")],
            vec![],
            vec![],
            200,
            "staging",
        ),
        (
            "unknown dataset",
            vec![key(KEY_MULTI), ds("nope")],
            vec![],
            vec![],
            403,
            "",
        ),
        (
            "dataset of another key",
            vec![key(KEY_PROD), ds("staging")],
            vec![],
            vec![],
            403,
            "",
        ),
        (
            "multi-dataset key without a dataset",
            vec![key(KEY_MULTI)],
            vec![],
            vec![],
            400,
            "",
        ),
        (
            "key without a list names any dataset",
            vec![key(KEY_ANY), ds("canary")],
            vec![],
            vec![],
            200,
            "canary",
        ),
        (
            "key without a list and no default",
            vec![key(KEY_ANY)],
            vec![],
            vec![],
            400,
            "",
        ),
        (
            "invalid dataset name",
            vec![key(KEY_ANY), ds("a b")],
            vec![],
            vec![],
            400,
            "",
        ),
        (
            "two dataset headers",
            vec![key(KEY_MULTI), ds("prod"), ds("staging")],
            vec![],
            vec![],
            400,
            "",
        ),
        (
            "resource dataset equal",
            vec![key(KEY_PROD)],
            vec![s("toudai.dataset", "prod")],
            vec![],
            200,
            "prod",
        ),
        (
            "resource dataset outside the list",
            vec![key(KEY_PROD)],
            vec![s("toudai.dataset", "staging")],
            vec![],
            403,
            "",
        ),
        (
            "resource dataset permitted but not requested",
            vec![key(KEY_MULTI), ds("prod")],
            vec![s("toudai.dataset", "staging")],
            vec![],
            400,
            "",
        ),
        (
            "producer event_id",
            vec![key(KEY_PROD)],
            vec![],
            vec![s("toudai.event_id", "00000000-0000-7000-8000-000000000000")],
            400,
            "",
        ),
        (
            "retired workspace attribute",
            vec![key(KEY_PROD)],
            vec![s("toudai.workspace_id", "ws")],
            vec![],
            400,
            "",
        ),
        (
            "retired scope marker",
            vec![key(KEY_PROD)],
            vec![s("toudai.scope_source", "gateway")],
            vec![],
            400,
            "",
        ),
        ("missing key", vec![], vec![], vec![], 401, ""),
        (
            "unknown key",
            vec![key("tk_example_not_a_real_key_000")],
            vec![],
            vec![],
            401,
            "",
        ),
        ("read key", vec![key(KEY_READONLY)], vec![], vec![], 401, ""),
        (
            "two key headers",
            vec![key(KEY_PROD), key(KEY_PROD)],
            vec![],
            vec![],
            401,
            "",
        ),
        (
            "authorization header is not a key",
            vec![("Authorization", bearer.as_str())],
            vec![],
            vec![],
            401,
            "",
        ),
    ];
    for (i, (name, headers, resource, record, status, dataset)) in cases.into_iter().enumerate() {
        let marker = format!("http-case-{i}");
        let (got, body) = post(
            &service.http,
            "/v1/logs",
            &log_body(&marker, &resource, &record),
            &headers,
        )
        .await;
        assert_eq!(got.as_u16(), status, "{name}: {body}");
        // Error bodies go back to the producer; none may echo a credential.
        assert!(!body.contains("tk_example"), "{name}: {body}");
        let rows = stored(server, &marker).await;
        if status != 200 {
            assert!(rows.is_empty(), "{name}: stored {rows:?}");
            assert_eq!(canonical_count(server, &marker).await, 0, "{name}");
            continue;
        }
        assert_eq!(rows.len(), 1, "{name}: {rows:?}");
        let row = &rows[0];
        assert_eq!(row["dataset"], dataset, "{name}");
        assert_eq!(
            row["canonical_dataset"], dataset,
            "{name}: every serving row has its canonical row"
        );
        // The dataset is the only scope on the wire (Decision 0002 D2).
        let resource: BTreeMap<String, String> =
            serde_json::from_value(row["resource"].clone()).unwrap();
        let want = BTreeMap::from([
            ("service.name".into(), "svc".into()),
            ("toudai.dataset".into(), dataset.into()),
        ]);
        assert_eq!(resource, want, "{name}");
        assert_uuid_v7(&row["event_id"], name);
    }
}

/// `TestGatewayAssignsDistinctIDsToIdenticalEvents`: a retried request is a
/// new request, and two identical records in one request are two events.
async fn identical_events_get_distinct_ids(server: &ClickHouseServer, service: &Service) {
    let record = json!({"timeUnixNano": "1", "body": {"stringValue": "identical"}});
    let body = json!({"resourceLogs": [{"scopeLogs": [{"logRecords": [record.clone(), record]}]}]});
    for attempt in 1..=2 {
        let (status, text) = post(
            &service.http,
            "/v1/logs",
            &body,
            &[("X-Toudai-Key", KEY_PROD)],
        )
        .await;
        assert_eq!(status, StatusCode::OK, "attempt {attempt}: {text}");
    }
    let rows = stored(server, "identical").await;
    assert_eq!(rows.len(), 4, "no deduplication: {rows:?}");
    let ids: std::collections::BTreeSet<_> = rows
        .iter()
        .map(|r| r["event_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 4, "an event_id was assigned twice: {rows:?}");
}

/// `TestGatewayTraces`.
async fn traces_are_stored(server: &ClickHouseServer, service: &Service) {
    let body = json!({"resourceSpans": [{"scopeSpans": [{"spans": [{
        "traceId": "0102030405060708090a0b0c0d0e0f10",
        "spanId": "0102030405060708",
        "name": "e2e-span",
        "startTimeUnixNano": "1",
        "endTimeUnixNano": "2",
    }]}]}]});
    let (status, _) = post(&service.http, "/v1/traces", &body, &[]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, text) = post(
        &service.http,
        "/v1/traces",
        &body,
        &[("X-Toudai-Key", KEY_STAGING)],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let rows = rows(
        server,
        &format!(
            "SELECT s.dataset AS dataset, s.event_id AS event_id, s.trace_id AS trace_id, \
             c.signal AS signal FROM {DATABASE}.serving_spans AS s \
             JOIN {DATABASE}.canonical_events AS c ON c.event_id = s.event_id WHERE s.span_name = 'e2e-span'"
        ),
    )
    .await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["dataset"], "staging");
    assert_eq!(rows[0]["trace_id"], "0102030405060708090a0b0c0d0e0f10");
    assert_eq!(rows[0]["signal"], "spans");
    assert_uuid_v7(&rows[0]["event_id"], "span");
}

/// `TestGatewayGRPC`: gRPC lowercases metadata names, so this also covers
/// the case-insensitive header lookup.
async fn grpc_requests_are_stored_or_refused(server: &ClickHouseServer, service: &Service) {
    let mut client = LogsServiceClient::connect(service.grpc.clone())
        .await
        .unwrap();
    let cases: [GrpcCase; 5] = [
        ("accepted", vec![("x-toudai-key", KEY_PROD)], Code::Ok),
        ("missing key", vec![], Code::Unauthenticated),
        (
            "unknown key",
            vec![("x-toudai-key", "tk_example_not_a_real_key_000")],
            Code::Unauthenticated,
        ),
        (
            "unknown dataset",
            vec![("x-toudai-key", KEY_PROD), ("x-toudai-dataset", "nope")],
            Code::PermissionDenied,
        ),
        (
            "invalid dataset name",
            vec![("x-toudai-key", KEY_ANY), ("x-toudai-dataset", "a b")],
            Code::InvalidArgument,
        ),
    ];
    for (i, (name, metadata, code)) in cases.into_iter().enumerate() {
        let marker = format!("grpc-case-{i}");
        let mut request = tonic::Request::new(ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(marker.clone())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });
        for (key, value) in metadata {
            request.metadata_mut().insert(key, value.parse().unwrap());
        }
        request.set_timeout(Duration::from_secs(5));
        let got = match client.export(request).await {
            Ok(_) => Code::Ok,
            Err(status) => {
                assert!(!status.message().contains("tk_example"), "{name}");
                status.code()
            }
        };
        assert_eq!(got, code, "{name}");
        let rows = stored(server, &marker).await;
        if code == Code::Ok {
            assert_eq!(rows.len(), 1, "{name}: {rows:?}");
            assert_eq!(rows[0]["dataset"], "prod", "{name}");
            assert_eq!(rows[0]["canonical_dataset"], "prod", "{name}");
        } else {
            assert!(rows.is_empty(), "{name}: stored {rows:?}");
            assert_eq!(canonical_count(server, &marker).await, 0, "{name}");
        }
    }
}

/// `TestGatewayDefaultDataset`: a request the header and key leave
/// unresolved lands in the default, which is still held to the key's list.
async fn default_dataset_resolves_unnamed_requests(server: &ClickHouseServer) {
    let service = spawn(server, "staging", "50ms");
    let cases = [
        ("key without a list", KEY_ANY, "staging"),
        (
            "multi-dataset key that permits the default",
            KEY_MULTI,
            "staging",
        ),
        ("single-dataset key keeps its own dataset", KEY_PROD, "prod"),
    ];
    for (i, (name, key, dataset)) in cases.into_iter().enumerate() {
        let marker = format!("default-case-{i}");
        let (status, text) = post(
            &service.http,
            "/v1/logs",
            &log_body(&marker, &[], &[]),
            &[("X-Toudai-Key", key)],
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{name}: {text}");
        let rows = stored(server, &marker).await;
        assert_eq!(rows.len(), 1, "{name}");
        assert_eq!(rows[0]["dataset"], dataset, "{name}");
    }
}

/// Group commit (Decision 0003 D2): concurrent requests within one flush
/// interval are written by one canonical and one serving insert, and each is
/// answered only once its rows are stored. The counters come from the
/// binary's own `/metrics`, which needs no key.
async fn concurrent_requests_share_one_insert(server: &ClickHouseServer) {
    // Longer than the 1 s default, so the answer time shows the setting is
    // read, and long enough that all ten requests land in one batch.
    let service = spawn(server, "", "3s");
    let client = reqwest::Client::new();
    let health = client
        .get(format!("{}/healthz", service.http))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let started = std::time::Instant::now();
    let requests: Vec<_> = (0..10)
        .map(|i| {
            let body = log_body(&format!("group-case-{i}"), &[], &[]);
            let http = service.http.clone();
            tokio::spawn(async move {
                post(&http, "/v1/logs", &body, &[("X-Toudai-Key", KEY_PROD)]).await
            })
        })
        .collect();
    for (i, request) in requests.into_iter().enumerate() {
        let (status, text) = request.await.unwrap();
        assert_eq!(status, StatusCode::OK, "request {i}: {text}");
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_millis(2500),
            "request {i} after {waited:?}"
        );
        // Answered means stored: the row is readable now.
        let rows = stored(server, &format!("group-case-{i}")).await;
        assert_eq!(rows.len(), 1, "request {i}");
        assert_eq!(rows[0]["canonical_dataset"], "prod", "request {i}");
    }

    let metrics = client
        .get(format!("{}/metrics", service.http))
        .send()
        .await
        .unwrap();
    assert_eq!(metrics.status(), StatusCode::OK);
    let text = metrics.text().await.unwrap();
    let series = |line: &str| text.lines().any(|l| l == line);
    assert!(
        series("toudai_ingest_batches_total{signal=\"logs\"} 1"),
        "not one insert:\n{text}"
    );
    for destination in ["canonical", "serving"] {
        let line = format!(
            "toudai_ingest_records_total{{signal=\"logs\",destination=\"{destination}\"}} 10"
        );
        assert!(series(&line), "missing {line}:\n{text}");
    }
    assert!(
        series("toudai_ingest_requests_total{signal=\"logs\",outcome=\"accepted\"} 10"),
        "{text}"
    );
}

/// No credential reached any stored column or payload.
async fn no_key_reaches_storage(server: &ClickHouseServer) {
    let holding = |table: &str, text: &str| {
        format!(
            "SELECT toString(count()) AS n FROM {DATABASE}.{table} \
             WHERE position(toString(tuple(*)), '{text}') > 0"
        )
    };
    // The scan sees every column: `svc` is in each log row's resource map.
    let seen = rows(server, &holding("serving_logs", "svc")).await;
    assert_ne!(seen[0]["n"], "0", "the scan does not read map values");
    for table in ["serving_logs", "serving_spans", "canonical_events"] {
        let found = rows(server, &holding(table, "tk_example")).await;
        assert_eq!(found[0]["n"], "0", "a key reached {table}");
    }
}
