//! End to end from the fixture emitter to the query service: the RFD 0001
//! fixture (Experiment 023), sent by the OpenTelemetry Rust SDK over OTLP/gRPC
//! and OTLP/HTTP, through the `toudai-ingest` binary into a real ClickHouse,
//! and read back through the query service's exact-event lookup.
//!
//! The expected contents are the emitter's own model, recorded in Experiment
//! 023 and kept in `fixture-emitter/expected.jsonl`, so nothing here is derived
//! from ingest's mapping.
//! Each transport writes its own database, which is not the default
//! (Decision 0002 D1).

#[allow(dead_code)]
#[path = "../../tests/e2e/server.rs"]
mod server;
mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use reqwest::StatusCode;
use serde_json::{Map, Value};
use toudai::api::{AppState, router};
use toudai::auth::Keys;
use toudai::clickhouse::{ClickHouse, ClickHouseConfig};
use toudai::query::ServingTables;
use uuid::Uuid;

use server::{ClickHouseServer, apply_ddl, execute, product_ddl};

const EXPECTED: &str = include_str!("../../fixture-emitter/expected.jsonl");

/// The emitter's datasets: one write key for both, as its README requires.
const WRITE_KEY: &str = "tk_fixture_writer_0123456789abcdef";
const WRITE_KEYS: &str = "tk_fixture_writer_0123456789abcdef fixture fixture-b\n";
const READ_KEY: &str = "tk_fixture_reader_0123456789abcdef";
const READ_KEY_B: &str = "tk_fixture_reader_b_0123456789abcd";
const READ_KEYS: &str = "tk_fixture_reader_0123456789abcdef fixture fixture-b
tk_fixture_reader_b_0123456789abcd fixture-b
";

#[tokio::test]
async fn fixture_emitter_through_ingest_to_the_query_service() {
    let Some(server) = server::start().await else {
        return;
    };
    let emitter = build_emitter();
    let expected: Vec<Value> = EXPECTED
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for (protocol, database) in [
        ("grpc", "toudai_ingest_fixture_grpc"),
        ("http", "toudai_ingest_fixture_http"),
    ] {
        for (_, sql) in product_ddl() {
            apply_ddl(&server, &sql, database).await;
        }
        let mut service = support::spawn(
            &server,
            WRITE_KEYS,
            database,
            &[("TOUDAI_INGEST_FLUSH_INTERVAL", "50ms")],
        );
        let endpoint = match protocol {
            "grpc" => &service.grpc,
            _ => &service.http,
        };
        emit(&emitter, protocol, endpoint);

        let stored = Stored::read(&server, database).await;
        rows_match_the_fixture(protocol, &expected, &stored);
        serving_rows_join_their_canonical_rows(&server, database, &stored).await;
        reserved_and_rejected_records_are_counted(&service).await;
        events_are_found_by_exact_lookup(&server, database, &stored).await;
        traces_are_rendered(&server, database, &expected, &stored).await;

        // Ingest logs only its start and failures today, so this guards
        // logging added later rather than proving much now.
        let log = service.stop();
        assert!(
            !log.iter().any(|line| line.contains(WRITE_KEY)),
            "the write key reached the ingest log"
        );
    }
}

/// Build the standalone fixture-emitter crate. It has its own lock file and
/// target directory, which CI caches and its own test step reuses, so the
/// nested build cannot wait on this test's build lock.
fn build_emitter() -> PathBuf {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixture-emitter");
    let target = crate_dir.join("target");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| env!("CARGO").into());
    let status = Command::new(cargo)
        .args(["build", "--locked", "--manifest-path"])
        .arg(crate_dir.join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", &target)
        .status()
        .expect("run cargo build for the fixture emitter");
    assert!(status.success(), "the fixture emitter did not build");
    target.join(format!(
        "debug/fixture-emitter{}",
        std::env::consts::EXE_SUFFIX
    ))
}

/// Send the logs and traces of both datasets. A Toudai gateway serves no
/// metrics, so the metric sample is left out.
fn emit(emitter: &Path, protocol: &str, endpoint: &str) {
    let mut command = Command::new(emitter);
    command
        .args(["emit", "--protocol", protocol, "--endpoint", endpoint])
        .args(["--signals", "logs,traces"]);
    // Only the write-key header configures the exporters.
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("OTEL_")) {
        command.env_remove(name);
    }
    command.env(
        "OTEL_EXPORTER_OTLP_HEADERS",
        format!("x-toudai-key={WRITE_KEY}"),
    );
    let output = command.output().expect("run the fixture emitter");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "emit over {protocol} failed:\n{stderr}"
    );
}

/// The rows of one database: serving rows by signal and then by
/// `(dataset, fixture.key)`, and canonical rows by `event_id`.
struct Stored {
    logs: BTreeMap<(String, String), Value>,
    spans: BTreeMap<(String, String), Value>,
    canonical: BTreeMap<String, Value>,
}

impl Stored {
    async fn read(server: &ClickHouseServer, database: &str) -> Stored {
        // Each payload parsed, with whether its hash covers the stored text
        // and whether its identity is the row's and nothing more.
        let canonical = rows(
            server,
            &format!(
                "SELECT event_id, payload, \
                 lower(hex(SHA256(payload))) = payload_sha256 AS hash_matches, \
                 JSONExtractKeys(payload, 'identity') = ['dataset', 'event_id'] \
                 AND JSONExtractString(payload, 'identity', 'dataset') = dataset \
                 AND JSONExtractString(payload, 'identity', 'event_id') = event_id AS identity \
                 FROM {database}.canonical_events"
            ),
        )
        .await
        .into_iter()
        .fold(BTreeMap::new(), |mut by_id, mut row| {
            let payload = serde_json::from_str(row["payload"].as_str().unwrap()).unwrap();
            row["payload"] = payload;
            let event_id = row["event_id"].as_str().unwrap().to_owned();
            let previous = by_id.insert(event_id.clone(), row);
            assert!(previous.is_none(), "canonical {event_id} is stored twice");
            by_id
        });
        Stored {
            logs: Self::table(server, database, "serving_logs", "record").await,
            spans: Self::table(server, database, "serving_spans", "span").await,
            canonical,
        }
    }

    async fn table(
        server: &ClickHouseServer,
        database: &str,
        table: &str,
        prefix: &str,
    ) -> BTreeMap<(String, String), Value> {
        // ClickHouse keeps a repeated Map key, which a JSON object would
        // hide, so each Map's keys are checked distinct before reading.
        let maps: Vec<String> = rows(
            server,
            &format!(
                "SELECT name FROM system.columns WHERE database = '{database}' \
                 AND table = '{table}' AND startsWith(type, 'Map')"
            ),
        )
        .await
        .iter()
        .map(|column| format!("mapKeys({})", column["name"].as_str().unwrap()))
        .collect();
        assert!(!maps.is_empty(), "{table} has no Map columns to check");
        // Doubles as text, so a negative zero keeps its sign through JSON.
        let sql = format!(
            "SELECT * REPLACE (mapApply((k, v) -> (k, toString(v)), {prefix}_double) \
             AS {prefix}_double), \
             arrayAll(k -> length(k) = length(arrayDistinct(k)), [{}]) AS map_keys_distinct \
             FROM {database}.{table}",
            maps.join(", ")
        );
        let mut by_key = BTreeMap::new();
        for mut row in rows(server, &sql).await {
            let distinct = row.as_object_mut().unwrap().remove("map_keys_distinct");
            assert_eq!(
                distinct,
                Some(1.into()),
                "{table}: a Map repeats a key: {row}"
            );
            let dataset = row["dataset"].as_str().unwrap().to_owned();
            let key = row[format!("{prefix}_string")]["fixture.key"]
                .as_str()
                .expect("every fixture record has a fixture.key")
                .to_owned();
            let previous = by_key.insert((dataset, key.clone()), row);
            assert!(previous.is_none(), "{table}: {key} is stored twice");
        }
        by_key
    }

    fn all(&self) -> impl Iterator<Item = (&'static str, &Value)> {
        let logs = self.logs.values().map(|row| ("logs", row));
        logs.chain(self.spans.values().map(|row| ("spans", row)))
    }
}

/// Every record the fixture expects served is stored once, with the expected
/// value in every serving column and the expected canonical payload; nothing
/// else is stored. Ingest owns `event_id` and `routing_epoch`, and the
/// fixture's `payload_sha256` hashes its own payload schema, so those are
/// checked apart.
fn rows_match_the_fixture(protocol: &str, expected: &[Value], stored: &Stored) {
    let mut problems = Vec::new();
    let mut unmatched_logs = stored.logs.clone();
    let mut unmatched_spans = stored.spans.clone();
    for want in expected {
        let (signal, dataset, key) = (
            want["signal"].as_str().unwrap(),
            want["dataset"].as_str().unwrap(),
            want["fixture_key"].as_str().unwrap(),
        );
        let (unmatched, doubles) = match signal {
            "logs" => (&mut unmatched_logs, "record_double"),
            "spans" => (&mut unmatched_spans, "span_double"),
            _ => continue, // The metric sample is not sent.
        };
        let got = unmatched.remove(&(dataset.to_owned(), key.to_owned()));
        let what = format!("{protocol} {dataset} {signal} {key}");
        match (want["disposition"].as_str().unwrap(), got) {
            ("serve", Some(got)) => {
                serving_problems(&what, &want["serving"], &got, doubles, &mut problems);
                match stored.canonical.get(got["event_id"].as_str().unwrap()) {
                    Some(c) => {
                        payload_problems(&what, &want["payload"], &c["payload"], &mut problems)
                    }
                    None => problems.push(format!("{what}: no canonical row")),
                }
            }
            ("serve", None) => problems.push(format!("{what}: not stored")),
            (disposition, Some(_)) => problems.push(format!("{what}: {disposition}, but stored")),
            (_, None) => {}
        }
    }
    for (dataset, key) in unmatched_logs.keys().chain(unmatched_spans.keys()) {
        problems.push(format!(
            "{protocol} {dataset} {key}: stored but not expected"
        ));
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// Every difference between a stored row and the fixture's serving values.
fn serving_problems(
    what: &str,
    want: &Value,
    got: &Value,
    doubles: &str,
    problems: &mut Vec<String>,
) {
    let (want, got) = (want.as_object().unwrap(), got.as_object().unwrap());
    let ingest_owned = ["event_id", "routing_epoch"];
    let mut columns: Vec<_> = got
        .keys()
        .filter(|c| !ingest_owned.contains(&c.as_str()))
        .collect();
    columns.sort();
    let mut expected_columns: Vec<_> = want.keys().collect();
    expected_columns.sort();
    if columns != expected_columns {
        problems.push(format!(
            "{what}: columns {columns:?}, expected {expected_columns:?}"
        ));
    }
    for (column, value) in want {
        let same = match column.as_str() {
            "payload_sha256" => true,
            column if column == doubles => same_doubles(&got[column], value),
            column => got[column] == *value,
        };
        if !same {
            problems.push(format!(
                "{what}: {column} stored {}, expected {value}",
                got[column]
            ));
        }
    }
    let event_id = Uuid::parse_str(got["event_id"].as_str().unwrap()).unwrap();
    if event_id.get_version_num() != 7 || got["routing_epoch"] != 1 {
        let epoch = &got["routing_epoch"];
        problems.push(format!(
            "{what}: event_id {event_id}, routing_epoch {epoch}"
        ));
    }
}

/// Every difference between ingest's canonical payload and the fixture's,
/// apart from the schema name and the `event_id` the fixture leaves blank.
fn payload_problems(what: &str, want: &Value, got: &Value, problems: &mut Vec<String>) {
    let mut want: Value = serde_json::from_str(want.as_str().unwrap()).unwrap();
    let mut got = got.clone();
    for payload in [&mut want, &mut got] {
        payload["schema"] = Value::Null;
        payload["identity"]["event_id"] = Value::Null;
    }
    if want != got {
        problems.push(format!("{what}: canonical payload {got}, expected {want}"));
    }
}

/// The same keys, and the same doubles bit for bit apart from NaN's payload.
/// The fixture writes `nan`, `inf`, `-0.0`, or Rust's shortest form; the
/// stored text is ClickHouse's.
fn same_doubles(got: &Value, want: &Value) -> bool {
    let parse = |map: &Value| -> BTreeMap<String, f64> {
        map.as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().parse().unwrap()))
            .collect()
    };
    let (got, want) = (parse(got), parse(want));
    got.len() == want.len()
        && want.iter().all(|(key, w)| {
            got.get(key)
                .is_some_and(|g| g.to_bits() == w.to_bits() || (g.is_nan() && w.is_nan()))
        })
}

/// Every serving row has exactly one canonical row with the same identity
/// and payload hash; each hash covers its payload, which names only that
/// identity; and no canonical row lacks a serving row.
async fn serving_rows_join_their_canonical_rows(
    server: &ClickHouseServer,
    database: &str,
    stored: &Stored,
) {
    for table in ["serving_logs", "serving_spans"] {
        let orphans = rows(
            server,
            &format!(
                "SELECT s.event_id FROM {database}.{table} AS s \
                 LEFT ANTI JOIN {database}.canonical_events AS c \
                 ON s.dataset = c.dataset AND s.event_id = c.event_id \
                 AND s.payload_sha256 = c.payload_sha256"
            ),
        )
        .await;
        assert!(orphans.is_empty(), "{table} rows without a canonical row");
    }
    assert_eq!(
        stored.canonical.len(),
        stored.logs.len() + stored.spans.len(),
        "{database}: one canonical row per serving row"
    );
    for (event_id, row) in &stored.canonical {
        assert_eq!(row["hash_matches"], 1, "{event_id}: payload_sha256");
        assert_eq!(row["identity"], 1, "{event_id}: identity");
    }
}

/// The spoofed record attribute `toudai.dataset` in each dataset is dropped
/// and counted, and each dataset's repeated-key record is rejected and
/// counted (Decision 0003 D2's partial success).
async fn reserved_and_rejected_records_are_counted(service: &support::Service) {
    let text = reqwest::get(format!("{}/metrics", service.http))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let value = |series: &str, labels: &[&str]| -> Option<u64> {
        text.lines()
            .filter(|line| line.starts_with(&format!("{series}{{")))
            .find(|line| labels.iter().all(|label| line.contains(label)))
            .and_then(|line| line.rsplit_once(' '))
            .map(|(_, value)| value.parse().unwrap())
    };
    let dropped = value(
        "toudai_ingest_reserved_attributes_dropped_total",
        &["signal=\"logs\""],
    );
    assert_eq!(dropped, Some(2), "{text}");
    let rejected = value(
        "toudai_ingest_rejected_records_total",
        &["signal=\"logs\"", "duplicate_or_invalid_record_attributes"],
    );
    assert_eq!(rejected, Some(2), "{text}");
}

/// Each stored event is found by the query service's exact lookup, under its
/// own dataset only, with the canonical row's payload and hash, and a key
/// for the other dataset alone cannot read it.
async fn events_are_found_by_exact_lookup(
    server: &ClickHouseServer,
    database: &str,
    stored: &Stored,
) {
    let base = query_service(server, database).await;
    let client = reqwest::Client::new();
    let lookup = |key: &str, dataset: &str, event_id: &str| {
        let mut url: reqwest::Url = format!("{base}/v1/events").parse().unwrap();
        url.path_segments_mut().unwrap().extend([dataset, event_id]);
        client.get(url).bearer_auth(key).send()
    };
    for (signal, row) in stored.all() {
        let (dataset, event_id) = (
            row["dataset"].as_str().unwrap(),
            row["event_id"].as_str().unwrap(),
        );
        let response = lookup(READ_KEY, dataset, event_id).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{dataset} {event_id}");
        let event: Value = response.json().await.unwrap();
        assert_eq!(event["dataset"], dataset);
        assert_eq!(event["event_id"], event_id);
        assert_eq!(event["signal"], signal);
        assert_eq!(event["payload_sha256"], row["payload_sha256"]);
        assert_eq!(
            event["payload"], stored.canonical[event_id]["payload"],
            "{event_id}: payload"
        );

        let other = if dataset == "fixture" {
            "fixture-b"
        } else {
            "fixture"
        };
        let status = lookup(READ_KEY, other, event_id).await.unwrap().status();
        assert_eq!(status, StatusCode::NOT_FOUND, "{event_id} under {other}");
        if dataset == "fixture" {
            let status = lookup(READ_KEY_B, dataset, event_id)
                .await
                .unwrap()
                .status();
            assert_eq!(
                status,
                StatusCode::FORBIDDEN,
                "{event_id} with a fixture-b key"
            );
        }
    }
}

/// The order trace's waterfall as the fixture builds it (`add_order_trace` in
/// `fixture-emitter/src/model.rs`): roots by start time, each followed
/// depth-first by its children by start time. `(fixture.key, depth, parent)`.
const ORDER_WATERFALL: [(&str, u64, &str); 7] = [
    ("span.order.root", 0, "none"),
    ("span.order.validate", 1, "found"),
    ("span.order.db", 1, "found"),
    ("span.order.payment", 1, "found"),
    ("span.order.charge", 2, "found"),
    ("span.order.unfinished", 1, "found"),
    // Its parent is never sent.
    ("span.order.orphan", 0, "missing"),
];

/// Query inventory items 10 and 11 through the query service. Every trace the
/// fixture sends comes back with exactly its own dataset's spans, each with
/// the span events and timing of the fixture's payload, and exactly the logs
/// and application events carrying its context, in time order. The order
/// trace, which both datasets send under one trace ID, also comes back in the
/// waterfall order the fixture defines. Expectations are the fixture's
/// expected rows; only `event_id`s come from what ingest stored.
async fn traces_are_rendered(
    server: &ClickHouseServer,
    database: &str,
    expected: &[Value],
    stored: &Stored,
) {
    let base = query_service(server, database).await;
    let client = reqwest::Client::new();
    let get = |key: &str, dataset: &str, trace_id: &str, query: &str| {
        let mut url: reqwest::Url = format!("{base}/v1/traces").parse().unwrap();
        url.path_segments_mut().unwrap().extend([dataset, trace_id]);
        url.set_query(Some(query));
        client.get(url).bearer_auth(key).send()
    };
    let payload = |want: &Value| -> Value {
        serde_json::from_str(want["payload"].as_str().unwrap()).unwrap()
    };
    let key_of = |want: &Value| want["fixture_key"].as_str().unwrap().to_owned();
    let event_id = |want: &Value| {
        let (dataset, key) = (want["dataset"].as_str().unwrap().to_owned(), key_of(want));
        let rows = match want["signal"].as_str().unwrap() {
            "spans" => &stored.spans,
            _ => &stored.logs,
        };
        rows[&(dataset, key)]["event_id"].clone()
    };

    // The served spans and logs of each (dataset, trace_id).
    type Records<'a> = (Vec<&'a Value>, Vec<&'a Value>);
    let mut traces: BTreeMap<(String, String), Records> = BTreeMap::new();
    for want in expected.iter().filter(|w| w["disposition"] == "serve") {
        let trace_id = want["serving"]["trace_id"].as_str().unwrap();
        if trace_id.is_empty() {
            continue;
        }
        let entry = traces
            .entry((want["dataset"].as_str().unwrap().into(), trace_id.into()))
            .or_default();
        match want["signal"].as_str().unwrap() {
            "spans" => entry.0.push(want),
            _ => entry.1.push(want),
        }
    }
    // Wide enough for the spans whose unset start is stored at the epoch.
    let everything = "start_ns=0&end_ns=1788332400000000000";
    for ((dataset, trace_id), (spans, logs)) in &traces {
        let response = get(READ_KEY, dataset, trace_id, everything).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{dataset} {trace_id}");
        let got: Value = response.json().await.unwrap();
        let what = format!("{database} {dataset} {trace_id}");
        assert_eq!(got["dataset"], dataset.as_str());
        assert_eq!(got["trace_id"], trace_id.as_str());

        let got_spans: BTreeMap<String, &Value> = got["spans"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| (s["event_id"].as_str().unwrap().to_owned(), s))
            .collect();
        assert_eq!(got_spans.len(), spans.len(), "{what}: span count");
        for want in spans {
            let p = payload(want);
            let id = event_id(want);
            let span = got_spans
                .get(id.as_str().unwrap())
                .unwrap_or_else(|| panic!("{what}: {} missing", key_of(want)));
            let fields = [
                ("span_id", p["trace"]["span_id"].clone()),
                ("parent_span_id", p["span"]["parent_span_id"].clone()),
                ("name", p["span"]["name"].clone()),
                ("span_kind", p["span"]["kind"].clone()),
                ("service_name", want["serving"]["service_name"].clone()),
                ("status_code", want["serving"]["status_code"].clone()),
                ("start_ns", p["span"]["start_ns"].clone()),
                ("end_ns", p["span"]["end_ns"].clone()),
                // Absent from the payload when unavailable: null, never zero.
                ("duration_ns", p["span"]["duration_ns"].clone()),
                (
                    "timing_status",
                    want["serving"]["span_timing_status"].clone(),
                ),
                ("events", p["span"]["events"].clone()),
            ];
            for (field, value) in fields {
                assert_eq!(span[field], value, "{what}: {} {field}", key_of(want));
            }
        }
        assert_eq!(got["diagnostics"]["span_events_unavailable"], "0", "{what}");
        assert_eq!(got["diagnostics"]["duplicate_span_ids"], "0", "{what}");

        let mut logs = logs.clone();
        let time = |w: &&Value| {
            payload(w)["time"]["timestamp_ns"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
        };
        logs.sort_by_key(time);
        let got_logs = got["logs"]
            .as_array()
            .expect("logs are returned by default");
        assert_eq!(got_logs.len(), logs.len(), "{what}: log count");
        for (got, want) in got_logs.iter().zip(&logs) {
            let p = payload(want);
            let fields = [
                ("event_id", event_id(want)),
                ("kind", p["kind"].clone()),
                ("timestamp_ns", p["time"]["timestamp_ns"].clone()),
                ("span_id", p["trace"]["span_id"].clone()),
                ("span_found", true.into()),
                ("body", want["serving"]["body"].clone()),
                ("event_name", p["log"]["event_name"].clone()),
                ("severity_text", p["log"]["severity_text"].clone()),
                ("severity_number", p["log"]["severity_number"].clone()),
            ];
            for (field, value) in fields {
                assert_eq!(got[field], value, "{what}: {} {field}", key_of(want));
            }
        }
    }

    // The order trace: one trace ID in both datasets, its own spans in each.
    let order: Vec<_> = traces
        .iter()
        .filter(|(_, (spans, _))| spans.iter().any(|w| key_of(w) == "span.order.root"))
        .map(|((dataset, trace_id), (spans, _))| (dataset.as_str(), trace_id.as_str(), spans))
        .collect();
    assert_eq!(order.len(), 2, "the order trace is sent by both datasets");
    assert_eq!(order[0].1, order[1].1, "under one trace ID");
    for (dataset, trace_id, spans) in order {
        let what = format!("{database} {dataset} order trace");
        let got: Value = get(READ_KEY, dataset, trace_id, everything)
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let key_by_event: BTreeMap<String, String> = spans
            .iter()
            .map(|w| (event_id(w).as_str().unwrap().to_owned(), key_of(w)))
            .collect();
        let shape: Vec<(String, u64, String)> = got["spans"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                (
                    key_by_event[s["event_id"].as_str().unwrap()].clone(),
                    s["depth"].as_u64().unwrap(),
                    s["parent"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        let want: Vec<_> = ORDER_WATERFALL
            .iter()
            .map(|(k, d, p)| (k.to_string(), *d, p.to_string()))
            .collect();
        assert_eq!(shape, want, "{what}: waterfall");
        let kinds: Vec<_> = got["logs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, ["log", "log", "event"], "{what}: logs and app event");

        // Item 10 alone: the waterfall without logs.
        let response = get(
            READ_KEY,
            dataset,
            trace_id,
            &format!("{everything}&logs=false"),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let spans_only: Value = response.json().await.unwrap();
        assert!(spans_only.get("logs").is_none(), "{what}: logs=false");
        assert_eq!(spans_only["spans"], got["spans"], "{what}: logs=false");

        // The window bounds spans and logs alike: ending before the
        // unfinished span and the application event, it drops both.
        let root_start: i64 = got["spans"][0]["start_ns"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let window = format!("start_ns={root_start}&end_ns={}", root_start + 3_500_000);
        let cut: Value = get(READ_KEY, dataset, trace_id, &window)
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let names: Vec<_> = cut["spans"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| key_by_event[s["event_id"].as_str().unwrap()].as_str())
            .collect();
        assert_eq!(
            names,
            [
                "span.order.root",
                "span.order.validate",
                "span.order.db",
                "span.order.payment",
                "span.order.charge",
                "span.order.orphan"
            ],
            "{what}: window"
        );
        let found: Vec<_> = cut["logs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l["span_found"].as_bool().unwrap())
            .collect();
        assert_eq!(found, [true, true], "{what}: window logs");

        let before = format!(
            "start_ns={}&end_ns={root_start}",
            root_start - 1_000_000_000
        );
        let response = get(READ_KEY, dataset, trace_id, &before).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{what}: before");
        if dataset == "fixture" {
            let response = get(READ_KEY_B, dataset, trace_id, everything)
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{what}: key b");
        }
    }
}

/// The query service on a port the system picks, reading `database`.
async fn query_service(server: &ClickHouseServer, database: &str) -> String {
    let mut config = ClickHouseConfig::new(server.url.clone());
    config.user = Some(server::USER.into());
    config.password = Some(server::PASSWORD.into());
    let state = Arc::new(AppState {
        clickhouse: ClickHouse::new(config).unwrap(),
        tables: ServingTables::new(database).unwrap(),
        keys: Keys::parse(READ_KEYS).unwrap(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    base
}

/// `JSONEachRow` rows of `sql`, with 64-bit integers as strings, non-finite
/// doubles as text, and times in UTC, as the fixture writes them.
async fn rows(server: &ClickHouseServer, sql: &str) -> Vec<Value> {
    let sql = format!(
        "{sql} SETTINGS output_format_json_quote_64bit_integers = 1, \
         output_format_json_quote_denormals = 1, session_timezone = 'UTC' \
         FORMAT JSONEachRow"
    );
    execute(server, &sql, None)
        .await
        .lines()
        .map(|line| {
            serde_json::from_str::<Map<String, Value>>(line)
                .unwrap()
                .into()
        })
        .collect()
}
