//! End to end from the fixture emitter to the query service: the RFD 0001
//! fixture (Experiment 023), sent by the OpenTelemetry Rust SDK over OTLP/gRPC
//! and OTLP/HTTP, through the `toudai-ingest` binary into a real ClickHouse,
//! and read back through the query service's exact-event lookup.
//!
//! The expected contents are the emitter's own model, recorded in Experiment
//! 023's `expected.jsonl`, so nothing here is derived from ingest's mapping.
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

const EXPECTED: &str =
    include_str!("../../rfd/0001/experiments/023-fixture-emitter-run/expected.jsonl");

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
            // The fixture serves an application event (a log record with
            // `event_name` set) as kind `event`. Ingest stores `log`, as the
            // Go exporter at collector `f780574` does: the application-event
            // encoding is still open in `acceptance-and-replay-impl.adoc`.
            "kind" if value == "event" => got[column] == "log",
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
    // An application event is stored as `log`, as in its serving row.
    if want["kind"] == "event" && got["kind"] == "log" {
        want["kind"] = "log".into();
    }
    // Ingest stores no span events, as the Go exporter at collector
    // `f780574` stores none; `product-contract-impl.adoc` records the gap.
    // Once a payload holds `events`, they are compared.
    if got["span"].get("events").is_none()
        && let Some(span) = want.get_mut("span").and_then(Value::as_object_mut)
    {
        span.remove("events");
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
