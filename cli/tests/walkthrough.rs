//! The operator walkthrough, end to end and checked: the RFD 0001 fixture
//! (Experiment 023), sent by the fixture emitter over OTLP/HTTP through
//! `toudai-ingest` into a real ClickHouse, then read by the `toudai-cli`
//! binary through the query service, the way an operator would.
//!
//! Each step runs once with `--json`, whose answer is checked against
//! `fixture-emitter/expected.jsonl` (the emitter's own model, so nothing here
//! derives from ingest or the query layer), and once as an operator sees it.
//! The second runs make up the transcript, written to
//! `TOUDAI_WALKTHROUGH_OUT` when set (the recorded walkthrough is this file),
//! otherwise under the test's target directory.
//!
//! Ingest and the query service run in-process from their libraries, as
//! their binaries wire them; the CLI runs as its own binary.

#[allow(dead_code)]
#[path = "../../tests/e2e/server.rs"]
mod server;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::net::TcpListener;
use toudai::api::{AppState, router};
use toudai::auth::Keys;
use toudai::clickhouse::{ClickHouse, ClickHouseConfig};
use toudai::query::ServingTables;
use toudai_ingest::batcher::FlushConfig;
use toudai_ingest::http::{self as ingest_http, Timeouts};
use toudai_ingest::pipeline::{Pipeline, Settings};
use toudai_ingest::writer::{Writer, WriterConfig};

use server::{ClickHouseServer, apply_ddl, product_ddl};

const EXPECTED: &str = include_str!("../../fixture-emitter/expected.jsonl");
/// Not the default `toudai` (Decision 0002 D1).
const DATABASE: &str = "toudai_walkthrough";
const WRITE_KEY: &str = "tk_walkthrough_writer_0123456789ab";
const READ_KEY: &str = "tk_walkthrough_reader_0123456789ab";
/// Spans with an unset start are stored at the epoch, so span steps start
/// there to see every span.
const EPOCH: &str = "1970-01-01T00:00:00Z";
const DAY_END: &str = "2026-09-02T00:00:00Z";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operator_walkthrough_over_the_fixture() {
    let Some(clickhouse) = server::start().await else {
        return;
    };
    for (_, sql) in product_ddl() {
        apply_ddl(&clickhouse, &sql, DATABASE).await;
    }
    let ingest = start_ingest(&clickhouse).await;
    let query = start_query_service(&clickhouse).await;
    let emitter = tokio::task::spawn_blocking(build_emitter).await.unwrap();
    tokio::task::spawn_blocking(move || emit(&emitter, &ingest))
        .await
        .unwrap();

    let fixture = Fixture::parse();
    let mut walk = Walk {
        base: query,
        transcript: String::new(),
        shown: String::new(),
    };
    time_comparison(&mut walk, &fixture).await;
    grouping(&mut walk, &fixture).await;
    percentiles(&mut walk, &fixture).await;
    let (outlier, expected) = outlier(&mut walk, &fixture).await;
    drill_down(&mut walk, &outlier, expected).await;
    refusal(&mut walk).await;

    let out = std::env::var_os("TOUDAI_WALKTHROUGH_OUT").map_or_else(
        || PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("walkthrough.txt"),
        PathBuf::from,
    );
    std::fs::write(&out, &walk.transcript).unwrap();
    eprintln!("walkthrough transcript: {}", out.display());
}

async fn start_ingest(clickhouse: &ClickHouseServer) -> String {
    let writer = Writer::new(
        WriterConfig {
            url: clickhouse.url.clone(),
            user: Some(server::USER.into()),
            password: Some(server::PASSWORD.into()),
            database: DATABASE.into(),
        },
        Arc::default(),
    )
    .unwrap();
    let keys = Keys::parse(&format!("{WRITE_KEY} fixture fixture-b\n")).unwrap();
    let settings = Settings {
        flush: FlushConfig {
            interval: Duration::from_millis(50),
            ..FlushConfig::default()
        },
        ..Settings::default()
    };
    let pipeline = Arc::new(Pipeline::new(keys, settings, writer).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(ingest_http::serve(
        listener,
        pipeline,
        Timeouts::default(),
        std::future::pending(),
    ));
    format!("http://{address}")
}

async fn start_query_service(clickhouse: &ClickHouseServer) -> String {
    let mut config = ClickHouseConfig::new(clickhouse.url.clone());
    config.user = Some(server::USER.into());
    config.password = Some(server::PASSWORD.into());
    let state = Arc::new(AppState {
        clickhouse: ClickHouse::new(config).unwrap(),
        tables: ServingTables::new(DATABASE).unwrap(),
        keys: Keys::parse(&format!("{READ_KEY} fixture fixture-b\n")).unwrap(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    format!("http://{address}")
}

/// The standalone fixture-emitter crate, built into its own target
/// directory as `ingest/tests/fixture_e2e.rs` builds it.
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

/// Both datasets' logs and traces; ingest serves no metrics.
fn emit(emitter: &Path, endpoint: &str) {
    let mut command = Command::new(emitter);
    command.args([
        "emit",
        "--protocol",
        "http",
        "--endpoint",
        endpoint,
        "--signals",
        "logs,traces",
    ]);
    for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("OTEL_")) {
        command.env_remove(name);
    }
    command.env(
        "OTEL_EXPORTER_OTLP_HEADERS",
        format!("x-toudai-key={WRITE_KEY}"),
    );
    let output = command.output().expect("run the fixture emitter");
    assert!(
        output.status.success(),
        "emit failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The served events of `expected.jsonl`.
struct Fixture {
    served: Vec<Value>,
}

impl Fixture {
    fn parse() -> Fixture {
        let served = EXPECTED
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|row| row["disposition"] == "serve")
            .collect();
        Fixture { served }
    }

    fn events<'a>(&'a self, dataset: &'a str, signal: &'a str) -> impl Iterator<Item = &'a Value> {
        self.served
            .iter()
            .filter(move |row| row["dataset"] == dataset && row["signal"] == signal)
    }
}

/// The event time the emitter recorded in its expected payload.
fn timestamp_ns(row: &Value) -> i64 {
    let payload: Value = serde_json::from_str(row["payload"].as_str().unwrap()).unwrap();
    payload["time"]["timestamp_ns"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

fn duration_ns(row: &Value) -> Option<i64> {
    row["serving"]["duration_ns"]
        .as_str()
        .map(|d| d.parse().unwrap())
}

fn count(value: &Value) -> u64 {
    value["value"].as_str().unwrap().parse().unwrap()
}

struct Walk {
    base: String,
    transcript: String,
    /// What the operator saw at the last step.
    shown: String,
}

struct Run {
    ok: bool,
    stdout: String,
    stderr: String,
}

impl Walk {
    async fn cli(&self, args: &[&str]) -> Run {
        let base = self.base.clone();
        let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        tokio::task::spawn_blocking(move || {
            let output = Command::new(env!("CARGO_BIN_EXE_toudai-cli"))
                .args(&args)
                .env("TOUDAI_URL", base)
                .env("TOUDAI_READ_KEY", READ_KEY)
                .output()
                .expect("run toudai-cli");
            Run {
                ok: output.status.success(),
                stdout: String::from_utf8(output.stdout).unwrap(),
                stderr: String::from_utf8(output.stderr).unwrap(),
            }
        })
        .await
        .unwrap()
    }

    /// The step's answer as JSON, after recording what the operator sees.
    /// The recorded run must succeed too: a transcript of a broken client
    /// is not a walkthrough, whatever `--json` returns.
    async fn step(&mut self, note: &str, args: &[&str]) -> Value {
        let shown = self.record(note, args).await;
        assert!(
            shown.ok,
            "{args:?} failed as the operator ran it: {}",
            shown.stderr
        );
        // Every query answer states how exact it is; `event` shows a payload.
        assert!(
            args[0] == "event" || shown.stdout.contains("\nexact, "),
            "{args:?} did not say it is exact:\n{}",
            shown.stdout
        );
        self.shown = shown.stdout;
        let mut json = args.to_vec();
        json.push("--json");
        let run = self.cli(&json).await;
        assert!(run.ok, "{args:?} failed: {}", run.stderr);
        serde_json::from_str(&run.stdout).unwrap()
    }

    async fn record(&mut self, note: &str, args: &[&str]) -> Run {
        let run = self.cli(args).await;
        let shown: Vec<String> = args
            .iter()
            .map(|a| {
                if a.contains([' ', '\'', '"', '<', '>']) {
                    format!("'{a}'")
                } else {
                    a.to_string()
                }
            })
            .collect();
        self.transcript.push_str(&format!(
            "# {note}\n$ toudai-cli {}\n{}{}\n",
            shown.join(" "),
            run.stdout,
            run.stderr
        ));
        run
    }
}

/// When did each dataset's logs arrive? One-second buckets over two seconds:
/// `fixture-b` is a second behind `fixture`, except for the collision record
/// it shares with `fixture` byte for byte. The fixture's order trace is
/// logged later, outside this window.
async fn time_comparison(walk: &mut Walk, fixture: &Fixture) {
    let start: i64 = 1_788_246_000_000_000_000;
    for dataset in ["fixture", "fixture-b"] {
        let response = walk
            .step(
                &format!("Time comparison: {dataset} logs per second"),
                &[
                    "series",
                    "--dataset",
                    dataset,
                    "--from",
                    "2026-09-01T07:00:00Z",
                    "--to",
                    "2026-09-01T07:00:02Z",
                    "--every",
                    "1s",
                ],
            )
            .await;
        let mut expected = [0u64; 2];
        for row in fixture.events(dataset, "logs") {
            let offset = timestamp_ns(row) - start;
            if (0..2_000_000_000).contains(&offset) {
                expected[(offset / 1_000_000_000) as usize] += 1;
            }
        }
        let mut got = [0u64; 2];
        for row in response["rows"].as_array().unwrap() {
            let bucket: i64 = row["bucket_start_ns"]["value"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            got[((bucket - start) / 1_000_000_000) as usize] = count(&row["events"]);
        }
        assert_eq!(got, expected, "{dataset}");
        let visible = format!("2026-09-01T07:00:01Z  {}\n", expected[1]);
        assert!(walk.shown.contains(&visible), "{dataset}:\n{}", walk.shown);
        assert!(
            walk.shown
                .contains("exact, complete over the events stored when it ran")
        );
    }
}

/// Which status did each dataset's request spans end with? Grouped by the
/// typed `http.status_code`, and a typed filter that matches only the type
/// it names.
async fn grouping(walk: &mut Walk, fixture: &Fixture) {
    for dataset in ["fixture", "fixture-b"] {
        let response = walk
            .step(
                &format!("Grouping: {dataset} spans by http.status_code"),
                &[
                    "count",
                    "--dataset",
                    dataset,
                    "--signal",
                    "spans",
                    "--from",
                    EPOCH,
                    "--to",
                    DAY_END,
                    "--by",
                    "http.status_code",
                ],
            )
            .await;
        let mut expected: Vec<(Value, u64)> = Vec::new();
        for row in fixture.events(dataset, "spans") {
            let group = match &row["serving"]["span_int"]["http.status_code"] {
                Value::String(code) => serde_json::json!({"type": "int", "value": code}),
                _ => serde_json::json!({"type": "missing"}),
            };
            match expected.iter_mut().find(|(g, _)| *g == group) {
                Some((_, n)) => *n += 1,
                None => expected.push((group, 1)),
            }
        }
        let mut got: Vec<(Value, u64)> = response["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| (row["groups"][0].clone(), count(&row["events"])))
            .collect();
        let key = |(g, _): &(Value, u64)| g.to_string();
        got.sort_by_key(key);
        expected.sort_by_key(key);
        assert_eq!(got, expected, "{dataset}");
        // An integer group prints bare, never quoted like a string.
        let code = if dataset == "fixture" { "200" } else { "500" };
        assert!(
            walk.shown.contains(&format!("\n{code} ")) && walk.shown.contains("\n(missing) "),
            "{dataset}:\n{}",
            walk.shown
        );
    }
    for (literal, ty) in [("int:3", "record_int"), ("string:3", "record_string")] {
        let filter = format!("cart.items = {literal}");
        let response = walk
            .step(
                &format!("Typed filter: cart.items = {literal}"),
                &[
                    "count",
                    "--dataset",
                    "fixture",
                    "--from",
                    "2026-09-01T07:00:00Z",
                    "--to",
                    DAY_END,
                    "--where",
                    &filter,
                ],
            )
            .await;
        let expected = fixture
            .events("fixture", "logs")
            .filter(|row| row["serving"][ty]["cart.items"] == "3")
            .count() as u64;
        assert_eq!(count(&response["rows"][0]["events"]), expected, "{literal}");
    }
}

/// How long do `fixture` spans take? Nearest-rank percentiles computed here
/// from the expected durations.
async fn percentiles(walk: &mut Walk, fixture: &Fixture) {
    let response = walk
        .step(
            "Percentiles: fixture span durations",
            &[
                "percentiles",
                "--dataset",
                "fixture",
                "--signal",
                "spans",
                "--from",
                EPOCH,
                "--to",
                DAY_END,
                "--span-duration",
                "--p",
                "50,90,100",
            ],
        )
        .await;
    let spans: Vec<&Value> = fixture.events("fixture", "spans").collect();
    let mut durations: Vec<i64> = spans.iter().filter_map(|row| duration_ns(row)).collect();
    durations.sort();
    let rank = |p: u64| durations[(p * durations.len() as u64).div_ceil(100).max(1) as usize - 1];
    let row = &response["rows"][0];
    assert_eq!(count(&row["events"]), spans.len() as u64);
    assert_eq!(count(&row["contributing"]), durations.len() as u64);
    let reversed = spans
        .iter()
        .filter(|row| row["serving"]["span_timing_status"] == "end_before_start")
        .count();
    assert_eq!(count(&row["timing_errors"]), reversed as u64);
    let got: Vec<i64> = row["percentiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["value"]["value"].as_str().unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, vec![rank(50), rank(90), rank(100)]);
    assert!(
        walk.shown
            .contains("percentiles are nearest-rank received values"),
        "{}",
        walk.shown
    );
}

/// Which span is the outlier? The longest three, with their fixture keys;
/// returns the top one's `event_id` and its expected row.
async fn outlier<'a>(walk: &mut Walk, fixture: &'a Fixture) -> (String, &'a Value) {
    let response = walk
        .step(
            "Outliers: the longest fixture spans",
            &[
                "events",
                "--dataset",
                "fixture",
                "--signal",
                "spans",
                "--from",
                EPOCH,
                "--to",
                DAY_END,
                "--order",
                "longest",
                "--limit",
                "3",
                "--show",
                "fixture.key",
            ],
        )
        .await;
    let mut spans: Vec<&Value> = fixture.events("fixture", "spans").collect();
    // The service's `longest` order: duration descending with unobservable
    // durations last, then newest first. Two fixture spans tie at 1.5 ms.
    spans.sort_by_key(|row| {
        (
            duration_ns(row).is_none(),
            std::cmp::Reverse(duration_ns(row)),
            std::cmp::Reverse(timestamp_ns(row)),
        )
    });
    let expected: Vec<&str> = spans[..3]
        .iter()
        .map(|row| row["fixture_key"].as_str().unwrap())
        .collect();
    let rows = response["rows"].as_array().unwrap();
    let got: Vec<&str> = rows
        .iter()
        .map(|row| row["fields"][0]["value"].as_str().unwrap())
        .collect();
    assert_eq!(got, expected);
    assert_eq!(count(&response["matched"]), spans.len() as u64);
    let partial = format!("PARTIAL: the first 3 of {} matching events", spans.len());
    assert!(walk.shown.contains(&partial), "{}", walk.shown);
    assert!(
        walk.shown.contains(&format!("\"{}\"", expected[0])),
        "{}",
        walk.shown
    );
    (rows[0]["event_id"].as_str().unwrap().to_owned(), spans[0])
}

/// The outlier's stored event: its checksum verifies and it is the span the
/// fixture sent.
async fn drill_down(walk: &mut Walk, event_id: &str, expected: &Value) {
    let response = walk
        .step(
            "Drill-down: the outlier's canonical event",
            &["event", "fixture", event_id],
        )
        .await;
    assert_eq!(response["event_id"], event_id);
    let payload = &response["payload"];
    let sent: Value = serde_json::from_str(expected["payload"].as_str().unwrap()).unwrap();
    assert_eq!(
        payload["record"]["fixture.key"]["v"], expected["fixture_key"],
        "{payload}"
    );
    assert_eq!(payload["span"]["name"], sent["span"]["name"], "{payload}");
    assert!(
        walk.shown.contains("(verified against the payload)"),
        "{}",
        walk.shown
    );
}

/// A request the typed-value contract refuses: percentiles of a key that
/// holds NaN and infinities.
async fn refusal(walk: &mut Walk) {
    let args = [
        "percentiles",
        "--dataset",
        "fixture",
        "--from",
        "2026-09-01T07:00:00Z",
        "--to",
        DAY_END,
        "--field",
        "double.nan",
        "--type",
        "double",
    ];
    let run = walk
        .record("Refused: percentiles over non-finite doubles", &args)
        .await;
    assert!(!run.ok);
    assert!(
        run.stderr.contains("unsupported_operation (422)"),
        "{}",
        run.stderr
    );
    assert!(run.stdout.is_empty());
}
