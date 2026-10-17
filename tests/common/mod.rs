//! Run generated statements against the Experiment 021 fixture loaded into a
//! throwaway `clickhouse local` database.
//!
//! The fixture rows are `tests/fixtures/021/*.jsonl`, exported from the
//! Experiment 021 harness by `scripts/export-fixture021-rows.py`; the tables
//! are created from that experiment's `serving-tables.sql` unchanged.
//! Parameters travel as `--param_<name>` exactly as they would over the
//! ClickHouse HTTP interface.

#![allow(dead_code)]

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};

use serde_json::Value;
use toudai::query::{Aggregate, Query, Scope, ServingTables, Signal, SqlStatement, TimeRange};

pub const WS_A: &str = "11111111-1111-4111-8111-111111111111";
pub const WS_B: &str = "22222222-2222-4222-8222-222222222222";
pub const DATASET: &str = "fixture";
/// 2026-09-01T07:00:00.123456789Z, the fixture's event time.
pub const TS: i64 = 1_788_246_000_123_456_789;
pub const DATABASE: &str = "lh021";

const DDL: &str = include_str!(
    "../../rfd/0001/experiments/021-hybrid-layout-collector-integration/harness/serving-tables.sql"
);
const LOG_ROWS: &str = include_str!("../fixtures/021/serving_logs.jsonl");
const SPAN_ROWS: &str = include_str!("../fixtures/021/serving_spans.jsonl");

pub struct ClickHouse {
    binary: String,
    path: PathBuf,
    // `clickhouse local` takes an exclusive lock on its data directory.
    lock: Mutex<()>,
}

impl ClickHouse {
    fn local(&self, args: &[String], stdin: Option<&str>) -> String {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut child = Command::new(&self.binary)
            .arg("local")
            .arg("--path")
            .arg(&self.path)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start clickhouse local");
        let mut input = child.stdin.take().expect("stdin");
        let data = stdin.unwrap_or_default().to_owned();
        let writer = std::thread::spawn(move || {
            let _ = input.write_all(data.as_bytes());
        });
        let output = child.wait_with_output().expect("clickhouse local output");
        writer.join().expect("stdin writer");
        assert!(
            output.status.success(),
            "clickhouse local failed: {}\nargs: {args:?}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("utf-8 output")
    }

    /// Execute a generated statement and return its `JSONEachRow` rows.
    pub fn run(&self, statement: &SqlStatement) -> Vec<Value> {
        let mut args = vec!["--query".to_owned(), statement.sql.clone()];
        args.extend(
            statement
                .params
                .iter()
                .map(|p| format!("--param_{}={}", p.name, p.wire_value())),
        );
        self.local(&args, None)
            .lines()
            .map(|line| serde_json::from_str(line).expect("JSONEachRow row"))
            .collect()
    }

    /// Execute hand-written SQL (fixture checks), returning the raw text.
    pub fn raw(&self, sql: &str, params: &[(&str, &str)]) -> String {
        let mut args = vec!["--query".to_owned(), sql.to_owned()];
        args.extend(params.iter().map(|(n, v)| format!("--param_{n}={v}")));
        self.local(&args, None)
    }
}

/// The fixture database, or `None` when no ClickHouse binary is available.
/// Set `LIGHTHOUSE_REQUIRE_CLICKHOUSE=1` to fail instead of skipping, and
/// `LIGHTHOUSE_CLICKHOUSE_BIN` to choose the binary (default: `clickhouse` on
/// `PATH`, provided by the Nix development shell).
pub fn clickhouse() -> Option<&'static ClickHouse> {
    static INSTANCE: OnceLock<Option<ClickHouse>> = OnceLock::new();
    INSTANCE
        .get_or_init(|| {
            let binary =
                std::env::var("LIGHTHOUSE_CLICKHOUSE_BIN").unwrap_or_else(|_| "clickhouse".into());
            let available = Command::new(&binary)
                .arg("--version")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success());
            if !available {
                assert!(
                    std::env::var_os("LIGHTHOUSE_REQUIRE_CLICKHOUSE").is_none(),
                    "LIGHTHOUSE_REQUIRE_CLICKHOUSE is set but `{binary}` is not runnable"
                );
                eprintln!("skipping fixture tests: `{binary}` is not runnable");
                return None;
            }
            let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("clickhouse-local");
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("create data dir");
            let ch = ClickHouse {
                binary,
                path,
                lock: Mutex::new(()),
            };
            let flag = |s: &str| s.to_owned();
            ch.local(
                &[flag("--query"), format!("CREATE DATABASE {DATABASE}")],
                None,
            );
            ch.local(
                &[
                    flag("--multiquery"),
                    flag("--query"),
                    DDL.replace("{{DB}}", DATABASE),
                ],
                None,
            );
            for (table, rows) in [("serving_logs", LOG_ROWS), ("serving_spans", SPAN_ROWS)] {
                ch.local(
                    &[
                        flag("--query"),
                        format!("INSERT INTO {DATABASE}.{table} FORMAT JSONEachRow"),
                    ],
                    Some(rows),
                );
            }
            Some(ch)
        })
        .as_ref()
}

pub fn tables() -> ServingTables {
    ServingTables::new(DATABASE).expect("valid database name")
}

/// Workspace A, dataset `fixture`, every fixture log event (all sit at `TS`).
pub fn logs_query() -> Query {
    Query {
        scope: Scope {
            workspace_id: WS_A.into(),
            dataset: DATASET.into(),
        },
        signal: Signal::Logs,
        time: TimeRange {
            start_ns: TS,
            end_ns: TS + 1,
        },
        filters: vec![],
        group_by: vec![],
        aggregate: Aggregate::Count,
    }
}

pub fn spans_query() -> Query {
    Query {
        signal: Signal::Spans,
        ..logs_query()
    }
}

pub fn u64_field(row: &Value, name: &str) -> u64 {
    match &row[name] {
        Value::Number(n) => n.as_u64().expect("u64"),
        Value::String(s) => s.parse().expect("u64 text"),
        other => panic!("{name} is not a count: {other}"),
    }
}
