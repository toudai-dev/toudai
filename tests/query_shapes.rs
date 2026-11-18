//! Query shapes beyond filter, group, count, and sum, executed under
//! `clickhouse local` against small purpose-built rows rather than the
//! Experiment 021 fixture (whose events all share one timestamp). Each dataset
//! below holds only the rows its test inserts, and expectations are written
//! out by hand from those rows.

mod common;

use std::sync::Once;

use common::*;
use serde_json::{Value, json};
use toudai::query::{
    AttrRef, GroupValue, Predicate, Query, QueryError, QueryResult, Scope, Signal, TimeRange,
    TypedValue, ValueType, interpret, plan,
};

/// Bucket origin: deliberately not a multiple of the 10 ns width, so buckets
/// aligned to the epoch instead of the range start would split differently.
const START: i64 = TS + 3;

fn ts(ns: i64) -> String {
    // `DateTime64(9)` accepts a decimal Unix timestamp.
    format!(
        "{}.{:09}",
        ns.div_euclid(1_000_000_000),
        ns.rem_euclid(1_000_000_000)
    )
}

/// The rows every test reads, inserted once per test binary.
fn ch() -> Option<&'static ClickHouse> {
    static LOADED: Once = Once::new();
    let ch = clickhouse()?;
    LOADED.call_once(|| {
        // (offset from START, route)
        let logs: [(i64, &str); 7] = [
            (-1, "/before"),
            (0, "/a"),
            (9, "/b"),
            (10, "/a"),
            (25, "/a"),
            (29, "/b"),
            (30, "/after"),
        ];
        let rows: Vec<String> = logs
            .iter()
            .enumerate()
            .map(|(i, (offset, route))| {
                json!({"dataset": "buckets", "event_id": format!("l{i}"), "routing_epoch": 1,
                       "timestamp": ts(START + offset),
                       "record_string": {"http.route": route}})
                .to_string()
            })
            .collect();
        ch.insert("serving_logs", &rows.join("\n"));
    });
    Some(ch)
}

fn query(dataset: &str, signal: Signal, start_ns: i64, end_ns: i64) -> Query {
    Query {
        scope: Scope {
            dataset: dataset.into(),
        },
        signal,
        time: TimeRange { start_ns, end_ns },
        ..logs_query()
    }
}

fn execute(ch: &ClickHouse, query: Query) -> Result<QueryResult, QueryError> {
    let planned = plan(query, &tables())?;
    interpret(&planned, &ch.run(&planned.statement), &[])
}

fn route() -> AttrRef {
    AttrRef::record("http.route")
}

fn string(value: &str) -> GroupValue {
    GroupValue::Value(TypedValue::String(value.into()))
}

#[test]
fn time_buckets_are_half_open_and_start_at_the_range_start() {
    let Some(ch) = ch() else { return };
    type Case = (&'static str, i64, i64, Vec<(i64, u64)>);
    let cases: Vec<Case> = vec![
        (
            "the boundary event opens the next bucket; the end is excluded",
            START,
            START + 30,
            vec![(START, 2), (START + 10, 1), (START + 20, 2)],
        ),
        (
            "the last bucket is clipped to the range end",
            START,
            START + 26,
            vec![(START, 2), (START + 10, 1), (START + 20, 1)],
        ),
        (
            "a bucket without events has no row",
            START + 10,
            START + 30,
            vec![(START + 10, 1), (START + 20, 2)],
        ),
        (
            "an earlier origin shifts every bucket",
            START - 1,
            START + 30,
            vec![
                (START - 1, 2),
                (START + 9, 2),
                (START + 19, 1),
                (START + 29, 1),
            ],
        ),
    ];
    for (name, start, end, expected) in cases {
        let mut q = query("buckets", Signal::Logs, start, end);
        q.bucket_ns = Some(10);
        let result = execute(ch, q).unwrap();
        let got: Vec<(i64, u64)> = result
            .rows
            .iter()
            .map(|row| (row.bucket_start_ns.unwrap(), row.events))
            .collect();
        assert_eq!(got, expected, "{name}");
    }

    // Grouped buckets order by bucket, then by group key.
    let mut q = query("buckets", Signal::Logs, START, START + 30);
    q.bucket_ns = Some(20);
    q.group_by = vec![route()];
    let got: Vec<(i64, GroupValue, u64)> = execute(ch, q)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| {
            (
                row.bucket_start_ns.unwrap(),
                row.groups[0].clone(),
                row.events,
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (START, string("/a"), 2),
            (START, string("/b"), 1),
            (START + 20, string("/a"), 1),
            (START + 20, string("/b"), 1),
        ]
    );
}

#[test]
fn bucket_responses_carry_typed_bucket_starts() {
    let Some(ch) = ch() else { return };
    let mut q = query("buckets", Signal::Logs, START, START + 10);
    q.bucket_ns = Some(10);
    q.filters = vec![Predicate::HasType(route(), ValueType::String)];
    let out = execute(ch, q).unwrap().to_json();
    assert_eq!(
        out["rows"],
        json!([{
            "bucket_start_ns": {"type": "int", "value": START.to_string()},
            "groups": [],
            "events": {"type": "uint64", "value": "2"},
        }])
    );
    // Without a bucket width the row has no bucket.
    let q = query("buckets", Signal::Logs, START, START + 10);
    let out = execute(ch, q).unwrap().to_json();
    assert_eq!(out["rows"][0].get("bucket_start_ns"), None::<&Value>);
}
