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
    Aggregate, AttrRef, GroupValue, Operation, Percentile, Predicate, Query, QueryError,
    QueryResult, Scope, Signal, TimeRange, TypedValue, UnsupportedValue, ValueType, interpret,
    plan,
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

        // `latency.ms`: 1..=9 and one outlier, shuffled, in two routes; plus a
        // string under the same key on one event (an incompatible value) and
        // doubles under another key.
        let mut rows: Vec<String> = [7, 1, 100, 3, 9, 2, 8, 4, 6, 5]
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let route = if i % 2 == 0 { "/a" } else { "/b" };
                let ratio = [0.3, 0.1, 0.2][i % 3];
                json!({"dataset": "latency", "event_id": format!("p{i}"), "routing_epoch": 1,
                       "timestamp": ts(START + i as i64),
                       "record_int": {"latency.ms": v},
                       "record_double": {"ratio": ratio},
                       "record_string": {"http.route": route}})
                .to_string()
            })
            .collect();
        rows.push(
            json!({"dataset": "latency", "event_id": "p-text", "routing_epoch": 1,
                   "timestamp": ts(START + 20),
                   "record_string": {"latency.ms": "slow", "http.route": "/c"}})
            .to_string(),
        );
        ch.insert("serving_logs", &rows.join("\n"));
        let nonfinite = json!({"dataset": "nonfinite", "event_id": "n1", "routing_epoch": 1,
                               "timestamp": ts(START), "record_double": {"ratio": "inf"}});
        ch.insert("serving_logs", &nonfinite.to_string());

        // Span durations: three observable, one missing, one reversed.
        let spans: [(Option<i64>, &str); 5] = [
            (Some(300), "ok"),
            (None, "missing"),
            (Some(100), "ok"),
            (None, "end_before_start"),
            (Some(200), "ok"),
        ];
        let rows: Vec<String> = spans
            .iter()
            .enumerate()
            .map(|(i, (duration, status))| {
                json!({"dataset": "spans", "event_id": format!("s{i}"), "routing_epoch": 1,
                       "timestamp": ts(START + i as i64), "duration_ns": duration,
                       "span_timing_status": status})
                .to_string()
            })
            .collect();
        ch.insert("serving_spans", &rows.join("\n"));
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

fn percentiles(list: &[&str]) -> Vec<Percentile> {
    list.iter().map(|p| Percentile::parse(p).unwrap()).collect()
}

fn latency_percentiles(list: &[&str]) -> Aggregate {
    Aggregate::Percentiles {
        attr: AttrRef::record("latency.ms"),
        value_type: ValueType::Int,
        percentiles: percentiles(list),
    }
}

/// `(percentile text, value)` pairs of a single-row result.
fn values(result: &QueryResult, row: usize) -> Vec<(String, Option<TypedValue>)> {
    result.rows[row]
        .values
        .as_ref()
        .unwrap()
        .percentiles
        .as_ref()
        .unwrap()
        .iter()
        .map(|(p, v)| (p.to_string(), v.clone()))
        .collect()
}

fn ints(pairs: &[(&str, i64)]) -> Vec<(String, Option<TypedValue>)> {
    pairs
        .iter()
        .map(|(p, v)| (p.to_string(), Some(TypedValue::Int(*v))))
        .collect()
}

#[test]
fn percentiles_are_exact_nearest_rank() {
    let Some(ch) = ch() else { return };
    let int_filter = Predicate::HasType(AttrRef::record("latency.ms"), ValueType::Int);
    let base = || {
        let mut q = query("latency", Signal::Logs, START, START + 100);
        q.filters = vec![int_filter.clone()];
        q
    };

    // Sorted: 1 2 3 4 5 6 7 8 9 100. Nearest rank gives p50 = 5, where linear
    // interpolation gives 5.5 and ClickHouse's `quantileExact` gives 6; just
    // above p90 the rank moves to the outlier.
    let mut q = base();
    q.aggregate = latency_percentiles(&["0", "50", "90", "90.001", "99", "100"]);
    let result = execute(ch, q).unwrap();
    assert_eq!(result.rows[0].values.as_ref().unwrap().contributing, 10);
    assert_eq!(
        values(&result, 0),
        ints(&[
            ("0", 1),
            ("50", 5),
            ("90", 9),
            ("90.001", 100),
            ("99", 100),
            ("100", 100)
        ])
    );

    // Per group: /a holds 7 100 9 8 6, /b holds 1 3 2 4 5.
    let mut q = base();
    q.group_by = vec![route()];
    q.aggregate = latency_percentiles(&["50", "100"]);
    let result = execute(ch, q).unwrap();
    assert_eq!(result.rows[0].groups, vec![string("/a")]);
    assert_eq!(values(&result, 0), ints(&[("50", 8), ("100", 100)]));
    assert_eq!(result.rows[1].groups, vec![string("/b")]);
    assert_eq!(values(&result, 1), ints(&[("50", 3), ("100", 5)]));

    // Per bucket of 5 ns: offsets 0..=4 hold 7 1 100 3 9, 5..=9 hold 2 8 4 6 5.
    let mut q = base();
    q.time.end_ns = START + 10;
    q.bucket_ns = Some(5);
    q.aggregate = latency_percentiles(&["50"]);
    let result = execute(ch, q).unwrap();
    assert_eq!(result.rows[0].bucket_start_ns, Some(START));
    assert_eq!(values(&result, 0), ints(&[("50", 7)]));
    assert_eq!(result.rows[1].bucket_start_ns, Some(START + 5));
    assert_eq!(values(&result, 1), ints(&[("50", 5)]));

    // Doubles are read back exactly.
    let mut q = query("latency", Signal::Logs, START, START + 100);
    q.aggregate = Aggregate::Percentiles {
        attr: AttrRef::record("ratio"),
        value_type: ValueType::Double,
        percentiles: percentiles(&["50", "100"]),
    };
    let result = execute(ch, q).unwrap();
    assert_eq!(
        values(&result, 0),
        vec![
            ("50".into(), Some(TypedValue::Double(0.2))),
            ("100".into(), Some(TypedValue::Double(0.3))),
        ]
    );

    // Nothing contributing is no value, not zero.
    let mut q = base();
    q.time = TimeRange {
        start_ns: START - 100,
        end_ns: START,
    };
    q.aggregate = latency_percentiles(&["50"]);
    let result = execute(ch, q).unwrap();
    assert_eq!(result.rows[0].values.as_ref().unwrap().contributing, 0);
    assert_eq!(values(&result, 0), vec![("50".to_owned(), None)]);
    assert_eq!(
        result.to_json()["rows"][0]["percentiles"],
        json!([{"percentile": "50", "value": null}])
    );
}

#[test]
fn percentiles_follow_the_typed_value_contract() {
    let Some(ch) = ch() else { return };
    // Without a type filter the string "slow" fails the aggregate.
    let mut q = query("latency", Signal::Logs, START, START + 100);
    q.aggregate = latency_percentiles(&["50"]);
    assert_eq!(
        execute(ch, q).unwrap_err(),
        QueryError::IncompatibleType {
            attr: "latency.ms".into(),
            expected: ValueType::Int,
            found: vec![ValueType::String],
        }
    );
    // A non-finite double fails a double percentile.
    let mut q = query("nonfinite", Signal::Logs, START, START + 100);
    q.aggregate = Aggregate::Percentiles {
        attr: AttrRef::record("ratio"),
        value_type: ValueType::Double,
        percentiles: percentiles(&["50"]),
    };
    assert_eq!(
        execute(ch, q).unwrap_err(),
        QueryError::UnsupportedOperation {
            operation: Operation::Aggregate,
            value: UnsupportedValue::NonFiniteFloat,
        }
    );
}

#[test]
fn span_duration_percentiles_skip_unobservable_durations() {
    let Some(ch) = ch() else { return };
    let mut q = query("spans", Signal::Spans, START, START + 100);
    q.aggregate = Aggregate::SpanDurationPercentiles {
        percentiles: percentiles(&["0", "50", "66.666", "66.667", "100"]),
    };
    let result = execute(ch, q).unwrap();
    let row = &result.rows[0];
    assert_eq!(row.events, 5);
    let stats = row.values.as_ref().unwrap();
    assert_eq!((stats.contributing, stats.timing_errors), (3, Some(1)));
    // Sorted 100 200 300: 66.666% of 3 is rank 2, 66.667% is rank 3.
    assert_eq!(
        values(&result, 0),
        ints(&[
            ("0", 100),
            ("50", 200),
            ("66.666", 200),
            ("66.667", 300),
            ("100", 300)
        ])
    );
    let out = result.to_json();
    assert_eq!(
        out["rows"][0]["timing_errors"],
        json!({"type": "uint64", "value": "1"})
    );
    assert_eq!(out["rows"][0].get("sum"), None::<&Value>);
    assert_eq!(
        out["rows"][0]["percentiles"][1],
        json!({"percentile": "50", "value": {"type": "int", "value": "200"}})
    );
}
