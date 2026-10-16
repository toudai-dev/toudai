//! Contract enforcement end to end: generated SQL executed against the
//! Experiment 021 fixture, then checked by `interpret`.
//!
//! Inventory facts (see `experiment021_sql.rs`): `metric.value` holds 12 ints
//! whose exact total exceeds Int64, 5 doubles including NaN/+Inf/-Inf, 3
//! strings, one bool, bytes, a map, and an explicit empty. Single-event keys
//! on `typed-1` (`int.one`, `str.one`, `bytes.raw`, `empty.value`,
//! `double.half`, `double.nan`) isolate individual type cases.

mod common;

use common::*;
use lighthouse::query::value::Literal as L;
use lighthouse::query::{
    Aggregate, AttrRef, CmpOp, GroupValue, Operation, Predicate, Query, QueryError, QueryResult,
    Sum, TypedValue, UnsupportedValue, ValueType, interpret, plan,
};

const I64_MAX: i128 = i64::MAX as i128;

type SumCase = (
    &'static str,
    Vec<Predicate>,
    Result<Option<i64>, QueryError>,
    u64,
);
type GroupCase = (&'static str, ValueType, Vec<(GroupValue, u64)>);

fn metric() -> AttrRef {
    AttrRef::record("metric.value")
}

fn cmp(attr: AttrRef, op: CmpOp, value: L) -> Predicate {
    Predicate::Compare { attr, op, value }
}

fn execute(ch: &ClickHouse, query: Query) -> Result<QueryResult, QueryError> {
    let planned = plan(query, &tables())?;
    interpret(&planned, &ch.run(&planned.statement))
}

fn sum(attr: AttrRef, value_type: ValueType) -> Aggregate {
    Aggregate::Sum { attr, value_type }
}

fn single(result: QueryResult) -> lighthouse::query::ResultRow {
    assert_eq!(result.rows.len(), 1);
    result.rows.into_iter().next().unwrap()
}

#[test]
fn integer_sums_are_exact_or_rejected() {
    let Some(ch) = clickhouse() else { return };
    let ints: [i128; 12] = [
        1,
        0,
        9,
        10,
        100,
        9_007_199_254_740_993,
        i64::MIN as i128,
        I64_MAX,
        I64_MAX,
        1,
        1,
        2,
    ];
    let int_total: i128 = ints.iter().sum();
    let double_max = 2 * I64_MAX;

    let cases: Vec<SumCase> = vec![
        (
            "0..=2 sums exactly",
            vec![
                cmp(metric(), CmpOp::Ge, L::Int(0)),
                cmp(metric(), CmpOp::Le, L::Int(2)),
            ],
            Ok(Some(5)),
            5,
        ),
        (
            "9..=100",
            vec![
                cmp(metric(), CmpOp::Ge, L::Int(9)),
                cmp(metric(), CmpOp::Le, L::Int(100)),
            ],
            Ok(Some(119)),
            3,
        ),
        (
            "2^53 + 1 is exact",
            vec![cmp(metric(), CmpOp::Eq, L::Int(9_007_199_254_740_993))],
            Ok(Some(9_007_199_254_740_993)),
            1,
        ),
        (
            "i64::MIN alone fits",
            vec![cmp(metric(), CmpOp::Lt, L::Int(0))],
            Ok(Some(i64::MIN)),
            1,
        ),
        // Nothing contributes: the sum is absent, not zero.
        (
            "no matching value",
            vec![cmp(metric(), CmpOp::Eq, L::Int(12345))],
            Ok(None),
            0,
        ),
        // Int64 `sum` wraps these to -2 and a different negative number.
        (
            "2 x i64::MAX is rejected",
            vec![cmp(metric(), CmpOp::Eq, L::Int(i64::MAX))],
            Err(QueryError::IntegerOverflow { total: double_max }),
            0,
        ),
        (
            "all ints are rejected",
            vec![Predicate::HasType(metric(), ValueType::Int)],
            Err(QueryError::IntegerOverflow { total: int_total }),
            0,
        ),
    ];
    for (name, filters, expected, contributing) in cases {
        let mut q = logs_query();
        q.filters = filters;
        q.aggregate = sum(metric(), ValueType::Int);
        match (execute(ch, q), expected) {
            (Ok(result), Ok(expected)) => {
                let row = single(result);
                let stats = row.values.unwrap();
                assert_eq!(stats.sum, expected.map(Sum::Int), "{name}");
                assert_eq!(stats.contributing, contributing, "{name}");
            }
            (got, expected) => assert_eq!(got.map(|_| ()), expected.map(|_| ()), "{name}"),
        }
    }
}

#[test]
fn contributing_counts_are_not_event_counts() {
    let Some(ch) = clickhouse() else { return };
    let mut q = logs_query();
    q.filters = vec![
        cmp(metric(), CmpOp::Ge, L::Int(9)),
        cmp(metric(), CmpOp::Le, L::Int(100)),
    ];
    q.aggregate = sum(metric(), ValueType::Int);
    let row = single(execute(ch, q).unwrap());
    assert_eq!((row.events, row.values.unwrap().contributing), (3, 3));

    // 27 events, 12 of which hold an int: missing and other-typed values do
    // not contribute and are not zero (Experiment 019 24 present / 12 int).
    let mut q = logs_query();
    q.aggregate = Aggregate::CountValues {
        attr: metric(),
        value_type: ValueType::Int,
    };
    let row = single(execute(ch, q).unwrap());
    assert_eq!((row.events, row.values.unwrap().contributing), (27, 12));

    // Explicit empty is present but contributes nothing, and is not an error.
    let mut q = logs_query();
    q.aggregate = sum(AttrRef::record("empty.value"), ValueType::Int);
    let row = single(execute(ch, q).unwrap());
    let stats = row.values.unwrap();
    assert_eq!((row.events, stats.contributing, stats.sum), (27, 0, None));
}

#[test]
fn incompatible_values_are_errors_unless_filtered_out() {
    let Some(ch) = clickhouse() else { return };
    let unsupported = |value| QueryError::UnsupportedOperation {
        operation: Operation::Aggregate,
        value,
    };
    // (name, key, declared type, expected error)
    let cases: Vec<(&str, &str, ValueType, QueryError)> = vec![
        // typed-1 alone holds `str.one` = "1": a string is not an int, and is
        // not silently parsed.
        (
            "string \"1\" under an int sum",
            "str.one",
            ValueType::Int,
            QueryError::IncompatibleType {
                attr: "str.one".into(),
                expected: ValueType::Int,
                found: vec![ValueType::String],
            },
        ),
        (
            "double 1.0 under an int sum",
            "double.one",
            ValueType::Int,
            QueryError::IncompatibleType {
                attr: "double.one".into(),
                expected: ValueType::Int,
                found: vec![ValueType::Double],
            },
        ),
        (
            "int 1 under a double sum",
            "int.one",
            ValueType::Double,
            QueryError::IncompatibleType {
                attr: "int.one".into(),
                expected: ValueType::Double,
                found: vec![ValueType::Int],
            },
        ),
        (
            "bytes",
            "bytes.raw",
            ValueType::Int,
            unsupported(UnsupportedValue::Bytes),
        ),
        (
            "array",
            "array.mixed",
            ValueType::Int,
            unsupported(UnsupportedValue::Array),
        ),
        (
            "map",
            "map.nested",
            ValueType::Int,
            unsupported(UnsupportedValue::Map),
        ),
        // The mixed `metric.value` key fails on its canonical-only values first.
        (
            "mixed key",
            "metric.value",
            ValueType::Int,
            unsupported(UnsupportedValue::Bytes),
        ),
        (
            "NaN under a double sum",
            "double.nan",
            ValueType::Double,
            unsupported(UnsupportedValue::NonFiniteFloat),
        ),
        (
            "+Inf under a double sum",
            "double.posinf",
            ValueType::Double,
            unsupported(UnsupportedValue::NonFiniteFloat),
        ),
    ];
    for (name, key, value_type, expected) in cases {
        let mut q = logs_query();
        q.aggregate = sum(AttrRef::record(key), value_type);
        assert_eq!(execute(ch, q).map(|_| ()), Err(expected), "{name}");
    }

    // An explicit type filter excludes the incompatible values.
    let mut q = logs_query();
    q.filters = vec![
        Predicate::HasType(metric(), ValueType::Int),
        cmp(metric(), CmpOp::Eq, L::Int(1)),
    ];
    q.aggregate = sum(metric(), ValueType::Int);
    let stats = single(execute(ch, q).unwrap()).values.unwrap();
    assert_eq!((stats.contributing, stats.sum), (3, Some(Sum::Int(3))));

    // Finite floats sum; the rows with non-finite values were filtered by type
    // and range, and the non-finite ones cannot be summed.
    let mut q = logs_query();
    q.aggregate = sum(AttrRef::record("double.half"), ValueType::Double);
    let stats = single(execute(ch, q).unwrap()).values.unwrap();
    assert_eq!((stats.contributing, stats.sum), (1, Some(Sum::Double(1.5))));

    let mut q = logs_query();
    q.filters = vec![Predicate::HasType(metric(), ValueType::Double)];
    q.aggregate = sum(metric(), ValueType::Double);
    assert_eq!(
        execute(ch, q).map(|_| ()),
        Err(unsupported(UnsupportedValue::NonFiniteFloat))
    );

    let mut q = logs_query();
    q.filters = vec![
        cmp(metric(), CmpOp::Gt, L::Double(0.5)),
        cmp(metric(), CmpOp::Lt, L::Double(2.0)),
    ];
    q.aggregate = sum(metric(), ValueType::Double);
    let stats = single(execute(ch, q).unwrap()).values.unwrap();
    assert_eq!((stats.contributing, stats.sum), (1, Some(Sum::Double(1.0))));
}

#[test]
fn grouping_rejects_unsupported_values_unless_filtered_out() {
    let Some(ch) = clickhouse() else { return };
    let unsupported = |value| QueryError::UnsupportedOperation {
        operation: Operation::Group,
        value,
    };

    // The whole mixed key contains bytes, a map, and non-finite doubles.
    let mut q = logs_query();
    q.group_by = vec![metric()];
    assert_eq!(
        execute(ch, q).map(|_| ()),
        Err(unsupported(UnsupportedValue::Bytes))
    );

    let mut q = logs_query();
    q.filters = vec![Predicate::HasType(metric(), ValueType::Double)];
    q.group_by = vec![metric()];
    assert_eq!(
        execute(ch, q).map(|_| ()),
        Err(unsupported(UnsupportedValue::NonFiniteFloat))
    );

    // Per type, the groups are typed and exact (Experiment 019 `group_by_type_value`).
    let int = |v: i64| GroupValue::Value(TypedValue::Int(v));
    let cases: Vec<GroupCase> = vec![
        (
            "ints",
            ValueType::Int,
            vec![
                (int(i64::MIN), 1),
                (int(0), 1),
                (int(1), 3),
                (int(10), 1),
                (int(100), 1),
                (int(2), 1),
                (int(9), 1),
                (int(9_007_199_254_740_993), 1),
                (int(i64::MAX), 2),
            ],
        ),
        (
            "strings keep \"1\" and \"\" apart from numbers",
            ValueType::String,
            vec![
                (GroupValue::Value(TypedValue::String(String::new())), 1),
                (GroupValue::Value(TypedValue::String("1".into())), 1),
                (GroupValue::Value(TypedValue::String("3".into())), 1),
            ],
        ),
        (
            "false",
            ValueType::Bool,
            vec![(GroupValue::Value(TypedValue::Bool(false)), 1)],
        ),
        (
            "explicit empty",
            ValueType::Empty,
            vec![(GroupValue::Value(TypedValue::Empty), 1)],
        ),
    ];
    for (name, ty, expected) in cases {
        let mut q = logs_query();
        q.filters = vec![Predicate::HasType(metric(), ty)];
        q.group_by = vec![metric()];
        let got: Vec<(GroupValue, u64)> = execute(ch, q)
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .rows
            .into_iter()
            .map(|r| (r.groups[0].clone(), r.events))
            .collect();
        assert_eq!(got, expected, "{name}");
    }

    // Finite doubles: 1.0 only, as a double.
    let mut q = logs_query();
    q.filters = vec![
        cmp(metric(), CmpOp::Gt, L::Double(0.5)),
        cmp(metric(), CmpOp::Lt, L::Double(2.0)),
    ];
    q.group_by = vec![metric()];
    let rows = execute(ch, q).unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].groups,
        vec![GroupValue::Value(TypedValue::Double(1.0))]
    );

    // Missing is its own group.
    let mut q = logs_query();
    q.filters = vec![Predicate::Absent(metric())];
    q.group_by = vec![metric()];
    let rows = execute(ch, q).unwrap().rows;
    assert_eq!(
        (rows[0].groups.clone(), rows[0].events),
        (vec![GroupValue::Missing], 3)
    );
}

#[test]
fn span_duration_counts_follow_the_experiment_021_contract() {
    let Some(ch) = clickhouse() else { return };
    let mut q = spans_query();
    q.time.start_ns = 0;
    q.aggregate = Aggregate::SpanDuration;
    let row = single(execute(ch, q).unwrap());
    let stats = row.values.unwrap();
    // 9 spans: 4 contributing (the zero duration included), 5 excluded, 2 timing errors.
    assert_eq!(row.events, 9);
    assert_eq!(stats.contributing, 4);
    assert_eq!(row.events - stats.contributing, 5);
    assert_eq!(stats.timing_errors, Some(2));
    // span-1, span-scope, the zero-duration span, and timing-mixed-valid.
    let durations: [i64; 4] = [1500, 10, 0, 2500];
    assert_eq!(stats.sum, Some(Sum::Int(durations.iter().sum())));
}

#[test]
fn plan_diagnostics_travel_with_the_result() {
    let Some(ch) = clickhouse() else { return };
    let mut q = logs_query();
    q.filters = vec![
        Predicate::HasType(metric(), ValueType::Int),
        cmp(metric(), CmpOp::Le, L::Int(2)),
    ];
    q.aggregate = sum(metric(), ValueType::Int);
    let json = execute(ch, q).unwrap_or_else(|e| panic!("{e}")).to_json();
    assert_eq!(json["diagnostics"]["storage_path"], "raw");
    assert_eq!(json["diagnostics"]["approximation"], "exact");
    assert_eq!(json["diagnostics"]["table"], "serving_logs");
    // The explicit type filter lets the sum read the int Map alone.
    assert_eq!(
        json["diagnostics"]["columns"],
        serde_json::json!(["dataset", "record_int", "ts", "workspace_id"])
    );
}
