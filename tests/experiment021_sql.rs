//! Generated SQL against the Experiment 021 fixture.
//!
//! Expected values are derived from the fixture inventory
//! (`harness/fixture021.py`, which imports Experiment 019's `fixture.py`) and
//! cross-checked against the 019/021 `results/checks-*.jsonl` expectations the
//! comments cite; none of them are read back from the code under test.
//!
//! The fixture's `metric.value` key holds one value per event (24 of the 27
//! workspace A log events carry it): 12 int, 5 double (1.0, -0.0, NaN, +Inf,
//! -Inf), 3 string ("1", "", "3"), 1 bool (false), 1 each of bytes, map, and
//! explicit empty. Events `typed-1`, `group-missing`, and `no-service-name`
//! have no `metric.value`.

mod common;

use common::*;
use serde_json::Value;
use toudai::query::{
    Aggregate, AttrRef, CmpOp, Predicate, Query, QueryError, ValueType, plan,
    value::Literal::{self as L},
};

const I64_MAX: i64 = i64::MAX;

fn metric() -> AttrRef {
    AttrRef::record("metric.value")
}

fn cmp(attr: AttrRef, op: CmpOp, value: L) -> Predicate {
    Predicate::Compare { attr, op, value }
}

fn run(ch: &ClickHouse, query: Query) -> Vec<Value> {
    let planned = plan(query, &tables()).expect("valid request");
    ch.run(&planned.statement)
}

fn events(ch: &ClickHouse, query: Query) -> u64 {
    let rows = run(ch, query);
    assert_eq!(rows.len(), 1, "an ungrouped aggregate returns one row");
    u64_field(&rows[0], "events")
}

#[test]
fn fixture_loaded() {
    let Some(ch) = clickhouse() else { return };
    // 28 inventory events minus the duplicate-key record the owned exporter
    // rejects (`dup-1`), in each of two workspaces.
    let per_workspace = ch.raw(
        &format!(
            "SELECT tenant_id, count() FROM {DATABASE}.serving_logs GROUP BY tenant_id ORDER BY tenant_id"
        ),
        &[],
    );
    assert_eq!(per_workspace, format!("{WS_A}\t27\n{WS_B}\t27\n"));
}

#[test]
fn type_matched_predicates_do_not_coerce() {
    let Some(ch) = clickhouse() else { return };
    let m = metric;
    let cases: Vec<(&str, Vec<Predicate>, u64)> = vec![
        // int 1, double 1.0, and string "1" are three different values:
        // Experiment 019 `typed_filter_{int,double,string}` = 3 / 1 / 1.
        (
            "int 1: group-int-1, sum-c, sum-small-a",
            vec![cmp(m(), CmpOp::Eq, L::Int(1))],
            3,
        ),
        (
            "double 1.0: group-double-1 only",
            vec![cmp(m(), CmpOp::Eq, L::Double(1.0))],
            1,
        ),
        (
            "string \"1\": group-string-1 only",
            vec![cmp(m(), CmpOp::Eq, L::String("1".into()))],
            1,
        ),
        // A missing key reads as the type default in ClickHouse; zero, false,
        // and empty string must match only values actually received.
        (
            "int 0: not the missing key, false, -0.0, or \"\"",
            vec![cmp(m(), CmpOp::Eq, L::Int(0))],
            1,
        ),
        (
            "bool false: not 0 or the missing key",
            vec![cmp(m(), CmpOp::Eq, L::Bool(false))],
            1,
        ),
        (
            "empty string: not explicit empty or missing",
            vec![cmp(m(), CmpOp::Eq, L::String("".into()))],
            1,
        ),
        (
            "double 0.0 equals the received -0.0",
            vec![cmp(m(), CmpOp::Eq, L::Double(0.0))],
            1,
        ),
        // Ne is also type-matched: strings, doubles, and the rest never match.
        (
            "int != 1: the other 9 ints only",
            vec![cmp(m(), CmpOp::Ne, L::Int(1))],
            9,
        ),
        // Numeric, not lexical, ordering (lexically "10" < "9").
        (
            "int > 9: order-10, order-100, big-int, sum-a, sum-b",
            vec![cmp(m(), CmpOp::Gt, L::Int(9))],
            5,
        ),
        (
            "int < 9: min-int, 0, 1, 1, 1, 2",
            vec![cmp(m(), CmpOp::Lt, L::Int(9))],
            6,
        ),
        (
            "9 <= int <= 100: order-9, order-10, order-100",
            vec![
                cmp(m(), CmpOp::Ge, L::Int(9)),
                cmp(m(), CmpOp::Le, L::Int(100)),
            ],
            3,
        ),
        // 2^53 + 1 is not representable as a double; an exact match proves no
        // float round trip.
        (
            "int == 2^53 + 1 matches only that value",
            vec![cmp(m(), CmpOp::Eq, L::Int(9_007_199_254_740_993))],
            1,
        ),
        (
            "int == 2^53 does not match 2^53 + 1",
            vec![cmp(m(), CmpOp::Eq, L::Int(9_007_199_254_740_992))],
            0,
        ),
        (
            "int == i64::MIN",
            vec![cmp(m(), CmpOp::Eq, L::Int(i64::MIN))],
            1,
        ),
        // Non-finite stored floats never satisfy a numeric comparison.
        (
            "double > 0.5: +Inf excluded",
            vec![cmp(m(), CmpOp::Gt, L::Double(0.5))],
            1,
        ),
        (
            "double < 0.5: -Inf excluded, -0.0 included",
            vec![cmp(m(), CmpOp::Lt, L::Double(0.5))],
            1,
        ),
        (
            "string > \"1\": only \"3\"",
            vec![cmp(m(), CmpOp::Gt, L::String("1".into()))],
            1,
        ),
    ];
    for (name, filters, expected) in cases {
        let mut q = logs_query();
        q.filters = filters;
        assert_eq!(events(ch, q), expected, "{name}");
    }
}

#[test]
fn presence_is_key_membership() {
    let Some(ch) = clickhouse() else { return };
    let rec = AttrRef::record;
    let cases: Vec<(&str, Vec<Predicate>, u64)> = vec![
        (
            "metric.value is present on 24 events",
            vec![Predicate::Exists(metric())],
            24,
        ),
        // group-missing, typed-1, and no-service-name have no metric.value.
        (
            "metric.value is absent on 3 events",
            vec![Predicate::Absent(metric())],
            3,
        ),
        (
            "explicit empty is present",
            vec![Predicate::Exists(rec("empty.value"))],
            1,
        ),
        (
            "empty string is present",
            vec![Predicate::Exists(rec("str.empty"))],
            1,
        ),
        (
            "numeric zero is present",
            vec![Predicate::Exists(rec("zero.int"))],
            1,
        ),
        (
            "false is present",
            vec![Predicate::Exists(rec("bool.false"))],
            1,
        ),
        (
            "bytes are present (key-only Map)",
            vec![Predicate::Exists(rec("bytes.raw"))],
            1,
        ),
        (
            "never-sent key is absent everywhere",
            vec![Predicate::Absent(rec("no.such.key"))],
            27,
        ),
        // Type predicates: counts match Experiment 019 `type_counts`.
        (
            "has type int",
            vec![Predicate::HasType(metric(), ValueType::Int)],
            12,
        ),
        (
            "has type double",
            vec![Predicate::HasType(metric(), ValueType::Double)],
            5,
        ),
        (
            "has type string",
            vec![Predicate::HasType(metric(), ValueType::String)],
            3,
        ),
        (
            "has type bool",
            vec![Predicate::HasType(metric(), ValueType::Bool)],
            1,
        ),
        (
            "has type bytes",
            vec![Predicate::HasType(metric(), ValueType::Bytes)],
            1,
        ),
        (
            "has type map",
            vec![Predicate::HasType(metric(), ValueType::Map)],
            1,
        ),
        (
            "has type empty",
            vec![Predicate::HasType(metric(), ValueType::Empty)],
            1,
        ),
        (
            "has type array",
            vec![Predicate::HasType(metric(), ValueType::Array)],
            0,
        ),
        // Namespaces and literal keys.
        (
            "record has no service.name",
            vec![Predicate::Exists(rec("service.name"))],
            0,
        ),
        (
            "resource service.name on all but no-service-name",
            vec![Predicate::Exists(AttrRef::resource("service.name"))],
            26,
        ),
        (
            "keys are case-sensitive",
            vec![Predicate::Exists(rec("foo"))],
            0,
        ),
        ("record Foo", vec![Predicate::Exists(rec("Foo"))], 1),
        (
            "resource Foo and foo are distinct keys",
            vec![Predicate::Exists(AttrRef::resource("foo"))],
            26,
        ),
        (
            "a dot does not nest: nested.int lives inside a map value",
            vec![Predicate::Exists(rec("nested.int"))],
            0,
        ),
        (
            "scope namespace",
            vec![Predicate::Exists(AttrRef::scope("scope.int"))],
            27,
        ),
    ];
    for (name, filters, expected) in cases {
        let mut q = logs_query();
        q.filters = filters;
        assert_eq!(events(ch, q), expected, "{name}");
    }
}

#[test]
fn namespaces_do_not_overwrite_each_other() {
    let Some(ch) = clickhouse() else { return };
    // `collide` is "record-value" in the record namespace of typed-1;
    // `res.collide` is "resource-value" in every resource.
    let cases: Vec<(&str, Predicate, u64)> = vec![
        (
            "record collide",
            cmp(
                AttrRef::record("collide"),
                CmpOp::Eq,
                L::String("record-value".into()),
            ),
            1,
        ),
        (
            "record collide is not the resource value",
            cmp(
                AttrRef::record("collide"),
                CmpOp::Eq,
                L::String("resource-value".into()),
            ),
            0,
        ),
        (
            "resource res.collide",
            cmp(
                AttrRef::resource("res.collide"),
                CmpOp::Eq,
                L::String("resource-value".into()),
            ),
            26,
        ),
        (
            "resource Foo",
            cmp(
                AttrRef::resource("Foo"),
                CmpOp::Eq,
                L::String("resource-upper".into()),
            ),
            26,
        ),
        (
            "record Foo",
            cmp(
                AttrRef::record("Foo"),
                CmpOp::Eq,
                L::String("record-upper".into()),
            ),
            1,
        ),
        // Resource values are text with a companion type: equality is still
        // type-matched, so the int 2^53+1 is not the string of its digits.
        (
            "resource int equality",
            cmp(
                AttrRef::resource("res.int64"),
                CmpOp::Eq,
                L::Int(9_007_199_254_740_993),
            ),
            26,
        ),
        (
            "resource int is not a string",
            cmp(
                AttrRef::resource("res.int64"),
                CmpOp::Eq,
                L::String("9007199254740993".into()),
            ),
            0,
        ),
        (
            "resource string \"1\" is not int 1",
            cmp(AttrRef::resource("res.str_one"), CmpOp::Eq, L::Int(1)),
            0,
        ),
        (
            "resource string \"1\"",
            cmp(
                AttrRef::resource("res.str_one"),
                CmpOp::Eq,
                L::String("1".into()),
            ),
            26,
        ),
        (
            "resource bool",
            cmp(AttrRef::resource("res.bool"), CmpOp::Eq, L::Bool(true)),
            26,
        ),
        (
            "resource empty string is not explicit empty",
            cmp(
                AttrRef::resource("res.empty"),
                CmpOp::Eq,
                L::String("".into()),
            ),
            0,
        ),
        (
            "resource empty string",
            cmp(
                AttrRef::resource("res.empty_string"),
                CmpOp::Eq,
                L::String("".into()),
            ),
            26,
        ),
        (
            "scope int",
            cmp(AttrRef::scope("scope.int"), CmpOp::Eq, L::Int(7)),
            27,
        ),
        (
            "resource has type empty",
            Predicate::HasType(AttrRef::resource("res.empty"), ValueType::Empty),
            26,
        ),
        (
            "resource has type bytes",
            Predicate::HasType(AttrRef::resource("res.bytes"), ValueType::Bytes),
            26,
        ),
        (
            "resource float is typed double",
            Predicate::HasType(AttrRef::resource("res.float"), ValueType::Double),
            26,
        ),
    ];
    for (name, filter, expected) in cases {
        let mut q = logs_query();
        q.filters = vec![filter];
        assert_eq!(events(ch, q), expected, "{name}");
    }
}

#[test]
fn scope_and_time_bounds_are_enforced() {
    let Some(ch) = clickhouse() else { return };
    // (workspace, dataset, [start, end), expected events)
    let cases: Vec<(&str, &str, i64, i64, u64)> = vec![
        (WS_A, DATASET, TS, TS + 1, 27),
        (WS_B, DATASET, TS, TS + 1, 27),
        (
            "99999999-9999-4999-8999-999999999999",
            DATASET,
            TS,
            TS + 1,
            0,
        ),
        (WS_A, "forged-dataset", TS, TS + 1, 0),
        // Half-open: the event at exactly TS is in [TS, TS+1) but not in
        // [TS-1, TS) or [TS+1, TS+2).
        (WS_A, DATASET, TS - 1, TS, 0),
        (WS_A, DATASET, TS + 1, TS + 2, 0),
        (WS_A, DATASET, TS - 1, TS + 1, 27),
    ];
    for (workspace, dataset, start_ns, end_ns, expected) in cases {
        let mut q = logs_query();
        q.scope.workspace_id = workspace.into();
        q.scope.dataset = dataset.into();
        q.time.start_ns = start_ns;
        q.time.end_ns = end_ns;
        assert_eq!(
            events(ch, q),
            expected,
            "{workspace} {dataset} [{start_ns}, {end_ns})"
        );
    }

    // The copied workspace-B fixture must carry B in both authenticated scope
    // and resource metadata; a mislabeled copy would weaken the isolation control.
    let mut q = logs_query();
    q.scope.workspace_id = WS_B.into();
    q.filters = vec![cmp(
        AttrRef::resource("toudai.workspace_id"),
        CmpOp::Eq,
        L::String(WS_B.into()),
    )];
    assert_eq!(events(ch, q.clone()), 27);
    q.filters = vec![cmp(
        AttrRef::resource("toudai.workspace_id"),
        CmpOp::Eq,
        L::String(WS_A.into()),
    )];
    assert_eq!(events(ch, q), 0);

    let mut q = spans_query();
    q.scope.workspace_id = WS_B.into();
    q.filters = vec![cmp(
        AttrRef::resource("toudai.workspace_id"),
        CmpOp::Eq,
        L::String(WS_B.into()),
    )];
    assert_eq!(events(ch, q), 7);
}

#[test]
fn request_values_never_change_the_statement() {
    let Some(ch) = clickhouse() else { return };
    // Keys and string literals are parameters: quotes, backslashes, tabs, and
    // newlines neither break the statement nor match anything.
    for hostile in [
        "'; DROP TABLE serving_logs; --",
        "a\\b",
        "tab\there",
        "line\nbreak",
        "\\' OR 1=1 --",
    ] {
        let mut q = logs_query();
        q.filters = vec![
            Predicate::Exists(AttrRef::record(hostile)),
            cmp(
                AttrRef::record(hostile),
                CmpOp::Eq,
                L::String(hostile.into()),
            ),
        ];
        assert_eq!(events(ch, q), 0, "{hostile:?}");
    }
    let mut q = logs_query();
    q.filters = vec![Predicate::Exists(metric())];
    assert_eq!(events(ch, q), 24, "serving_logs survived");
}

#[test]
fn group_keys_include_type() {
    let Some(ch) = clickhouse() else { return };
    let mut q = logs_query();
    q.group_by = vec![metric()];
    let rows = run(ch, q);
    let got: Vec<(String, String, u64)> = rows
        .iter()
        .map(|r| {
            (
                r["g0_type"].as_str().unwrap().to_owned(),
                r["g0_value"].as_str().unwrap().to_owned(),
                u64_field(r, "events"),
            )
        })
        .collect();
    // Experiment 019 `group_by_type_value`/`group_by_literals` (int 1 x3,
    // double NaN, string "", empty, bytes, bool false), with ClickHouse's text
    // for non-finite doubles. Groups order by (type, text), not numerically.
    let expected: Vec<(&str, &str, u64)> = vec![
        ("bool", "false", 1),
        ("bytes", "", 1),
        ("double", "-0", 1),
        ("double", "-inf", 1),
        ("double", "1", 1),
        ("double", "inf", 1),
        ("double", "nan", 1),
        ("empty", "", 1),
        ("int", "-9223372036854775808", 1),
        ("int", "0", 1),
        ("int", "1", 3),
        ("int", "10", 1),
        ("int", "100", 1),
        ("int", "2", 1),
        ("int", "9", 1),
        ("int", "9007199254740993", 1),
        ("int", "9223372036854775807", 2),
        ("map", "", 1),
        // Missing is its own group: not empty, not empty string, not zero.
        ("missing", "", 3),
        ("string", "", 1),
        ("string", "1", 1),
        ("string", "3", 1),
    ];
    let expected: Vec<(String, String, u64)> = expected
        .into_iter()
        .map(|(t, v, n)| (t.to_owned(), v.to_owned(), n))
        .collect();
    assert_eq!(got, expected);
    assert_eq!(
        got.iter().map(|g| g.2).sum::<u64>(),
        27,
        "every event is in exactly one group"
    );
}

#[test]
fn group_by_resource_attribute_and_multiple_keys() {
    let Some(ch) = clickhouse() else { return };
    let mut q = logs_query();
    q.group_by = vec![AttrRef::resource("res.int64")];
    let rows = run(ch, q);
    let got: Vec<(&str, &str, u64)> = rows
        .iter()
        .map(|r| {
            (
                r["g0_type"].as_str().unwrap(),
                r["g0_value"].as_str().unwrap(),
                u64_field(r, "events"),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![("int", "9007199254740993", 26), ("missing", "", 1)]
    );

    // Two genuinely varying keys: the three events missing metric.value split
    // into one with int.one and two where int.one is also missing.
    let mut q = logs_query();
    q.group_by = vec![metric(), AttrRef::record("int.one")];
    let rows = run(ch, q);
    assert!(rows.iter().any(|r| r["g0_type"] == "missing"
        && r["g1_type"] == "int"
        && u64_field(r, "events") == 1));
    assert!(rows.iter().any(|r| r["g0_type"] == "missing"
        && r["g1_type"] == "missing"
        && u64_field(r, "events") == 2));
}

#[test]
fn counts_report_contributing_values_apart_from_events() {
    let Some(ch) = clickhouse() else { return };
    // (declared type, contributing events) over 27 events: Experiment 019
    // `aggregate_contributing_counts` is 24 present / 12 int.
    for (value_type, contributing) in [
        (ValueType::Int, 12),
        (ValueType::Double, 5),
        (ValueType::String, 3),
        (ValueType::Bool, 1),
    ] {
        let mut q = logs_query();
        q.aggregate = Aggregate::CountValues {
            attr: metric(),
            value_type,
        };
        let rows = run(ch, q);
        assert_eq!(
            u64_field(&rows[0], "events"),
            27,
            "{value_type:?}: events are not values"
        );
        assert_eq!(
            u64_field(&rows[0], "contributing"),
            contributing,
            "{value_type:?}"
        );
    }
}

#[test]
fn raw_sum_columns() {
    let Some(ch) = clickhouse() else { return };
    // Independent oracle: the twelve int `metric.value`s in i128.
    let ints: [i128; 12] = [
        1,
        0,
        9,
        10,
        100,
        9_007_199_254_740_993,
        i64::MIN as i128,
        I64_MAX as i128,
        I64_MAX as i128,
        1,
        1,
        2,
    ];
    let total: i128 = ints.iter().sum();
    assert!(
        total > I64_MAX as i128,
        "the fixture's int total overflows Int64"
    );

    let total_text = total.to_string();
    let double_max_text = (2 * I64_MAX as i128).to_string();
    let cases: Vec<(&str, Vec<Predicate>, &str, u64)> = vec![
        (
            "all ints (exceeds Int64; the true total survives)",
            vec![Predicate::HasType(metric(), ValueType::Int)],
            &total_text,
            12,
        ),
        (
            "0..=2",
            vec![
                cmp(metric(), CmpOp::Ge, L::Int(0)),
                cmp(metric(), CmpOp::Le, L::Int(2)),
            ],
            "5",
            5,
        ),
        (
            "sum-small-a + sum-small-b style small values",
            vec![
                cmp(metric(), CmpOp::Ge, L::Int(9)),
                cmp(metric(), CmpOp::Le, L::Int(100)),
            ],
            "119",
            3,
        ),
        // Experiment 019 `sum_int128_wide`: Int64 `sum` wraps this to -2.
        (
            "2 x i64::MAX",
            vec![cmp(metric(), CmpOp::Eq, L::Int(I64_MAX))],
            &double_max_text,
            2,
        ),
        (
            "i64::MIN alone",
            vec![cmp(metric(), CmpOp::Lt, L::Int(0))],
            "-9223372036854775808",
            1,
        ),
        (
            "nothing matches",
            vec![cmp(metric(), CmpOp::Eq, L::Int(12345))],
            "0",
            0,
        ),
    ];
    for (name, filters, sum, contributing) in cases {
        let mut q = logs_query();
        q.filters = filters;
        q.aggregate = Aggregate::Sum {
            attr: metric(),
            value_type: ValueType::Int,
        };
        let rows = run(ch, q);
        assert_eq!(rows[0]["sum_value"], *sum, "{name}");
        assert_eq!(u64_field(&rows[0], "contributing"), contributing, "{name}");
        assert!(
            rows[0].get("incompatible_types").is_none(),
            "{name}: a type filter replaces the scan"
        );
    }

    // Without a type filter the statement reports the incompatible types
    // present so the result check can fail the aggregate.
    let mut q = logs_query();
    q.aggregate = Aggregate::Sum {
        attr: metric(),
        value_type: ValueType::Int,
    };
    let rows = run(ch, q);
    assert_eq!(
        rows[0]["incompatible_types"],
        serde_json::json!(["bool", "bytes", "double", "map", "string"]),
        "explicit empty is not incompatible; missing is not a type"
    );
    assert_eq!(u64_field(&rows[0], "contributing"), 12);
}

#[test]
fn raw_float_sum_columns() {
    let Some(ch) = clickhouse() else { return };
    let mut q = logs_query();
    q.filters = vec![Predicate::HasType(metric(), ValueType::Double)];
    q.aggregate = Aggregate::Sum {
        attr: metric(),
        value_type: ValueType::Double,
    };
    let rows = run(ch, q);
    // 1.0 + -0.0 contribute; NaN, +Inf, -Inf are counted, not summed.
    assert_eq!(rows[0]["sum_value"], "1");
    assert_eq!(u64_field(&rows[0], "non_finite"), 3);
    assert_eq!(u64_field(&rows[0], "contributing"), 5);
}

#[test]
fn span_duration_counts() {
    let Some(ch) = clickhouse() else { return };
    // Experiment 021 count contract over the fixture's 9 spans: 4 contributing
    // (1500 + 10 + 0 + 2500; the zero-duration span included), 5 excluded
    // (3 missing + 2 reversed), 2 timing errors. Two spans have an unset start
    // and are stored at the Unix epoch, so only a range that reaches 1970
    // sees all nine.
    let cases = [
        ("whole fixture", 0, 9, 4, 2, "4010"),
        (
            "range from the event time hides the epoch-0 spans",
            TS,
            7,
            4,
            2,
            "4010",
        ),
    ];
    for (name, start_ns, events_expected, contributing, errors, sum) in cases {
        let mut q = spans_query();
        q.time.start_ns = start_ns;
        q.aggregate = Aggregate::SpanDuration;
        let rows = run(ch, q);
        assert_eq!(u64_field(&rows[0], "events"), events_expected, "{name}");
        assert_eq!(u64_field(&rows[0], "contributing"), contributing, "{name}");
        assert_eq!(u64_field(&rows[0], "timing_errors"), errors, "{name}");
        assert_eq!(rows[0]["sum_value"], sum, "{name}");
    }

    // The same workspace's log rows are never part of a span count; the other
    // workspace's spans are not either.
    let mut q = spans_query();
    q.time.start_ns = 0;
    q.scope.workspace_id = WS_B.into();
    q.aggregate = Aggregate::SpanDuration;
    assert_eq!(u64_field(&run(ch, q)[0], "events"), 9);
}

#[test]
fn span_attribute_namespaces() {
    let Some(ch) = clickhouse() else { return };
    let cases: Vec<(&str, Vec<Predicate>, u64)> = vec![
        (
            "typed span attribute",
            vec![cmp(
                AttrRef::record("int.big"),
                CmpOp::Eq,
                L::Int(9_007_199_254_740_993),
            )],
            1,
        ),
        (
            "span scope attributes keep their own namespace",
            vec![cmp(AttrRef::scope("scope.span.int"), CmpOp::Eq, L::Int(11))],
            1,
        ),
        (
            "the default scope is not on the span-scope span",
            vec![Predicate::Exists(AttrRef::scope("scope.attr"))],
            6,
        ),
        (
            "span record has no scope keys",
            vec![Predicate::Exists(AttrRef::record("scope.span.int"))],
            0,
        ),
    ];
    for (name, filters, expected) in cases {
        let mut q = spans_query();
        q.filters = filters;
        assert_eq!(events(ch, q), expected, "{name}");
    }
}

#[test]
fn rejected_requests_fail_during_planning() {
    let cases = [
        cmp(metric(), CmpOp::Eq, L::Double(f64::NAN)),
        cmp(metric(), CmpOp::Eq, L::Bytes(vec![0, 1, 0xfe, 0xff])),
    ];
    for filter in cases {
        let mut q = logs_query();
        q.filters = vec![filter.clone()];
        assert!(
            matches!(
                plan(q, &tables()),
                Err(QueryError::UnsupportedOperation { .. })
            ),
            "{filter:?}"
        );
    }
}

#[test]
fn string_parameters_survive_the_wire_encoding() {
    use toudai::query::Param;
    let Some(ch) = clickhouse() else { return };
    for text in [
        "plain",
        "a\\b",
        "tab\there",
        "line\nbreak",
        "cr\rhere",
        "\\n is not a newline",
        "héllo\u{2014}\u{1f30d}",
        "'quoted'",
    ] {
        let param = Param {
            name: "p0".into(),
            ch_type: "String",
            value: text.into(),
        };
        let out = ch.raw(
            "SELECT {p0:String} AS v FORMAT JSONEachRow",
            &[("p0", &param.wire_value())],
        );
        let row: Value = serde_json::from_str(out.trim_end()).unwrap();
        assert_eq!(row["v"], text, "{text:?}");
    }
}
