use serde_json::json;

use super::*;
use crate::query::model::{AttrRef, Predicate, Query, Scope, Signal, TimeRange};
use crate::query::plan::plan;
use crate::query::schema::ServingTables;

fn query(aggregate: Aggregate, group_by: Vec<AttrRef>) -> Plan {
    let q = Query {
        scope: Scope {
            workspace_id: "ws".into(),
            dataset: "fixture".into(),
        },
        signal: Signal::Logs,
        time: TimeRange {
            start_ns: 0,
            end_ns: 1,
        },
        filters: vec![],
        group_by,
        aggregate,
    };
    plan(q, &ServingTables::new("lh").unwrap()).unwrap()
}

fn sum_int() -> Plan {
    query(
        Aggregate::Sum {
            attr: AttrRef::record("metric.value"),
            value_type: ValueType::Int,
        },
        vec![],
    )
}

fn sum_row(contributing: u64, total: &str) -> Value {
    json!({"events": "27", "contributing": contributing.to_string(), "sum_value": total, "incompatible_types": []})
}

type SumResult = Result<Option<Sum>, QueryError>;

const I64_MAX: i128 = i64::MAX as i128;
const I64_MIN: i128 = i64::MIN as i128;

#[test]
fn integer_totals_are_range_checked() {
    use QueryError::IntegerOverflow;
    // (name, contributing, Int128 total, expected)
    let cases: Vec<(&str, u64, i128, SumResult)> = vec![
        ("zero contributing is not zero", 0, 0, Ok(None)),
        (
            "small exact sum (fixture sum-small-a + sum-small-b)",
            2,
            3,
            Ok(Some(Sum::Int(3))),
        ),
        (
            "2^53 + 1 stays exact",
            1,
            9_007_199_254_740_993,
            Ok(Some(Sum::Int(9_007_199_254_740_993))),
        ),
        ("i64::MAX fits", 1, I64_MAX, Ok(Some(Sum::Int(i64::MAX)))),
        ("i64::MIN fits", 1, I64_MIN, Ok(Some(Sum::Int(i64::MIN)))),
        ("a legitimate zero total", 2, 0, Ok(Some(Sum::Int(0)))),
        (
            "i64::MAX + 1 overflows",
            2,
            I64_MAX + 1,
            Err(IntegerOverflow { total: I64_MAX + 1 }),
        ),
        (
            "i64::MIN - 1 overflows",
            2,
            I64_MIN - 1,
            Err(IntegerOverflow { total: I64_MIN - 1 }),
        ),
        // Experiment 019: sum-a + sum-b + sum-c; Int64 `sum` wraps this to -1.
        (
            "fixture sum-a + sum-b + sum-c",
            3,
            18_446_744_073_709_551_615,
            Err(IntegerOverflow {
                total: 18_446_744_073_709_551_615,
            }),
        ),
        (
            "beyond Int64 by far",
            2,
            10i128.pow(30),
            Err(IntegerOverflow {
                total: 10i128.pow(30),
            }),
        ),
    ];
    for (name, contributing, total, expected) in cases {
        let got = interpret(
            &sum_int(),
            &[sum_row(contributing, &total.to_string())],
            &[],
        )
        .map(|r| r.rows[0].values.as_ref().unwrap().sum);
        assert_eq!(got, expected, "{name}");
    }
}

#[test]
fn incompatible_values_fail_unless_excluded() {
    use QueryError::*;
    let attr = || "metric.value".to_owned();
    // (found types reported by the statement, expected)
    let cases: Vec<(serde_json::Value, Result<(), QueryError>)> = vec![
        (json!([]), Ok(())),
        (
            json!(["string"]),
            Err(IncompatibleType {
                attr: attr(),
                expected: ValueType::Int,
                found: vec![ValueType::String],
            }),
        ),
        (
            json!(["bool", "double", "string"]),
            Err(IncompatibleType {
                attr: attr(),
                expected: ValueType::Int,
                found: vec![ValueType::Bool, ValueType::Double, ValueType::String],
            }),
        ),
        (
            json!(["bytes"]),
            Err(UnsupportedOperation {
                operation: Operation::Aggregate,
                value: UnsupportedValue::Bytes,
            }),
        ),
        // A canonical-only value outranks a plain type mismatch.
        (
            json!(["string", "map"]),
            Err(UnsupportedOperation {
                operation: Operation::Aggregate,
                value: UnsupportedValue::Map,
            }),
        ),
        (
            json!(["wat"]),
            Err(MalformedResult {
                detail: "unknown incompatible type \"wat\"".into(),
            }),
        ),
    ];
    for (found, expected) in cases {
        let row =
            json!({"events": 1, "contributing": 1, "sum_value": "1", "incompatible_types": found});
        let got = interpret(&sum_int(), &[row], &[]).map(|_| ());
        assert_eq!(got, expected, "{found}");
    }
    // Missing validation output is malformed when the request did not restrict
    // the aggregate input to its declared type.
    let row = json!({"events": 1, "contributing": 1, "sum_value": "1"});
    assert_eq!(
        interpret(&sum_int(), std::slice::from_ref(&row), &[]),
        Err(MalformedResult {
            detail: "missing column incompatible_types".into(),
        })
    );

    // An actual matching type filter makes the omitted column safe.
    let mut filtered = sum_int().query;
    filtered.filters = vec![Predicate::HasType(
        AttrRef::record("metric.value"),
        ValueType::Int,
    )];
    let filtered = plan(filtered, &ServingTables::new("lh").unwrap()).unwrap();
    assert!(interpret(&filtered, &[row], &[]).is_ok());
}

#[test]
fn float_aggregates_reject_non_finite_values() {
    use QueryError::UnsupportedOperation;
    let plan = query(
        Aggregate::Sum {
            attr: AttrRef::record("metric.value"),
            value_type: ValueType::Double,
        },
        vec![],
    );
    let nf = Err(UnsupportedOperation {
        operation: Operation::Aggregate,
        value: UnsupportedValue::NonFiniteFloat,
    });
    // (contributing, non-finite count, sum text, expected)
    let cases: Vec<(u64, u64, &str, SumResult)> = vec![
        (1, 0, "1.5", Ok(Some(Sum::Double(1.5)))),
        (0, 0, "0", Ok(None)),
        (5, 3, "1", nf.clone()),
        (1, 0, "inf", nf.clone()),
        (1, 0, "nan", nf),
    ];
    for (contributing, non_finite, sum, expected) in cases {
        let row = json!({
            "events": 5,
            "contributing": contributing,
            "sum_value": sum,
            "non_finite": non_finite,
            "incompatible_types": [],
        });
        let got = interpret(&plan, &[row], &[]).map(|r| r.rows[0].values.as_ref().unwrap().sum);
        assert_eq!(got, expected, "{contributing} {non_finite} {sum}");
    }
}

#[test]
fn group_keys_keep_their_type() {
    use QueryError::UnsupportedOperation;
    let plan = query(Aggregate::Count, vec![AttrRef::record("metric.value")]);
    let unsupported = |value| {
        Err(UnsupportedOperation {
            operation: Operation::Group,
            value,
        })
    };
    // (type, value text as ClickHouse renders it, expected)
    let cases: Vec<(&str, &str, Result<GroupValue, QueryError>)> = vec![
        ("int", "1", Ok(GroupValue::Value(TypedValue::Int(1)))),
        (
            "double",
            "1",
            Ok(GroupValue::Value(TypedValue::Double(1.0))),
        ),
        (
            "string",
            "1",
            Ok(GroupValue::Value(TypedValue::String("1".into()))),
        ),
        (
            "string",
            "",
            Ok(GroupValue::Value(TypedValue::String(String::new()))),
        ),
        (
            "bool",
            "false",
            Ok(GroupValue::Value(TypedValue::Bool(false))),
        ),
        ("empty", "", Ok(GroupValue::Value(TypedValue::Empty))),
        ("missing", "", Ok(GroupValue::Missing)),
        (
            "int",
            "9007199254740993",
            Ok(GroupValue::Value(TypedValue::Int(9_007_199_254_740_993))),
        ),
        (
            "int",
            "-9223372036854775808",
            Ok(GroupValue::Value(TypedValue::Int(i64::MIN))),
        ),
        (
            "double",
            "nan",
            unsupported(UnsupportedValue::NonFiniteFloat),
        ),
        (
            "double",
            "inf",
            unsupported(UnsupportedValue::NonFiniteFloat),
        ),
        (
            "double",
            "-inf",
            unsupported(UnsupportedValue::NonFiniteFloat),
        ),
        (
            "double",
            "Infinity",
            unsupported(UnsupportedValue::NonFiniteFloat),
        ),
        ("bytes", "", unsupported(UnsupportedValue::Bytes)),
        ("array", "", unsupported(UnsupportedValue::Array)),
        ("map", "", unsupported(UnsupportedValue::Map)),
        (
            "int",
            "1.0",
            Err(malformed_result("\"1.0\" is not a valid int")),
        ),
        (
            "wat",
            "",
            Err(malformed_result("unknown group type \"wat\"")),
        ),
    ];
    for (ty, value, expected) in cases {
        let row = json!({"g0_type": ty, "g0_value": value, "events": "2"});
        let got = interpret(&plan, &[row], &[]).map(|r| r.rows[0].groups[0].clone());
        assert_eq!(got, expected, "{ty} {value}");
    }
    // Same text, three types: three distinct keys.
    let one = |t: &str| group_value(t, "1").unwrap();
    assert_ne!(one("int"), one("double"));
    assert_ne!(one("int"), one("string"));
    assert_ne!(one("double"), one("string"));
}

fn malformed_result(detail: &str) -> QueryError {
    QueryError::MalformedResult {
        detail: detail.into(),
    }
}

#[test]
fn counts_accept_quoted_and_plain_json_numbers() {
    let plan = query(Aggregate::Count, vec![]);
    for events in [json!(27), json!("27")] {
        let result = interpret(&plan, &[json!({ "events": events })], &[]).unwrap();
        assert_eq!(result.rows[0].events, 27);
        assert_eq!(result.rows[0].values, None);
    }
    assert_eq!(
        interpret(&plan, &[json!({})], &[]),
        Err(malformed_result("missing column events"))
    );
}

#[test]
fn response_json_is_lossless() {
    let plan = query(
        Aggregate::Sum {
            attr: AttrRef::record("metric.value"),
            value_type: ValueType::Int,
        },
        vec![AttrRef::record("grp")],
    );
    let row = json!({
        "g0_type": "int", "g0_value": "9007199254740993", "events": "1",
        "contributing": "1", "sum_value": "9007199254740993", "incompatible_types": [],
    });
    let result = interpret(&plan, &[row], &[]).unwrap();
    let out = result.to_json();
    assert_eq!(
        out["rows"][0],
        json!({
            "groups": [{"type": "int", "value": "9007199254740993"}],
            "events": {"type": "uint64", "value": "1"},
            "contributing": {"type": "uint64", "value": "1"},
            "sum": {"type": "int", "value": "9007199254740993"},
        })
    );
    assert_eq!(out["diagnostics"]["storage_path"], "raw");
    assert_eq!(out["diagnostics"]["approximation"], "exact");
    let guards = out["diagnostics"]["guards"].as_array().unwrap();
    assert!(guards.iter().any(|g| g == "int128_sum"));
}

#[test]
fn response_count_json_is_lossless_and_typed() {
    let plan = query(Aggregate::Count, vec![]);
    let result = QueryResult {
        rows: vec![ResultRow {
            groups: vec![],
            events: 9_007_199_254_740_993,
            values: Some(ValueStats {
                contributing: u64::MAX,
                timing_errors: Some(9_007_199_254_740_994),
                sum: None,
            }),
        }],
        diagnostics: plan.diagnostics,
        non_finite_excluded: Some(u64::MAX - 1),
    };
    let out = result.to_json();
    assert_eq!(
        out["rows"][0]["events"],
        json!({"type": "uint64", "value": "9007199254740993"})
    );
    assert_eq!(
        out["rows"][0]["contributing"],
        json!({"type": "uint64", "value": "18446744073709551615"})
    );
    assert_eq!(
        out["rows"][0]["timing_errors"],
        json!({"type": "uint64", "value": "9007199254740994"})
    );
    assert_eq!(
        out["diagnostics"]["non_finite_excluded"],
        json!({"type": "uint64", "value": "18446744073709551614"})
    );
}

#[test]
fn non_finite_exclusions_are_reported_not_fatal() {
    use crate::query::model::Predicate;
    use crate::query::value::Literal;
    let mut with_double = query(Aggregate::Count, vec![]);
    with_double.query.filters = vec![Predicate::Compare {
        attr: AttrRef::record("metric.value"),
        op: crate::query::model::CmpOp::Gt,
        value: Literal::Double(0.5),
    }];
    let q = with_double.query.clone();
    let with_double = plan(q, &ServingTables::new("lh").unwrap()).unwrap();
    assert!(with_double.non_finite_probe.is_some());
    // (probe rows, expected)
    type ProbeCase = (Vec<Value>, Result<Option<u64>, QueryError>);
    let cases: Vec<ProbeCase> = vec![
        (vec![json!({"non_finite_excluded": "3"})], Ok(Some(3))),
        (vec![json!({"non_finite_excluded": 0})], Ok(Some(0))),
        (
            vec![],
            Err(malformed_result(
                "the non-finite probe returned no single row",
            )),
        ),
    ];
    for (probe, expected) in cases {
        let got =
            interpret(&with_double, &[json!({"events": 1})], &probe).map(|r| r.non_finite_excluded);
        assert_eq!(got, expected, "{probe:?}");
    }
    let result = interpret(
        &with_double,
        &[json!({"events": 1})],
        &[json!({"non_finite_excluded": 3})],
    )
    .unwrap();
    assert_eq!(
        result.to_json()["diagnostics"]["non_finite_excluded"],
        json!({"type": "uint64", "value": "3"})
    );

    // A request without a double comparison has no probe and reports nothing.
    let plain = query(Aggregate::Count, vec![]);
    assert!(plain.non_finite_probe.is_none());
    let result = interpret(&plain, &[json!({"events": 1})], &[]).unwrap();
    assert_eq!(result.non_finite_excluded, None);
    assert!(
        result.to_json()["diagnostics"]
            .get("non_finite_excluded")
            .is_none()
    );
}
