use super::*;
use crate::query::model::{Scope, TimeRange};
use crate::query::value::Literal;

// Fixture identity from Experiment 021 (`fixture021.py`): workspace A, dataset
// `fixture`, events at 2026-09-01T07:00:00.123456789Z.
const WS_A: &str = "11111111-1111-4111-8111-111111111111";
const TS: i64 = 1_788_246_000_123_456_789;

fn query(signal: Signal) -> Query {
    Query {
        scope: Scope {
            workspace_id: WS_A.into(),
            dataset: "fixture".into(),
        },
        signal,
        time: TimeRange {
            start_ns: TS - 1,
            end_ns: TS + 1,
        },
        filters: vec![],
        group_by: vec![],
        aggregate: Aggregate::Count,
    }
}

fn logs_with(f: impl FnOnce(&mut Query)) -> Query {
    let mut q = query(Signal::Logs);
    f(&mut q);
    q
}

fn compare(attr: AttrRef, op: CmpOp, value: Literal) -> Predicate {
    Predicate::Compare { attr, op, value }
}

fn metric() -> AttrRef {
    AttrRef::record("metric.value")
}

#[test]
fn validation_table() {
    use Literal::*;
    use QueryError::*;
    let unsupported = |operation, value| UnsupportedOperation { operation, value };
    let cases: Vec<(&str, Query, Result<(), QueryError>)> = vec![
        ("baseline count", query(Signal::Logs), Ok(())),
        (
            "typed int equality",
            logs_with(|q| q.filters = vec![compare(metric(), CmpOp::Eq, Int(1))]),
            Ok(()),
        ),
        (
            "missing workspace",
            logs_with(|q| q.scope.workspace_id.clear()),
            Err(MissingScope {
                field: "workspace_id",
            }),
        ),
        (
            "missing dataset",
            logs_with(|q| q.scope.dataset.clear()),
            Err(MissingScope { field: "dataset" }),
        ),
        (
            "empty time range",
            logs_with(|q| q.time.end_ns = q.time.start_ns),
            Err(InvalidTimeRange {
                start_ns: TS - 1,
                end_ns: TS - 1,
            }),
        ),
        (
            "empty attribute key",
            logs_with(|q| q.filters = vec![Predicate::Exists(AttrRef::record(""))]),
            Err(EmptyAttributeKey),
        ),
        (
            "NaN literal (fixture row `nan`)",
            logs_with(|q| q.filters = vec![compare(metric(), CmpOp::Eq, Double(f64::NAN))]),
            Err(unsupported(
                Operation::Compare,
                UnsupportedValue::NonFiniteFloat,
            )),
        ),
        (
            "infinity range bound (fixture row `posinf`)",
            logs_with(|q| q.filters = vec![compare(metric(), CmpOp::Lt, Double(f64::INFINITY))]),
            Err(unsupported(
                Operation::Order,
                UnsupportedValue::NonFiniteFloat,
            )),
        ),
        (
            "bytes literal (fixture row `bytes-row`)",
            logs_with(|q| {
                q.filters = vec![compare(metric(), CmpOp::Eq, Bytes(vec![0, 1, 0xfe, 0xff]))]
            }),
            Err(unsupported(Operation::Compare, UnsupportedValue::Bytes)),
        ),
        (
            "map literal (fixture row `nested-row`)",
            logs_with(|q| {
                q.filters = vec![compare(
                    metric(),
                    CmpOp::Eq,
                    Map(vec![("nested.int".into(), Int(1))]),
                )]
            }),
            Err(unsupported(Operation::Compare, UnsupportedValue::Map)),
        ),
        (
            "array literal",
            logs_with(|q| q.filters = vec![compare(metric(), CmpOp::Ne, Array(vec![Int(1)]))]),
            Err(unsupported(Operation::Compare, UnsupportedValue::Array)),
        ),
        (
            "ordering over bool",
            logs_with(|q| q.filters = vec![compare(metric(), CmpOp::Lt, Bool(true))]),
            Err(OrderingNotDefined {
                value_type: ValueType::Bool,
            }),
        ),
        (
            "ordering over int and string is defined",
            logs_with(|q| {
                q.filters = vec![
                    compare(metric(), CmpOp::Ge, Int(10)),
                    compare(AttrRef::record("unicode"), CmpOp::Lt, String("z".into())),
                ]
            }),
            Ok(()),
        ),
        (
            "resource int equality",
            logs_with(|q| {
                q.filters = vec![compare(
                    AttrRef::resource("res.int64"),
                    CmpOp::Eq,
                    Int(9_007_199_254_740_993),
                )]
            }),
            Ok(()),
        ),
        (
            "resource float equality is text",
            logs_with(|q| {
                q.filters = vec![compare(
                    AttrRef::resource("res.float"),
                    CmpOp::Eq,
                    Double(1.0),
                )]
            }),
            Err(UnsupportedForNamespace {
                namespace: Namespace::Resource,
                operation: Operation::Compare,
                value_type: ValueType::Double,
            }),
        ),
        (
            "scope int ordering is text",
            logs_with(|q| {
                q.filters = vec![compare(AttrRef::scope("scope.int"), CmpOp::Gt, Int(5))]
            }),
            Err(UnsupportedForNamespace {
                namespace: Namespace::Scope,
                operation: Operation::Order,
                value_type: ValueType::Int,
            }),
        ),
        (
            "sum declared string",
            logs_with(|q| {
                q.aggregate = Aggregate::Sum {
                    attr: metric(),
                    value_type: ValueType::String,
                }
            }),
            Err(NotNumeric {
                value_type: ValueType::String,
            }),
        ),
        (
            "sum declared bytes",
            logs_with(|q| {
                q.aggregate = Aggregate::Sum {
                    attr: metric(),
                    value_type: ValueType::Bytes,
                }
            }),
            Err(unsupported(Operation::Aggregate, UnsupportedValue::Bytes)),
        ),
        (
            "sum over resource attribute",
            logs_with(|q| {
                q.aggregate = Aggregate::Sum {
                    attr: AttrRef::resource("res.int64"),
                    value_type: ValueType::Int,
                }
            }),
            Err(UnsupportedForNamespace {
                namespace: Namespace::Resource,
                operation: Operation::Aggregate,
                value_type: ValueType::Int,
            }),
        ),
        (
            "count values of explicit empty",
            logs_with(|q| {
                q.aggregate = Aggregate::CountValues {
                    attr: metric(),
                    value_type: ValueType::Empty,
                }
            }),
            Err(NotScalar {
                value_type: ValueType::Empty,
            }),
        ),
        (
            "span duration on logs",
            logs_with(|q| q.aggregate = Aggregate::SpanDuration),
            Err(AggregateRequiresSpans),
        ),
        (
            "span duration on spans",
            {
                let mut q = query(Signal::Spans);
                q.aggregate = Aggregate::SpanDuration;
                q
            },
            Ok(()),
        ),
    ];
    for (name, q, expected) in cases {
        assert_eq!(validate(&q), expected, "{name}");
    }
}

#[test]
fn diagnostics_table() {
    use Guard::*;
    let sum_int = Aggregate::Sum {
        attr: metric(),
        value_type: ValueType::Int,
    };
    let int_filter = Predicate::HasType(metric(), ValueType::Int);
    let cases: Vec<(&str, Query, Vec<&str>, Vec<Guard>)> = vec![
        (
            "plain count reads only the envelope",
            query(Signal::Logs),
            vec!["dataset", "ts", "workspace_id"],
            vec![],
        ),
        (
            "known-type filter reads only the matching typed Map",
            logs_with(|q| q.filters = vec![compare(metric(), CmpOp::Eq, Literal::Int(1))]),
            vec!["dataset", "record_int", "ts", "workspace_id"],
            vec![TypeMatchedPredicates, KeyMembershipPresence],
        ),
        (
            "float comparison is finite-only",
            logs_with(|q| q.filters = vec![compare(metric(), CmpOp::Gt, Literal::Double(1.5))]),
            vec!["dataset", "record_double", "ts", "workspace_id"],
            vec![
                TypeMatchedPredicates,
                FiniteFloatComparison,
                KeyMembershipPresence,
            ],
        ),
        (
            "presence of an unknown-type key reads every record Map",
            logs_with(|q| q.filters = vec![Predicate::Exists(metric())]),
            vec![
                "dataset",
                "record_bool",
                "record_double",
                "record_int",
                "record_other",
                "record_string",
                "ts",
                "workspace_id",
            ],
            vec![KeyMembershipPresence],
        ),
        (
            "explicit type filter lets the sum skip the other Maps",
            logs_with(|q| {
                q.filters = vec![int_filter.clone()];
                q.aggregate = sum_int.clone();
            }),
            vec!["dataset", "record_int", "ts", "workspace_id"],
            vec![KeyMembershipPresence, Int128Sum, ContributingCounts],
        ),
        (
            "sum without a type filter checks for incompatible values",
            logs_with(|q| q.aggregate = sum_int.clone()),
            vec![
                "dataset",
                "record_bool",
                "record_double",
                "record_int",
                "record_other",
                "record_string",
                "ts",
                "workspace_id",
            ],
            vec![
                KeyMembershipPresence,
                Int128Sum,
                ContributingCounts,
                IncompatibleTypeCheck,
            ],
        ),
        (
            "a type filter for another type does not excuse the sum",
            logs_with(|q| {
                q.filters = vec![Predicate::HasType(metric(), ValueType::String)];
                q.aggregate = sum_int.clone();
            }),
            vec![
                "dataset",
                "record_bool",
                "record_double",
                "record_int",
                "record_other",
                "record_string",
                "ts",
                "workspace_id",
            ],
            vec![
                KeyMembershipPresence,
                Int128Sum,
                ContributingCounts,
                IncompatibleTypeCheck,
            ],
        ),
        (
            "resource group reads the type and value Maps",
            logs_with(|q| q.group_by = vec![AttrRef::resource("res.int64")]),
            vec![
                "dataset",
                "resource",
                "resource_types",
                "ts",
                "workspace_id",
            ],
            vec![
                KeyMembershipPresence,
                GroupKeyIncludesType,
                NonFiniteFloatCheck,
            ],
        ),
        (
            "span duration",
            {
                let mut q = query(Signal::Spans);
                q.aggregate = Aggregate::SpanDuration;
                q
            },
            vec![
                "dataset",
                "duration_ns",
                "span_timing_status",
                "ts",
                "workspace_id",
            ],
            vec![Int128Sum, ContributingCounts],
        ),
    ];
    for (name, q, columns, guards) in cases {
        assert_eq!(validate(&q), Ok(()), "{name}");
        let d = diagnostics(&q);
        assert_eq!(d.columns, columns, "{name}: columns");
        assert_eq!(
            d.guards.iter().collect::<std::collections::BTreeSet<_>>(),
            guards.iter().collect::<std::collections::BTreeSet<_>>(),
            "{name}: guards"
        );
        assert_eq!(d.storage_path, StoragePath::Raw);
        assert_eq!(d.approximation, ApproximationMode::Exact);
    }
}

#[test]
fn typed_comparison_counts_as_a_type_filter() {
    let sum = Aggregate::Sum {
        attr: metric(),
        value_type: ValueType::Int,
    };
    // (filter, whether incompatible values are left to fail the aggregate)
    let cases = [
        (compare(metric(), CmpOp::Ge, Literal::Int(0)), false),
        (compare(metric(), CmpOp::Eq, Literal::Double(1.0)), true),
        (
            compare(AttrRef::record("other.key"), CmpOp::Eq, Literal::Int(0)),
            true,
        ),
        (
            compare(
                AttrRef::resource("metric.value"),
                CmpOp::Eq,
                Literal::Int(0),
            ),
            true,
        ),
    ];
    for (filter, checks_incompatible) in cases {
        let q = logs_with(|q| {
            q.filters = vec![filter.clone()];
            q.aggregate = sum.clone();
        });
        assert_eq!(
            diagnostics(&q)
                .guards
                .contains(&Guard::IncompatibleTypeCheck),
            checks_incompatible,
            "{filter:?}"
        );
    }
}
