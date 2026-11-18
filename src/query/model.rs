//! The query request model.

use super::value::{Literal, ValueType};

/// The scope every query runs under: one dataset of the deployment (Decision
/// 0002). It is mandatory and bound as a parameter; a request cannot widen it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub dataset: String,
}

/// Half-open event-time range `[start_ns, end_ns)` in Unix nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub start_ns: i64,
    pub end_ns: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Logs,
    Spans,
}

/// Field identity includes its namespace: the same key in different namespaces
/// is a different field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Namespace {
    Record,
    Resource,
    Scope,
}

impl Namespace {
    pub fn as_str(self) -> &'static str {
        match self {
            Namespace::Record => "record",
            Namespace::Resource => "resource",
            Namespace::Scope => "scope",
        }
    }
}

/// A literal attribute key within one namespace. A dot in the key does not
/// define nesting and case is preserved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttrRef {
    pub namespace: Namespace,
    pub key: String,
}

impl AttrRef {
    pub fn record(key: &str) -> AttrRef {
        AttrRef {
            namespace: Namespace::Record,
            key: key.to_owned(),
        }
    }

    pub fn resource(key: &str) -> AttrRef {
        AttrRef {
            namespace: Namespace::Resource,
            key: key.to_owned(),
        }
    }

    pub fn scope(key: &str) -> AttrRef {
        AttrRef {
            namespace: Namespace::Scope,
            key: key.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    pub fn is_ordering(self) -> bool {
        matches!(self, CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge)
    }
}

/// Filters combine with AND.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// The key is present, whatever its type or value (explicit empty, empty
    /// string, false, and zero are all present).
    Exists(AttrRef),
    /// The key is not present.
    Absent(AttrRef),
    /// The key was received with this type.
    HasType(AttrRef, ValueType),
    /// The key was received with the literal's type and the value compares true.
    /// Values of any other type never match, including `Ne`.
    Compare {
        attr: AttrRef,
        op: CmpOp,
        value: Literal,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregate {
    /// Event count only.
    Count,
    /// Event count plus the number of events whose attribute has `value_type`.
    CountValues {
        attr: AttrRef,
        value_type: ValueType,
    },
    /// Event count, contributing-value count, and the sum of an attribute of
    /// the declared numeric type. Incompatible non-empty values are an error
    /// unless an explicit type filter excludes them.
    Sum {
        attr: AttrRef,
        value_type: ValueType,
    },
    /// Span duration (nanoseconds): contributing, timing-error, and sum.
    SpanDuration,
    /// Event count, contributing-value count, and exact nearest-rank
    /// percentiles of an attribute of the declared numeric type, with the same
    /// incompatible-value and non-finite rules as [`Aggregate::Sum`].
    Percentiles {
        attr: AttrRef,
        value_type: ValueType,
        percentiles: Vec<Percentile>,
    },
    /// Span duration counts as [`Aggregate::SpanDuration`] plus exact
    /// nearest-rank percentiles of the observable durations.
    SpanDurationPercentiles { percentiles: Vec<Percentile> },
}

/// A percentile in thousandths of a percent (`99.9` is `99_900`), so a
/// requested percentile is an exact decimal and its rank is integer
/// arithmetic: percentile `p` of `n` values is the value at 1-based rank
/// `max(1, ceil(p * n / 100))` in ascending order (nearest rank), always a
/// value that was received.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Percentile(u32);

impl Percentile {
    /// Thousandths of a percent in 100%.
    pub const SCALE: u32 = 100_000;

    /// Parse `0` to `100` with at most three decimal places.
    pub fn parse(text: &str) -> Option<Percentile> {
        let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
        let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
        if !digits(whole) || (text.contains('.') && !digits(fraction)) || fraction.len() > 3 {
            return None;
        }
        let whole: u32 = whole.parse().ok().filter(|w| *w <= 100)?;
        let fraction: u32 = format!("{fraction:0<3}").parse().ok()?;
        let thousandths = whole * 1000 + fraction;
        (thousandths <= Percentile::SCALE).then_some(Percentile(thousandths))
    }

    pub fn thousandths(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for Percentile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (whole, fraction) = (self.0 / 1000, self.0 % 1000);
        if fraction == 0 {
            write!(f, "{whole}")
        } else {
            let fraction = format!("{fraction:03}");
            write!(f, "{whole}.{}", fraction.trim_end_matches('0'))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub scope: Scope,
    pub signal: Signal,
    pub time: TimeRange,
    /// Time-series bucket width in nanoseconds. Buckets are half-open and
    /// start at `time.start_ns`, so bucket `i` is `[start + i * w, start +
    /// (i + 1) * w)` clipped to the range; no clock or time zone is involved.
    pub bucket_ns: Option<i64>,
    pub filters: Vec<Predicate>,
    /// Group keys include the value type, so string `"1"`, integer `1`, and
    /// float `1.0` are distinct groups, as are missing and explicit empty.
    pub group_by: Vec<AttrRef>,
    pub aggregate: Aggregate,
}
