//! The fixture model: every event the emitter sends, defined once, with fixed
//! timestamps and identifiers derived from the seed. Both the OTLP stream and
//! the expected-contents file are produced from this model.

use crate::value::{Attrs, Val};

/// 2026-09-01T07:00:00Z in Unix nanoseconds. Every timestamp is an offset from
/// this constant, so no run reads a clock.
pub const BASE_NS: u64 = 1_788_246_000_000_000_000;

/// Attribute carrying the fixture's own event name. It is ordinary producer
/// data (it lands in the record namespace), unique within a tenant and dataset,
/// and lets any consumer join a stored event to its expected row.
pub const KEY_ATTR: &str = "fixture.key";

pub const WORKSPACE_ATTR: &str = "toudai.workspace_id";
pub const DATASET_ATTR: &str = "toudai.dataset";
/// Reserved namespace: producer-supplied attributes in it never override the
/// trusted scope and are dropped from record, scope, and span attributes.
pub const RESERVED_PREFIX: &str = "toudai.";

pub const DATASET: &str = "checkout";
const TENANTS: [(&str, &str); 2] = [
    ("tenant-a", "11111111-1111-4111-8111-111111111111"),
    ("tenant-b", "22222222-2222-4222-8222-222222222222"),
];

/// Resource and instrumentation scope of one signal stream.
#[derive(Clone, Debug)]
pub struct Ctx {
    pub resource: Attrs,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attrs: Attrs,
}

/// Fixture annotations that are not on the wire: what the record is meant to
/// exercise and which keys must stay absent.
#[derive(Clone, Debug, Default)]
pub struct Notes {
    pub class: &'static str,
    pub absent_keys: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct LogRec {
    pub ctx: Ctx,
    pub timestamp_ns: u64,
    pub observed_ns: u64,
    pub severity_number: i32,
    pub severity_text: &'static str,
    /// Non-empty marks an application event.
    pub event_name: String,
    pub body: Option<Val>,
    pub trace: Option<([u8; 16], [u8; 8])>,
    pub attrs: Attrs,
    pub notes: Notes,
}

#[derive(Clone, Debug)]
pub struct SpanEventRec {
    pub name: String,
    pub time_ns: u64,
    pub attrs: Attrs,
}

#[derive(Clone, Debug)]
pub struct SpanRec {
    pub ctx: Ctx,
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: SpanKind,
    pub start_ns: u64,
    pub end_ns: u64,
    pub status: Status,
    pub attrs: Attrs,
    pub events: Vec<SpanEventRec>,
    pub notes: Notes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
}

impl SpanKind {
    /// The kind name the serving rows store.
    pub fn name(self) -> &'static str {
        match self {
            SpanKind::Internal => "Internal",
            SpanKind::Server => "Server",
            SpanKind::Client => "Client",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Unset,
    Ok,
    Error,
}

impl Status {
    /// The OTLP status code.
    pub fn code(self) -> i32 {
        match self {
            Status::Unset => 0,
            Status::Ok => 1,
            Status::Error => 2,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetricRec {
    pub ctx: Ctx,
    pub name: String,
    pub unit: String,
    pub description: String,
    pub value: u64,
    pub attrs: Attrs,
}

#[derive(Clone, Debug)]
pub struct Fixture {
    pub logs: Vec<LogRec>,
    pub spans: Vec<SpanRec>,
    pub metrics: Vec<MetricRec>,
}

/// SplitMix64: a fixed, dependency-free generator, so the same seed yields the
/// same identifiers and values on every platform and toolchain.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn nonzero(&mut self) -> u64 {
        loop {
            let v = self.next_u64();
            if v != 0 {
                return v;
            }
        }
    }

    pub fn trace_id(&mut self) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&self.nonzero().to_be_bytes());
        id[8..].copy_from_slice(&self.next_u64().to_be_bytes());
        id
    }

    pub fn span_id(&mut self) -> [u8; 8] {
        self.nonzero().to_be_bytes()
    }
}

fn s(v: &str) -> Val {
    Val::Str(v.to_string())
}

fn attr(key: &str, value: Val) -> (String, Val) {
    (key.to_string(), value)
}

fn keyed(key: &str, mut rest: Attrs) -> Attrs {
    let mut attrs = vec![attr(KEY_ATTR, s(key))];
    attrs.append(&mut rest);
    attrs
}

fn notes(class: &'static str) -> Notes {
    Notes {
        class,
        absent_keys: Vec::new(),
    }
}

fn ctx(workspace_id: &str, signal: &str) -> Ctx {
    Ctx {
        resource: vec![
            attr("service.name", s("checkout-api")),
            attr("service.version", s("1.4.2")),
            attr("deployment.environment", s("staging")),
            attr("fixture.replica", Val::Int(3)),
            attr(WORKSPACE_ATTR, s(workspace_id)),
            attr(DATASET_ATTR, s(DATASET)),
        ],
        scope_name: format!("toudai.fixture.{signal}"),
        scope_version: "1.0.0".to_string(),
        scope_attrs: vec![
            attr("scope.attr", s("scope-value")),
            attr("scope.int", Val::Int(7)),
        ],
    }
}

/// Build the whole fixture. Everything is a pure function of `seed`.
pub fn build(seed: u64) -> Fixture {
    let mut fixture = Fixture {
        logs: Vec::new(),
        spans: Vec::new(),
        metrics: Vec::new(),
    };
    for (index, (_, workspace_id)) in TENANTS.iter().enumerate() {
        let mut rng = Rng::new(seed ^ ((index as u64 + 1) << 56));
        tenant(&mut fixture, &mut rng, index, workspace_id);
    }
    fixture
}

fn tenant(f: &mut Fixture, rng: &mut Rng, index: usize, workspace_id: &str) {
    let other_workspace = TENANTS[1 - index].1;
    // Tenants differ in values, not in dataset name: both use `checkout`.
    let t0 = BASE_NS + (index as u64) * 1_000_000_000 + 123_456_789;
    let (status_code, total, root_status) = if index == 0 {
        (200, 129.95, Status::Ok)
    } else {
        (500, 59.5, Status::Error)
    };

    let spans = ctx(workspace_id, "spans");
    let logs = ctx(workspace_id, "logs");
    let events = ctx(workspace_id, "events");

    // Trace 1: a server span with two children and two span events.
    let trace1 = rng.trace_id();
    let root = rng.span_id();
    let db = rng.span_id();
    let cache = rng.span_id();
    let span =
        |key: &str, trace_id, span_id, parent, name: &str, kind, start, end, status, attrs| {
            SpanRec {
                ctx: spans.clone(),
                trace_id,
                span_id,
                parent_span_id: parent,
                name: name.to_string(),
                kind,
                start_ns: start,
                end_ns: end,
                status,
                attrs: keyed(key, attrs),
                events: Vec::new(),
                notes: notes("valid"),
            }
        };
    let mut root_span = span(
        "span.root",
        trace1,
        root,
        None,
        "GET /checkout",
        SpanKind::Server,
        t0,
        t0 + 1_500_000,
        root_status,
        vec![
            attr("http.method", s("GET")),
            attr("http.route", s("/checkout")),
            attr("http.status_code", Val::Int(status_code)),
        ],
    );
    root_span.events = vec![
        SpanEventRec {
            name: "cart.loaded".into(),
            time_ns: t0 + 100_000,
            attrs: vec![attr("cart.items", Val::Int(3))],
        },
        SpanEventRec {
            name: "exception".into(),
            time_ns: t0 + 900_000,
            attrs: vec![
                attr("exception.type", s("TimeoutError")),
                attr("exception.message", s("upstream timeout")),
                attr("exception.escaped", Val::Bool(false)),
            ],
        },
    ];
    f.spans.push(root_span);
    f.spans.push(span(
        "span.db",
        trace1,
        db,
        Some(root),
        "db.query",
        SpanKind::Client,
        t0 + 200_000,
        t0 + 800_000,
        Status::Unset,
        vec![
            attr("db.system", s("clickhouse")),
            attr("db.rows", Val::Int(120 + (rng.next_u64() % 100) as i64)),
        ],
    ));
    // A valid zero duration: start equals end.
    f.spans.push(span(
        "span.zero-duration",
        trace1,
        cache,
        Some(root),
        "cache.get",
        SpanKind::Internal,
        t0 + 300_000,
        t0 + 300_000,
        Status::Unset,
        vec![attr("cache.hit", Val::Bool(true))],
    ));

    // Span timing hazards, one trace each. OTLP has no timestamp presence, so
    // an unset timestamp is 0; missing timing takes precedence over reversed.
    let timing: [(&str, &str, u64, u64); 4] = [
        (
            "span.missing-start",
            "timing/missing-start",
            0,
            t0 + 5_000_000,
        ),
        ("span.missing-end", "timing/missing-end", t0 + 6_000_000, 0),
        ("span.missing-both", "timing/missing-both", 0, 0),
        (
            "span.reversed",
            "timing/reversed",
            t0 + 9_000_000,
            t0 + 8_000_000,
        ),
    ];
    for (key, name, start, end) in timing {
        let (trace_id, span_id) = (rng.trace_id(), rng.span_id());
        let mut rec = span(
            key,
            trace_id,
            span_id,
            None,
            name,
            SpanKind::Server,
            start,
            end,
            Status::Unset,
            Vec::new(),
        );
        rec.notes = notes("boundary");
        f.spans.push(rec);
    }

    // Span attribute values the SDK can carry: non-finite and extreme numbers,
    // homogeneous arrays, empty string, and an empty array. (The SDK's span
    // `Value` has no bytes or map type; those are exercised on log records.)
    let (trace_id, span_id) = (rng.trace_id(), rng.span_id());
    let mut rec = span(
        "span.invalid-values",
        trace_id,
        span_id,
        None,
        "invalid/values",
        SpanKind::Internal,
        t0 + 10_000_000,
        t0 + 10_001_000,
        Status::Unset,
        vec![
            attr("double.nan", Val::Double(f64::NAN)),
            attr("double.posinf", Val::Double(f64::INFINITY)),
            attr("double.neginf", Val::Double(f64::NEG_INFINITY)),
            attr("double.negzero", Val::Double(-0.0)),
            attr("int.2p53plus1", Val::Int(9_007_199_254_740_993)),
            attr("int.max", Val::Int(i64::MAX)),
            attr("int.min", Val::Int(i64::MIN)),
            attr("empty.string", s("")),
            attr("empty.array", Val::Array(Vec::new())),
            attr("array.strings", Val::Array(vec![s("a"), s("b")])),
            attr("array.ints", Val::Array(vec![Val::Int(1), Val::Int(2)])),
            attr("zero.int", Val::Int(0)),
            attr("false.bool", Val::Bool(false)),
        ],
    );
    rec.notes = notes("unsupported");
    rec.notes.absent_keys = vec!["missing.key".into()];
    f.spans.push(rec);

    // Logs.
    let log = |key: &str, ts, sev: (i32, &'static str), body: Option<&str>, trace, attrs| LogRec {
        ctx: logs.clone(),
        timestamp_ns: ts,
        observed_ns: ts + 1_000_000,
        severity_number: sev.0,
        severity_text: sev.1,
        event_name: String::new(),
        body: body.map(s),
        trace,
        attrs: keyed(key, attrs),
        notes: notes("valid"),
    };
    const INFO: (i32, &str) = (9, "INFO");
    const WARN: (i32, &str) = (13, "WARN");
    const ERROR: (i32, &str) = (17, "ERROR");
    let root_ctx = Some((trace1, root));

    f.logs.push(log(
        "log.info",
        t0 + 150_000,
        INFO,
        Some("cart loaded"),
        root_ctx,
        vec![
            attr("http.route", s("/checkout")),
            attr("cart.items", Val::Int(3)),
            attr("cart.total", Val::Double(total)),
            attr("cache.hit", Val::Bool(false)),
            attr("retry.count", Val::Int(0)),
        ],
    ));
    f.logs.push(log(
        "log.error",
        t0 + 900_000,
        ERROR,
        Some("upstream timeout"),
        root_ctx,
        vec![
            attr("error.type", s("TimeoutError")),
            attr("retry.count", Val::Int(2)),
        ],
    ));
    f.logs.push(log(
        "log.no-trace-context",
        t0 + 2_500_000,
        WARN,
        Some("no trace context"),
        None,
        vec![attr(
            "queue.depth",
            Val::Int(17 + (rng.next_u64() % 8) as i64),
        )],
    ));
    // Identical in both tenants (same dataset name, timestamp, body, attributes):
    // only the trusted scope tells the two rows apart.
    let mut collision = log(
        "log.collision",
        BASE_NS + 777_777_777,
        INFO,
        Some("identical across tenants"),
        None,
        vec![attr("shared.attr", s("same"))],
    );
    collision.notes = notes("boundary");
    f.logs.push(collision);

    // Application events: a log record whose `event_name` is set. The name is
    // the event type; the body is optional; typed attributes carry the fields.
    let event = |key: &str, name: &str, ts, trace, attrs| LogRec {
        ctx: events.clone(),
        timestamp_ns: ts,
        observed_ns: ts + 1_000_000,
        severity_number: 9,
        severity_text: "INFO",
        event_name: name.to_string(),
        body: None,
        trace,
        attrs: keyed(key, attrs),
        notes: notes("valid"),
    };
    f.logs.push(event(
        "event.checkout.completed",
        "checkout.completed",
        t0 + 1_600_000,
        root_ctx,
        vec![
            attr("order.items", Val::Int(3)),
            attr("order.total", Val::Double(total)),
            attr("user.tier", s("gold")),
            attr("payment.retry", Val::Bool(false)),
        ],
    ));
    f.logs.push(event(
        "event.deploy.finished",
        "deploy.finished",
        t0 + 2_000_000,
        None,
        vec![
            attr("deploy.version", s("2026.09.1")),
            attr("deploy.duration_ms", Val::Int(41_200)),
            attr("deploy.rollback", Val::Bool(false)),
        ],
    ));

    // A producer trying to override the trusted scope through record attributes.
    let mut spoof = log(
        "log.spoofed-scope",
        t0 + 3_000_000,
        INFO,
        Some("spoofed scope"),
        None,
        vec![
            attr(WORKSPACE_ATTR, s(other_workspace)),
            attr(DATASET_ATTR, s("other-dataset")),
        ],
    );
    spoof.notes = notes("policy");
    f.logs.push(spoof);

    // Invalid and edge records.
    let invalid = |key: &str, class: &'static str, attrs: Attrs| {
        let mut rec = log(key, t0 + 4_000_000, INFO, Some(key), None, attrs);
        rec.notes = notes(class);
        rec
    };
    let recs = vec![
        invalid(
            "invalid.bytes",
            "unsupported",
            vec![attr("payload.raw", Val::Bytes(vec![0, 1, 254, 255]))],
        ),
        invalid(
            "invalid.array",
            "unsupported",
            vec![attr(
                "payload.list",
                Val::Array(vec![s("a"), Val::Int(1), Val::Bool(true)]),
            )],
        ),
        invalid(
            "invalid.map",
            "unsupported",
            vec![attr(
                "payload.map",
                Val::Map(vec![
                    ("nested.int".into(), Val::Int(2)),
                    ("nested.bool".into(), Val::Bool(false)),
                    ("nested.map".into(), Val::Map(vec![("deep".into(), s("x"))])),
                ]),
            )],
        ),
        invalid(
            "invalid.nonfinite",
            "unsupported",
            vec![
                attr("double.nan", Val::Double(f64::NAN)),
                attr("double.posinf", Val::Double(f64::INFINITY)),
                attr("double.neginf", Val::Double(f64::NEG_INFINITY)),
                attr("double.negzero", Val::Double(-0.0)),
                attr("double.half", Val::Double(1.5)),
            ],
        ),
        invalid(
            "invalid.bigint",
            "boundary",
            vec![
                attr("int.2p53", Val::Int(9_007_199_254_740_992)),
                attr("int.2p53plus1", Val::Int(9_007_199_254_740_993)),
                attr("int.max", Val::Int(i64::MAX)),
                attr("int.min", Val::Int(i64::MIN)),
                attr("int.neg", Val::Int(-7)),
                // Same magnitude as int.2p53 but a different type.
                attr("double.2p53", Val::Double(9_007_199_254_740_992.0)),
            ],
        ),
        invalid(
            "invalid.empty-vs-missing",
            "boundary",
            vec![
                attr("empty.string", s("")),
                attr("empty.bytes", Val::Bytes(Vec::new())),
                attr("empty.array", Val::Array(Vec::new())),
                attr("empty.map", Val::Map(Vec::new())),
                attr("zero.int", Val::Int(0)),
                attr("zero.float", Val::Double(0.0)),
                attr("false.bool", Val::Bool(false)),
            ],
        ),
        invalid(
            "invalid.duplicate-key",
            "rejected",
            vec![attr("dup.key", s("first")), attr("dup.key", s("second"))],
        ),
    ];
    for (i, mut rec) in recs.into_iter().enumerate() {
        // Distinct timestamps keep the invalid group in a stable time order.
        rec.timestamp_ns += i as u64 * 1_000;
        rec.observed_ns = rec.timestamp_ns + 1_000_000;
        if rec.attrs.iter().any(|(k, _)| k == "empty.string") {
            // `missing.key` is never sent. The explicit empty value (an
            // AnyValue with no value set) cannot be produced by the Rust SDK.
            rec.notes.absent_keys = vec!["missing.key".into()];
        }
        f.logs.push(rec);
    }

    // One native metric sample, for the first tenant only.
    if index == 0 {
        f.metrics.push(MetricRec {
            ctx: ctx(workspace_id, "metrics"),
            name: "checkout.requests".into(),
            unit: "{request}".into(),
            description: "Checkout requests handled.".into(),
            value: 7,
            attrs: keyed("metric.requests", vec![attr("http.method", s("GET"))]),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix64_reference_vector() {
        // First outputs of SplitMix64 seeded with 0 (published reference values).
        let mut rng = Rng::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
    }

    #[test]
    fn same_seed_same_ids_different_seed_different_ids() {
        let a = build(1);
        let b = build(1);
        let c = build(2);
        let ids = |f: &Fixture| {
            f.spans
                .iter()
                .map(|s| (s.trace_id, s.span_id))
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&a), ids(&b));
        assert_ne!(ids(&a), ids(&c));
    }

    #[test]
    fn tenants_collide_on_dataset_but_not_on_ids() {
        let f = build(1);
        let datasets: std::collections::BTreeSet<_> = f
            .logs
            .iter()
            .flat_map(|l| l.ctx.resource.iter())
            .filter(|(k, _)| k == DATASET_ATTR)
            .map(|(_, v)| format!("{v:?}"))
            .collect();
        assert_eq!(datasets.len(), 1);
        let traces: std::collections::BTreeSet<_> = f.spans.iter().map(|s| s.trace_id).collect();
        assert_eq!(traces.len(), 2 * 6);
    }
}
