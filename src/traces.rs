//! Trace lookup: query inventory items 10 (trace waterfall by trace ID) and 11
//! (the trace with its correlated logs).
//!
//! Logs, spans, and application events are one medium, so a trace is read from
//! the same serving rows every other query reads: the spans of the trace from
//! `serving_spans`, their span events from the canonical payload (the serving
//! row has no events column), and the logs and application events carrying the
//! trace context from `serving_logs`. Every statement is bounded by the
//! dataset, the trace ID, and an event-time window, like every other query; no
//! trace-ordered projection or table exists yet, so the lookup reads the raw
//! path and says so in its diagnostics.
//!
//! The payloads are read after the spans, for exactly the spans selected:
//! ingest commits a payload before its serving row, so every selected span's
//! payload is visible then. The logs are read independently of the spans, so
//! the two signals are not one snapshot: a span or log committed while the
//! lookup runs may be in one list and not yet in the other.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;

use crate::query::{Param, ServingTables, Signal, SqlStatement, TimeRange};

/// The most spans, and separately the most logs, one lookup returns. A larger
/// trace fails with [`TraceError::TooLarge`] instead of returning part of it.
pub const MAX_TRACE_ROWS: usize = 10_000;

/// A validated trace lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceLookup {
    pub dataset: String,
    /// 32 lowercase hex digits, as ingest stores it.
    pub trace_id: String,
    /// Half-open window on event time: span start for spans, the record time
    /// for logs.
    pub time: TimeRange,
    /// Whether to read the correlated logs (inventory item 11).
    pub logs: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceError {
    InvalidTraceId,
    InvalidTimeRange {
        start_ns: i64,
        end_ns: i64,
    },
    /// More than [`MAX_TRACE_ROWS`] spans or logs match.
    TooLarge {
        signal: Signal,
    },
    /// One `event_id` holds rows with different payload hashes.
    Conflict {
        event_id: String,
    },
    /// ClickHouse returned a row the statement cannot have produced.
    Malformed(String),
}

impl TraceError {
    pub fn code(&self) -> &'static str {
        match self {
            TraceError::InvalidTraceId => "invalid_trace_id",
            TraceError::InvalidTimeRange { .. } => "invalid_time_range",
            TraceError::TooLarge { .. } => "trace_too_large",
            TraceError::Conflict { .. } => "event_conflict",
            TraceError::Malformed(_) => "malformed_result",
        }
    }
}

impl fmt::Display for TraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraceError::InvalidTraceId => {
                write!(f, "a trace ID is 32 hex digits and not all zero")
            }
            TraceError::InvalidTimeRange { start_ns, end_ns } => {
                write!(f, "time range [{start_ns}, {end_ns}) is empty")
            }
            TraceError::TooLarge { signal } => write!(
                f,
                "the trace has more than {MAX_TRACE_ROWS} {} in this window; narrow the window",
                match signal {
                    Signal::Spans => "spans",
                    Signal::Logs => "logs",
                }
            ),
            TraceError::Conflict { event_id } => {
                write!(f, "event {event_id:?} holds more than one distinct payload")
            }
            TraceError::Malformed(detail) => write!(f, "unexpected ClickHouse result: {detail}"),
        }
    }
}

impl std::error::Error for TraceError {}

impl TraceLookup {
    /// Check a lookup before any statement runs. Upper-case hex is accepted
    /// and stored lower-case; the all-zero ID is OTLP's "no trace".
    pub fn new(
        dataset: String,
        trace_id: &str,
        time: TimeRange,
        logs: bool,
    ) -> Result<TraceLookup, TraceError> {
        let valid = trace_id.len() == 32
            && trace_id.bytes().all(|b| b.is_ascii_hexdigit())
            && trace_id.bytes().any(|b| b != b'0');
        if !valid {
            return Err(TraceError::InvalidTraceId);
        }
        if time.start_ns >= time.end_ns {
            return Err(TraceError::InvalidTimeRange {
                start_ns: time.start_ns,
                end_ns: time.end_ns,
            });
        }
        Ok(TraceLookup {
            dataset,
            trace_id: trace_id.to_ascii_lowercase(),
            time,
            logs,
        })
    }

    fn params(&self) -> Vec<Param> {
        let param = |name: &str, ch_type, value: String| Param {
            name: name.into(),
            ch_type,
            value,
        };
        vec![
            param("dataset", "String", self.dataset.clone()),
            param("trace_id", "String", self.trace_id.clone()),
            param("start_ns", "Int64", self.time.start_ns.to_string()),
            param("end_ns", "Int64", self.time.end_ns.to_string()),
        ]
    }

    fn statement(&self, sql: String) -> SqlStatement {
        SqlStatement {
            sql,
            params: self.params(),
        }
    }
}

/// The trace's rows of one serving table. An at-least-once replay can store
/// one event many times, which is one event (`LIMIT 1 BY event_id,
/// payload_sha256`); two payloads under one `event_id` stay two rows, so the
/// decoder reports the conflict. One row past the cap is read so that a trace
/// over it is detected rather than cut.
fn rows_of_trace(tables: &ServingTables, signal: Signal, columns: &str) -> String {
    format!(
        "SELECT {columns} FROM {} \
         WHERE dataset = {{dataset:String}} AND trace_id = {{trace_id:String}} \
         AND timestamp >= fromUnixTimestamp64Nano({{start_ns:Int64}}) \
         AND timestamp < fromUnixTimestamp64Nano({{end_ns:Int64}}) \
         ORDER BY timestamp, event_id, payload_sha256 \
         LIMIT 1 BY event_id, payload_sha256 LIMIT {} FORMAT JSONEachRow",
        tables.table(signal),
        MAX_TRACE_ROWS + 1
    )
}

pub fn spans_statement(lookup: &TraceLookup, tables: &ServingTables) -> SqlStatement {
    lookup.statement(rows_of_trace(
        tables,
        Signal::Spans,
        "event_id, span_id, parent_span_id, span_name, span_kind, service_name, \
         status_code, toUnixTimestamp64Nano(timestamp) AS start_ns, \
         toUnixTimestamp64Nano(end_ts) AS end_ns, duration_ns, span_timing_status, \
         payload_sha256",
    ))
}

/// How many bytes of hex-encoded IDs one payload lookup binds. Parameters
/// travel in the request URL, which the HTTP client caps at 64 KiB (`http`'s
/// `Uri` limit), so a large trace's IDs are split over several lookups.
pub const PAYLOAD_ID_BYTES_PER_LOOKUP: usize = 32 * 1024;

/// The canonical payloads of the spans [`spans_statement`] selected, read
/// after it so a payload ingest committed before its span is always found.
/// `event_ids` are those spans' IDs, distinct and at most [`MAX_TRACE_ROWS`]
/// (see [`selected_span_ids`]); they are split into lookups of at most
/// [`PAYLOAD_ID_BYTES_PER_LOOKUP`] bound bytes, about 440 UUIDs each. IDs are
/// bound hex-encoded and joined by `.`, so no ID text can reach the SQL. The
/// canonical table is sorted by `(dataset, event_id)`, so the `IN` is a
/// primary-key search; its `timestamp` is the span start, which prunes
/// partitions with the same window. Each lookup reads at most one row past
/// its ID count: more distinct payloads than IDs means an ID holds two, and
/// that extra row is enough to report the conflict.
pub fn span_payloads_statements(
    lookup: &TraceLookup,
    tables: &ServingTables,
    event_ids: &[String],
) -> Vec<SqlStatement> {
    let mut chunks: Vec<Vec<String>> = Vec::new();
    let mut bytes = 0;
    for id in event_ids {
        let hex: String = id.bytes().map(|b| format!("{b:02x}")).collect();
        if chunks.is_empty() || bytes + hex.len() + 1 > PAYLOAD_ID_BYTES_PER_LOOKUP {
            chunks.push(Vec::new());
            bytes = 0;
        }
        bytes += hex.len() + 1;
        chunks.last_mut().expect("a chunk was pushed").push(hex);
    }
    chunks
        .into_iter()
        .map(|chunk| {
            let mut statement = lookup.statement(format!(
                "SELECT event_id, payload_sha256, payload FROM {} \
                 WHERE dataset = {{dataset:String}} \
                 AND timestamp >= fromUnixTimestamp64Nano({{start_ns:Int64}}) \
                 AND timestamp < fromUnixTimestamp64Nano({{end_ns:Int64}}) \
                 AND event_id IN (SELECT unhex(arrayJoin(splitByChar('.', {{event_ids:String}})))) \
                 ORDER BY event_id, payload_sha256 \
                 LIMIT 1 BY event_id, payload_sha256 LIMIT {} FORMAT JSONEachRow",
                tables.canonical_events(),
                chunk.len() + 1
            ));
            statement.params.push(Param {
                name: "event_ids".into(),
                ch_type: "String",
                value: chunk.join("."),
            });
            statement
        })
        .collect()
}

pub fn logs_statement(lookup: &TraceLookup, tables: &ServingTables) -> SqlStatement {
    lookup.statement(rows_of_trace(
        tables,
        Signal::Logs,
        "event_id, kind, toUnixTimestamp64Nano(timestamp) AS timestamp_ns, span_id, \
         service_name, severity_text, severity_number, body, event_name, payload_sha256",
    ))
}

/// How a span hangs in the waterfall.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Parent {
    /// A root span: no parent span ID.
    None,
    /// The parent is in the result.
    Found,
    /// The parent span ID is not in the result: never sent, stored under
    /// another dataset, or outside the window. The span is shown as a root.
    Missing,
    /// The span is the earliest of spans whose parents form a loop. Its own
    /// parent link is cut and it is shown as a root; the rest of the loop
    /// hangs under it.
    Cycle,
}

#[derive(Debug, Serialize)]
pub struct WaterfallSpan {
    pub event_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub name: String,
    pub span_kind: String,
    pub service_name: String,
    pub status_code: i64,
    /// Nanoseconds as decimal strings, as stored. `end_ns` is "0" when the
    /// span carried no end.
    pub start_ns: String,
    pub end_ns: String,
    /// `None` (JSON null) when the duration is unavailable: missing or
    /// reversed timing, which `timing_status` names. Never zero in its place.
    pub duration_ns: Option<String>,
    pub timing_status: String,
    /// Distance from the span's root in the waterfall.
    pub depth: u32,
    pub parent: Parent,
    /// The span events as the canonical payload stores them (name, time_ns,
    /// type-tagged attributes, producer order). JSON null when unavailable:
    /// the payload has no `span.events` (a writer that drops them) or the
    /// span has no canonical row. The text is the stored text, byte for byte.
    pub events: Option<Box<RawValue>>,
}

#[derive(Debug, Serialize)]
pub struct TraceLog {
    pub event_id: String,
    /// `log` or `event` (an application event).
    pub kind: String,
    pub timestamp_ns: String,
    pub span_id: String,
    /// Whether `span_id` names a span in the result.
    pub span_found: bool,
    pub service_name: String,
    pub severity_text: String,
    pub severity_number: i64,
    /// The serving body text; the exact typed body is in the canonical event.
    pub body: String,
    pub event_name: String,
}

#[derive(Debug, Serialize)]
pub struct TraceDiagnostics {
    pub storage_path: &'static str,
    pub approximation: &'static str,
    pub tables: Vec<&'static str>,
    /// Spans whose `events` is null.
    pub span_events_unavailable: String,
    /// Span IDs held by more than one span. A producer retry that ingest
    /// accepted twice stores one span under two `event_id`s; both are shown.
    pub duplicate_span_ids: String,
}

#[derive(Debug, Serialize)]
pub struct Trace {
    pub dataset: String,
    pub trace_id: String,
    /// In waterfall order: each root by start time, followed depth-first by
    /// its descendants, siblings by start time.
    pub spans: Vec<WaterfallSpan>,
    /// The correlated logs and application events by time; absent when the
    /// lookup did not ask for them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs: Option<Vec<TraceLog>>,
    pub diagnostics: TraceDiagnostics,
}

fn text(row: &Value, name: &str) -> Result<String, TraceError> {
    row.get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| TraceError::Malformed(format!("column {name} is not text")))
}

/// A 64-bit integer column: ClickHouse quotes them in JSON by default, and a
/// profile may not. Returned as its decimal text.
fn int_text(row: &Value, name: &str) -> Result<String, TraceError> {
    match row.get(name) {
        Some(Value::String(s)) if s.parse::<i64>().is_ok() => Ok(s.clone()),
        Some(Value::Number(n)) if n.is_i64() => Ok(n.to_string()),
        _ => Err(TraceError::Malformed(format!(
            "column {name} is not an integer"
        ))),
    }
}

fn small_int(row: &Value, name: &str) -> Result<i64, TraceError> {
    row.get(name)
        .and_then(Value::as_i64)
        .ok_or_else(|| TraceError::Malformed(format!("column {name} is not an integer")))
}

fn check_cap(rows: &[Value], signal: Signal) -> Result<(), TraceError> {
    if rows.len() > MAX_TRACE_ROWS {
        return Err(TraceError::TooLarge { signal });
    }
    Ok(())
}

/// The distinct `event_id`s of the spans [`spans_statement`] returned, for
/// [`span_payloads_statements`]. A trace over the cap is refused here, before
/// any payload is read.
pub fn selected_span_ids(span_rows: &[Value]) -> Result<Vec<String>, TraceError> {
    check_cap(span_rows, Signal::Spans)?;
    let mut seen = HashSet::new();
    let mut ids = Vec::new();
    for row in span_rows {
        let event_id = text(row, "event_id")?;
        if seen.insert(event_id.clone()) {
            ids.push(event_id);
        }
    }
    Ok(ids)
}

/// One identity, one payload: every row the lookup read (spans, logs, and
/// canonical payloads) must agree on the payload hash of its `event_id`.
/// Replays share the hash and are one event; any other second hash is a
/// conflict, reported rather than picked.
fn check_identities<'a>(rows: impl Iterator<Item = &'a Value>) -> Result<(), TraceError> {
    let mut hashes: HashMap<String, String> = HashMap::new();
    for row in rows {
        let hash = text(row, "payload_sha256")?;
        match hashes.entry(text(row, "event_id")?) {
            Entry::Vacant(slot) => {
                slot.insert(hash);
            }
            Entry::Occupied(seen) if *seen.get() != hash => {
                return Err(TraceError::Conflict {
                    event_id: seen.key().clone(),
                });
            }
            Entry::Occupied(_) => {}
        }
    }
    Ok(())
}

/// The part of a stored canonical payload the trace reads. `events` borrows
/// the stored text, so the span events are returned byte for byte as stored.
#[derive(Deserialize)]
struct StoredPayload<'a> {
    #[serde(borrow)]
    span: Option<StoredSpan<'a>>,
}

#[derive(Deserialize)]
struct StoredSpan<'a> {
    #[serde(borrow)]
    events: Option<&'a RawValue>,
}

/// Assemble the trace from the rows of [`spans_statement`],
/// [`span_payloads_statements`] (for those spans), and [`logs_statement`]
/// (`None` when the lookup did not read logs). `None` when no span and no log
/// matched.
pub fn assemble(
    lookup: &TraceLookup,
    span_rows: &[Value],
    payload_rows: &[Value],
    log_rows: Option<&[Value]>,
) -> Result<Option<Trace>, TraceError> {
    check_cap(span_rows, Signal::Spans)?;
    if let Some(rows) = log_rows {
        check_cap(rows, Signal::Logs)?;
    }
    check_identities(
        span_rows
            .iter()
            .chain(payload_rows)
            .chain(log_rows.unwrap_or_default()),
    )?;
    if span_rows.is_empty() && log_rows.is_none_or(<[Value]>::is_empty) {
        return Ok(None);
    }

    // event_id -> stored span events. Identities agree on their hash, so a
    // canonical row is the payload of the span with its event_id.
    let mut events: HashMap<String, Option<Box<RawValue>>> = HashMap::new();
    for row in payload_rows {
        let payload = text(row, "payload")?;
        let stored: StoredPayload<'_> = serde_json::from_str(&payload)
            .map_err(|e| TraceError::Malformed(format!("payload is not JSON: {e}")))?;
        let raw = stored.span.and_then(|s| s.events).map(RawValue::to_owned);
        events.insert(text(row, "event_id")?, raw);
    }

    let mut spans = span_rows
        .iter()
        .map(|row| {
            let key = text(row, "event_id")?;
            let duration_ns = match row.get("duration_ns") {
                Some(Value::Null) => None,
                _ => Some(int_text(row, "duration_ns")?),
            };
            Ok(WaterfallSpan {
                span_id: text(row, "span_id")?,
                parent_span_id: text(row, "parent_span_id")?,
                name: text(row, "span_name")?,
                span_kind: text(row, "span_kind")?,
                service_name: text(row, "service_name")?,
                status_code: small_int(row, "status_code")?,
                start_ns: int_text(row, "start_ns")?,
                end_ns: int_text(row, "end_ns")?,
                duration_ns,
                timing_status: text(row, "span_timing_status")?,
                depth: 0,
                parent: Parent::None,
                events: events.remove(&key).flatten(),
                event_id: key,
            })
        })
        .collect::<Result<Vec<_>, TraceError>>()?;
    // The statement orders by start time already; the numeric sort keeps the
    // order exact without trusting the text form, and the span ID breaks ties.
    spans.sort_by(|a, b| {
        let start = |s: &WaterfallSpan| s.start_ns.parse::<i64>().unwrap_or_default();
        start(a)
            .cmp(&start(b))
            .then_with(|| a.span_id.cmp(&b.span_id))
            .then_with(|| a.event_id.cmp(&b.event_id))
    });
    let (spans, duplicate_span_ids) = waterfall(spans);

    // An empty span ID (absent or all-zero on the wire) names no span.
    let span_ids: HashSet<&str> = spans
        .iter()
        .map(|s| s.span_id.as_str())
        .filter(|id| !id.is_empty())
        .collect();
    let logs = log_rows
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let span_id = text(row, "span_id")?;
                    Ok(TraceLog {
                        event_id: text(row, "event_id")?,
                        kind: text(row, "kind")?,
                        timestamp_ns: int_text(row, "timestamp_ns")?,
                        span_found: span_ids.contains(span_id.as_str()),
                        span_id,
                        service_name: text(row, "service_name")?,
                        severity_text: text(row, "severity_text")?,
                        severity_number: small_int(row, "severity_number")?,
                        body: text(row, "body")?,
                        event_name: text(row, "event_name")?,
                    })
                })
                .collect::<Result<Vec<_>, TraceError>>()
        })
        .transpose()?;

    let mut tables = vec!["serving_spans", "canonical_events"];
    if logs.is_some() {
        tables.push("serving_logs");
    }
    let unavailable = spans.iter().filter(|s| s.events.is_none()).count();
    Ok(Some(Trace {
        dataset: lookup.dataset.clone(),
        trace_id: lookup.trace_id.clone(),
        spans,
        logs,
        diagnostics: TraceDiagnostics {
            storage_path: "raw",
            approximation: "exact",
            tables,
            span_events_unavailable: unavailable.to_string(),
            duplicate_span_ids: duplicate_span_ids.to_string(),
        },
    }))
}

/// Put spans sorted by start time into waterfall order and set their depth
/// and parent status; also returns how many span IDs repeat. A child of a
/// repeated span ID hangs under its earliest holder. An empty span ID (absent
/// or all-zero on the wire) names no span: it is never a parent and never a
/// duplicate. Nothing is dropped.
fn waterfall(spans: Vec<WaterfallSpan>) -> (Vec<WaterfallSpan>, usize) {
    let n = spans.len();
    let mut first: HashMap<&str, usize> = HashMap::new();
    let mut duplicates: HashSet<&str> = HashSet::new();
    for (i, span) in spans.iter().enumerate() {
        if span.span_id.is_empty() {
            continue;
        }
        // Keep the earliest holder; a later one only counts as a duplicate.
        match first.entry(span.span_id.as_str()) {
            Entry::Vacant(slot) => {
                slot.insert(i);
            }
            Entry::Occupied(_) => {
                duplicates.insert(span.span_id.as_str());
            }
        }
    }
    let mut parent_of: Vec<Option<usize>> = vec![None; n];
    let mut parent = vec![Parent::None; n];
    for (i, span) in spans.iter().enumerate() {
        if span.parent_span_id.is_empty() {
            continue;
        }
        match first.get(span.parent_span_id.as_str()) {
            Some(&p) => {
                parent_of[i] = Some(p);
                parent[i] = Parent::Found;
            }
            None => parent[i] = Parent::Missing,
        }
    }

    // Each span has at most one parent, so following parents from any span
    // either ends at a root or enters a loop. Cut each loop at its earliest
    // span, which becomes a root marked `cycle`; the rest of the loop and
    // every span hanging off it keep their real parents.
    let mut state = vec![0u8; n]; // 0 unseen, 1 on the current walk, 2 done
    for start in 0..n {
        let mut walk = Vec::new();
        let mut next = Some(start);
        while let Some(i) = next {
            if state[i] == 1 {
                let at = walk.iter().position(|&w| w == i).expect("on the walk");
                let earliest = *walk[at..].iter().min().expect("a loop is non-empty");
                parent_of[earliest] = None;
                parent[earliest] = Parent::Cycle;
            }
            if state[i] != 0 {
                break;
            }
            state[i] = 1;
            walk.push(i);
            next = parent_of[i];
        }
        for i in walk {
            state[i] = 2;
        }
    }

    // Now a forest: every root in start order, each followed depth-first by
    // its children in start order.
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (i, p) in parent_of.iter().enumerate() {
        if let Some(p) = p {
            children[*p].push(i);
        }
    }
    let mut order = Vec::with_capacity(n);
    let mut depth = vec![0u32; n];
    for root in (0..n).filter(|&i| parent_of[i].is_none()) {
        let mut stack = vec![(root, 0u32)];
        while let Some((i, d)) = stack.pop() {
            depth[i] = d;
            order.push(i);
            // Reversed, so the earliest child is visited first.
            stack.extend(children[i].iter().rev().map(|&c| (c, d + 1)));
        }
    }
    debug_assert_eq!(order.len(), n, "every span is reached once");
    let duplicates = duplicates.len();

    let mut slots: Vec<Option<WaterfallSpan>> = spans.into_iter().map(Some).collect();
    let ordered = order
        .into_iter()
        .map(|i| {
            let mut span = slots[i].take().expect("each span is visited once");
            span.depth = depth[i];
            span.parent = parent[i];
            span
        })
        .collect();
    (ordered, duplicates)
}

#[cfg(test)]
mod tests;
