use serde_json::{Value, json};

use super::*;

fn lookup() -> TraceLookup {
    TraceLookup::new(
        "fixture".into(),
        "0123456789ABCDEF0123456789abcdef",
        TimeRange {
            start_ns: 0,
            end_ns: 100,
        },
        true,
    )
    .unwrap()
}

/// A span row as `spans_statement` returns it: 64-bit integers quoted.
fn span(id: &str, parent: &str, start: i64, duration: Option<i64>) -> Value {
    json!({
        "event_id": format!("e-{id}"), "span_id": id, "parent_span_id": parent,
        "span_name": format!("op {id}"), "span_kind": "Internal", "service_name": "svc",
        "status_code": 0, "start_ns": start.to_string(),
        "end_ns": duration.map_or("0".into(), |d| (start + d).to_string()),
        "duration_ns": duration.map(|d| d.to_string()),
        "span_timing_status": if duration.is_some() { "ok" } else { "missing" },
        "payload_sha256": format!("h-{id}"),
    })
}

/// A canonical row as `span_payloads_statement` returns it, with `span.events`
/// set to the given JSON text or left out.
fn payload(id: &str, events: Option<&str>) -> Value {
    let span = match events {
        Some(events) => format!(r#"{{"name":"op","events":{events}}}"#),
        None => r#"{"name":"op"}"#.into(),
    };
    json!({
        "event_id": format!("e-{id}"), "payload_sha256": format!("h-{id}"),
        "payload": format!(r#"{{"kind":"span","span":{span}}}"#),
    })
}

fn log(id: &str, span_id: &str, ts: i64, kind: &str) -> Value {
    json!({
        "event_id": id, "kind": kind, "timestamp_ns": ts.to_string(), "span_id": span_id,
        "service_name": "svc", "severity_text": "INFO", "severity_number": 9,
        "body": "", "event_name": if kind == "event" { "x.done" } else { "" },
        "payload_sha256": "h",
    })
}

/// A span stored without a span ID (absent or all-zero on the wire).
fn no_span_id(event_id: &str, start: i64) -> Value {
    let mut row = span("", "", start, Some(1));
    row["event_id"] = json!(event_id);
    row
}

/// `(span_id, depth, parent)` in waterfall order.
fn shape(trace: &Trace) -> Vec<(&str, u32, Parent)> {
    trace
        .spans
        .iter()
        .map(|s| (s.span_id.as_str(), s.depth, s.parent))
        .collect()
}

#[test]
fn lookups_are_validated() {
    let time = TimeRange {
        start_ns: 1,
        end_ns: 2,
    };
    let cases = [
        ("0123456789abcdef0123456789abcdef", time, Ok(())),
        (
            "0123456789abcdef0123456789abcde",
            time,
            Err("invalid_trace_id"),
        ),
        (
            "0123456789abcdef0123456789abcdeg",
            time,
            Err("invalid_trace_id"),
        ),
        (
            "00000000000000000000000000000000",
            time,
            Err("invalid_trace_id"),
        ),
        // Multi-byte text of the right byte length is not hex.
        (
            "é123456789abcdef0123456789abcde",
            time,
            Err("invalid_trace_id"),
        ),
        (
            "0123456789abcdef0123456789abcdef",
            TimeRange {
                start_ns: 2,
                end_ns: 2,
            },
            Err("invalid_time_range"),
        ),
    ];
    for (trace_id, time, want) in cases {
        let got = TraceLookup::new("d".into(), trace_id, time, false)
            .map(|_| ())
            .map_err(|e| e.code());
        assert_eq!(got, want, "{trace_id}");
    }
    // Stored IDs are lower-case hex.
    assert_eq!(lookup().trace_id, "0123456789abcdef0123456789abcdef");
}

#[test]
fn statements_bind_every_value() {
    let tables = ServingTables::new("db").unwrap();
    let hostile = TraceLookup {
        dataset: "x' OR 1 = 1 --".into(),
        ..lookup()
    };
    let ids = ["e-1".to_owned(), "x'.,) OR 1".to_owned()];
    let payloads = span_payloads_statements(&hostile, &tables, &ids).remove(0);
    for statement in [
        spans_statement(&hostile, &tables),
        payloads.clone(),
        logs_statement(&hostile, &tables),
    ] {
        assert!(!statement.sql.contains(&hostile.dataset));
        assert!(!statement.sql.contains(&hostile.trace_id));
        assert!(statement.sql.contains("{dataset:String}"));
        // The payload lookup is keyed by the selected spans' IDs instead.
        if statement != payloads {
            assert!(statement.sql.contains("{trace_id:String}"));
        }
        assert!(statement.sql.contains("{start_ns:Int64}"));
        let bound: Vec<_> = statement.params.iter().map(|p| p.value.as_str()).collect();
        assert_eq!(
            bound[..4],
            [
                hostile.dataset.as_str(),
                hostile.trace_id.as_str(),
                "0",
                "100"
            ]
        );
    }
    // The selected IDs travel hex-encoded and `.`-joined, and the output is
    // capped one row past them so a second payload still shows.
    assert!(!payloads.sql.contains(&ids[1]));
    assert_eq!(payloads.params[4].value, "652d31.78272e2c29204f522031");
    assert!(payloads.sql.contains("LIMIT 3 FORMAT"));
    assert!(
        spans_statement(&hostile, &tables)
            .sql
            .contains("FROM db.serving_spans ")
    );
    assert!(
        logs_statement(&hostile, &tables)
            .sql
            .contains("FROM db.serving_logs ")
    );
    assert!(payloads.sql.contains("FROM db.canonical_events "));
}

/// Waterfall order follows parent links depth-first with siblings by start
/// time, whatever order the rows arrive in; spans whose parent is absent or
/// that sit on a parent cycle are kept and shown as roots.
#[test]
fn waterfall_order_table() {
    type Case = (&'static str, Vec<Value>, Vec<(&'static str, u32, Parent)>);
    let cases: Vec<Case> = vec![
        (
            // Start order a, d, b, c, e differs from the tree order.
            "tree",
            vec![
                span("c", "b", 30, Some(5)),
                span("a", "", 10, Some(90)),
                span("e", "a", 50, Some(1)),
                span("b", "a", 25, Some(20)),
                span("d", "a", 20, Some(1)),
            ],
            vec![
                ("a", 0, Parent::None),
                ("d", 1, Parent::Found),
                ("b", 1, Parent::Found),
                ("c", 2, Parent::Found),
                ("e", 1, Parent::Found),
            ],
        ),
        (
            "missing parent is a root in start order",
            vec![
                span("a", "", 10, Some(90)),
                span("o", "lost", 5, Some(1)),
                span("k", "o", 6, Some(1)),
            ],
            vec![
                ("o", 0, Parent::Missing),
                ("k", 1, Parent::Found),
                ("a", 0, Parent::None),
            ],
        ),
        (
            "cycle starts at its earliest span",
            vec![
                span("y", "x", 20, Some(1)),
                span("x", "y", 10, Some(1)),
                span("z", "y", 30, Some(1)),
            ],
            vec![
                ("x", 0, Parent::Cycle),
                ("y", 1, Parent::Found),
                ("z", 2, Parent::Found),
            ],
        ),
        (
            "self parent",
            vec![span("s", "s", 1, Some(1))],
            vec![("s", 0, Parent::Cycle)],
        ),
        (
            // Only the loop is cut: the early child of a loop member keeps
            // its parent, and a later ordinary root stays last.
            "child of a loop",
            vec![
                span("child", "a", 1, Some(1)),
                span("a", "b", 2, Some(1)),
                span("b", "a", 3, Some(1)),
                span("normal", "", 4, Some(1)),
            ],
            vec![
                ("a", 0, Parent::Cycle),
                ("child", 1, Parent::Found),
                ("b", 1, Parent::Found),
                ("normal", 0, Parent::None),
            ],
        ),
        (
            "two loops, each cut at its earliest span",
            vec![
                span("p", "q", 5, Some(1)),
                span("q", "p", 1, Some(1)),
                span("r", "s", 2, Some(1)),
                span("s", "r", 3, Some(1)),
            ],
            vec![
                ("q", 0, Parent::Cycle),
                ("p", 1, Parent::Found),
                ("r", 0, Parent::Cycle),
                ("s", 1, Parent::Found),
            ],
        ),
        (
            // An empty span ID names no span: an empty parent is a root and
            // two empty IDs are no duplicate.
            "empty span IDs",
            vec![no_span_id("e-1", 1), no_span_id("e-2", 2)],
            vec![("", 0, Parent::None), ("", 0, Parent::None)],
        ),
    ];
    for (name, rows, want) in cases {
        let trace = assemble(&lookup(), &rows, &[], Some(&[])).unwrap().unwrap();
        assert_eq!(shape(&trace), want, "{name}");
        assert_eq!(trace.diagnostics.duplicate_span_ids, "0", "{name}");
    }
}

#[test]
fn spans_logs_and_events_are_joined() {
    let spans = [
        span("a", "", 10, Some(90)),
        span("b", "a", 20, None),
        span("c", "a", 30, Some(0)),
    ];
    // Spacing and an escape spelled in lower case come back exactly as
    // stored, so the text matches what the payload hash covers.
    let stored = r#"[ {"attributes":{},"name":"ev\u000b","time_ns":"15"}]"#;
    let event_rows = [
        payload("a", Some(stored)),
        // A payload written without `span.events`.
        payload("b", None),
        // Span c has no canonical row.
    ];
    let logs = [
        log("l1", "b", 21, "log"),
        log("l2", "gone", 40, "log"),
        log("l3", "a", 90, "event"),
        // A log carrying the trace but no span ID.
        log("l4", "", 95, "log"),
    ];
    let trace = assemble(&lookup(), &spans, &event_rows, Some(&logs))
        .unwrap()
        .unwrap();
    let a = &trace.spans[0];
    assert_eq!(a.events.as_ref().unwrap().get(), stored);
    assert!(trace.spans[1].events.is_none());
    assert!(trace.spans[2].events.is_none());
    assert_eq!(trace.diagnostics.span_events_unavailable, "2");
    // Missing timing is null, never zero; a valid zero stays "0".
    assert_eq!(trace.spans[1].duration_ns, None);
    assert_eq!(trace.spans[1].timing_status, "missing");
    assert_eq!(trace.spans[2].duration_ns.as_deref(), Some("0"));

    let logs = trace.logs.as_ref().unwrap();
    let got: Vec<_> = logs
        .iter()
        .map(|l| (l.event_id.as_str(), l.kind.as_str(), l.span_found))
        .collect();
    assert_eq!(
        got,
        [
            ("l1", "log", true),
            ("l2", "log", false),
            ("l3", "event", true),
            ("l4", "log", false)
        ]
    );
    assert_eq!(
        trace.diagnostics.tables,
        ["serving_spans", "canonical_events", "serving_logs"]
    );

    let json = serde_json::to_value(&trace).unwrap();
    assert_eq!(json["spans"][1]["duration_ns"], Value::Null);
    assert_eq!(json["spans"][1]["events"], Value::Null);
    assert_eq!(json["spans"][0]["parent"], "none");
    assert_eq!(json["spans"][0]["events"][0]["time_ns"], "15");

    // Without logs the response has no `logs` member at all.
    let trace = assemble(&lookup(), &spans, &event_rows, None)
        .unwrap()
        .unwrap();
    let json = serde_json::to_value(&trace).unwrap();
    assert!(json.get("logs").is_none());
    assert_eq!(
        trace.diagnostics.tables,
        ["serving_spans", "canonical_events"]
    );
}

#[test]
fn duplicate_span_ids_are_kept_and_counted() {
    // A producer retry stored under two event IDs: both shown, children hang
    // under the earlier one.
    let mut retry = span("a", "", 12, Some(80));
    retry["event_id"] = json!("e-a2");
    let rows = [
        span("a", "", 10, Some(90)),
        retry,
        span("b", "a", 20, Some(1)),
    ];
    let trace = assemble(&lookup(), &rows, &[], None).unwrap().unwrap();
    let got: Vec<_> = trace
        .spans
        .iter()
        .map(|s| (s.event_id.as_str(), s.depth))
        .collect();
    assert_eq!(got, [("e-a", 0), ("e-b", 1), ("e-a2", 0)]);
    assert_eq!(trace.diagnostics.duplicate_span_ids, "1");
}

#[test]
fn refusals() {
    let l = lookup();
    // Nothing matched: no trace.
    assert!(assemble(&l, &[], &[], Some(&[])).unwrap().is_none());
    assert!(assemble(&l, &[], &[], None).unwrap().is_none());
    // Logs alone are still a trace.
    assert!(
        assemble(&l, &[], &[], Some(&[log("l", "a", 1, "log")]))
            .unwrap()
            .is_some()
    );

    let with_hash = |mut row: Value, hash: &str| {
        row["payload_sha256"] = json!(hash);
        row
    };
    let a = || span("a", "", 10, Some(1));
    // (case, spans, canonical payloads, logs, error code)
    type Case = (
        &'static str,
        Vec<Value>,
        Vec<Value>,
        Vec<Value>,
        &'static str,
    );
    let cases: Vec<Case> = vec![
        (
            "two payloads under one span event_id",
            vec![a(), with_hash(a(), "h-other")],
            vec![],
            vec![],
            "event_conflict",
        ),
        (
            "one event_id as a span and as a log with another hash",
            vec![a()],
            vec![],
            vec![with_hash(log("e-a", "a", 1, "log"), "h-log")],
            "event_conflict",
        ),
        (
            "two canonical variants of one returned span",
            vec![a()],
            vec![
                payload("a", Some("[]")),
                with_hash(payload("a", Some("[]")), "h-x"),
            ],
            vec![],
            "event_conflict",
        ),
        (
            "a canonical row whose hash is not the span's",
            vec![a()],
            vec![with_hash(payload("a", Some("[]")), "h-x")],
            vec![],
            "event_conflict",
        ),
        (
            "spans over the cap",
            (0..=MAX_TRACE_ROWS)
                .map(|i| span(&i.to_string(), "", 1, Some(1)))
                .collect(),
            vec![],
            vec![],
            "trace_too_large",
        ),
        (
            "logs over the cap",
            vec![],
            vec![],
            (0..=MAX_TRACE_ROWS)
                .map(|i| log(&i.to_string(), "a", 1, "log"))
                .collect(),
            "trace_too_large",
        ),
        (
            "unquoted text where an integer belongs",
            vec![{
                let mut s = a();
                s["start_ns"] = json!("ten");
                s
            }],
            vec![],
            vec![],
            "malformed_result",
        ),
        (
            "a payload that is not JSON",
            vec![a()],
            vec![{
                let mut p = payload("a", None);
                p["payload"] = json!("not json");
                p
            }],
            vec![],
            "malformed_result",
        ),
    ];
    for (name, spans, payloads, logs, code) in cases {
        let got = assemble(&l, &spans, &payloads, Some(&logs))
            .map(|_| ())
            .unwrap_err();
        assert_eq!(got.code(), code, "{name}");
    }
    // Replays of one payload, as span, log, and canonical rows, are one event.
    let replayed = assemble(
        &l,
        &[a()],
        &[payload("a", Some("[]")), payload("a", Some("[]"))],
        None,
    );
    assert_eq!(replayed.unwrap().unwrap().spans.len(), 1);
}

/// The largest trace's IDs are split into lookups that each fit a request
/// URL, cover every ID once, and are each capped one row past their IDs.
#[test]
fn payload_lookups_are_split_to_fit_a_request() {
    let tables = ServingTables::new("db").unwrap();
    let ids: Vec<String> = (0..MAX_TRACE_ROWS)
        .map(|i| format!("019a0000-0000-7000-8000-{i:012}"))
        .collect();
    let statements = span_payloads_statements(&lookup(), &tables, &ids);
    assert!(statements.len() > 1);
    let mut covered = Vec::new();
    for statement in &statements {
        let bound = &statement.params[4].value;
        assert!(bound.len() <= PAYLOAD_ID_BYTES_PER_LOOKUP);
        let chunk: Vec<String> = bound
            .split('.')
            .map(|hex| {
                let bytes: Vec<u8> = (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                    .collect();
                String::from_utf8(bytes).unwrap()
            })
            .collect();
        assert!(
            statement
                .sql
                .contains(&format!("LIMIT {} FORMAT", chunk.len() + 1))
        );
        covered.extend(chunk);
    }
    assert_eq!(covered, ids);
    assert!(span_payloads_statements(&lookup(), &tables, &[]).is_empty());
}

#[test]
fn payloads_are_read_for_the_selected_spans_only() {
    // A replayed span is selected once; over the cap, nothing is selected and
    // no payload is read.
    let rows = [
        span("a", "", 1, Some(1)),
        span("a", "", 1, Some(1)),
        span("b", "a", 2, Some(1)),
    ];
    assert_eq!(selected_span_ids(&rows).unwrap(), ["e-a", "e-b"]);
    let over: Vec<Value> = (0..=MAX_TRACE_ROWS)
        .map(|i| span(&i.to_string(), "", 1, Some(1)))
        .collect();
    assert_eq!(
        selected_span_ids(&over).unwrap_err().code(),
        "trace_too_large"
    );
}
