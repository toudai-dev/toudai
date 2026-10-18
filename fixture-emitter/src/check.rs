//! Compare what the wire carried with what the model said it would carry.

use std::collections::BTreeMap;

use serde_json::Value as Json;

use crate::emit::{self, Transport};
use crate::expected::{self, Rec, Row, row};
use crate::model;
use crate::observe::{self, Observed};
use crate::receiver::Receiver;

pub fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| e.to_string())
}

/// Emit the fixture to a loopback receiver over `transport` and compare the
/// decoded requests with the expected rows. Returns the expected rows and the
/// comparison; an `Err` is a failure to run, not a mismatch.
pub fn selfcheck(seed: u64, transport: Transport) -> Result<(Vec<Row>, Outcome), String> {
    let fixture = model::build(seed);
    let rows = expected::rows(&fixture.logs, &fixture.spans, &fixture.metrics)?;
    let rt = runtime()?;
    let receiver = Receiver::start(rt.handle())?;
    send(&rt, &receiver, transport, &fixture)?;
    let observed = {
        let received = receiver.shared.lock().unwrap();
        observe::observe(&received)?
    };
    let outcome = compare(&rows, &observed);
    Ok((rows, outcome))
}

/// Emit the fixture to `receiver`, failing if any export was not accepted.
fn send(
    rt: &tokio::runtime::Runtime,
    receiver: &Receiver,
    transport: Transport,
    fixture: &model::Fixture,
) -> Result<(), String> {
    let endpoint = match transport {
        Transport::Grpc => format!("http://{}", receiver.grpc),
        Transport::Http => format!("http://{}", receiver.http),
    };
    // The tonic exporter needs a runtime context; the blocking HTTP client
    // must not be built inside one.
    let _enter = (transport == Transport::Grpc).then(|| rt.enter());
    emit::emit(fixture, transport, &endpoint)
}

pub struct Outcome {
    /// Observed rows in expected order, with the fixture annotations (which are
    /// not on the wire) copied from the expected row they matched.
    pub rows: Vec<Row>,
    pub problems: Vec<String>,
}

/// Fixture annotations that no wire content can reproduce.
const ANNOTATIONS: [&str; 2] = ["class", "absent_keys"];

pub fn compare(expected: &[Row], observed: &Observed) -> Outcome {
    let mut problems = Vec::new();
    let mut by_key: BTreeMap<String, Row> = BTreeMap::new();
    let recs = observed
        .logs
        .iter()
        .map(Rec::Log)
        .chain(observed.spans.iter().map(Rec::Span))
        .chain(observed.metrics.iter().map(Rec::Metric));
    for rec in recs {
        match row(&rec) {
            Ok(r) => {
                let key = r.join_key();
                if by_key.insert(key.clone(), r).is_some() {
                    problems.push(format!("{key}: received more than once"));
                }
            }
            Err(e) => problems.push(format!("undecodable record: {e}")),
        }
    }

    let mut rows = Vec::new();
    for want in expected {
        let key = want.join_key();
        let Some(mut got) = by_key.remove(&key) else {
            problems.push(format!("{key}: not received"));
            continue;
        };
        for field in ANNOTATIONS {
            got.json[field] = want.json[field].clone();
        }
        if got.json != want.json {
            problems.push(format!(
                "{key}: differs in {}",
                differing_fields(&want.json, &got.json)
            ));
        }
        problems.extend(annotation_problems(&key, want, &got));
        rows.push(got);
    }
    for (key, _) in by_key {
        problems.push(format!("{key}: received but not expected"));
    }
    Outcome { rows, problems }
}

fn differing_fields(want: &Json, got: &Json) -> String {
    let (Some(w), Some(g)) = (want.as_object(), got.as_object()) else {
        return "<non-object>".into();
    };
    let mut fields = Vec::new();
    for (name, value) in w {
        match (value, g.get(name)) {
            (Json::Object(wm), Some(Json::Object(gm))) => {
                for (inner, wv) in wm {
                    if gm.get(inner) != Some(wv) {
                        fields.push(format!("{name}.{inner}"));
                    }
                }
                for inner in gm.keys().filter(|k| !wm.contains_key(*k)) {
                    fields.push(format!("{name}.{inner}"));
                }
            }
            (wv, Some(gv)) if wv == gv => {}
            _ => fields.push(name.clone()),
        }
    }
    fields.join(", ")
}

fn is_empty_list(v: &Json) -> bool {
    v.as_array().is_none_or(Vec::is_empty)
}

/// The annotations must agree with the observed content: a record annotated
/// as unsupported must carry an unsupported value, a rejected one must not be
/// served, a policy record must have had a key dropped, and absent keys must
/// really be absent from the observed attributes.
fn annotation_problems(key: &str, want: &Row, got: &Row) -> Vec<String> {
    let mut out = Vec::new();
    let class = want.json["class"].as_str().unwrap_or_default();
    let j = &got.json;
    let rejected = j["disposition"] == "reject";
    if (class == "rejected") != rejected {
        out.push(format!(
            "{key}: class {class} but disposition {}",
            j["disposition"]
        ));
    }
    if class == "unsupported" && is_empty_list(&j["unsupported_keys"]) {
        out.push(format!(
            "{key}: class unsupported but no unsupported value was observed"
        ));
    }
    if class == "valid" && !is_empty_list(&j["unsupported_keys"]) {
        out.push(format!(
            "{key}: class valid but an unsupported value was observed"
        ));
    }
    if (class == "policy") != !is_empty_list(&j["dropped_keys"]) {
        out.push(format!(
            "{key}: class {class} but dropped_keys {}",
            j["dropped_keys"]
        ));
    }
    if let Some(serving) = j["serving"].as_object() {
        let absent = want.json["absent_keys"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for name in absent.iter().filter_map(Json::as_str) {
            for (map_name, map) in serving {
                let typed = map_name.starts_with("record_") || map_name.starts_with("span_");
                if typed && map.get(name).is_some() {
                    out.push(format!("{key}: absent key {name} present in {map_name}"));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receiver::Rejects;
    use crate::value::Val;

    fn rows_and_observed() -> (Vec<Row>, Observed) {
        let f = model::build(7);
        let rows = expected::rows(&f.logs, &f.spans, &f.metrics).unwrap();
        let observed = Observed {
            logs: f.logs,
            spans: f.spans,
            metrics: f.metrics,
        };
        (rows, observed)
    }

    fn log<'a>(o: &'a mut Observed, key: &str) -> &'a mut crate::model::LogRec {
        o.logs
            .iter_mut()
            .find(|l| {
                l.attrs
                    .iter()
                    .any(|(k, v)| k == "fixture.key" && matches!(v, Val::Str(s) if s == key))
            })
            .unwrap()
    }

    fn span<'a>(o: &'a mut Observed, key: &str) -> &'a mut crate::model::SpanRec {
        o.spans
            .iter_mut()
            .find(|s| {
                s.attrs
                    .iter()
                    .any(|(k, v)| k == "fixture.key" && matches!(v, Val::Str(s) if s == key))
            })
            .unwrap()
    }

    type Tamper = fn(&mut Observed);

    #[test]
    fn untouched_model_matches_itself() {
        let (rows, observed) = rows_and_observed();
        let outcome = compare(&rows, &observed);
        assert_eq!(outcome.problems, Vec::<String>::new());
        assert_eq!(outcome.rows.len(), rows.len());
    }

    /// Each tamper models a plausible wire-level corruption; the comparison
    /// must name it.
    #[test]
    fn tampering_is_detected() {
        let cases: &[(&str, Tamper, &str)] = &[
            (
                "dropped span",
                |o| {
                    o.spans.remove(0);
                },
                "not received",
            ),
            (
                "duplicated log",
                |o| {
                    let l = o.logs[0].clone();
                    o.logs.push(l);
                },
                "more than once",
            ),
            (
                "unknown record",
                |o| {
                    let mut l = o.logs[0].clone();
                    l.attrs[0].1 = Val::Str("log.unknown".into());
                    o.logs.push(l);
                },
                "received but not expected",
            ),
            (
                "int became string",
                |o| {
                    let l = log(o, "log.info");
                    for (k, v) in &mut l.attrs {
                        if k == "cart.items" {
                            *v = Val::Str("3".into());
                        }
                    }
                },
                "serving.record_int",
            ),
            (
                "int above 2^53 rounded through a double",
                |o| {
                    let l = log(o, "invalid.bigint");
                    for (k, v) in &mut l.attrs {
                        if k == "int.2p53plus1" {
                            *v = Val::Int(9_007_199_254_740_992);
                        }
                    }
                },
                "serving.record_int",
            ),
            (
                "NaN became zero",
                |o| {
                    let l = log(o, "invalid.nonfinite");
                    for (k, v) in &mut l.attrs {
                        if k == "double.nan" {
                            *v = Val::Double(0.0);
                        }
                    }
                },
                "serving.record_double",
            ),
            (
                "negative zero lost its sign",
                |o| {
                    let l = log(o, "invalid.nonfinite");
                    for (k, v) in &mut l.attrs {
                        if k == "double.negzero" {
                            *v = Val::Double(0.0);
                        }
                    }
                },
                "serving.record_double",
            ),
            (
                "empty string became missing",
                |o| {
                    let l = log(o, "invalid.empty-vs-missing");
                    l.attrs.retain(|(k, _)| k != "empty.string");
                },
                "serving.record_string",
            ),
            (
                "bytes became string",
                |o| {
                    let l = log(o, "invalid.bytes");
                    l.attrs[1].1 = Val::Str("AAH+/w==".into());
                },
                "serving.record",
            ),
            (
                "reversed span end repaired",
                |o| {
                    let s = span(o, "span.reversed");
                    s.end_ns = s.start_ns + 1;
                },
                "serving.duration_ns",
            ),
            (
                "missing start filled",
                |o| {
                    span(o, "span.missing-start").start_ns = 1;
                },
                "serving.span_timing_status",
            ),
            (
                "span event lost",
                |o| {
                    span(o, "span.root").events.pop();
                },
                "payload",
            ),
            (
                "trace context lost",
                |o| {
                    log(o, "log.info").trace = None;
                },
                "serving.trace_id",
            ),
            (
                "event name lost",
                |o| {
                    log(o, "event.checkout.completed").event_name.clear();
                },
                "serving.kind",
            ),
            (
                "tenant swapped",
                |o| {
                    let l = log(o, "log.info");
                    l.ctx.resource.iter_mut().for_each(|(k, v)| {
                        if k == "toudai.workspace_id" {
                            *v = Val::Str("33333333-3333-4333-8333-333333333333".into());
                        }
                    });
                },
                "not received",
            ),
            (
                "spoofed scope accepted",
                |o| {
                    let l = log(o, "log.spoofed-scope");
                    l.ctx.resource.iter_mut().for_each(|(k, v)| {
                        if k == "toudai.workspace_id" {
                            *v = Val::Str("22222222-2222-4222-8222-222222222222".into());
                        }
                    });
                },
                "not received",
            ),
            (
                "duplicate key survived",
                |o| {
                    let l = log(o, "invalid.duplicate-key");
                    l.attrs.retain(|(k, _)| k != "dup.key");
                },
                "disposition",
            ),
        ];
        for (name, tamper, want) in cases {
            let (rows, mut observed) = rows_and_observed();
            tamper(&mut observed);
            let problems = compare(&rows, &observed).problems;
            assert!(
                problems.iter().any(|p| p.contains(want)),
                "{name}: wanted a problem containing {want:?}, got {problems:?}"
            );
        }
    }

    #[test]
    fn annotations_must_match_content() {
        let (rows, mut observed) = rows_and_observed();
        // The record is annotated "absent" for missing.key; sending it makes the
        // annotation false even though the row would otherwise match.
        log(&mut observed, "invalid.empty-vs-missing")
            .attrs
            .push(("missing.key".into(), Val::Str("x".into())));
        let problems = compare(&rows, &observed).problems;
        assert!(
            problems.iter().any(|p| p.contains("differs")),
            "{problems:?}"
        );
    }

    #[test]
    fn wire_roundtrip_matches_expected_over_both_transports() {
        for transport in [Transport::Grpc, Transport::Http] {
            let (rows, outcome) = selfcheck(3, transport).unwrap();
            assert_eq!(outcome.problems, Vec::<String>::new(), "{transport:?}");
            assert_eq!(outcome.rows.len(), rows.len(), "{transport:?}");
        }
    }

    /// The SDK's batch processors swallow export failures; a destination that
    /// refuses one signal must still fail the run, naming the signal.
    #[test]
    fn a_refused_signal_fails_the_run() {
        let cases = [
            (
                "logs",
                Rejects {
                    logs: true,
                    ..Rejects::default()
                },
            ),
            (
                "spans",
                Rejects {
                    traces: true,
                    ..Rejects::default()
                },
            ),
            (
                "metrics",
                Rejects {
                    metrics: true,
                    ..Rejects::default()
                },
            ),
        ];
        let fixture = model::build(9);
        for transport in [Transport::Grpc, Transport::Http] {
            for (signal, rejects) in cases {
                let rt = runtime().unwrap();
                let receiver = Receiver::start_rejecting(rt.handle(), rejects).unwrap();
                let err = send(&rt, &receiver, transport, &fixture).expect_err(&format!(
                    "{transport:?} {signal} was refused but the run succeeded"
                ));
                assert!(err.contains(signal), "{transport:?} {signal}: {err}");
                // The other signals were still accepted.
                let received = receiver.shared.lock().unwrap();
                assert_eq!(
                    received.logs.is_empty(),
                    signal == "logs",
                    "{transport:?} {signal}"
                );
                assert_eq!(
                    received.traces.is_empty(),
                    signal == "spans",
                    "{transport:?} {signal}"
                );
            }
        }
    }

    #[test]
    fn transports_observe_identical_contents() {
        let grpc = selfcheck(5, Transport::Grpc).unwrap().1;
        let http = selfcheck(5, Transport::Http).unwrap().1;
        assert_eq!(
            expected::to_jsonl(&grpc.rows),
            expected::to_jsonl(&http.rows)
        );
    }

    #[test]
    fn same_seed_gives_identical_expected_text_and_other_seed_does_not() {
        let text = |seed| {
            let f = model::build(seed);
            expected::to_jsonl(&expected::rows(&f.logs, &f.spans, &f.metrics).unwrap())
        };
        assert_eq!(text(11), text(11));
        assert_ne!(text(11), text(12));
    }
}
