//! Command lines to query-service requests.
//!
//! Field and literal syntax follows the typed-value contract: a field names
//! its namespace (`record:` is the default), and a literal names its type, so
//! `int:1`, `double:1`, and `string:1` are three different filters and the
//! client never infers a type from how a value looks.

use serde_json::{Map, Value, json};

use crate::time::{parse_duration, parse_time};

pub const USAGE: &str = "\
usage: toudai-cli <command> [options]

Reads TOUDAI_URL (default http://127.0.0.1:8080) and TOUDAI_READ_KEY (required).

Commands over a dataset and a half-open time range [--from, --to):
  count        events matching the filters, optionally grouped (--by)
  series       counts per --every bucket, starting at --from
  percentiles  exact nearest-rank percentiles of --field (with --type) or of
               --span-duration, optionally grouped (--by) or bucketed (--every)
  events       the matching events, newest first, with their event_id
  event <dataset> <event_id>
               one event's canonical payload, with its checksum verified

Options for count, series, percentiles, and events:
  --dataset NAME          required
  --signal logs|spans     default logs
  --from TIME --to TIME   RFC 3339 UTC (2026-09-01T07:00:00Z) or Unix nanoseconds
  --where FILTER          repeatable; all must hold:
                            FIELD = TYPE:VALUE   (also !=, <, <=, >, >=)
                            FIELD exists | FIELD absent | FIELD is TYPE
  --by FIELD              group key, repeatable (not for events)
  --every DURATION        bucket width: 500ms, 10s, 1h (series, percentiles)
  --field FIELD --type int|double | --span-duration   (percentiles)
  --p LIST                percentiles, default 50,90,99 (percentiles)
  --limit N --order newest|oldest|longest --show FIELD  (events)
  --json                  print the service's response unchanged

FIELD is record:KEY (or just KEY), resource:KEY, or scope:KEY.
TYPE is string, bool, int, double, bytes, array, map, or empty.
";

/// How to show a query response.
#[derive(Debug, Clone, PartialEq)]
pub enum View {
    Aggregate,
    /// Every bucket of `[start, end)` is shown, including those without rows.
    Series {
        start_ns: i64,
        end_ns: i64,
        every_ns: i64,
    },
    Events,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Help,
    Query {
        body: Value,
        view: View,
        json: bool,
    },
    Event {
        dataset: String,
        event_id: String,
        json: bool,
    },
}

/// The most buckets the query service returns in one answer.
const MAX_BUCKETS: i128 = 1_000;

const SWITCHES: [&str; 3] = ["json", "span-duration", "help"];
const COMMON: [&str; 7] = ["dataset", "signal", "from", "to", "where", "json", "help"];

struct Flags {
    values: Vec<(String, String)>,
    positional: Vec<String>,
}

impl Flags {
    fn parse(args: &[String]) -> Result<Flags, String> {
        let mut flags = Flags {
            values: Vec::new(),
            positional: Vec::new(),
        };
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            match arg.strip_prefix("--") {
                Some(name) if SWITCHES.contains(&name) => {
                    flags.values.push((name.to_owned(), String::new()))
                }
                Some(name) => {
                    let value = args
                        .next()
                        .ok_or_else(|| format!("--{name} needs a value"))?;
                    flags.values.push((name.to_owned(), value.clone()));
                }
                None => flags.positional.push(arg.clone()),
            }
        }
        Ok(flags)
    }

    fn check(&self, command: &str, allowed: &[&str]) -> Result<(), String> {
        for (name, _) in &self.values {
            if !COMMON.contains(&name.as_str()) && !allowed.contains(&name.as_str()) {
                return Err(format!("{command} does not take --{name}"));
            }
        }
        Ok(())
    }

    fn all(&self, name: &str) -> impl Iterator<Item = &str> {
        self.values
            .iter()
            .filter(move |(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    fn one(&self, name: &str) -> Result<Option<&str>, String> {
        let mut values = self.all(name);
        let first = values.next();
        if values.next().is_some() {
            return Err(format!("--{name} is given more than once"));
        }
        Ok(first)
    }

    fn required(&self, name: &str) -> Result<&str, String> {
        self.one(name)?
            .ok_or_else(|| format!("--{name} is required"))
    }

    fn has(&self, name: &str) -> bool {
        self.all(name).next().is_some()
    }
}

pub fn parse(args: &[String]) -> Result<Command, String> {
    let Some((command, rest)) = args.split_first() else {
        return Ok(Command::Help);
    };
    let flags = Flags::parse(rest)?;
    if flags.has("help") || command == "help" || command == "--help" {
        return Ok(Command::Help);
    }
    let json = flags.has("json");
    if command == "event" {
        flags.check(command, &[])?;
        let [dataset, event_id] = &flags.positional[..] else {
            return Err("event takes <dataset> <event_id>".into());
        };
        if flags.values.iter().any(|(n, _)| n != "json") {
            return Err("event takes only --json".into());
        }
        return Ok(Command::Event {
            dataset: dataset.clone(),
            event_id: event_id.clone(),
            json,
        });
    }
    if let Some(extra) = flags.positional.first() {
        return Err(format!("unexpected argument {extra:?}"));
    }
    let (body, view) = match command.as_str() {
        "count" => {
            flags.check(command, &["by"])?;
            let body = request(&flags, json!({"kind": "count"}))?;
            (body, View::Aggregate)
        }
        "series" => {
            flags.check(command, &["by", "every"])?;
            let mut body = request(&flags, json!({"kind": "count"}))?;
            let view = bucket(&flags, &mut body)?.ok_or("series needs --every")?;
            (body, view)
        }
        "percentiles" => {
            flags.check(
                command,
                &["by", "every", "field", "type", "p", "span-duration"],
            )?;
            let percentiles: Vec<Value> = flags
                .one("p")?
                .unwrap_or("50,90,99")
                .split(',')
                .map(|p| percentile(p.trim()).map(Value::String))
                .collect::<Result<_, _>>()?;
            let aggregate = match (flags.has("span-duration"), flags.one("field")?) {
                (true, None) => {
                    if flags.has("type") {
                        return Err("--span-duration has no --type".into());
                    }
                    json!({"kind": "span_duration_percentiles", "percentiles": percentiles})
                }
                (false, Some(field)) => json!({
                    "kind": "percentiles",
                    "attr": parse_attr(field)?,
                    "value_type": flags.required("type")?,
                    "percentiles": percentiles,
                }),
                _ => {
                    return Err("percentiles needs --field (with --type) or --span-duration".into());
                }
            };
            let mut body = request(&flags, aggregate)?;
            let view = bucket(&flags, &mut body)?.unwrap_or(View::Aggregate);
            (body, view)
        }
        "events" => {
            flags.check(command, &["limit", "order", "show"])?;
            let limit: u32 = match flags.one("limit")? {
                None => 20,
                Some(text) => text
                    .parse()
                    .map_err(|_| format!("--limit {text:?} is not a count"))?,
            };
            let order = flags.one("order")?.unwrap_or("newest");
            let fields = flags
                .all("show")
                .map(parse_attr)
                .collect::<Result<Vec<_>, _>>()?;
            let aggregate =
                json!({"kind": "events", "limit": limit, "order": order, "fields": fields});
            (request(&flags, aggregate)?, View::Events)
        }
        other => return Err(format!("unknown command {other:?}")),
    };
    Ok(Command::Query { body, view, json })
}

/// The request body shared by every query command.
fn request(flags: &Flags, aggregate: Value) -> Result<Value, String> {
    let signal = flags.one("signal")?.unwrap_or("logs");
    if !matches!(signal, "logs" | "spans") {
        return Err(format!("--signal {signal:?} is not logs or spans"));
    }
    let start = parse_time(flags.required("from")?)?;
    let end = parse_time(flags.required("to")?)?;
    if end <= start {
        return Err("--to must be after --from".into());
    }
    let filters = flags
        .all("where")
        .map(parse_filter)
        .collect::<Result<Vec<_>, _>>()?;
    let group_by = flags
        .all("by")
        .map(parse_attr)
        .collect::<Result<Vec<_>, _>>()?;
    let mut body = Map::new();
    body.insert("dataset".into(), json!(flags.required("dataset")?));
    body.insert("signal".into(), json!(signal));
    // Decimal strings: the service takes 64-bit integers either way, and a
    // reader of the request never sees them rounded.
    body.insert(
        "time".into(),
        json!({"start_ns": start.to_string(), "end_ns": end.to_string()}),
    );
    body.insert("filters".into(), Value::Array(filters));
    body.insert("group_by".into(), Value::Array(group_by));
    body.insert("aggregate".into(), aggregate);
    Ok(Value::Object(body))
}

fn bucket(flags: &Flags, body: &mut Value) -> Result<Option<View>, String> {
    let Some(every) = flags.one("every")? else {
        return Ok(None);
    };
    let every_ns = parse_duration(every)?;
    body["time"]["bucket_ns"] = json!(every_ns.to_string());
    let time = &body["time"];
    let read = |name: &str| {
        time[name]
            .as_str()
            .and_then(|t| t.parse().ok())
            .unwrap_or(0)
    };
    let (start_ns, end_ns) = (read("start_ns"), read("end_ns"));
    // Checked here, before any reply is expanded into buckets.
    if every_ns <= 0 {
        return Err(format!("--every {every:?} is not a positive duration"));
    }
    let width = i128::from(every_ns);
    let buckets = (i128::from(end_ns) - i128::from(start_ns) + width - 1) / width;
    if buckets > MAX_BUCKETS {
        return Err(format!(
            "--every {every} splits the range into {buckets} buckets; at most {MAX_BUCKETS} fit one answer"
        ));
    }
    Ok(Some(View::Series {
        start_ns,
        end_ns,
        every_ns,
    }))
}

/// A percentile from 0 to 100 with at most three decimals, in the form the
/// service labels it in its reply (`100.000` is `100`, `99.90` is `99.9`), so
/// each returned value can be matched to the percentile requested.
fn percentile(text: &str) -> Result<String, String> {
    let bad = || format!("{text:?} is not a percentile from 0 to 100 with at most three decimals");
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !digits(whole) || (text.contains('.') && !digits(fraction)) || fraction.len() > 3 {
        return Err(bad());
    }
    let thousandths = whole
        .parse::<u32>()
        .ok()
        .and_then(|w| {
            w.checked_mul(1000)?
                .checked_add(format!("{fraction:0<3}").parse().ok()?)
        })
        .filter(|t| *t <= 100_000)
        .ok_or_else(bad)?;
    let (whole, fraction) = (thousandths / 1000, thousandths % 1000);
    Ok(if fraction == 0 {
        whole.to_string()
    } else {
        format!("{whole}.{}", format!("{fraction:03}").trim_end_matches('0'))
    })
}

/// `record:KEY`, `resource:KEY`, `scope:KEY`, or a bare record `KEY`. A dot
/// in a key is part of the key, never nesting.
pub fn parse_attr(text: &str) -> Result<Value, String> {
    let (namespace, key) = ["record", "resource", "scope"]
        .iter()
        .find_map(|ns| text.strip_prefix(&format!("{ns}:")).map(|key| (*ns, key)))
        .unwrap_or(("record", text));
    if key.is_empty() {
        return Err(format!("{text:?} names no attribute key"));
    }
    Ok(json!({"namespace": namespace, "key": key}))
}

const OPERATORS: [(&str, &str); 6] = [
    (" != ", "ne"),
    (" <= ", "le"),
    (" >= ", "ge"),
    (" = ", "eq"),
    (" < ", "lt"),
    (" > ", "gt"),
];

fn parse_filter(text: &str) -> Result<Value, String> {
    // Leading spaces only: a string literal keeps its trailing spaces.
    let text = text.trim_start();
    // The leftmost comparison operator wins, and everything after it is the
    // literal, so `message = string:file exists` compares against
    // "file exists" rather than testing presence of a key `message = ...`.
    let comparison = OPERATORS
        .iter()
        .filter_map(|(token, op)| text.find(token).map(|at| (at, *token, *op)))
        .min_by_key(|(at, _, _)| *at);
    if let Some((at, token, op)) = comparison {
        let literal = parse_literal(&text[at + token.len()..])?;
        return Ok(
            json!({"kind": "compare", "attr": parse_attr(&text[..at])?, "op": op, "value": literal}),
        );
    }
    let text = text.trim_end();
    if let Some(field) = text.strip_suffix(" exists") {
        return Ok(json!({"kind": "exists", "attr": parse_attr(field)?}));
    }
    if let Some(field) = text.strip_suffix(" absent") {
        return Ok(json!({"kind": "absent", "attr": parse_attr(field)?}));
    }
    if let Some((field, ty)) = text.rsplit_once(" is ") {
        return Ok(json!({"kind": "has_type", "attr": parse_attr(field)?, "value_type": ty}));
    }
    Err(format!(
        "{text:?} is not FIELD OP TYPE:VALUE, FIELD exists, FIELD absent, or FIELD is TYPE"
    ))
}

/// `TYPE:VALUE` in the service's typed literal form. Integers and doubles
/// travel as written; the service rejects what it cannot represent exactly.
fn parse_literal(text: &str) -> Result<Value, String> {
    let (ty, value) = text.split_once(':').ok_or_else(|| {
        format!("{text:?} names no type: write int:3, double:1.5, string:x, or bool:true")
    })?;
    let bad = |expected: &str| format!("{text:?}: a {ty} literal needs {expected}");
    let value = match ty {
        "string" => json!(value),
        "int" => {
            value.parse::<i64>().map_err(|_| bad("a 64-bit integer"))?;
            json!(value)
        }
        "double" => match value {
            "NaN" | "Infinity" | "-Infinity" => json!(value),
            _ => {
                let parsed: f64 = value.parse().map_err(|_| bad("a number"))?;
                serde_json::Number::from_f64(parsed)
                    .map(Value::Number)
                    .ok_or_else(|| bad("a finite number"))?
            }
        },
        "bool" => match value {
            "true" => json!(true),
            "false" => json!(false),
            _ => return Err(bad("true or false")),
        },
        _ => return Err(format!("{text:?}: unknown literal type {ty:?}")),
    };
    Ok(json!({"type": ty, "value": value}))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(line: &str) -> Vec<String> {
        line.split(' ').map(str::to_owned).collect()
    }

    fn body(line: &str) -> Value {
        match parse(&args(line)) {
            Ok(Command::Query { body, .. }) => body,
            other => panic!("{line}: {other:?}"),
        }
    }

    const RANGE: &str = "--dataset fixture --from 2026-09-01T07:00:00Z --to 1788246060000000000";

    #[test]
    fn literals_carry_their_declared_type() {
        let cases = [
            ("int:1", json!({"type": "int", "value": "1"})),
            ("double:1", json!({"type": "double", "value": 1.0})),
            ("string:1", json!({"type": "string", "value": "1"})),
            ("string:", json!({"type": "string", "value": ""})),
            (
                "string:a:b = c",
                json!({"type": "string", "value": "a:b = c"}),
            ),
            ("bool:false", json!({"type": "bool", "value": false})),
            (
                "int:9007199254740993",
                json!({"type": "int", "value": "9007199254740993"}),
            ),
            ("double:NaN", json!({"type": "double", "value": "NaN"})),
        ];
        for (text, expected) in cases {
            assert_eq!(parse_literal(text), Ok(expected), "{text}");
        }
        for bad in [
            "1",
            "int:1.5",
            "int:9223372036854775808",
            "bool:1",
            "float:1",
            "double:x",
        ] {
            assert!(parse_literal(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn filters_and_fields() {
        let cases = [
            (
                "http.route = string:/checkout",
                json!({"kind": "compare", "op": "eq", "attr": {"namespace": "record", "key": "http.route"},
                       "value": {"type": "string", "value": "/checkout"}}),
            ),
            (
                "resource:service.name != string:a = b",
                json!({"kind": "compare", "op": "ne", "attr": {"namespace": "resource", "key": "service.name"},
                       "value": {"type": "string", "value": "a = b"}}),
            ),
            (
                "record:retry.count >= int:2",
                json!({"kind": "compare", "op": "ge", "attr": {"namespace": "record", "key": "retry.count"},
                       "value": {"type": "int", "value": "2"}}),
            ),
            (
                "scope:scope.int exists",
                json!({"kind": "exists", "attr": {"namespace": "scope", "key": "scope.int"}}),
            ),
            (
                "never.sent absent",
                json!({"kind": "absent", "attr": {"namespace": "record", "key": "never.sent"}}),
            ),
            (
                "cart.total is double",
                json!({"kind": "has_type", "attr": {"namespace": "record", "key": "cart.total"},
                       "value_type": "double"}),
            ),
        ];
        for (text, expected) in cases {
            assert_eq!(parse_filter(text), Ok(expected), "{text}");
        }
        // The literal is everything after the first operator, verbatim.
        let literal = |text: &str| parse_filter(text).map(|f| f["value"]["value"].clone());
        assert_eq!(
            literal("message = string:file exists"),
            Ok(json!("file exists"))
        );
        assert_eq!(literal("message = string:x is int"), Ok(json!("x is int")));
        assert_eq!(literal("message = string:x absent"), Ok(json!("x absent")));
        assert_eq!(literal("message = string:padded  "), Ok(json!("padded  ")));
        assert_eq!(
            parse_filter("message = string:file exists").unwrap()["attr"]["key"],
            json!("message")
        );
        for bad in ["k", "k == int:1", "k = 1", "record: exists"] {
            assert!(parse_filter(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn commands_build_requests() {
        let mut line = args(&format!("count {RANGE} --by resource:service.name --where"));
        line.push("k exists".into());
        let Ok(Command::Query { body: count, .. }) = parse(&line) else {
            panic!()
        };
        assert_eq!(
            count,
            json!({
                "dataset": "fixture",
                "signal": "logs",
                "time": {"start_ns": "1788246000000000000", "end_ns": "1788246060000000000"},
                "filters": [{"kind": "exists", "attr": {"namespace": "record", "key": "k"}}],
                "group_by": [{"namespace": "resource", "key": "service.name"}],
                "aggregate": {"kind": "count"},
            })
        );
        let series = parse(&args(&format!("series {RANGE} --every 10s"))).unwrap();
        let Command::Query {
            body: series_body,
            view,
            ..
        } = series
        else {
            panic!()
        };
        assert_eq!(series_body["time"]["bucket_ns"], json!("10000000000"));
        assert_eq!(
            view,
            View::Series {
                start_ns: 1_788_246_000_000_000_000,
                end_ns: 1_788_246_060_000_000_000,
                every_ns: 10_000_000_000,
            }
        );
        assert_eq!(
            body(&format!(
                "percentiles {RANGE} --signal spans --span-duration --p 50,99.9"
            ))["aggregate"],
            json!({"kind": "span_duration_percentiles", "percentiles": ["50", "99.9"]})
        );
        assert_eq!(
            body(&format!(
                "percentiles {RANGE} --field latency.ms --type int"
            ))["aggregate"],
            json!({"kind": "percentiles", "attr": {"namespace": "record", "key": "latency.ms"},
                   "value_type": "int", "percentiles": ["50", "90", "99"]})
        );
        assert_eq!(
            body(&format!(
                "events {RANGE} --limit 3 --order longest --show fixture.key"
            ))["aggregate"],
            json!({"kind": "events", "limit": 3, "order": "longest",
                   "fields": [{"namespace": "record", "key": "fixture.key"}]})
        );
        assert_eq!(
            parse(&args("event fixture-b 0190-abc --json")),
            Ok(Command::Event {
                dataset: "fixture-b".into(),
                event_id: "0190-abc".into(),
                json: true,
            })
        );
        assert_eq!(
            body(&format!(
                "percentiles {RANGE} --span-duration --p 100.000,99.90,0.5,7"
            ))["aggregate"]["percentiles"],
            json!(["100", "99.9", "0.5", "7"])
        );
        // Exactly the most buckets: 60 s in 60 ms buckets.
        assert!(parse(&args(&format!("series {RANGE} --every 60ms"))).is_ok());
        assert_eq!(parse(&[]), Ok(Command::Help));
    }

    #[test]
    fn mistakes_are_usage_errors() {
        let cases = [
            ("count --from 0 --to 1".to_owned(), "--dataset is required"),
            (format!("count {RANGE} --every 1s"), "does not take --every"),
            (format!("series {RANGE}"), "needs --every"),
            (format!("events {RANGE} --by k"), "does not take --by"),
            (
                format!("count {RANGE} --signal metrics"),
                "not logs or spans",
            ),
            (format!("count {RANGE} --dataset other"), "more than once"),
            (format!("percentiles {RANGE}"), "needs --field"),
            (
                format!("percentiles {RANGE} --span-duration --p 101"),
                "not a percentile",
            ),
            (
                format!("percentiles {RANGE} --span-duration --p 99.9999"),
                "not a percentile",
            ),
            (
                format!("series {RANGE} --every 0ns"),
                "not a positive duration",
            ),
            (format!("series {RANGE} --every 1ms"), "at most 1000"),
            (
                "count --dataset d --from 5 --to 5".to_owned(),
                "after --from",
            ),
            (
                format!("percentiles {RANGE} --field k"),
                "--type is required",
            ),
            (format!("count {RANGE} --where"), "needs a value"),
            ("event fixture".to_owned(), "<dataset> <event_id>"),
            ("frobnicate".to_owned(), "unknown command"),
        ];
        for (line, detail) in cases {
            let err = parse(&args(&line)).unwrap_err();
            assert!(err.contains(detail), "{line}: {err}");
        }
    }
}
