//! Responses as text an operator reads.
//!
//! Three things are always on screen rather than in `--json` only:
//!
//! * Exact or approximate: the plan's `approximation`. Every PoC answer is
//!   exact; anything else is printed in capitals so it cannot pass unnoticed.
//! * Complete or partial: an aggregate is complete over the events stored when
//!   it ran (the service fails rather than return a truncated aggregate); a
//!   listing is partial whenever fewer rows came back than matched.
//! * Typed values: no two types print alike. Strings are quoted, integers are
//!   bare, doubles always carry a decimal point or exponent, and markers in
//!   parentheses (`(missing)`, `(empty)`, `(bytes)`) are never quoted, so
//!   `"1"`, `1`, and `1.0` are three different values on screen as in storage.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::args::View;
use crate::time::format_time;

/// A typed value from a response, in the display form above.
pub fn typed(value: &Value) -> String {
    let ty = value["type"].as_str().unwrap_or("?");
    let inner = &value["value"];
    match ty {
        "string" => json_text(&serde_json::to_string(inner).unwrap_or_default()),
        // Canonical forms, decoded from the reply rather than echoed, so one
        // value always prints (and compares) one way: `"01"` is int 1, and the
        // double tokens `1000000000000000000` and `1e18` are both `1e18`.
        "int" | "uint64" => match inner.as_str().and_then(|v| v.parse::<i128>().ok()) {
            Some(v) => v.to_string(),
            None => inner.to_string(),
        },
        "double" => match inner {
            // Shortest round-trip form, always with a `.` or an exponent.
            Value::Number(n) => n.as_f64().map_or_else(|| "?".into(), |v| format!("{v:?}")),
            // NaN, Infinity, -Infinity: listed fields only.
            Value::String(s) => s.clone(),
            _ => "?".into(),
        },
        "bool" => inner.to_string(),
        "missing" | "empty" | "bytes" | "array" | "map" => format!("({ty})"),
        other => format!("({other}?)"),
    }
}

type Checked<T> = Result<T, String>;

fn malformed(what: impl std::fmt::Display) -> String {
    format!("malformed response: {}", plain(&what.to_string()))
}

/// The decimal text of `{"type": ty, "value": "..."}` and nothing else.
fn typed_text<'a>(value: &'a Value, ty: &str) -> Option<&'a str> {
    let object = value.as_object()?;
    (object.len() == 2 && object.get("type")?.as_str()? == ty)
        .then(|| object.get("value")?.as_str())
        .flatten()
}

/// A typed count (`{"type": "uint64", "value": "7"}`). Anything else fails
/// the answer rather than reading as zero.
fn uint(value: &Value, what: &str) -> Checked<u64> {
    typed_text(value, "uint64")
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| malformed(format!("{what} is not a typed count: {value}")))
}

/// A typed integer (`{"type": "int", "value": "-7"}`).
fn int(value: &Value, what: &str) -> Checked<i64> {
    typed_text(value, "int")
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| malformed(format!("{what} is not a typed int: {value}")))
}

/// Whether a double's JSON number denotes exactly one binary64. The service
/// writes doubles with a `.` or an exponent; an integer token in the `i64` or
/// `u64` range, such as `9007199254740993`, would be rounded on decoding, so
/// it is accepted only when that rounding loses nothing. An integer token
/// outside both ranges is rounded to `f64` by serde_json before this sees it,
/// so its loss cannot be detected here; the value still displays and compares
/// as the binary64 it decoded to.
fn lossless_double(n: &serde_json::Number) -> bool {
    let exact = |i: i128| (i as f64) as i128 == i;
    match (n.as_i64(), n.as_u64()) {
        (Some(i), _) => exact(i128::from(i)),
        (_, Some(u)) => exact(i128::from(u)),
        _ => n.as_f64().is_some_and(f64::is_finite),
    }
}

/// Group values: a received scalar, explicit empty, or missing.
const GROUP_TYPES: &[&str] = &["string", "bool", "int", "double", "empty", "missing"];
/// Permits a non-finite double in its literal spelling (`"NaN"`).
const NON_FINITE: &str = "non_finite";
/// Listed field values: any received value, by type alone for those the query
/// surface cannot operate on, and non-finite doubles by their spelling.
const FIELD_TYPES: &[&str] = &[
    "string", "bool", "int", "double", "empty", "missing", "bytes", "array", "map", NON_FINITE,
];

/// A typed value, checked against the response encoding and the types this
/// position may hold before it is shown, so `{"type": "string", "value": 1}`
/// or a percentile of the wrong type fails instead of printing like another
/// type.
fn checked_typed(value: &Value, what: &str, allowed: &[&str]) -> Checked<String> {
    let bad = || malformed(format!("{what} is not a valid typed value here: {value}"));
    let object = value.as_object().ok_or_else(bad)?;
    let ty = object.get("type").and_then(Value::as_str).ok_or_else(bad)?;
    let inner = object.get("value");
    let valid = allowed.contains(&ty)
        && object.len() == 1 + usize::from(inner.is_some())
        && match (ty, inner) {
            ("string", Some(Value::String(_))) | ("bool", Some(Value::Bool(_))) => true,
            ("int", Some(Value::String(s))) => s.parse::<i64>().is_ok(),
            ("double", Some(Value::Number(n))) => lossless_double(n),
            ("double", Some(Value::String(s))) => {
                allowed.contains(&NON_FINITE)
                    && matches!(s.as_str(), "NaN" | "Infinity" | "-Infinity")
            }
            ("empty" | "missing" | "bytes" | "array" | "map", None) => true,
            _ => false,
        };
    if !valid {
        return Err(bad());
    }
    Ok(typed(value))
}

fn text<'a>(row: &'a Value, name: &str) -> Checked<&'a str> {
    row[name]
        .as_str()
        .ok_or_else(|| malformed(format!("{name} is not text")))
}

/// Characters that would move the cursor, break a line, or reorder text on a
/// terminal: every Unicode control (C0, DEL, and C1 such as U+0085 NEXT
/// LINE), the line and paragraph separators, and the bidirectional controls.
fn unsafe_char(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{061c}'
                | '\u{200e}'
                | '\u{200f}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2066}'..='\u{2069}'
        )
}

/// Text from the service or the command line, with those characters and
/// backslashes escaped so one value is one line and cannot move the cursor.
/// Only human-readable output is escaped; `--json` prints the reply as is.
pub fn plain(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c == '\\' || unsafe_char(c) {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
    out
}

/// JSON text (a quoted string or a pretty document) with what serde_json
/// leaves raw, such as C1 controls, escaped as `\uXXXX`: it stays valid JSON
/// and each string stays on its line. Line breaks between tokens are kept;
/// serde_json already escapes those inside strings.
fn json_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c != '\n' && unsafe_char(c) {
            out.push_str(&format!("\\u{:04x}", u32::from(c)));
        } else {
            out.push(c);
        }
    }
    out
}

fn attr_label(attr: &Value) -> String {
    let key = plain(attr["key"].as_str().unwrap_or("?"));
    match attr["namespace"].as_str() {
        Some("record") | None => key,
        Some(ns) => format!("{ns}:{key}"),
    }
}

/// Nanoseconds as an exact decimal of the largest unit that keeps it whole
/// or short: `1500000` is `1.5ms`.
pub fn format_nanos(ns: i64) -> String {
    let (scale, unit) = match ns.unsigned_abs() {
        n if n >= 1_000_000_000 => (1_000_000_000, "s"),
        n if n >= 1_000_000 => (1_000_000, "ms"),
        n if n >= 1_000 => (1_000, "us"),
        _ => (1, "ns"),
    };
    let whole = ns / scale;
    let fraction = (ns % scale).unsigned_abs();
    if fraction == 0 {
        return format!("{whole}{unit}");
    }
    let digits = scale.ilog10() as usize;
    let fraction = format!("{fraction:0digits$}");
    let sign = if ns < 0 && whole == 0 { "-" } else { "" };
    format!("{sign}{whole}.{}{unit}", fraction.trim_end_matches('0'))
}

fn table(header: &[String], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = header.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let line = |cells: &[String]| {
        let padded: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        padded.join("  ").trim_end().to_owned()
    };
    let mut out = line(header);
    out.push('\n');
    for row in rows {
        out.push_str(&line(row));
        out.push('\n');
    }
    out
}

fn heading(request: &Value) -> String {
    let time = &request["time"];
    let ns = |name: &str| {
        time[name]
            .as_str()
            .and_then(|t| t.parse().ok())
            .unwrap_or(0)
    };
    format!(
        "{} {} [{}, {})\n",
        plain(request["dataset"].as_str().unwrap_or("?")),
        request["signal"].as_str().unwrap_or("?"),
        format_time(ns("start_ns")),
        format_time(ns("end_ns"))
    )
}

/// Exact/approximate, complete/partial, skipped values, and the plan. A reply
/// without its diagnostics is malformed: it cannot say how exact it is.
fn footer(request: &Value, response: &Value, completeness: &str) -> Checked<String> {
    let d = &response["diagnostics"];
    let approximation = text(d, "approximation")?;
    let mut out = if approximation == "exact" {
        format!("exact, {completeness}\n")
    } else {
        // Every PoC plan is exact, and this client cannot say what an
        // approximate answer covers, so it never calls one complete.
        format!(
            "APPROXIMATE ({}), coverage unknown: rows are shown as returned\n",
            plain(approximation)
        )
    };
    // A double comparison always reports the events it could not compare.
    let compares_double = request["filters"].as_array().is_some_and(|filters| {
        filters
            .iter()
            .any(|f| f["kind"] == "compare" && f["value"]["type"] == "double")
    });
    if compares_double || d.get("non_finite_excluded").is_some() {
        let skipped = uint(&d["non_finite_excluded"], "non_finite_excluded")?;
        if skipped > 0 {
            out.push_str(&format!(
                "note: {skipped} events hold a non-finite double at a compared field and were not compared\n"
            ));
        }
    }
    let guards = d["guards"]
        .as_array()
        .ok_or_else(|| malformed("guards is not a list"))?
        .iter()
        .map(|g| {
            g.as_str()
                .map(plain)
                .ok_or_else(|| malformed("a guard is not text"))
        })
        .collect::<Checked<Vec<_>>>()?;
    out.push_str(&format!(
        "plan: {} path, {}; guards: {}\n",
        plain(text(d, "storage_path")?),
        plain(text(d, "table")?),
        if guards.is_empty() {
            "none".into()
        } else {
            guards.join(", ")
        }
    ));
    Ok(out)
}

/// The shape of an aggregate request, as the response must mirror it.
struct Shape {
    groups: Vec<String>,
    /// Requested percentiles as the service labels them (`args` normalizes).
    percentiles: Vec<String>,
    /// The type every percentile value must have.
    percentile_type: &'static str,
    valued: bool,
    spans: bool,
}

impl Shape {
    fn of(request: &Value) -> Shape {
        let aggregate = &request["aggregate"];
        let kind = aggregate["kind"].as_str().unwrap_or("");
        Shape {
            groups: request["group_by"]
                .as_array()
                .map(|g| g.iter().map(attr_label).collect())
                .unwrap_or_default(),
            percentiles: aggregate["percentiles"]
                .as_array()
                .map(|ps| {
                    ps.iter()
                        .map(|p| p.as_str().unwrap_or("?").to_owned())
                        .collect()
                })
                .unwrap_or_default(),
            percentile_type: if kind == "percentiles" && aggregate["value_type"] == "double" {
                "double"
            } else {
                "int"
            },
            valued: kind != "count",
            spans: kind == "span_duration_percentiles",
        }
    }

    /// One row's cells after the bucket, every value checked.
    fn cells(&self, row: &Value) -> Checked<Vec<String>> {
        let groups = row["groups"]
            .as_array()
            .filter(|g| g.len() == self.groups.len())
            .ok_or_else(|| malformed("a row's groups do not match the request"))?;
        let mut cells = groups
            .iter()
            .map(|g| checked_typed(g, "a group value", GROUP_TYPES))
            .collect::<Checked<Vec<_>>>()?;
        cells.push(uint(&row["events"], "events")?.to_string());
        let contributing = match self.valued {
            true => uint(&row["contributing"], "contributing")?,
            false => 0,
        };
        if self.valued {
            cells.push(contributing.to_string());
        }
        if self.spans {
            cells.push(uint(&row["timing_errors"], "timing_errors")?.to_string());
        }
        if !self.percentiles.is_empty() {
            let values = row["percentiles"]
                .as_array()
                .filter(|p| p.len() == self.percentiles.len())
                .ok_or_else(|| malformed("a row's percentiles do not match the request"))?;
            for (label, p) in self.percentiles.iter().zip(values) {
                // Values are matched to percentiles by label, never by position.
                if p["percentile"].as_str() != Some(label.as_str()) {
                    return Err(malformed(format!(
                        "percentile {} returned where p{label} was requested",
                        p["percentile"]
                    )));
                }
                let value = p
                    .get("value")
                    .ok_or_else(|| malformed(format!("p{label} has no value")))?;
                cells.push(match (value, contributing) {
                    (Value::Null, 0) => "-".into(),
                    (Value::Null, n) => {
                        return Err(malformed(format!(
                            "p{label} has no value although {n} values contributed"
                        )));
                    }
                    (_, 0) => {
                        return Err(malformed(format!(
                            "p{label} has a value although nothing contributed"
                        )));
                    }
                    (value, _) if self.spans => {
                        format_nanos(int(value, "a span duration percentile")?)
                    }
                    (value, _) => {
                        checked_typed(value, "a percentile value", &[self.percentile_type])?
                    }
                });
            }
        }
        Ok(cells)
    }

    /// The cells of a bucket without a row: nothing matched, so zero.
    fn empty(&self) -> Vec<String> {
        let mut line: Vec<String> = self.groups.iter().map(|_| String::new()).collect();
        line.push("0".into());
        if self.valued {
            line.push("0".into());
        }
        if self.spans {
            line.push("0".into());
        }
        line.extend(self.percentiles.iter().map(|_| "-".to_owned()));
        line
    }
}

/// A query response as an operator reads it, or why the reply cannot be
/// trusted.
pub fn query(view: &View, request: &Value, response: &Value) -> Checked<String> {
    let mut out = heading(request);
    if *view == View::Events {
        out.push_str(&events(request, response)?);
        return Ok(out);
    }
    let shape = Shape::of(request);
    // Only an exact answer's missing bucket means nothing matched there; an
    // approximate one may have missed events, so its gaps are not filled.
    let exact = text(&response["diagnostics"], "approximation")? == "exact";
    let rows = response["rows"]
        .as_array()
        .ok_or_else(|| malformed("no rows"))?;

    let mut header = Vec::new();
    if matches!(view, View::Series { .. }) {
        header.push("bucket".to_owned());
    }
    header.extend(shape.groups.iter().cloned());
    header.push("events".into());
    if shape.valued {
        header.push("values".into());
    }
    if shape.spans {
        header.push("timing_errors".into());
    }
    header.extend(shape.percentiles.iter().map(|p| format!("p{p}")));

    // Each (bucket, typed group tuple) appears once; different groups may
    // share a bucket.
    let mut seen = BTreeSet::new();
    let mut checked = |bucket: Option<i64>, row: &Value| -> Checked<Vec<String>> {
        let cells = shape.cells(row)?;
        if !seen.insert((bucket, cells[..shape.groups.len()].to_vec())) {
            return Err(malformed("a group appears twice in one bucket"));
        }
        Ok(cells)
    };

    let body: Vec<Vec<String>> = match view {
        // Every bucket of an exact series, so a gap reads as zero events
        // rather than a missing line; every row is checked to name a bucket
        // of the request first.
        View::Series {
            start_ns,
            end_ns,
            every_ns,
        } => {
            let (start, end, every) = (
                i128::from(*start_ns),
                i128::from(*end_ns),
                i128::from(*every_ns),
            );
            if every <= 0 {
                return Err(malformed("the bucket width is not positive"));
            }
            let mut by_bucket: BTreeMap<i64, Vec<Vec<String>>> = BTreeMap::new();
            for row in rows {
                let bucket = int(&row["bucket_start_ns"], "bucket_start_ns")?;
                let offset = i128::from(bucket) - start;
                if offset < 0 || i128::from(bucket) >= end || offset % every != 0 {
                    return Err(malformed(format!(
                        "bucket {bucket} is not a bucket of the request"
                    )));
                }
                let cells = checked(Some(bucket), row)?;
                by_bucket.entry(bucket).or_default().push(cells);
            }
            let mut body = Vec::new();
            let mut bucket = start;
            while bucket < end {
                let label = format_time(bucket as i64);
                match by_bucket.remove(&(bucket as i64)) {
                    Some(lines) => {
                        for cells in lines {
                            let mut line = vec![label.clone()];
                            line.extend(cells);
                            body.push(line);
                        }
                    }
                    None if exact => {
                        let mut line = vec![label];
                        line.extend(shape.empty());
                        body.push(line);
                    }
                    None => {}
                }
                bucket += every;
            }
            body
        }
        _ => {
            // An ungrouped aggregate always has its one row, zero or not.
            if shape.groups.is_empty() && rows.len() != 1 {
                return Err(malformed(format!(
                    "an ungrouped aggregate returned {} rows",
                    rows.len()
                )));
            }
            rows.iter()
                .map(|row| checked(None, row))
                .collect::<Checked<_>>()?
        }
    };
    out.push_str(&table(&header, &body));
    if !shape.groups.is_empty() {
        out.push_str(&legend());
    }
    let mut completeness = "complete over the events stored when it ran".to_owned();
    if !shape.percentiles.is_empty() {
        completeness.push_str("; percentiles are nearest-rank received values");
    }
    out.push_str(&footer(request, response, &completeness)?);
    Ok(out)
}

fn legend() -> String {
    "values: \"text\" is a string, 1 an int, 1.0 a double; (missing) means the key was not sent\n"
        .into()
}

fn events(request: &Value, response: &Value) -> Checked<String> {
    let spans = request["signal"] == "spans";
    let fields: Vec<String> = request["aggregate"]["fields"]
        .as_array()
        .map(|f| f.iter().map(attr_label).collect())
        .unwrap_or_default();
    let mut header: Vec<String> = ["time", "event_id"].map(str::to_owned).to_vec();
    let envelope: &[&str] = if spans {
        &["span_name", "duration", "status_code", "trace_id"]
    } else {
        // `body_text` is the serving row's text projection of the body; the
        // typed body is in the event's canonical payload.
        &["kind", "severity_text", "body_text", "trace_id"]
    };
    header.extend(envelope.iter().map(|c| c.to_string()));
    header.extend(fields.iter().cloned());
    let matched = uint(&response["matched"], "matched")?;
    let rows = response["rows"]
        .as_array()
        .ok_or_else(|| malformed("no rows"))?;
    if rows.len() as u64 > matched {
        return Err(malformed(format!(
            "{} rows but only {matched} matched",
            rows.len()
        )));
    }
    let body = rows
        .iter()
        .map(|row| {
            let time = format_time(int(&row["timestamp_ns"], "timestamp_ns")?);
            let mut cells = vec![time, plain(text(row, "event_id")?)];
            for column in envelope {
                cells.push(match *column {
                    // An unavailable duration shows why, never as zero.
                    "duration" => match &row["duration_ns"] {
                        Value::Null => format!("({})", plain(text(row, "span_timing_status")?)),
                        value => format_nanos(int(value, "duration_ns")?),
                    },
                    "status_code" => int(&row["status_code"], "status_code")?.to_string(),
                    name => plain(text(row, name)?),
                });
            }
            let values = row["fields"]
                .as_array()
                .filter(|f| f.len() == fields.len())
                .ok_or_else(|| malformed("a row's fields do not match the request"))?;
            for field in values {
                cells.push(checked_typed(field, "a field value", FIELD_TYPES)?);
            }
            Ok(cells)
        })
        .collect::<Checked<Vec<_>>>()?;
    let mut out = table(&header, &body);
    if !fields.is_empty() {
        out.push_str(&legend());
    }
    let completeness = if rows.len() as u64 == matched {
        format!("complete: all {matched} matching events")
    } else {
        format!(
            "PARTIAL: the first {} of {matched} matching events; narrow the filters or raise --limit",
            rows.len()
        )
    };
    out.push_str(&footer(request, response, &completeness)?);
    Ok(out)
}

/// One canonical event, its checksum checked against the payload text as
/// served.
pub fn event(event: &crate::Event, verified: bool) -> String {
    let timestamp = event
        .timestamp_ns
        .parse()
        .map_or_else(|_| "?".into(), format_time);
    let mut out = format!(
        "{} {} {} at {timestamp}\nschema {}\npayload_sha256 {} ({})\n",
        plain(&event.dataset),
        plain(&event.signal),
        plain(&event.event_id),
        plain(&event.schema_version),
        plain(&event.payload_sha256),
        if verified {
            "verified against the payload"
        } else {
            "MISMATCH: the payload does not hash to it"
        }
    );
    let payload: Value = serde_json::from_str(event.payload.get()).unwrap_or(Value::Null);
    out.push_str(&json_text(
        &serde_json::to_string_pretty(&payload).unwrap_or_default(),
    ));
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn no_two_types_print_alike() {
        let cases = [
            (json!({"type": "string", "value": "1"}), "\"1\""),
            (json!({"type": "int", "value": "1"}), "1"),
            (json!({"type": "double", "value": 1.0}), "1.0"),
            (json!({"type": "double", "value": 1.5}), "1.5"),
            (json!({"type": "double", "value": 1e23}), "1e23"),
            (json!({"type": "double", "value": 1e18}), "1e18"),
            (json!({"type": "double", "value": -0.0}), "-0.0"),
            (json!({"type": "int", "value": "01"}), "1"),
            (json!({"type": "double", "value": "-Infinity"}), "-Infinity"),
            (json!({"type": "string", "value": ""}), "\"\""),
            (json!({"type": "empty"}), "(empty)"),
            (json!({"type": "missing"}), "(missing)"),
            (
                json!({"type": "string", "value": "(missing)"}),
                "\"(missing)\"",
            ),
            (json!({"type": "bool", "value": false}), "false"),
            (json!({"type": "string", "value": "false"}), "\"false\""),
            (json!({"type": "bytes"}), "(bytes)"),
            (
                json!({"type": "int", "value": "9007199254740993"}),
                "9007199254740993",
            ),
        ];
        for (value, shown) in cases {
            assert_eq!(typed(&value), shown, "{value}");
        }
    }

    #[test]
    fn nanos_stay_exact() {
        let cases = [
            (0, "0ns"),
            (999, "999ns"),
            (1_000, "1us"),
            (1_500_000, "1.5ms"),
            (600_000, "600us"),
            (1_000_000_001, "1.000000001s"),
            (-500, "-500ns"),
            (-1_500, "-1.5us"),
        ];
        for (ns, shown) in cases {
            assert_eq!(format_nanos(ns), shown, "{ns}");
        }
    }

    fn diagnostics(approximation: &str) -> Value {
        json!({"storage_path": "raw", "approximation": approximation, "table": "serving_logs",
               "columns": [], "guards": ["key_membership_presence"]})
    }

    #[test]
    fn series_show_every_bucket_and_how_exact_they_are() {
        let request = json!({
            "dataset": "fixture", "signal": "logs",
            "time": {"start_ns": "0", "end_ns": "30", "bucket_ns": "10"},
            "group_by": [], "aggregate": {"kind": "count"},
        });
        let response = json!({
            "rows": [{"bucket_start_ns": {"type": "int", "value": "10"}, "groups": [],
                      "events": {"type": "uint64", "value": "4"}}],
            "diagnostics": diagnostics("exact"),
        });
        let view = View::Series {
            start_ns: 0,
            end_ns: 30,
            every_ns: 10,
        };
        let text = query(&view, &request, &response).unwrap();
        assert_eq!(
            text,
            "fixture logs [1970-01-01T00:00:00Z, 1970-01-01T00:00:00.00000003Z)\n\
             bucket                         events\n\
             1970-01-01T00:00:00Z           0\n\
             1970-01-01T00:00:00.00000001Z  4\n\
             1970-01-01T00:00:00.00000002Z  0\n\
             exact, complete over the events stored when it ran\n\
             plan: raw path, serving_logs; guards: key_membership_presence\n"
        );
        let mut approximate = response.clone();
        approximate["diagnostics"] = diagnostics("sampled");
        // Approximate: no zero buckets synthesized, and never "complete".
        let text = query(&view, &request, &approximate).unwrap();
        assert!(
            text.contains("APPROXIMATE (sampled), coverage unknown"),
            "{text}"
        );
        assert!(!text.contains("complete"), "{text}");
        assert_eq!(text.lines().count(), 5, "{text}");
    }

    #[test]
    fn malformed_replies_fail_instead_of_claiming_completeness() {
        let count = json!({
            "dataset": "fixture", "signal": "logs",
            "time": {"start_ns": "0", "end_ns": "30"}, "group_by": [],
            "aggregate": {"kind": "count"},
        });
        let mut series_request = count.clone();
        series_request["time"]["bucket_ns"] = json!("10");
        let series = View::Series {
            start_ns: 0,
            end_ns: 30,
            every_ns: 10,
        };
        let listing = json!({
            "dataset": "fixture", "signal": "logs",
            "time": {"start_ns": "0", "end_ns": "30"}, "group_by": [],
            "aggregate": {"kind": "events", "limit": 5, "order": "newest", "fields": []},
        });
        let bucket = |start: &str| {
            json!({"bucket_start_ns": {"type": "int", "value": start}, "groups": [],
                   "events": {"type": "uint64", "value": "1"}})
        };
        let d = diagnostics("exact");
        let four = json!({"type": "uint64", "value": "4"});
        let cases: Vec<(&str, &View, &Value, Value)> = vec![
            (
                "a listing without rows or matched",
                &View::Events,
                &listing,
                json!({"diagnostics": d}),
            ),
            (
                "a listing without matched",
                &View::Events,
                &listing,
                json!({"rows": [], "diagnostics": d}),
            ),
            (
                "more rows than matched",
                &View::Events,
                &listing,
                json!({"rows": [{}], "matched": {"type": "uint64", "value": "0"}, "diagnostics": d}),
            ),
            (
                "an aggregate without rows",
                &View::Aggregate,
                &count,
                json!({"diagnostics": d}),
            ),
            (
                "an ungrouped aggregate without its row",
                &View::Aggregate,
                &count,
                json!({"rows": [], "diagnostics": d}),
            ),
            (
                "an untyped count",
                &View::Aggregate,
                &count,
                json!({"rows": [{"groups": [], "events": 4}], "diagnostics": d}),
            ),
            (
                "no diagnostics",
                &View::Aggregate,
                &count,
                json!({"rows": [{"groups": [], "events": four}]}),
            ),
            (
                "a series without rows",
                &series,
                &series_request,
                json!({"diagnostics": d}),
            ),
            (
                "a bucket off the grid",
                &series,
                &series_request,
                json!({"rows": [bucket("5")], "diagnostics": d}),
            ),
            (
                "a bucket past the end",
                &series,
                &series_request,
                json!({"rows": [bucket("30")], "diagnostics": d}),
            ),
            (
                "a bucket twice",
                &series,
                &series_request,
                json!({"rows": [bucket("10"), bucket("10")], "diagnostics": d}),
            ),
        ];
        for (name, view, request, response) in cases {
            let err = query(view, request, &response).expect_err(name);
            assert!(err.starts_with("malformed response"), "{name}: {err}");
        }
    }

    #[test]
    fn control_characters_stay_on_one_line() {
        assert_eq!(
            plain("two\nlines\t\u{1b}[2J\\"),
            "two\\nlines\\t\\u{1b}[2J\\\\"
        );
        let request = json!({
            "dataset": "fixture", "signal": "logs",
            "time": {"start_ns": "0", "end_ns": "10"}, "group_by": [],
            "aggregate": {"kind": "events", "limit": 1, "order": "newest", "fields": []},
        });
        let response = json!({
            "rows": [{"event_id": "e1", "timestamp_ns": {"type": "int", "value": "5"},
                      "kind": "log", "severity_text": "INFO", "body_text": "a\nb", "trace_id": "",
                      "fields": []}],
            "matched": {"type": "uint64", "value": "1"},
            "diagnostics": diagnostics("exact"),
        });
        let text = query(&View::Events, &request, &response).unwrap();
        assert!(text.contains("a\\nb"), "{text}");
        // Heading, header, one row, status, plan.
        assert_eq!(text.lines().count(), 5, "{text}");
        // JSON quoting leaves C1 controls such as U+0085 raw; they are escaped.
        let group = typed(&json!({"type": "string", "value": "a\u{85}b\u{202e}"}));
        assert_eq!(group, "\"a\\u0085b\\u202e\"");
        assert_eq!(
            json_text("{\n  \"k\": \"\u{85}\"\n}"),
            "{\n  \"k\": \"\\u0085\"\n}"
        );
        assert_eq!(plain("a\u{85}b"), "a\\u{85}b");
    }

    #[test]
    fn percentiles_and_groups_are_checked_before_display() {
        let request = json!({
            "dataset": "fixture", "signal": "logs",
            "time": {"start_ns": "0", "end_ns": "30"},
            "group_by": [{"namespace": "record", "key": "route"}],
            "aggregate": {"kind": "percentiles", "attr": {"namespace": "record", "key": "ms"},
                          "value_type": "int", "percentiles": ["50", "100"]},
        });
        let int = |v: &str| json!({"type": "int", "value": v});
        let p = |label: &str, value: Value| json!({"percentile": label, "value": value});
        let row = |group: Value, contributing: &str, percentiles: Value| {
            json!({"groups": [group], "events": {"type": "uint64", "value": "2"},
                   "contributing": {"type": "uint64", "value": contributing},
                   "percentiles": percentiles})
        };
        let reply = |row: Value| json!({"rows": [row], "diagnostics": diagnostics("exact")});
        let route = json!({"type": "string", "value": "/a"});
        let ints = json!([p("50", int("10")), p("100", int("90"))]);

        let text = query(
            &View::Aggregate,
            &request,
            &reply(row(route.clone(), "2", ints.clone())),
        )
        .unwrap();
        assert!(text.contains("10   90"), "{text}");
        let none = json!([p("50", Value::Null), p("100", Value::Null)]);
        let text = query(
            &View::Aggregate,
            &request,
            &reply(row(route.clone(), "0", none.clone())),
        )
        .unwrap();
        assert!(text.contains("-    -"), "{text}");

        let cases = [
            (
                "percentiles out of order",
                row(
                    route.clone(),
                    "2",
                    json!([p("100", int("90")), p("50", int("10"))]),
                ),
            ),
            (
                "no value although values contributed",
                row(route.clone(), "2", none),
            ),
            (
                "a value although nothing contributed",
                row(route.clone(), "0", ints.clone()),
            ),
            (
                "a value key absent",
                row(
                    route.clone(),
                    "2",
                    json!([{"percentile": "50"}, p("100", int("90"))]),
                ),
            ),
            (
                "a double where ints were asked",
                row(
                    route.clone(),
                    "2",
                    json!([
                        p("50", json!({"type": "double", "value": 10.0})),
                        p("100", int("90"))
                    ]),
                ),
            ),
            (
                "a string group holding a number",
                row(json!({"type": "string", "value": 1}), "2", ints.clone()),
            ),
            (
                "an int group that is not an integer",
                row(json!({"type": "int", "value": "1.5"}), "2", ints.clone()),
            ),
            (
                "a bytes group",
                row(json!({"type": "bytes"}), "2", ints.clone()),
            ),
            (
                "a group with an extra field",
                row(
                    json!({"type": "int", "value": "1", "x": 1}),
                    "2",
                    ints.clone(),
                ),
            ),
        ];
        for (name, bad) in cases {
            let err = query(&View::Aggregate, &request, &reply(bad)).expect_err(name);
            assert!(err.starts_with("malformed response"), "{name}: {err}");
        }
    }

    #[test]
    fn double_comparisons_must_report_what_they_skipped() {
        let request = json!({
            "dataset": "fixture", "signal": "logs",
            "time": {"start_ns": "0", "end_ns": "30"}, "group_by": [],
            "filters": [{"kind": "compare", "op": "gt", "attr": {"namespace": "record", "key": "r"},
                         "value": {"type": "double", "value": 1.5}}],
            "aggregate": {"kind": "count"},
        });
        let rows = json!([{"groups": [], "events": {"type": "uint64", "value": "1"}}]);
        let err = query(
            &View::Aggregate,
            &request,
            &json!({"rows": rows, "diagnostics": diagnostics("exact")}),
        )
        .unwrap_err();
        assert!(err.contains("non_finite_excluded"), "{err}");
        let mut d = diagnostics("exact");
        d["non_finite_excluded"] = json!({"type": "uint64", "value": "2"});
        let text = query(
            &View::Aggregate,
            &request,
            &json!({"rows": rows, "diagnostics": d}),
        )
        .unwrap();
        assert!(
            text.contains("note: 2 events hold a non-finite double"),
            "{text}"
        );
    }

    #[test]
    fn numbers_compare_by_value_not_by_spelling() {
        let request = json!({
            "dataset": "fixture", "signal": "logs",
            "time": {"start_ns": "0", "end_ns": "10"},
            "group_by": [{"namespace": "record", "key": "k"}],
            "aggregate": {"kind": "count"},
        });
        let reply = |text: &str| {
            let groups: Value = serde_json::from_str(text).unwrap();
            let rows: Vec<Value> = groups
                .as_array()
                .unwrap()
                .iter()
                .map(|g| json!({"groups": [g], "events": {"type": "uint64", "value": "1"}}))
                .collect();
            json!({"rows": rows, "diagnostics": diagnostics("exact")})
        };
        let duplicates = [
            r#"[{"type":"int","value":"1"},{"type":"int","value":"01"}]"#,
            r#"[{"type":"double","value":1000000000000000000},{"type":"double","value":1e18}]"#,
            r#"[{"type":"double","value":0.5},{"type":"double","value":5e-1}]"#,
        ];
        for text in duplicates {
            let err = query(&View::Aggregate, &request, &reply(text)).expect_err(text);
            assert!(err.contains("appears twice"), "{text}: {err}");
        }
        // One spelling of each type stays three groups.
        let distinct = r#"[{"type":"int","value":"1"},{"type":"string","value":"1"},{"type":"double","value":1.0}]"#;
        let text = query(&View::Aggregate, &request, &reply(distinct)).unwrap();
        assert!(
            text.contains("\n1 ") && text.contains("\n\"1\" ") && text.contains("\n1.0 "),
            "{text}"
        );
        // No binary64 is 2^53 + 1; the token is refused, not rounded.
        let lossy = r#"[{"type":"double","value":9007199254740993}]"#;
        let err = query(&View::Aggregate, &request, &reply(lossy)).unwrap_err();
        assert!(err.starts_with("malformed response"), "{err}");
        let exact = r#"[{"type":"double","value":9007199254740992}]"#;
        assert!(
            query(&View::Aggregate, &request, &reply(exact))
                .unwrap()
                .contains("9007199254740992.0")
        );
    }

    #[test]
    fn counts_and_ints_have_no_extra_fields() {
        assert!(uint(&json!({"type": "uint64", "value": "3", "unit": "ms"}), "n").is_err());
        assert!(int(&json!({"type": "int", "value": "3", "unit": "ms"}), "n").is_err());
        assert!(int(&json!({"type": "uint64", "value": "3"}), "n").is_err());
        assert_eq!(int(&json!({"type": "int", "value": "-3"}), "n"), Ok(-3));
        assert_eq!(
            plain("a\u{61c}b\u{200e}c\u{200f}"),
            "a\\u{61c}b\\u{200e}c\\u{200f}"
        );
    }

    #[test]
    fn a_group_appears_once_per_bucket() {
        let request = json!({
            "dataset": "fixture", "signal": "logs",
            "time": {"start_ns": "0", "end_ns": "20", "bucket_ns": "10"},
            "group_by": [{"namespace": "record", "key": "route"}],
            "aggregate": {"kind": "count"},
        });
        let view = View::Series {
            start_ns: 0,
            end_ns: 20,
            every_ns: 10,
        };
        let row = |bucket: &str, route: &str| {
            json!({"bucket_start_ns": {"type": "int", "value": bucket},
                   "groups": [{"type": "string", "value": route}],
                   "events": {"type": "uint64", "value": "1"}})
        };
        let reply = |rows: Value| json!({"rows": rows, "diagnostics": diagnostics("exact")});
        let ok = reply(json!([row("0", "/a"), row("0", "/b"), row("10", "/a")]));
        assert!(query(&view, &request, &ok).is_ok());
        let twice = reply(json!([row("0", "/a"), row("0", "/a")]));
        let err = query(&view, &request, &twice).unwrap_err();
        assert!(err.contains("appears twice"), "{err}");
    }

    #[test]
    fn a_short_page_says_it_is_partial() {
        let request = json!({
            "dataset": "fixture", "signal": "spans",
            "time": {"start_ns": "0", "end_ns": "10"}, "group_by": [],
            "aggregate": {"kind": "events", "limit": 1, "order": "longest",
                          "fields": [{"namespace": "resource", "key": "v"}]},
        });
        let row = json!({
            "event_id": "e1", "timestamp_ns": {"type": "int", "value": "5"},
            "span_name": "GET /", "duration_ns": null, "span_timing_status": "missing",
            "status_code": {"type": "int", "value": "0"}, "trace_id": "t",
            "fields": [{"type": "missing"}],
        });
        let mut response = json!({"rows": [row], "matched": {"type": "uint64", "value": "3"},
                                  "diagnostics": diagnostics("exact")});
        let text = query(&View::Events, &request, &response).unwrap();
        assert!(
            text.contains("e1        GET /      (missing)  0            t         (missing)"),
            "{text}"
        );
        assert!(
            text.contains("exact, PARTIAL: the first 1 of 3 matching events"),
            "{text}"
        );
        response["matched"] = json!({"type": "uint64", "value": "1"});
        let text = query(&View::Events, &request, &response).unwrap();
        assert!(
            text.contains("exact, complete: all 1 matching events"),
            "{text}"
        );
    }
}
