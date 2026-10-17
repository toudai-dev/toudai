//! Exact-event retrieval from the canonical payload table.
//!
//! `canonical_events` is sorted by `(tenant_id, dataset, event_id)` (Decision
//! 0001 D3), so a lookup is a primary-key search that needs no event time. The
//! tenant comes from the caller's credential and is bound as a parameter like
//! the dataset and event ID.

use std::fmt;

use serde::Serialize;
use serde_json::Value;
use serde_json::value::RawValue;

use crate::query::{Param, ServingTables, SqlStatement};

/// A canonical event as stored. `payload` is the stored type-preserving JSON
/// text, embedded without re-encoding, so `payload_sha256` verifies against it.
/// Stored payloads carry no leading or trailing whitespace (see [`decode`]).
#[derive(Debug, Serialize)]
pub struct CanonicalEvent {
    pub dataset: String,
    pub event_id: String,
    pub signal: String,
    /// Event time in Unix nanoseconds, a decimal string so no client rounds it.
    pub timestamp_ns: String,
    pub schema_version: String,
    pub payload: Box<RawValue>,
    pub payload_sha256: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum EventError {
    /// The same identity holds payloads with different hashes.
    Conflict { event_id: String },
    /// ClickHouse returned a row the lookup cannot have produced.
    Malformed(String),
}

impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventError::Conflict { event_id } => {
                write!(f, "event {event_id:?} holds more than one distinct payload")
            }
            EventError::Malformed(detail) => write!(f, "unexpected ClickHouse result: {detail}"),
        }
    }
}

impl std::error::Error for EventError {}

fn string_param(name: &str, value: &str) -> Param {
    Param {
        name: name.into(),
        ch_type: "String",
        value: value.into(),
    }
}

/// The lookup for one event. Ingestion is at-least-once, so a replay can store
/// the same payload many times, which is one event; two different payloads
/// under one identity is an error, not a pick. `LIMIT 1 BY payload_sha256`
/// collapses replays before the final `LIMIT 2`, so any number of identical
/// copies cannot crowd a differing payload out of the two rows read.
pub fn lookup_statement(
    tables: &ServingTables,
    tenant_id: &str,
    dataset: &str,
    event_id: &str,
) -> SqlStatement {
    SqlStatement {
        sql: format!(
            "SELECT dataset, event_id, signal, \
             toUnixTimestamp64Nano(timestamp) AS timestamp_ns, \
             schema_version, payload, payload_sha256 \
             FROM {} \
             WHERE tenant_id = {{tenant_id:String}} \
             AND dataset = {{dataset:String}} \
             AND event_id = {{event_id:String}} \
             ORDER BY payload_sha256 LIMIT 1 BY payload_sha256 LIMIT 2 FORMAT JSONEachRow",
            tables.canonical_events()
        ),
        params: vec![
            string_param("tenant_id", tenant_id),
            string_param("dataset", dataset),
            string_param("event_id", event_id),
        ],
    }
}

fn text(row: &Value, name: &str) -> Result<String, EventError> {
    row.get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| EventError::Malformed(format!("column {name} is not text")))
}

/// ClickHouse quotes 64-bit integers in JSON by default; accept both forms.
fn timestamp_ns(row: &Value) -> Result<String, EventError> {
    match row.get("timestamp_ns") {
        Some(Value::String(s)) if s.parse::<i64>().is_ok() => Ok(s.clone()),
        Some(Value::Number(n)) if n.is_i64() => Ok(n.to_string()),
        _ => Err(EventError::Malformed(
            "column timestamp_ns is not an integer".into(),
        )),
    }
}

/// Interpret the rows of [`lookup_statement`]; `None` when there is no event.
pub fn decode(rows: &[Value]) -> Result<Option<CanonicalEvent>, EventError> {
    let events = rows
        .iter()
        .map(|row| {
            let payload = text(row, "payload")?;
            // `RawValue` drops boundary whitespace, which would leave the
            // returned text unequal to the text `payload_sha256` covers. The
            // canonical producer must not emit any, so a row with it is
            // reported, not silently altered.
            if payload.trim_matches([' ', '\t', '\n', '\r']) != payload {
                return Err(EventError::Malformed(
                    "payload has leading or trailing whitespace".into(),
                ));
            }
            Ok(CanonicalEvent {
                dataset: text(row, "dataset")?,
                event_id: text(row, "event_id")?,
                signal: text(row, "signal")?,
                timestamp_ns: timestamp_ns(row)?,
                schema_version: text(row, "schema_version")?,
                payload: RawValue::from_string(payload)
                    .map_err(|e| EventError::Malformed(format!("payload is not JSON: {e}")))?,
                payload_sha256: text(row, "payload_sha256")?,
            })
        })
        .collect::<Result<Vec<_>, EventError>>()?;
    let mut events = events.into_iter();
    let Some(first) = events.next() else {
        return Ok(None);
    };
    match events.next() {
        Some(second) if second.payload_sha256 != first.payload_sha256 => {
            Err(EventError::Conflict {
                event_id: first.event_id,
            })
        }
        _ => Ok(Some(first)),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn row(payload: &str, sha: &str) -> Value {
        json!({
            "dataset": "fixture", "event_id": "e1", "signal": "logs",
            "timestamp_ns": "1788246000123456789", "schema_version": "1",
            "payload": payload, "payload_sha256": sha,
        })
    }

    #[test]
    fn lookup_binds_every_identifier() {
        let tables = ServingTables::new("toudai").unwrap();
        let hostile = "x' OR tenant_id != '";
        let statement = lookup_statement(&tables, "tenant-a", "ds", hostile);
        assert!(statement.sql.contains("FROM toudai.canonical_events "));
        assert!(!statement.sql.contains(hostile));
        assert!(!statement.sql.contains("tenant-a"));
        let bound: Vec<(&str, &str)> = statement
            .params
            .iter()
            .map(|p| (p.name.as_str(), p.value.as_str()))
            .collect();
        assert_eq!(
            bound,
            [
                ("tenant_id", "tenant-a"),
                ("dataset", "ds"),
                ("event_id", hostile)
            ]
        );
    }

    #[test]
    fn decodes_rows() {
        let payload = r#"{"a": 9007199254740993,  "b":[1,"x"]}"#;
        let found = decode(&[row(payload, "h1")]).unwrap().unwrap();
        // Spacing and the integer beyond 2^53 survive: the payload is not re-encoded.
        assert_eq!(
            serde_json::to_string(&found.payload).unwrap(),
            payload.to_owned()
        );
        assert_eq!(found.timestamp_ns, "1788246000123456789");

        assert!(decode(&[]).unwrap().is_none());
        // A replayed duplicate is one event.
        assert!(
            decode(&[row("{}", "h1"), row("{}", "h1")])
                .unwrap()
                .is_some()
        );
        assert_eq!(
            decode(&[row("{}", "h1"), row("{\"x\":1}", "h2")]).unwrap_err(),
            EventError::Conflict {
                event_id: "e1".into()
            }
        );
        let mut numeric = row("{}", "h1");
        numeric["timestamp_ns"] = json!(5);
        assert_eq!(decode(&[numeric]).unwrap().unwrap().timestamp_ns, "5");
        assert!(matches!(
            decode(&[row("not json", "h1")]),
            Err(EventError::Malformed(_))
        ));
        // Boundary whitespace would not survive embedding, so it is refused.
        for padded in ["{}\n", " {}", "\t{}"] {
            assert!(matches!(
                decode(&[row(padded, "h1")]),
                Err(EventError::Malformed(_))
            ));
        }
        let mut bad = row("{}", "h1");
        bad.as_object_mut().unwrap().remove("signal");
        assert!(matches!(decode(&[bad]), Err(EventError::Malformed(_))));
    }
}
