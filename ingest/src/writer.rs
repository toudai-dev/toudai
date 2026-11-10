//! Writes mapped rows to ClickHouse: the canonical insert first, then the
//! serving insert (Decision 0001 D3, Decision 0003 D2 and D4).
//!
//! Each destination is retried on its own, within the caller's deadline: once
//! the canonical insert has committed, a serving failure retries only the
//! serving rows, so the retry cannot duplicate the canonical rows the way the
//! Go exporter's whole-batch retry did (Experiment 021, `ambiguous-serving`).
//! The two inserts are still not atomic, and an attempt that times out after
//! ClickHouse committed it is still written twice when the producer retries;
//! both are the at-least-once delivery Decision 0001 D5 accepts.
//!
//! The service never runs DDL: the tables come from `clickhouse/schema`, and
//! the `clickhouse` crate checks each row struct against the live table schema
//! before the first insert into it.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

use clickhouse::{Client, Row, RowWrite};
use tokio::time::Instant;
use toudai::query::schema::{CANONICAL_EVENTS_TABLE, table_name};
use toudai::query::{ServingTables, Signal};

use crate::mapping::{CanonicalRow, LogRow, Mapped, SpanRow};
use crate::metrics::Counters;

/// The first retry waits up to this long; each later one up to twice as long,
/// capped at [`MAX_BACKOFF`].
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(1);
/// No insert starts with less time than this left before the deadline. The
/// crate sends an insert from a spawned task that keeps running when the
/// caller stops waiting, so an insert started at the deadline can commit after
/// the request has already been answered with an error.
pub const MIN_ATTEMPT: Duration = Duration::from_millis(100);

/// A serving row type: the table it goes to and its counter label.
pub trait ServingRow:
    Row + RowWrite + for<'a> Row<Value<'a> = Self> + Send + Sync + 'static
{
    const LABEL: &'static str;
    const SIGNAL: Signal;
}

impl ServingRow for LogRow {
    const LABEL: &'static str = "logs";
    const SIGNAL: Signal = Signal::Logs;
}

impl ServingRow for SpanRow {
    const LABEL: &'static str = "spans";
    const SIGNAL: Signal = Signal::Spans;
}

#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// The ClickHouse HTTP interface, such as `http://127.0.0.1:8123`.
    pub url: String,
    pub user: Option<String>,
    pub password: Option<String>,
    /// The deployment database the schema was applied to (Decision 0002 D1).
    pub database: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Destination {
    Canonical,
    Serving,
}

impl Destination {
    pub fn label(self) -> &'static str {
        match self {
            Destination::Canonical => "canonical",
            Destination::Serving => "serving",
        }
    }
}

/// A write that did not complete. A producer retry after it assigns new
/// `event_id`s (Decision 0001 D5).
#[derive(Debug)]
pub struct WriteError {
    /// The insert that failed. `Serving` means the canonical rows committed.
    /// The failed destination itself holds nothing, except after a timeout:
    /// an insert cut off at the deadline may still commit, as when its
    /// response is lost.
    pub destination: Destination,
    pub source: clickhouse::error::Error,
}

impl WriteError {
    /// Whether ClickHouse was unavailable or slow (a 503 or `UNAVAILABLE` for
    /// the producer to retry), rather than refusing these rows outright.
    pub fn retryable(&self) -> bool {
        retryable(&self.source)
    }
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} insert failed: {}",
            self.destination.label(),
            self.source
        )
    }
}

impl std::error::Error for WriteError {}

pub struct Writer {
    client: Client,
    counters: Arc<Counters>,
}

impl Writer {
    pub fn new(config: WriterConfig, counters: Arc<Counters>) -> Result<Writer, String> {
        // The same identifier rule as the query service, which names the
        // database in SQL.
        ServingTables::new(&config.database).map_err(|e| e.to_string())?;
        let mut client = Client::default()
            .with_url(config.url)
            .with_database(config.database);
        if let Some(user) = config.user {
            client = client.with_user(user);
        }
        if let Some(password) = config.password {
            client = client.with_password(password);
        }
        Ok(Writer { client, counters })
    }

    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    pub async fn write_logs(
        &self,
        mapped: &Mapped<LogRow>,
        deadline: Instant,
    ) -> Result<(), WriteError> {
        self.write(mapped, deadline).await
    }

    pub async fn write_spans(
        &self,
        mapped: &Mapped<SpanRow>,
        deadline: Instant,
    ) -> Result<(), WriteError> {
        self.write(mapped, deadline).await
    }

    pub async fn write<R: ServingRow>(
        &self,
        mapped: &Mapped<R>,
        deadline: Instant,
    ) -> Result<(), WriteError> {
        let (label, signal) = (R::LABEL, R::SIGNAL);
        let counters = &self.counters;
        for r in &mapped.rejections {
            counters.add(
                "rejected_records",
                &[
                    ("signal", label),
                    ("reason", r.reason),
                    ("namespace", r.namespace),
                ],
                r.records as u64,
            );
        }
        counters.add(
            "reserved_attributes_dropped",
            &[("signal", label)],
            mapped.dropped_reserved as u64,
        );
        if mapped.serving.is_empty() {
            if !mapped.rejections.is_empty() {
                counters.add(
                    "rejected_batches",
                    &[("signal", label), ("reason", "all_records_rejected")],
                    1,
                );
            }
            return Ok(());
        }
        counters.add("batches", &[("signal", label)], 1);
        self.insert_with_retry::<CanonicalRow>(
            label,
            Destination::Canonical,
            CANONICAL_EVENTS_TABLE,
            &mapped.canonical,
            deadline,
        )
        .await
        .map_err(|source| WriteError {
            destination: Destination::Canonical,
            source,
        })?;
        self.insert_with_retry::<R>(
            label,
            Destination::Serving,
            table_name(signal),
            &mapped.serving,
            deadline,
        )
        .await
        .map_err(|source| {
            counters.add("partial_write_batches", &[("signal", label)], 1);
            WriteError {
                destination: Destination::Serving,
                source,
            }
        })
    }

    /// Insert `rows` into `table`, retrying with jittered exponential backoff
    /// until it commits, the error cannot be retried, or the next attempt
    /// would start after `deadline`. Each attempt is cut off at the deadline.
    async fn insert_with_retry<R>(
        &self,
        signal: &'static str,
        destination: Destination,
        table: &str,
        rows: &[R],
        deadline: Instant,
    ) -> Result<(), clickhouse::error::Error>
    where
        R: Row + RowWrite + for<'a> Row<Value<'a> = R>,
    {
        let labels = [("signal", signal), ("destination", destination.label())];
        let mut backoff = INITIAL_BACKOFF;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining < MIN_ATTEMPT {
                return Err(clickhouse::error::Error::TimedOut);
            }
            let result = tokio::time::timeout(remaining, self.insert(table, rows))
                .await
                .unwrap_or(Err(clickhouse::error::Error::TimedOut));
            let error = match result {
                Ok(()) => {
                    self.counters.add("records", &labels, rows.len() as u64);
                    return Ok(());
                }
                Err(error) => error,
            };
            self.counters.add("insert_failures", &labels, 1);
            let wait = jitter(backoff);
            if !retryable(&error) || Instant::now() + wait + MIN_ATTEMPT > deadline {
                return Err(error);
            }
            tokio::time::sleep(wait).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    async fn insert<R>(&self, table: &str, rows: &[R]) -> Result<(), clickhouse::error::Error>
    where
        R: Row + RowWrite + for<'a> Row<Value<'a> = R>,
    {
        let mut insert = self.client.insert::<R>(table).await?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await
    }
}

/// Whether another attempt could succeed. ClickHouse being unreachable or slow,
/// and server errors such as `TOO_MANY_PARTS` or a table being replaced, can
/// clear up. A row that does not serialize or match the table's schema cannot,
/// so it fails at once instead of holding the request until its deadline.
fn retryable(error: &clickhouse::error::Error) -> bool {
    matches!(
        error,
        clickhouse::error::Error::Network(_)
            | clickhouse::error::Error::TimedOut
            | clickhouse::error::Error::BadResponse(_)
    )
}

/// A random wait in `[backoff / 2, backoff]`, so requests that failed together
/// do not retry together. `RandomState` seeds each instance randomly, which is
/// all the randomness this needs.
fn jitter(backoff: Duration) -> Duration {
    let random = RandomState::new().build_hasher().finish();
    let half = backoff / 2;
    half + half.mul_f64((random as f64) / (u64::MAX as f64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::RecordRejection;

    fn writer(url: &str) -> Writer {
        Writer::new(
            WriterConfig {
                url: url.into(),
                user: None,
                password: None,
                database: "toudai".into(),
            },
            Arc::default(),
        )
        .unwrap()
    }

    fn one_row() -> Mapped<LogRow> {
        let mapped = crate::mapping::map_logs(
            &opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest {
                resource_logs: vec![opentelemetry_proto::tonic::logs::v1::ResourceLogs {
                    scope_logs: vec![opentelemetry_proto::tonic::logs::v1::ScopeLogs {
                        log_records: vec![Default::default()],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            },
            "prod",
            &[uuid::Uuid::now_v7()],
            1,
        );
        assert_eq!(mapped.serving.len(), 1);
        mapped
    }

    #[test]
    fn database_must_be_an_identifier() {
        for (database, ok) in [
            ("toudai", true),
            ("toudai_e2e", true),
            ("a-b", false),
            ("", false),
        ] {
            let config = WriterConfig {
                url: "http://127.0.0.1:1".into(),
                user: None,
                password: None,
                database: database.into(),
            };
            assert_eq!(
                Writer::new(config, Arc::default()).is_ok(),
                ok,
                "{database:?}"
            );
        }
    }

    /// An unreachable ClickHouse is retried until the deadline, then fails
    /// with nothing written: the serving insert is never attempted.
    #[tokio::test]
    async fn unreachable_clickhouse_fails_at_the_deadline() {
        let writer = writer("http://127.0.0.1:1");
        let started = Instant::now();
        let error = writer
            .write_logs(&one_row(), started + Duration::from_millis(600))
            .await
            .unwrap_err();
        assert_eq!(error.destination, Destination::Canonical);
        assert!(started.elapsed() < Duration::from_secs(2));
        let c = writer.counters();
        let canonical = [("signal", "logs"), ("destination", "canonical")];
        let serving = [("signal", "logs"), ("destination", "serving")];
        assert!(c.get("insert_failures", &canonical) >= 2, "it retried");
        assert_eq!(c.get("insert_failures", &serving), 0);
        assert_eq!(c.get("records", &canonical), 0);
        assert_eq!(c.get("partial_write_batches", &[("signal", "logs")]), 0);
        assert_eq!(c.get("batches", &[("signal", "logs")]), 1);
    }

    /// A request whose records were all rejected writes nothing and succeeds,
    /// counting the rejections; the unreachable URL and past deadline would
    /// fail any insert it tried.
    #[tokio::test]
    async fn all_rejected_writes_nothing() {
        let writer = writer("http://127.0.0.1:1");
        let mapped: Mapped<LogRow> = Mapped {
            serving: vec![],
            canonical: vec![],
            rejections: vec![
                RecordRejection {
                    reason: "invalid_scope_attributes",
                    namespace: "scope",
                    records: 3,
                },
                RecordRejection {
                    reason: "invalid_body",
                    namespace: "body",
                    records: 1,
                },
            ],
            dropped_reserved: 2,
        };
        writer.write_logs(&mapped, Instant::now()).await.unwrap();
        let c = writer.counters();
        let scope = [
            ("signal", "logs"),
            ("reason", "invalid_scope_attributes"),
            ("namespace", "scope"),
        ];
        assert_eq!(c.get("rejected_records", &scope), 3);
        assert_eq!(
            c.get(
                "rejected_batches",
                &[("signal", "logs"), ("reason", "all_records_rejected")]
            ),
            1
        );
        assert_eq!(
            c.get("reserved_attributes_dropped", &[("signal", "logs")]),
            2
        );
        assert_eq!(c.get("batches", &[("signal", "logs")]), 0);
    }

    /// A deadline too close to start an insert fails without sending one,
    /// so nothing can commit behind the error.
    #[tokio::test]
    async fn no_insert_starts_at_the_deadline() {
        let writer = writer("http://127.0.0.1:1");
        for deadline in [Instant::now(), Instant::now() + MIN_ATTEMPT / 2] {
            let error = writer.write_logs(&one_row(), deadline).await.unwrap_err();
            assert_eq!(error.destination, Destination::Canonical);
            assert!(matches!(error.source, clickhouse::error::Error::TimedOut));
            assert!(error.retryable());
        }
        let canonical = [("signal", "logs"), ("destination", "canonical")];
        assert_eq!(writer.counters().get("insert_failures", &canonical), 0);
    }

    /// Unavailability can clear up within the deadline; rows the table
    /// refuses cannot, so they fail at once instead of holding the request.
    #[test]
    fn classifies_errors_by_whether_a_retry_can_succeed() {
        use clickhouse::error::Error;
        let cases = [
            (Error::Network("connection refused".into()), true),
            (Error::TimedOut, true),
            (Error::BadResponse("Code: 252. TOO_MANY_PARTS".into()), true),
            (Error::BadResponse("Code: 60. UNKNOWN_TABLE".into()), true),
            (Error::SchemaMismatch("column type".into()), false),
            (Error::InvalidColumnsHeader("unknown type".into()), false),
            (Error::Custom("serialize".into()), false),
            (Error::InvalidParams("url".into()), false),
            (Error::Other("other".into()), false),
        ];
        for (error, want) in cases {
            let text = error.to_string();
            let error = WriteError {
                destination: Destination::Canonical,
                source: error,
            };
            assert_eq!(error.retryable(), want, "{text}");
        }
    }

    #[test]
    fn jitter_stays_in_range() {
        for _ in 0..1000 {
            let wait = jitter(Duration::from_millis(100));
            assert!((Duration::from_millis(50)..=Duration::from_millis(100)).contains(&wait));
        }
    }
}
