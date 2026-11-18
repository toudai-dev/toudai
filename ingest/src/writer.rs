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
//! before each insert attempt.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use clickhouse::{Client, Row, RowWrite};
use hyper_util::client::legacy::Client as HttpClient;
use hyper_util::client::legacy::connect::HttpConnector;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio::time::Instant;
use toudai::query::schema::{CANONICAL_EVENTS_TABLE, table_name};
use toudai::query::{ServingTables, Signal};
use tower::Service;

use crate::mapping::{CanonicalRow, LogRow, Mapped, SpanRow};
use crate::metrics::Counters;

/// The first retry waits up to this long; each later one up to twice as long,
/// capped at [`MAX_BACKOFF`].
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(1);
/// No insert starts with less time than this left before the deadline. An
/// insert whose rows have reached ClickHouse can commit even after the attempt
/// is cut off, so one started at the deadline could commit after the request
/// has already been answered with an error.
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

/// Own the HTTP connection tasks, which close their sockets on cancellation.
/// clickhouse 0.15.3 detaches its response task on drop; aborting only its
/// request body can itself stall behind an unsent chunk. A transport per
/// attempt isolates cancellation from concurrent inserts. Connections and
/// metadata cannot be shared across these clients with the crate's public
/// API, so each attempt also fetches its schema again rather than weakening
/// row validation. Remove this workaround when insert drop aborts its task.
struct Connections {
    tasks: Mutex<Vec<AbortHandle>>,
    // Dropping the last sender cancels DNS/TCP work that has not reached the
    // executor yet. A connection-task abort handle alone cannot cover that.
    _cancelled: watch::Sender<()>,
}

impl Drop for Connections {
    fn drop(&mut self) {
        for task in self
            .tasks
            .get_mut()
            .expect("connection tasks lock")
            .drain(..)
        {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct AttemptExecutor(Weak<Connections>);

impl<F> hyper::rt::Executor<F> for AttemptExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        if let Some(connections) = self.0.upgrade() {
            connections
                .tasks
                .lock()
                .expect("connection tasks lock")
                .push(tokio::spawn(future).abort_handle());
        }
    }
}

#[derive(Clone)]
struct AttemptConnector {
    inner: HttpConnector,
    deadline: Instant,
    cancelled: watch::Receiver<()>,
}

impl Service<http::Uri> for AttemptConnector {
    type Response = <HttpConnector as Service<http::Uri>>::Response;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map(|r| r.map_err(Into::into))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let connect = self.inner.call(uri);
        let deadline = self.deadline;
        let mut cancelled = self.cancelled.clone();
        Box::pin(async move {
            tokio::select! {
                _ = cancelled.changed() => Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "insert cancelled").into()),
                result = tokio::time::timeout_at(deadline, connect) => match result {
                    Ok(result) => result.map_err(Into::into),
                    Err(error) => Err(error.into()),
                },
            }
        })
    }
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
    config: WriterConfig,
    counters: Arc<Counters>,
}

impl Writer {
    pub fn new(config: WriterConfig, counters: Arc<Counters>) -> Result<Writer, String> {
        // The same identifier rule as the query service, which names the
        // database in SQL.
        ServingTables::new(&config.database).map_err(|e| e.to_string())?;
        Ok(Writer { config, counters })
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
    /// would start after `deadline`. Each attempt ends at the same absolute
    /// deadline, including a stalled final send.
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
            let error = match self.insert(table, rows, deadline).await {
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

    /// One insert attempt, cut off at `deadline`.
    async fn insert<R>(
        &self,
        table: &str,
        rows: &[R],
        deadline: Instant,
    ) -> Result<(), clickhouse::error::Error>
    where
        R: Row + RowWrite + for<'a> Row<Value<'a> = R>,
    {
        let (cancelled, receiver) = watch::channel(());
        let connections = Arc::new(Connections {
            tasks: Mutex::default(),
            _cancelled: cancelled,
        });
        let http = HttpClient::builder(AttemptExecutor(Arc::downgrade(&connections)))
            .pool_max_idle_per_host(0)
            .build(AttemptConnector {
                inner: HttpConnector::new(),
                deadline,
                cancelled: receiver,
            });
        let mut client = Client::with_http_client(http)
            .with_url(&self.config.url)
            .with_database(&self.config.database);
        if let Some(user) = &self.config.user {
            client = client.with_user(user);
        }
        if let Some(password) = &self.config.password {
            client = client.with_password(password);
        }
        let send = async {
            let mut insert = client.insert::<R>(table).await?;
            for row in rows {
                insert.write(row).await?;
            }
            Ok::<_, clickhouse::error::Error>(insert)
        };
        // `connections` is dropped on every exit, including cancellation of
        // this future, closing even a socket blocked in its final send.
        let insert = tokio::time::timeout_at(deadline, send)
            .await
            .unwrap_or(Err(clickhouse::error::Error::TimedOut))?;
        // The full remaining budget is available to send AND acknowledgement;
        // neither phase gets an arbitrary half-budget that could force a retry
        // after ClickHouse committed. The guard closes the transport on timeout.
        let remaining = deadline.saturating_duration_since(Instant::now());
        let end = insert.with_timeouts(Some(remaining), Some(remaining)).end();
        tokio::time::timeout_at(deadline, end)
            .await
            .unwrap_or(Err(clickhouse::error::Error::TimedOut))
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
