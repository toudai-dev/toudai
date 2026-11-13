//! One request from its headers to stored rows, shared by the gRPC and HTTP
//! receivers (Decision 0003 D2).
//!
//! A receiver calls [`Pipeline::authorize`] before decoding the body, then
//! [`Pipeline::export_logs`] or [`Pipeline::export_traces`] with the decoded
//! request. Success is returned only after the canonical and serving rows of
//! every accepted record have committed; records rejected one by one come back
//! as a count for OTLP `partial_success`. Anything else is a [`Refusal`] that
//! stored nothing the producer needs to keep: a retry after it is a new
//! request with new `event_id`s (Decision 0001 D5).

use std::sync::Arc;
use std::time::Duration;

use http::HeaderMap;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use toudai::auth::{Keys, valid_dataset};

use crate::admit::{admit_logs, admit_traces};
use crate::auth::{Scope, authorize};
use crate::batcher::{Batcher, FlushConfig, Outcome};
use crate::mapping::{LogRow, Mapped, SpanRow};
use crate::mapping::{map_logs, map_traces};
use crate::metrics::Counters;
use crate::writer::{ServingRow, Writer};
use crate::{Refusal, Rejection};

/// The signal a request carries, which is also its counter label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Logs,
    Traces,
}

impl Signal {
    pub fn label(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Traces => "spans",
        }
    }
}

/// A request whose accepted records are all stored. `rejected` records were
/// left out one by one and are reported in OTLP `partial_success`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Accepted {
    pub rejected: i64,
}

impl Accepted {
    /// The fixed `partial_success` message: empty when nothing was rejected,
    /// as the OTLP spec asks for a full success.
    pub fn message(self) -> &'static str {
        if self.rejected == 0 {
            ""
        } else {
            "records not stored: an attribute list, body, scope, or resource repeats a key"
        }
    }
}

/// How the pipeline treats every request.
#[derive(Debug, Clone)]
pub struct Settings {
    /// The dataset of a request whose header and key name none. Empty means
    /// none.
    pub default_dataset: Option<String>,
    /// Stamped on every row (Decision 0001).
    pub routing_epoch: u16,
    /// The longest a request may take, including in-service retries. A
    /// shorter producer deadline (`grpc-timeout`) wins. OTLP SDK exporters
    /// time out after 10 s by default, so this answers well before they give
    /// up on their own.
    pub budget: Duration,
    /// Request memory held at once, from the body's arrival until the
    /// request is answered (Decision 0003 D3). A request is charged its body
    /// as received, then as decompressed, then that plus [`RECORD_BYTES`] per
    /// record once decoded. A request that needs more than the whole budget
    /// is refused as invalid, since no retry could pass.
    pub memory_budget: usize,
    /// When concurrent requests' rows are written together.
    pub flush: FlushConfig,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            default_dataset: None,
            routing_epoch: 1,
            budget: Duration::from_secs(5),
            memory_budget: 256 * 1024 * 1024,
            flush: FlushConfig::default(),
        }
    }
}

/// What a decoded record costs on top of its bytes on the wire. Decoding and
/// mapping cost about 2.5 KiB per record plus several times its wire size:
/// measured at 2.4 KiB per empty log record (over 1000× its 2 wire bytes),
/// 4.9 KiB per attribute-heavy golden fixture log record (36× its wire size),
/// and 3.1 KiB per fixture span. Charging only wire bytes would let a body of
/// empty records hold a thousand times the budget.
pub const RECORD_BYTES: usize = 4 * 1024;

pub struct Pipeline {
    keys: Keys,
    default_dataset: Option<String>,
    routing_epoch: u16,
    budget: Duration,
    memory_budget: usize,
    admission: Arc<Semaphore>,
    writer: Arc<Writer>,
    logs: Batcher<LogRow>,
    spans: Batcher<SpanRow>,
}

/// A request's share of the admission budget, given back when it is dropped.
#[derive(Debug, Default)]
pub struct Held(Option<OwnedSemaphorePermit>);

impl Held {
    fn bytes(&self) -> usize {
        self.0.as_ref().map_or(0, OwnedSemaphorePermit::num_permits)
    }
}

impl Pipeline {
    /// An empty default dataset means none. A name that is set must be valid,
    /// so a misconfigured default fails at startup rather than turning every
    /// request that falls back to it into a 400. Starts the batching tasks,
    /// so it must run on a Tokio runtime.
    pub fn new(keys: Keys, settings: Settings, writer: Writer) -> Result<Pipeline, String> {
        let writer = Arc::new(writer);
        let default_dataset = settings
            .default_dataset
            .map(|d| d.trim().to_owned())
            .filter(|d| !d.is_empty());
        if let Some(dataset) = &default_dataset
            && !valid_dataset(dataset)
        {
            return Err("the default dataset is not a valid dataset name".into());
        }
        Ok(Pipeline {
            keys,
            default_dataset,
            routing_epoch: settings.routing_epoch,
            budget: settings.budget,
            memory_budget: settings.memory_budget,
            admission: Arc::new(Semaphore::new(settings.memory_budget)),
            logs: Batcher::spawn(writer.clone(), settings.flush),
            spans: Batcher::spawn(writer.clone(), settings.flush),
            writer,
        })
    }

    /// When a request arriving now must be answered: after the budget, or the
    /// producer's own deadline when that is sooner. A deadline under the
    /// writer's minimum attempt (100 ms) stores nothing and is answered as
    /// unavailable, which is the producer's own timeout showing, not
    /// ClickHouse being down.
    pub fn deadline(&self, requested: Option<Duration>) -> Instant {
        Instant::now() + requested.map_or(self.budget, |r| r.min(self.budget))
    }

    pub fn counters(&self) -> &Counters {
        self.writer.counters()
    }

    /// Authenticate the write key and resolve the dataset from the headers
    /// alone, before the body is read.
    pub fn authorize(&self, signal: Signal, headers: &HeaderMap) -> Result<Scope, Refusal> {
        authorize(&self.keys, headers, self.default_dataset.as_deref())
            .map_err(|rejection| self.refuse(signal, rejection.into()))
    }

    /// Refuse a request whose body, `bytes` as declared, the budget has no
    /// room for now, before any of it is read. Nothing is taken: the body is
    /// charged as it arrives ([`Pipeline::hold`]), so a client that declares
    /// a large body and sends it slowly, or never, holds nothing it has not
    /// sent.
    pub fn has_room(&self, signal: Signal, bytes: usize) -> Result<(), Refusal> {
        if bytes > self.memory_budget {
            Err(self.refuse(signal, Refusal::OVER_BUDGET))
        } else if bytes > self.admission.available_permits() {
            Err(self.refuse(signal, Refusal::OVERLOADED))
        } else {
            Ok(())
        }
    }

    /// Grow `held` to `bytes` of the admission budget, or refuse the request
    /// with backpressure when the budget cannot cover it now. Nothing waits:
    /// the producer's own retry queue is the buffer (Decision 0003 D2).
    pub fn hold(&self, signal: Signal, held: &mut Held, bytes: usize) -> Result<(), Refusal> {
        if bytes > self.memory_budget {
            return Err(self.refuse(signal, Refusal::OVER_BUDGET));
        }
        let more = bytes.saturating_sub(held.bytes());
        if more == 0 {
            return Ok(());
        }
        // The budget is below 4 GiB (checked at startup), so `more` fits a
        // permit count.
        let permit = u32::try_from(more)
            .ok()
            .and_then(|n| self.admission.clone().try_acquire_many_owned(n).ok())
            .ok_or_else(|| self.refuse(signal, Refusal::OVERLOADED))?;
        match &mut held.0 {
            Some(existing) => existing.merge(permit),
            None => held.0 = Some(permit),
        }
        Ok(())
    }

    /// Count a refusal and return it, for refusals a receiver makes itself,
    /// such as a body it cannot decode.
    pub fn refuse(&self, signal: Signal, refusal: Refusal) -> Refusal {
        self.counters().add(
            "requests",
            &[
                ("signal", signal.label()),
                ("outcome", refusal.kind.label()),
            ],
            1,
        );
        refusal
    }

    /// `held` is the request's share of the admission budget, given back
    /// once the insert holding its rows has finished.
    pub async fn export_logs(
        &self,
        scope: &Scope,
        request: &ExportLogsServiceRequest,
        mut held: Held,
        deadline: Instant,
    ) -> Result<Accepted, Refusal> {
        let signal = Signal::Logs;
        let ids = admit_logs(request, scope).map_err(|r| self.reject(signal, r))?;
        self.hold_records(signal, &mut held, ids.len())?;
        let mapped = map_logs(request, &scope.dataset, &ids, self.routing_epoch);
        self.store(signal, &self.logs, mapped, deadline, held).await
    }

    pub async fn export_traces(
        &self,
        scope: &Scope,
        request: &ExportTraceServiceRequest,
        mut held: Held,
        deadline: Instant,
    ) -> Result<Accepted, Refusal> {
        let signal = Signal::Traces;
        let ids = admit_traces(request, scope).map_err(|r| self.reject(signal, r))?;
        self.hold_records(signal, &mut held, ids.len())?;
        let mapped = map_traces(request, &scope.dataset, &ids, self.routing_epoch);
        self.store(signal, &self.spans, mapped, deadline, held)
            .await
    }

    /// Write a request's rows with the next batch, or, when every record was
    /// rejected, only count it: there is nothing to wait for.
    async fn store<R: ServingRow>(
        &self,
        signal: Signal,
        batcher: &Batcher<R>,
        mapped: Mapped<R>,
        deadline: Instant,
        held: Held,
    ) -> Result<Accepted, Refusal> {
        let rejected = mapped.rejected_records();
        let outcome = if mapped.serving.is_empty() {
            self.writer.write(&mapped, deadline).await.map_err(Arc::new)
        } else {
            batcher.submit(mapped, deadline, held).await
        };
        self.finish(signal, outcome, rejected)
    }

    /// Top up `held`, which covers the decompressed body, by
    /// [`RECORD_BYTES`] per record before mapping copies the records into
    /// rows.
    fn hold_records(&self, signal: Signal, held: &mut Held, records: usize) -> Result<(), Refusal> {
        let bytes = held
            .bytes()
            .saturating_add(records.saturating_mul(RECORD_BYTES));
        self.hold(signal, held, bytes)
    }

    fn reject(&self, signal: Signal, rejection: Rejection) -> Refusal {
        self.refuse(signal, rejection.into())
    }

    fn finish(
        &self,
        signal: Signal,
        written: Outcome,
        rejected: usize,
    ) -> Result<Accepted, Refusal> {
        match written {
            Ok(()) => {
                self.counters().add(
                    "requests",
                    &[("signal", signal.label()), ("outcome", "accepted")],
                    1,
                );
                Ok(Accepted {
                    rejected: rejected as i64,
                })
            }
            Err(error) => {
                let refusal = if error.retryable() {
                    Refusal::UNAVAILABLE
                } else {
                    Refusal::INTERNAL
                };
                Err(self.refuse(signal, refusal))
            }
        }
    }
}

/// A pipeline whose ClickHouse is unreachable: nothing listens on port 1, so
/// every insert fails with a network error at once. Tests that reach the
/// writer use it only to show that they did.
#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;
    use std::time::Duration;

    use toudai::auth::Keys;

    use super::{Pipeline, Settings};
    use crate::writer::{Writer, WriterConfig};

    /// Permits only `prod`.
    pub const PROD: &str = "tk_test_prod_only_0123456789";
    /// Permits every dataset.
    pub const ALL: &str = "tk_test_every_dataset_0123456789";

    pub fn pipeline(default_dataset: Option<&str>) -> Result<Pipeline, String> {
        with(Settings {
            default_dataset: default_dataset.map(String::from),
            budget: Duration::from_millis(300),
            ..Settings::default()
        })
    }

    pub fn with(settings: Settings) -> Result<Pipeline, String> {
        let writer = Writer::new(
            WriterConfig {
                url: "http://127.0.0.1:1".into(),
                user: None,
                password: None,
                database: "toudai".into(),
            },
            Arc::default(),
        )?;
        Pipeline::new(
            Keys::parse(&format!("{PROD} prod\n{ALL}\n")).unwrap(),
            settings,
            writer,
        )
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use super::testing::{self, ALL, PROD, pipeline};
    use super::*;
    use crate::StatusKind;

    fn headers(key: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-toudai-key", HeaderValue::from_str(key).unwrap());
        headers
    }

    fn s(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
            ..Default::default()
        }
    }

    fn logs(records: Vec<LogRecord>, resource: Vec<KeyValue>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                    attributes: resource,
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn duplicate_record() -> LogRecord {
        LogRecord {
            attributes: vec![s("a", "1"), s("a", "2")],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn default_dataset_is_checked_at_startup() {
        for (default, ok) in [
            (None, true),
            (Some(""), true),
            (Some("  "), true),
            (Some("staging"), true),
            (Some("a b"), false),
            (Some("-staging"), false),
        ] {
            assert_eq!(pipeline(default).is_ok(), ok, "{default:?}");
        }
    }

    /// An empty default is no default: a request the key does not resolve is
    /// "dataset is required", not "invalid dataset name".
    #[tokio::test]
    async fn empty_default_means_none() {
        let refusal = pipeline(Some(""))
            .unwrap()
            .authorize(Signal::Logs, &headers(ALL))
            .unwrap_err();
        assert_eq!(refusal, Rejection::DatasetRequired.into());
    }

    #[tokio::test]
    async fn outcomes_and_counters() {
        let p = pipeline(None).unwrap();
        let scope = p.authorize(Signal::Logs, &headers(PROD)).unwrap();
        let deadline = || p.deadline(None);

        // Refused before the body: counted, nothing else happens.
        let refusal = p
            .authorize(Signal::Logs, &headers("tk_test_not_a_real_key_0000"))
            .unwrap_err();
        assert_eq!(refusal.kind, StatusKind::Unauthenticated);

        // Refused by admit: the whole request, before any write.
        let reserved = logs(vec![LogRecord::default()], vec![s("toudai.other", "x")]);
        let refusal = p
            .export_logs(&scope, &reserved, Held::default(), deadline())
            .await;
        assert_eq!(refusal, Err(Rejection::ReservedResource.into()));

        // Every record rejected one by one: nothing to write, so no insert is
        // attempted and the request succeeds with all of them reported.
        let all_rejected = logs(vec![duplicate_record(), duplicate_record()], vec![]);
        let accepted = p
            .export_logs(&scope, &all_rejected, Held::default(), deadline())
            .await;
        assert_eq!(accepted, Ok(Accepted { rejected: 2 }));
        assert!(!accepted.unwrap().message().is_empty());

        // Something to write and ClickHouse unreachable: retryable.
        let mixed = logs(vec![LogRecord::default(), duplicate_record()], vec![]);
        let refusal = p
            .export_logs(&scope, &mixed, Held::default(), deadline())
            .await;
        assert_eq!(refusal, Err(Refusal::UNAVAILABLE));

        let count = |outcome| {
            p.counters()
                .get("requests", &[("signal", "logs"), ("outcome", outcome)])
        };
        assert_eq!(count("unauthenticated"), 1);
        assert_eq!(count("invalid_argument"), 1);
        assert_eq!(count("accepted"), 1);
        assert_eq!(count("unavailable"), 1);
    }

    /// A write error a retry cannot clear, such as a schema mismatch, is not
    /// answered as retryable: the producer would resend it forever.
    #[tokio::test]
    async fn write_errors_map_by_retryability() {
        use crate::writer::{Destination, WriteError};
        let p = pipeline(None).unwrap();
        let error = |source| WriteError {
            destination: Destination::Canonical,
            source,
        };
        let cases = [
            (clickhouse::error::Error::TimedOut, Refusal::UNAVAILABLE),
            (
                clickhouse::error::Error::Custom("schema".into()),
                Refusal::INTERNAL,
            ),
        ];
        for (source, expected) in cases {
            assert_eq!(
                p.finish(Signal::Logs, Err(Arc::new(error(source))), 0),
                Err(expected)
            );
        }
        let count = |outcome| {
            p.counters()
                .get("requests", &[("signal", "logs"), ("outcome", outcome)])
        };
        assert_eq!((count("unavailable"), count("internal")), (1, 1));
    }

    #[tokio::test]
    async fn admission_budget_is_held_until_dropped() {
        let p = testing::with(Settings {
            memory_budget: 100,
            ..Settings::default()
        })
        .unwrap();
        let mut first = Held::default();
        // Grows in steps: the body as received, then as decompressed.
        p.hold(Signal::Logs, &mut first, 30).unwrap();
        p.hold(Signal::Logs, &mut first, 60).unwrap();
        // Shrinking is a no-op; the share only grows.
        p.hold(Signal::Logs, &mut first, 10).unwrap();
        let mut second = Held::default();
        assert_eq!(
            p.hold(Signal::Logs, &mut second, 41),
            Err(Refusal::OVERLOADED),
            "60 of 100 are held"
        );
        p.hold(Signal::Logs, &mut second, 40).unwrap();
        drop(first);
        let mut third = Held::default();
        p.hold(Signal::Logs, &mut third, 60).unwrap();
        // A body is checked for room before it is read, without taking any.
        assert_eq!(p.has_room(Signal::Logs, 1), Err(Refusal::OVERLOADED));
        drop((second, third));
        p.has_room(Signal::Logs, 100).unwrap();
        p.hold(Signal::Logs, &mut Held::default(), 100).unwrap();
        // More than the whole budget, and more than u32 can ask for: no retry
        // could pass, so not backpressure.
        for bytes in [101, usize::MAX] {
            assert_eq!(
                p.hold(Signal::Logs, &mut Held::default(), bytes),
                Err(Refusal::OVER_BUDGET)
            );
        }
        assert_eq!(p.has_room(Signal::Logs, 101), Err(Refusal::OVER_BUDGET));
        let count = |outcome| {
            p.counters()
                .get("requests", &[("signal", "logs"), ("outcome", outcome)])
        };
        assert_eq!((count("overloaded"), count("over_budget")), (2, 3));
    }

    /// Records are charged on top of their bytes once decoded, since an
    /// empty record costs kilobytes once mapped but two bytes on the wire.
    #[tokio::test]
    async fn records_are_charged_before_mapping() {
        let p = testing::with(Settings {
            memory_budget: 3 * RECORD_BYTES,
            ..Settings::default()
        })
        .unwrap();
        let scope = p.authorize(Signal::Logs, &headers(PROD)).unwrap();
        let export = |records, held| {
            let request = logs(vec![LogRecord::default(); records], vec![]);
            let p = &p;
            let scope = &scope;
            async move { p.export_logs(scope, &request, held, p.deadline(None)).await }
        };
        // Admitted, then refused by the unreachable ClickHouse.
        assert_eq!(export(3, Held::default()).await, Err(Refusal::UNAVAILABLE));
        // Four records need more than the whole budget.
        assert_eq!(export(4, Held::default()).await, Err(Refusal::OVER_BUDGET));
        // Three records fit alone, but not on top of the body bytes the
        // request already holds, nor beside another request.
        let mut body = Held::default();
        p.hold(Signal::Logs, &mut body, RECORD_BYTES / 2).unwrap();
        assert_eq!(export(3, body).await, Err(Refusal::OVER_BUDGET));
        let mut other = Held::default();
        p.hold(Signal::Logs, &mut other, RECORD_BYTES).unwrap();
        assert_eq!(export(3, Held::default()).await, Err(Refusal::OVERLOADED));
        drop(other);
        // Spans are charged the same way.
        let spans = |n| ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span::default(); n],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let export_spans = |n| {
            let request = spans(n);
            let (p, scope) = (&p, &scope);
            async move {
                p.export_traces(scope, &request, Held::default(), p.deadline(None))
                    .await
            }
        };
        assert_eq!(export_spans(3).await, Err(Refusal::UNAVAILABLE));
        assert_eq!(export_spans(4).await, Err(Refusal::OVER_BUDGET));
    }

    /// A producer that hangs up leaves its rows queued or being written, so
    /// its share of the budget stays held until that write ends.
    #[tokio::test]
    async fn a_dropped_request_keeps_its_share_until_written() {
        let memory_budget = 16 * RECORD_BYTES;
        let p = testing::with(Settings {
            memory_budget,
            budget: Duration::from_millis(1500),
            flush: FlushConfig {
                interval: Duration::from_millis(100),
                reserve: Duration::ZERO,
                ..FlushConfig::default()
            },
            ..Settings::default()
        })
        .unwrap();
        let scope = p.authorize(Signal::Logs, &headers(PROD)).unwrap();
        let request = logs(vec![LogRecord::default()], vec![]);
        let export = p.export_logs(&scope, &request, Held::default(), p.deadline(None));
        let hung_up = tokio::time::timeout(Duration::from_millis(50), export).await;
        assert!(hung_up.is_err(), "answered before its batch flushed");
        let whole = |p: &Pipeline| p.hold(Signal::Logs, &mut Held::default(), memory_budget);
        assert_eq!(whole(&p), Err(Refusal::OVERLOADED), "given back on hang-up");
        // The write against the unreachable ClickHouse gives up by the
        // request's 1.5 s deadline.
        tokio::time::sleep(Duration::from_millis(1700)).await;
        assert_eq!(whole(&p), Ok(()));
    }

    #[tokio::test]
    async fn producer_deadline_wins_only_when_sooner() {
        let p = pipeline(None).unwrap();
        let within = |requested, expected: u64| {
            let left = p.deadline(requested) - Instant::now();
            let expected = Duration::from_millis(expected);
            assert!(left <= expected && left + Duration::from_millis(50) > expected);
        };
        within(None, 300);
        within(Some(Duration::from_millis(120)), 120);
        within(Some(Duration::from_secs(10)), 300);
    }

    #[test]
    fn full_success_has_no_message() {
        assert_eq!(Accepted { rejected: 0 }.message(), "");
        assert_ne!(Accepted { rejected: 1 }.message(), "");
    }
}
