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

use std::time::Duration;

use http::HeaderMap;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tokio::time::Instant;
use toudai::auth::{Keys, valid_dataset};

use crate::admit::{admit_logs, admit_traces};
use crate::auth::{Scope, authorize};
use crate::mapping::{map_logs, map_traces};
use crate::metrics::Counters;
use crate::writer::{WriteError, Writer};
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
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            default_dataset: None,
            routing_epoch: 1,
            budget: Duration::from_secs(5),
        }
    }
}

pub struct Pipeline {
    keys: Keys,
    default_dataset: Option<String>,
    routing_epoch: u16,
    budget: Duration,
    writer: Writer,
}

impl Pipeline {
    /// An empty default dataset means none. A name that is set must be valid,
    /// so a misconfigured default fails at startup rather than turning every
    /// request that falls back to it into a 400.
    pub fn new(keys: Keys, settings: Settings, writer: Writer) -> Result<Pipeline, String> {
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

    pub async fn export_logs(
        &self,
        scope: &Scope,
        request: &ExportLogsServiceRequest,
        deadline: Instant,
    ) -> Result<Accepted, Refusal> {
        let signal = Signal::Logs;
        let ids = admit_logs(request, scope).map_err(|r| self.reject(signal, r))?;
        let mapped = map_logs(request, &scope.dataset, &ids, self.routing_epoch);
        let rejected = mapped.rejected_records();
        let written = self.writer.write_logs(&mapped, deadline).await;
        self.finish(signal, written, rejected)
    }

    pub async fn export_traces(
        &self,
        scope: &Scope,
        request: &ExportTraceServiceRequest,
        deadline: Instant,
    ) -> Result<Accepted, Refusal> {
        let signal = Signal::Traces;
        let ids = admit_traces(request, scope).map_err(|r| self.reject(signal, r))?;
        let mapped = map_traces(request, &scope.dataset, &ids, self.routing_epoch);
        let rejected = mapped.rejected_records();
        let written = self.writer.write_spans(&mapped, deadline).await;
        self.finish(signal, written, rejected)
    }

    fn reject(&self, signal: Signal, rejection: Rejection) -> Refusal {
        self.refuse(signal, rejection.into())
    }

    fn finish(
        &self,
        signal: Signal,
        written: Result<(), WriteError>,
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
                // The error names the destination and ClickHouse's reason. It
                // never holds the key, which is not part of any row.
                eprintln!("toudai-ingest: {} write: {error}", signal.label());
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
        with_budget(default_dataset, Duration::from_millis(300))
    }

    pub fn with_budget(
        default_dataset: Option<&str>,
        budget: Duration,
    ) -> Result<Pipeline, String> {
        let writer = Writer::new(
            WriterConfig {
                url: "http://127.0.0.1:1".into(),
                user: None,
                password: None,
                database: "toudai".into(),
            },
            Arc::default(),
        )?;
        let settings = Settings {
            default_dataset: default_dataset.map(String::from),
            budget,
            ..Settings::default()
        };
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

    use super::testing::{ALL, PROD, pipeline};
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

    #[test]
    fn default_dataset_is_checked_at_startup() {
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
    #[test]
    fn empty_default_means_none() {
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
        let refusal = p.export_logs(&scope, &reserved, deadline()).await;
        assert_eq!(refusal, Err(Rejection::ReservedResource.into()));

        // Every record rejected one by one: nothing to write, so no insert is
        // attempted and the request succeeds with all of them reported.
        let all_rejected = logs(vec![duplicate_record(), duplicate_record()], vec![]);
        let accepted = p.export_logs(&scope, &all_rejected, deadline()).await;
        assert_eq!(accepted, Ok(Accepted { rejected: 2 }));
        assert!(!accepted.unwrap().message().is_empty());

        // Something to write and ClickHouse unreachable: retryable.
        let mixed = logs(vec![LogRecord::default(), duplicate_record()], vec![]);
        let refusal = p.export_logs(&scope, &mixed, deadline()).await;
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
    #[test]
    fn write_errors_map_by_retryability() {
        use crate::writer::Destination;
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
            assert_eq!(p.finish(Signal::Logs, Err(error(source)), 0), Err(expected));
        }
        let count = |outcome| {
            p.counters()
                .get("requests", &[("signal", "logs"), ("outcome", outcome)])
        };
        assert_eq!((count("unavailable"), count("internal")), (1, 1));
    }

    #[test]
    fn producer_deadline_wins_only_when_sooner() {
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
