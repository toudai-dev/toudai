//! OTLP/gRPC: the Logs and Trace services (Decision 0003 D1). Metrics are
//! not served; tonic answers an unknown service with `UNIMPLEMENTED`, as the
//! Collector build did.
//!
//! The write key and dataset are checked by an interceptor, which runs on the
//! metadata before tonic reads the message, so a request without a valid key
//! is refused before anything is decoded.

use std::sync::Arc;
use std::time::Duration;

use http::HeaderValue;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataMap;
use tonic::service::Routes;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::TcpIncoming;
use tonic::{Code, Request, Response, Status};
use tower::util::MapResponseLayer;

use crate::Refusal;
use crate::auth::{KEY_HEADER, Scope};
use crate::pipeline::{Held, Pipeline, Signal};

/// The largest message accepted, after decompression: grpc-go's default,
/// which the Collector's OTLP receiver kept (Decision 0003 D3).
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// The Logs and Trace services, each behind the key interceptor.
fn routes(pipeline: Arc<Pipeline>) -> Routes {
    let logs = LogsServiceServer::new(Logs(pipeline.clone()))
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(MAX_MESSAGE_BYTES);
    let traces = TraceServiceServer::new(Traces(pipeline.clone()))
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(MAX_MESSAGE_BYTES);
    Routes::new(InterceptedService::new(
        logs,
        authorizer(pipeline.clone(), Signal::Logs),
    ))
    .add_service(InterceptedService::new(
        traces,
        authorizer(pipeline, Signal::Traces),
    ))
}

/// Serve [`routes`] on `listener` until `shutdown` completes, then wait for
/// requests in flight, up to `drain`. tonic itself waits on every connection
/// with no bound, so a stream stalled mid-message would otherwise hold exit
/// until the process is killed.
pub async fn serve(
    listener: TcpListener,
    pipeline: Arc<Pipeline>,
    shutdown: impl Future<Output = ()>,
    drain: Duration,
) -> Result<(), tonic::transport::Error> {
    let (stopping, stopped) = tokio::sync::oneshot::channel();
    let shutdown = async move {
        shutdown.await;
        let _ = stopping.send(());
    };
    let served = tonic::transport::Server::builder()
        .layer(MapResponseLayer::new(oversize_is_not_retryable))
        .add_routes(routes(pipeline))
        .serve_with_incoming_shutdown(TcpIncoming::from(listener), shutdown);
    let drained = async {
        match stopped.await {
            Ok(()) => tokio::time::sleep(drain).await,
            // The server ended on its own; its result wins.
            Err(_) => std::future::pending().await,
        }
    };
    tokio::select! {
        served = served => served,
        () = drained => Ok(()),
    }
}

/// tonic refuses a message over the size limit with `OUT_OF_RANGE`, which the
/// OTLP spec lists as retryable, so a producer would resend the same
/// oversized batch until its retry window ends. grpc-go, and so the Collector,
/// answered `RESOURCE_EXHAUSTED`, which without `RetryInfo` is not retried.
/// The handlers never return `OUT_OF_RANGE`, so the rewrite touches only that
/// refusal, which tonic sends in the response headers.
fn oversize_is_not_retryable(
    mut response: http::Response<tonic::body::Body>,
) -> http::Response<tonic::body::Body> {
    let code = |c: Code| HeaderValue::from(c as i32);
    let headers = response.headers_mut();
    if headers.get(GRPC_STATUS) == Some(&code(Code::OutOfRange)) {
        headers.insert(GRPC_STATUS, code(Code::ResourceExhausted));
    }
    response
}

const GRPC_STATUS: &str = "grpc-status";

fn authorizer(
    pipeline: Arc<Pipeline>,
    signal: Signal,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |mut request: Request<()>| {
        let headers = request.metadata().clone().into_headers();
        let scope = pipeline.authorize(signal, &headers).map_err(status)?;
        // Nothing after this point reads the key; dropping it keeps it out of
        // anything that could log or forward the request's metadata.
        request.metadata_mut().remove(KEY_HEADER);
        request.extensions_mut().insert(scope);
        Ok(request)
    }
}

fn scope<T>(request: &mut Request<T>) -> Scope {
    request
        .extensions_mut()
        .remove::<Scope>()
        .expect("the interceptor authorizes every request")
}

/// The message's share of the admission budget, held until it is answered.
/// tonic has already decoded it, so this bounds how many decoded messages are
/// held at once rather than the decoding itself, which the 4 MiB limit bounds.
fn hold(
    pipeline: &Pipeline,
    signal: Signal,
    message: &impl prost::Message,
) -> Result<Held, Status> {
    let mut held = Held::default();
    pipeline
        .hold(signal, &mut held, message.encoded_len())
        .map_err(status)?;
    Ok(held)
}

fn status(refusal: Refusal) -> Status {
    Status::new(Code::from_i32(refusal.kind.rpc_code()), refusal.message)
}

/// The producer's deadline from `grpc-timeout` (gRPC over HTTP/2: at most
/// eight digits and a unit). tonic enforces it too but does not expose it to
/// the handler. A malformed value counts as absent, as it does for tonic.
fn grpc_timeout(metadata: &MetadataMap) -> Option<Duration> {
    let value = metadata.get("grpc-timeout")?.to_str().ok()?;
    let split = value.len().checked_sub(1)?;
    let (digits, unit) = value.split_at(split);
    if digits.is_empty() || digits.len() > 8 {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n * 3600),
        "M" => Duration::from_secs(n * 60),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}

struct Logs(Arc<Pipeline>);

#[tonic::async_trait]
impl LogsService for Logs {
    async fn export(
        &self,
        mut request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let deadline = self.0.deadline(grpc_timeout(request.metadata()));
        let scope = scope(&mut request);
        let held = hold(&self.0, Signal::Logs, request.get_ref())?;
        let accepted = self
            .0
            .export_logs(&scope, request.get_ref(), held, deadline)
            .await
            .map_err(status)?;
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: (accepted.rejected > 0).then(|| ExportLogsPartialSuccess {
                rejected_log_records: accepted.rejected,
                error_message: accepted.message().into(),
            }),
        }))
    }
}

struct Traces(Arc<Pipeline>);

#[tonic::async_trait]
impl TraceService for Traces {
    async fn export(
        &self,
        mut request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let deadline = self.0.deadline(grpc_timeout(request.metadata()));
        let scope = scope(&mut request);
        let held = hold(&self.0, Signal::Traces, request.get_ref())?;
        let accepted = self
            .0
            .export_traces(&scope, request.get_ref(), held, deadline)
            .await
            .map_err(status)?;
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: (accepted.rejected > 0).then(|| ExportTracePartialSuccess {
                rejected_spans: accepted.rejected,
                error_message: accepted.message().into(),
            }),
        }))
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use tonic::transport::Channel;

    use super::*;
    use crate::pipeline::testing::{PROD, pipeline};

    /// grpc-go's default maximum received message size, written out here
    /// rather than taken from the code under test.
    const COLLECTOR_LIMIT: usize = 4 * 1024 * 1024;

    async fn server() -> (Channel, Arc<Pipeline>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let pipeline = Arc::new(pipeline(None).unwrap());
        tokio::spawn(serve(
            listener,
            pipeline.clone(),
            std::future::pending(),
            Duration::from_secs(10),
        ));
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        (channel, pipeline)
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
                resource: Some(Resource {
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

    /// Rejected by the mapping on its own, so a request of only these
    /// succeeds without reaching ClickHouse.
    fn duplicate_record() -> LogRecord {
        LogRecord {
            attributes: vec![s("a", "1"), s("a", "2")],
            ..Default::default()
        }
    }

    fn body(bytes: usize) -> LogRecord {
        LogRecord {
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue("a".repeat(bytes))),
            }),
            ..duplicate_record()
        }
    }

    fn with_headers<T>(message: T, headers: &[(&'static str, &str)]) -> Request<T> {
        let mut request = Request::new(message);
        for (name, value) in headers {
            request.metadata_mut().append(*name, value.parse().unwrap());
        }
        request.set_timeout(Duration::from_millis(300));
        request
    }

    /// (name, headers, request, compression, expected code, expected
    /// rejected count)
    type Case = (
        &'static str,
        Vec<(&'static str, &'static str)>,
        ExportLogsServiceRequest,
        Option<CompressionEncoding>,
        Code,
        i64,
    );

    #[tokio::test]
    async fn logs_statuses() {
        let (channel, _) = server().await;
        let key = |k| ("x-toudai-key", k);
        let cases: Vec<Case> = vec![
            (
                "missing key",
                vec![],
                logs(vec![duplicate_record()], vec![]),
                None,
                Code::Unauthenticated,
                0,
            ),
            (
                "unknown key",
                vec![key("tk_test_not_a_real_key_0000")],
                logs(vec![duplicate_record()], vec![]),
                None,
                Code::Unauthenticated,
                0,
            ),
            (
                "dataset outside the key's list",
                vec![key(PROD), ("x-toudai-dataset", "staging")],
                logs(vec![duplicate_record()], vec![]),
                None,
                Code::PermissionDenied,
                0,
            ),
            (
                "reserved resource attribute",
                vec![key(PROD)],
                logs(vec![duplicate_record()], vec![s("toudai.other", "x")]),
                None,
                Code::InvalidArgument,
                0,
            ),
            (
                "every record rejected on its own",
                vec![key(PROD)],
                logs(vec![duplicate_record(), duplicate_record()], vec![]),
                None,
                Code::Ok,
                2,
            ),
            (
                "gzip",
                vec![key(PROD)],
                logs(vec![duplicate_record()], vec![]),
                Some(CompressionEncoding::Gzip),
                Code::Ok,
                1,
            ),
            (
                "a record to store and ClickHouse unreachable",
                vec![key(PROD)],
                logs(vec![LogRecord::default()], vec![]),
                None,
                Code::Unavailable,
                0,
            ),
            (
                "message just under the limit",
                vec![key(PROD)],
                logs(vec![body(COLLECTOR_LIMIT - 1024)], vec![]),
                None,
                Code::Ok,
                1,
            ),
            (
                "zstd",
                vec![key(PROD)],
                logs(vec![duplicate_record()], vec![]),
                Some(CompressionEncoding::Zstd),
                Code::Ok,
                1,
            ),
            (
                "full success leaves partial_success unset",
                vec![key(PROD)],
                ExportLogsServiceRequest::default(),
                None,
                Code::Ok,
                0,
            ),
            (
                "key checked before the message is decoded",
                vec![],
                logs(vec![body(COLLECTOR_LIMIT)], vec![]),
                None,
                Code::Unauthenticated,
                0,
            ),
            (
                "message over the limit",
                vec![key(PROD)],
                logs(vec![body(COLLECTOR_LIMIT)], vec![]),
                None,
                Code::ResourceExhausted,
                0,
            ),
            (
                "message over the limit only once decompressed",
                vec![key(PROD)],
                logs(vec![body(COLLECTOR_LIMIT)], vec![]),
                Some(CompressionEncoding::Gzip),
                Code::ResourceExhausted,
                0,
            ),
        ];
        for (name, headers, message, compression, code, rejected) in cases {
            let mut client = LogsServiceClient::new(channel.clone());
            if let Some(encoding) = compression {
                client = client.send_compressed(encoding);
            }
            match client.export(with_headers(message, &headers)).await {
                Ok(response) => {
                    assert_eq!(code, Code::Ok, "{name}");
                    // Set only when something was rejected, with a message.
                    let partial = response.into_inner().partial_success;
                    match partial {
                        None => assert_eq!(rejected, 0, "{name}"),
                        Some(p) => {
                            assert_eq!(p.rejected_log_records, rejected, "{name}");
                            assert_ne!(rejected, 0, "{name}");
                            assert!(!p.error_message.is_empty(), "{name}");
                        }
                    }
                }
                Err(status) => {
                    assert_eq!(status.code(), code, "{name}: {status:?}");
                    // Messages are fixed strings; none carries the key.
                    assert!(!status.message().contains(PROD), "{name}");
                }
            }
        }
    }

    #[tokio::test]
    async fn traces_are_authorized_and_answered() {
        let (channel, _) = server().await;
        let mut client = TraceServiceClient::new(channel);
        let duplicate = Span {
            attributes: vec![s("a", "1"), s("a", "2")],
            ..Default::default()
        };
        let message = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![duplicate],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let refused = client.export(with_headers(message.clone(), &[])).await;
        assert_eq!(refused.unwrap_err().code(), Code::Unauthenticated);
        let accepted = client
            .export(with_headers(message, &[("x-toudai-key", PROD)]))
            .await
            .unwrap();
        let partial = accepted.into_inner().partial_success.unwrap();
        assert_eq!(partial.rejected_spans, 1);
    }

    /// On shutdown a stream stalled mid-message holds exit only until the
    /// drain bound. The client opens a stream with a valid key and never sends
    /// its message, which tonic would otherwise wait on forever.
    #[tokio::test]
    async fn shutdown_waits_for_a_stalled_stream_only_until_the_drain_bound() {
        use tokio::io::AsyncWriteExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve(
            listener,
            Arc::new(pipeline(None).unwrap()),
            async {
                let _ = stopped.await;
            },
            Duration::from_secs(1),
        ));
        // HTTP/2 by hand: the preface, empty SETTINGS, then HEADERS without
        // END_STREAM, each header a literal without indexing or Huffman.
        let mut block = Vec::new();
        for (name, value) in [
            (":method", "POST"),
            (":scheme", "http"),
            (
                ":path",
                "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            ),
            (":authority", "x"),
            ("content-type", "application/grpc"),
            ("te", "trailers"),
            ("x-toudai-key", PROD),
        ] {
            block.push(0);
            block.push(name.len() as u8);
            block.extend_from_slice(name.as_bytes());
            block.push(value.len() as u8);
            block.extend_from_slice(value.as_bytes());
        }
        let frame = |kind: u8, flags: u8, stream: u32, payload: &[u8]| {
            let mut out = (payload.len() as u32).to_be_bytes()[1..].to_vec();
            out.extend_from_slice(&[kind, flags]);
            out.extend_from_slice(&stream.to_be_bytes());
            out.extend_from_slice(payload);
            out
        };
        let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
        wire.extend(frame(4, 0, 0, &[]));
        wire.extend(frame(1, 0x4, 1, &block));
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(&wire).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let started = std::time::Instant::now();
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("shutdown waited on the stalled stream past the drain bound")
            .unwrap()
            .unwrap();
        // Not before the bound either: the stalled stream was waited on.
        assert!(
            started.elapsed() >= Duration::from_millis(900),
            "{:?}",
            started.elapsed()
        );
        drop(stream);
    }

    /// With the 300 ms budget an unreachable ClickHouse is tried several
    /// times; a 150 ms `grpc-timeout` leaves room for one attempt only (the
    /// shortest backoff plus the writer's 100 ms minimum attempt overruns it).
    #[tokio::test]
    async fn producer_deadline_limits_retries() {
        let (channel, pipeline) = server().await;
        let mut client = LogsServiceClient::new(channel);
        let mut request = with_headers(
            logs(vec![LogRecord::default()], vec![]),
            &[("x-toudai-key", PROD)],
        );
        request.set_timeout(Duration::from_millis(150));
        let refused = client.export(request).await.unwrap_err();
        assert_eq!(refused.code(), Code::Unavailable);
        let attempts = pipeline.counters().get(
            "insert_failures",
            &[("signal", "logs"), ("destination", "canonical")],
        );
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn full_admission_budget_is_unavailable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let settings = crate::pipeline::Settings {
            memory_budget: 1 << 20,
            ..Default::default()
        };
        let pipeline = Arc::new(crate::pipeline::testing::with(settings).unwrap());
        // Another request holds all but 16 bytes.
        let mut other = Held::default();
        pipeline
            .hold(Signal::Logs, &mut other, (1 << 20) - 16)
            .unwrap();
        tokio::spawn(serve(
            listener,
            pipeline,
            std::future::pending(),
            Duration::from_secs(10),
        ));
        let mut client = LogsServiceClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        let message = logs(vec![duplicate_record(), duplicate_record()], vec![]);
        assert!(prost::Message::encoded_len(&message) > 16);
        let refused = client
            .export(with_headers(message, &[("x-toudai-key", PROD)]))
            .await
            .unwrap_err();
        // RESOURCE_EXHAUSTED would not be retried without RetryInfo.
        assert_eq!(refused.code(), Code::Unavailable);
    }

    #[test]
    fn parses_grpc_timeout() {
        let cases = [
            ("1H", Some(Duration::from_secs(3600))),
            ("2M", Some(Duration::from_secs(120))),
            ("10S", Some(Duration::from_secs(10))),
            ("250m", Some(Duration::from_millis(250))),
            ("7u", Some(Duration::from_micros(7))),
            ("99999999n", Some(Duration::from_nanos(99_999_999))),
            ("123456789m", None),
            ("m", None),
            ("", None),
            ("10s", None),
            ("-1S", None),
        ];
        for (value, expected) in cases {
            let mut metadata = MetadataMap::new();
            if let Ok(value) = value.parse() {
                metadata.insert("grpc-timeout", value);
            }
            assert_eq!(grpc_timeout(&metadata), expected, "{value:?}");
        }
    }
}
