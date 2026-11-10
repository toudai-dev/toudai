//! OTLP/HTTP: `POST /v1/logs` and `POST /v1/traces` with protobuf or JSON
//! bodies (Decision 0003 D1), following the Collector's OTLP receiver at the
//! version collector `f780574` pins: replies use the request's content type,
//! and errors are a `google.rpc.Status` in it (JSON when it is neither).
//! OTLP metrics are not served (404). `GET /healthz` answers while the
//! process runs, and `GET /metrics` serves the process counters as Prometheus
//! text, so both stay readable while ClickHouse is down.
//!
//! The write key and dataset are checked from the headers before the body is
//! read. A body is held to 20 MiB as sent and again once decompressed, so a
//! small compressed body cannot expand past the limit (Decision 0003 D3).
//! Connections are held to the Collector's read timeouts, so a client that
//! stalls cannot keep one, or a body buffer, open indefinitely.

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, Method, StatusCode};
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::Message;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpListener;

use crate::pipeline::{Accepted, Pipeline, Signal};
use crate::{Refusal, StatusKind};

/// The largest body accepted, as sent and once decompressed: the
/// Collector's `confighttp` default.
pub const MAX_BODY_BYTES: usize = 20 * 1024 * 1024;

// Refusals of the body itself. As in the Collector, each is a 400.
const BODY_TOO_LARGE: Refusal = Refusal {
    kind: StatusKind::InvalidArgument,
    message: "request body is larger than 20 MiB, or was not received in full in time",
};
const UNSUPPORTED_ENCODING: Refusal = Refusal {
    kind: StatusKind::InvalidArgument,
    message: "unsupported Content-Encoding",
};
const BAD_COMPRESSION: Refusal = Refusal {
    kind: StatusKind::InvalidArgument,
    message: "request body could not be decompressed",
};
const MALFORMED: Refusal = Refusal {
    kind: StatusKind::InvalidArgument,
    message: "request body is not a valid OTLP export request",
};

/// How long a client may take to send its request. The headers get the
/// minute `confighttp` allowed (its `ReadHeaderTimeout`). `confighttp` set no
/// body timeout, but its 30 s `WriteTimeout` started once the headers were
/// read, so this minute for the body is the more lenient of the two. The
/// request's own budget starts once the body is in.
#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    /// For each request's headers. hyper also closes a kept-alive connection
    /// that sends no next request within it.
    pub header_read: Duration,
    /// For the whole body, after the headers.
    pub body_read: Duration,
    /// How long shutdown waits for open connections. Requests in flight
    /// finish within their own budget; without a bound, a client stalled in
    /// its headers would hold shutdown for the whole header timeout, past
    /// Kubernetes' default 30 s grace period.
    pub drain: Duration,
}

impl Default for Timeouts {
    fn default() -> Timeouts {
        Timeouts {
            header_read: Duration::from_secs(60),
            body_read: Duration::from_secs(60),
            drain: Duration::from_secs(10),
        }
    }
}

#[derive(Clone)]
struct Shared {
    pipeline: Arc<Pipeline>,
    body_read: Duration,
}

pub fn router(pipeline: Arc<Pipeline>, timeouts: Timeouts) -> Router {
    Router::new()
        .route("/v1/logs", any(logs))
        .route("/v1/traces", any(traces))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(Shared {
            pipeline,
            body_read: timeouts.body_read,
        })
}

/// Serve [`router`] on `listener` until `shutdown` completes, then wait for
/// requests in flight, up to [`Timeouts::drain`]. `axum::serve` is not used
/// because it gives hyper no timer, which turns hyper's header read timeout
/// off.
pub async fn serve(
    listener: TcpListener,
    pipeline: Arc<Pipeline>,
    timeouts: Timeouts,
    shutdown: impl Future<Output = ()>,
) {
    let service = TowerToHyperService::new(router(pipeline, timeouts));
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(timeouts.header_read);
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(error) => {
                    accept_failed(error).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        let connection = builder.serve_connection(TokioIo::new(stream), service.clone());
        let connection = graceful.watch(connection);
        tokio::spawn(async move {
            // A client that resets or times out is not the service's error.
            let _ = connection.await;
        });
    }
    drop(listener);
    let _ = tokio::time::timeout(timeouts.drain, graceful.shutdown()).await;
}

/// A connection the client gave up on before it was accepted is routine.
/// Anything else, such as running out of file descriptors, would fail again
/// at once, so wait before accepting again, as `axum::serve` does.
async fn accept_failed(error: std::io::Error) {
    use std::io::ErrorKind::{ConnectionAborted, ConnectionRefused, ConnectionReset};
    if !matches!(
        error.kind(),
        ConnectionAborted | ConnectionRefused | ConnectionReset
    ) {
        eprintln!("toudai-ingest: OTLP/HTTP accept failed: {error}");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn healthz() -> Response {
    plain(StatusCode::OK, "ok\n")
}

async fn metrics(State(shared): State<Shared>) -> Response {
    let body = shared.pipeline.counters().render();
    let content_type = "text/plain; version=0.0.4; charset=utf-8";
    (StatusCode::OK, [(CONTENT_TYPE, content_type)], body).into_response()
}

async fn logs(
    State(shared): State<Shared>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let signal = Signal::Logs;
    let pipeline = &shared.pipeline;
    let (scope, format, request) =
        match receive::<ExportLogsServiceRequest>(&shared, signal, &method, &headers, body).await {
            Ok(received) => received,
            Err(response) => return *response,
        };
    let deadline = pipeline.deadline(None);
    match pipeline.export_logs(&scope, &request, deadline).await {
        Ok(accepted) => format.accepted(accepted, |a| ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: a.rejected,
                error_message: a.message().into(),
            }),
        }),
        Err(refusal) => format.refusal(refusal),
    }
}

async fn traces(
    State(shared): State<Shared>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let signal = Signal::Traces;
    let pipeline = &shared.pipeline;
    let (scope, format, request) = match receive::<ExportTraceServiceRequest>(
        &shared, signal, &method, &headers, body,
    )
    .await
    {
        Ok(received) => received,
        Err(response) => return *response,
    };
    let deadline = pipeline.deadline(None);
    match pipeline.export_traces(&scope, &request, deadline).await {
        Ok(accepted) => format.accepted(accepted, |a| ExportTraceServiceResponse {
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans: a.rejected,
                error_message: a.message().into(),
            }),
        }),
        Err(refusal) => format.refusal(refusal),
    }
}

/// Authorize, check the method and content type, then read, decompress, and
/// decode the body, in that order. A refusal is the finished response, boxed
/// because a `Response` is large and the happy path should not carry it.
async fn receive<M>(
    shared: &Shared,
    signal: Signal,
    method: &Method,
    headers: &HeaderMap,
    body: Body,
) -> Result<(crate::auth::Scope, Format, M), Box<Response>>
where
    M: Message + Default + DeserializeOwned,
{
    let pipeline = &shared.pipeline;
    let refuse =
        |format: Format, refusal| Box::new(format.refusal(pipeline.refuse(signal, refusal)));
    let error_format = Format::of(headers).unwrap_or(Format::Json);
    let scope = pipeline
        .authorize(signal, headers)
        .map_err(|refusal| Box::new(error_format.refusal(refusal)))?;
    if method != Method::POST {
        let text = "405 method not allowed, supported: [POST]";
        return Err(Box::new(plain(StatusCode::METHOD_NOT_ALLOWED, text)));
    }
    let Some(format) = Format::of(headers) else {
        let text =
            "415 unsupported media type, supported: [application/json, application/x-protobuf]";
        return Err(Box::new(plain(StatusCode::UNSUPPORTED_MEDIA_TYPE, text)));
    };
    let body = tokio::time::timeout(shared.body_read, axum::body::to_bytes(body, MAX_BODY_BYTES))
        .await
        .ok()
        .and_then(Result::ok)
        .ok_or_else(|| refuse(format, BODY_TOO_LARGE))?;
    let body = decompress(headers, body).map_err(|refusal| refuse(format, refusal))?;
    let message = format
        .decode(&body)
        .ok_or_else(|| refuse(format, MALFORMED))?;
    Ok((scope, format, message))
}

/// The body as sent, decompressed per its `Content-Encoding`. Only the first
/// header value counts, and names match exactly, as in `confighttp`: no
/// encoding, `gzip`, `zstd`, `zlib` and its alias `deflate`, `snappy`, `lz4`,
/// and `x-snappy-framed`.
fn decompress(headers: &HeaderMap, body: Bytes) -> Result<Bytes, Refusal> {
    let encoding = match headers.get(CONTENT_ENCODING) {
        None => "",
        Some(value) => value.to_str().map_err(|_| UNSUPPORTED_ENCODING)?,
    };
    let input = &body[..];
    match encoding {
        "" => Ok(body),
        "gzip" => read_limited(flate2::read::MultiGzDecoder::new(input)),
        "zlib" | "deflate" => read_limited(flate2::read::ZlibDecoder::new(input)),
        "zstd" => {
            read_limited(zstd::stream::read::Decoder::new(input).map_err(|_| BAD_COMPRESSION)?)
        }
        "lz4" => read_limited(lz4_flex::frame::FrameDecoder::new(input)),
        "x-snappy-framed" => read_limited(snap::read::FrameDecoder::new(input)),
        // The Collector's encoders once sent the framed format under this
        // name, so `confighttp` tells the two apart by the stream header; the
        // block format's decoded length is checked before it is allocated.
        "snappy" if input.starts_with(SNAPPY_FRAMED_HEADER) => {
            read_limited(snap::read::FrameDecoder::new(input))
        }
        "snappy" => {
            let length = snap::raw::decompress_len(input).map_err(|_| BAD_COMPRESSION)?;
            if length > MAX_BODY_BYTES {
                return Err(BODY_TOO_LARGE);
            }
            snap::raw::Decoder::new()
                .decompress_vec(input)
                .map(Bytes::from)
                .map_err(|_| BAD_COMPRESSION)
        }
        _ => Err(UNSUPPORTED_ENCODING),
    }
}

const SNAPPY_FRAMED_HEADER: &[u8] = b"\xff\x06\x00\x00sNaPpY";

/// Decompress up to the body limit, refusing a body that expands past it.
fn read_limited(reader: impl Read) -> Result<Bytes, Refusal> {
    let mut out = Vec::new();
    reader
        .take(MAX_BODY_BYTES as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|_| BAD_COMPRESSION)?;
    if out.len() > MAX_BODY_BYTES {
        return Err(BODY_TOO_LARGE);
    }
    Ok(out.into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Protobuf,
    Json,
}

/// `google.rpc.Status` without `details`, which Toudai never sets.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

impl Format {
    /// The format named by `Content-Type`, ignoring parameters and case.
    fn of(headers: &HeaderMap) -> Option<Format> {
        let value = headers.get(CONTENT_TYPE)?.to_str().ok()?;
        let media_type = value.split(';').next()?.trim();
        if media_type.eq_ignore_ascii_case("application/x-protobuf") {
            Some(Format::Protobuf)
        } else if media_type.eq_ignore_ascii_case("application/json") {
            Some(Format::Json)
        } else {
            None
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Format::Protobuf => "application/x-protobuf",
            Format::Json => "application/json",
        }
    }

    fn decode<M: Message + Default + DeserializeOwned>(self, body: &[u8]) -> Option<M> {
        match self {
            Format::Protobuf => M::decode(body).ok(),
            Format::Json => serde_json::from_slice(body).ok(),
        }
    }

    fn reply(self, status: StatusCode, body: Vec<u8>) -> Response {
        (status, [(CONTENT_TYPE, self.content_type())], body).into_response()
    }

    /// 200 with an empty response for a full success, which is `{}` in JSON
    /// as protojson writes it, else with `partial_success` filled in.
    fn accepted<R>(self, accepted: Accepted, response: impl FnOnce(Accepted) -> R) -> Response
    where
        R: Message + Serialize,
    {
        let body = match (accepted.rejected, self) {
            (0, Format::Protobuf) => Vec::new(),
            (0, Format::Json) => b"{}".to_vec(),
            (_, Format::Protobuf) => response(accepted).encode_to_vec(),
            (_, Format::Json) => serde_json::to_vec(&response(accepted))
                .expect("an OTLP response serializes to JSON"),
        };
        self.reply(StatusCode::OK, body)
    }

    fn refusal(self, refusal: Refusal) -> Response {
        let code = refusal.kind.rpc_code();
        let body = match self {
            Format::Protobuf => RpcStatus {
                code,
                message: refusal.message.into(),
            }
            .encode_to_vec(),
            Format::Json => serde_json::to_vec(&serde_json::json!({
                "code": code,
                "message": refusal.message,
            }))
            .expect("a status serializes to JSON"),
        };
        self.reply(refusal.kind.http_status(), body)
    }
}

fn plain(status: StatusCode, text: &'static str) -> Response {
    (status, [(CONTENT_TYPE, "text/plain")], text).into_response()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;
    use crate::pipeline::testing::{PROD, pipeline};

    /// confighttp's default `max_request_body_size`, written out here rather
    /// than taken from the code under test.
    const COLLECTOR_LIMIT: usize = 20 * 1024 * 1024;
    const JSON: &str = "application/json";
    const PROTOBUF: &str = "application/x-protobuf";

    /// Two records, each rejected by the mapping on its own (a repeated
    /// attribute key), so the request succeeds without reaching ClickHouse
    /// and reports both in `partial_success`.
    fn rejected_logs_json() -> Vec<u8> {
        let record = json!({
            "timeUnixNano": "1788246000123456789",
            "attributes": [
                {"key": "a", "value": {"stringValue": "1"}},
                {"key": "a", "value": {"intValue": "2"}},
            ],
        });
        serde_json::to_vec(&json!({
            "resourceLogs": [{"scopeLogs": [{"logRecords": [record.clone(), record]}]}]
        }))
        .unwrap()
    }

    fn rejected_logs_protobuf() -> Vec<u8> {
        serde_json::from_slice::<ExportLogsServiceRequest>(&rejected_logs_json())
            .unwrap()
            .encode_to_vec()
    }

    /// One record the mapping keeps, so the request reaches the (unreachable)
    /// ClickHouse.
    fn storable_logs_protobuf() -> Vec<u8> {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec()
    }

    fn gzip(body: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), Default::default());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }

    fn zlib(body: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), Default::default());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }

    fn snappy_framed(body: &[u8]) -> Vec<u8> {
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        encoder.write_all(body).unwrap();
        encoder.into_inner().unwrap()
    }

    fn lz4(body: &[u8]) -> Vec<u8> {
        let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }

    struct Reply {
        status: StatusCode,
        content_type: String,
        body: Bytes,
    }

    async fn send(method: Method, path: &str, headers: &[(&str, &str)], body: Vec<u8>) -> Reply {
        let mut request = http::Request::builder().method(method).uri(path);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let router = router(Arc::new(pipeline(None).unwrap()), Timeouts::default());
        let response = router
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .map_or(String::new(), |v| v.to_str().unwrap().to_owned());
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        Reply {
            status,
            content_type,
            body,
        }
    }

    /// The `google.rpc.Code` of an error body, read in its own format.
    fn rpc_code(reply: &Reply) -> i32 {
        match reply.content_type.as_str() {
            PROTOBUF => RpcStatus::decode(&reply.body[..]).unwrap().code,
            JSON => serde_json::from_slice::<Value>(&reply.body).unwrap()["code"]
                .as_i64()
                .unwrap() as i32,
            other => panic!("error body in {other:?}"),
        }
    }

    /// (name, method, headers, body, status, reply content type, rpc code or
    /// rejected count)
    type Case = (
        &'static str,
        Method,
        Vec<(&'static str, &'static str)>,
        Vec<u8>,
        StatusCode,
        &'static str,
        i64,
    );

    #[tokio::test]
    async fn logs_requests() {
        let key = ("x-toudai-key", PROD);
        let json = (CONTENT_TYPE.as_str(), JSON);
        let protobuf = (CONTENT_TYPE.as_str(), PROTOBUF);
        let encoding = |e| (CONTENT_ENCODING.as_str(), e);
        let pb = rejected_logs_protobuf;
        let ok = StatusCode::OK;
        let bad = StatusCode::BAD_REQUEST;
        // A valid request that would succeed (its record is rejected on its
        // own), so only the size limit can refuse it.
        let mut big =
            serde_json::from_slice::<ExportLogsServiceRequest>(&rejected_logs_json()).unwrap();
        big.resource_logs[0].scope_logs[0].log_records[0].body = Some(AnyValue {
            value: Some(any_value::Value::StringValue("a".repeat(COLLECTOR_LIMIT))),
        });
        let over_limit = big.encode_to_vec();
        big.resource_logs[0].scope_logs[0].log_records[0].body = Some(AnyValue {
            value: Some(any_value::Value::StringValue(
                "a".repeat(COLLECTOR_LIMIT - 1024),
            )),
        });
        let under_limit = big.encode_to_vec();
        let mut invalid_utf8 = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("MARK".into())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec();
        let at = invalid_utf8.windows(4).position(|w| w == b"MARK").unwrap();
        invalid_utf8[at] = 0xff;
        let cases: Vec<Case> = vec![
            (
                "json",
                Method::POST,
                vec![key, json],
                rejected_logs_json(),
                ok,
                JSON,
                2,
            ),
            (
                "protobuf",
                Method::POST,
                vec![key, protobuf],
                pb(),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "content type parameters and case",
                Method::POST,
                vec![
                    key,
                    (CONTENT_TYPE.as_str(), "Application/JSON; charset=utf-8"),
                ],
                rejected_logs_json(),
                ok,
                JSON,
                2,
            ),
            (
                "gzip",
                Method::POST,
                vec![key, protobuf, encoding("gzip")],
                gzip(&pb()),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "zlib",
                Method::POST,
                vec![key, protobuf, encoding("zlib")],
                zlib(&pb()),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "deflate",
                Method::POST,
                vec![key, protobuf, encoding("deflate")],
                zlib(&pb()),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "zstd",
                Method::POST,
                vec![key, protobuf, encoding("zstd")],
                zstd::encode_all(&pb()[..], 0).unwrap(),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "snappy block",
                Method::POST,
                vec![key, protobuf, encoding("snappy")],
                snap::raw::Encoder::new().compress_vec(&pb()).unwrap(),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "snappy framed",
                Method::POST,
                vec![key, protobuf, encoding("snappy")],
                snappy_framed(&pb()),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "x-snappy-framed",
                Method::POST,
                vec![key, protobuf, encoding("x-snappy-framed")],
                snappy_framed(&pb()),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "lz4",
                Method::POST,
                vec![key, protobuf, encoding("lz4")],
                lz4(&pb()),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "missing key, json error",
                Method::POST,
                vec![json],
                rejected_logs_json(),
                StatusCode::UNAUTHORIZED,
                JSON,
                16,
            ),
            (
                "missing key, protobuf error",
                Method::POST,
                vec![protobuf],
                pb(),
                StatusCode::UNAUTHORIZED,
                PROTOBUF,
                16,
            ),
            (
                "missing key, unknown content type",
                Method::POST,
                vec![],
                pb(),
                StatusCode::UNAUTHORIZED,
                JSON,
                16,
            ),
            (
                "dataset outside the key's list",
                Method::POST,
                vec![key, json, ("x-toudai-dataset", "staging")],
                rejected_logs_json(),
                StatusCode::FORBIDDEN,
                JSON,
                7,
            ),
            (
                "a record to store and ClickHouse unreachable",
                Method::POST,
                vec![key, protobuf],
                storable_logs_protobuf(),
                StatusCode::SERVICE_UNAVAILABLE,
                PROTOBUF,
                14,
            ),
            (
                "not POST",
                Method::GET,
                vec![key, json],
                vec![],
                StatusCode::METHOD_NOT_ALLOWED,
                "text/plain",
                0,
            ),
            (
                "unsupported content type",
                Method::POST,
                vec![key, (CONTENT_TYPE.as_str(), "text/plain")],
                rejected_logs_json(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "text/plain",
                0,
            ),
            (
                "unsupported encoding",
                Method::POST,
                vec![key, protobuf, encoding("br")],
                pb(),
                bad,
                PROTOBUF,
                3,
            ),
            (
                "encoding names match exactly",
                Method::POST,
                vec![key, protobuf, encoding("GZIP")],
                gzip(&pb()),
                bad,
                PROTOBUF,
                3,
            ),
            (
                "corrupt gzip",
                Method::POST,
                vec![key, protobuf, encoding("gzip")],
                pb(),
                bad,
                PROTOBUF,
                3,
            ),
            (
                "body just under the limit",
                Method::POST,
                vec![key, protobuf],
                under_limit.clone(),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "body just under the limit once gunzipped",
                Method::POST,
                vec![key, protobuf, encoding("gzip")],
                gzip(&under_limit),
                ok,
                PROTOBUF,
                2,
            ),
            (
                "key checked before the method",
                Method::GET,
                vec![json],
                vec![],
                StatusCode::UNAUTHORIZED,
                JSON,
                16,
            ),
            (
                "body over the limit",
                Method::POST,
                vec![key, protobuf],
                over_limit.clone(),
                bad,
                PROTOBUF,
                3,
            ),
            (
                "body over the limit once gunzipped",
                Method::POST,
                vec![key, protobuf, encoding("gzip")],
                gzip(&over_limit),
                bad,
                PROTOBUF,
                3,
            ),
            (
                "snappy block over the limit once decompressed",
                Method::POST,
                vec![key, protobuf, encoding("snappy")],
                snap::raw::Encoder::new().compress_vec(&over_limit).unwrap(),
                bad,
                PROTOBUF,
                3,
            ),
            (
                "not protobuf",
                Method::POST,
                vec![key, protobuf],
                b"\xff\xff\xff".to_vec(),
                bad,
                PROTOBUF,
                3,
            ),
            (
                "not JSON",
                Method::POST,
                vec![key, json],
                b"{".to_vec(),
                bad,
                JSON,
                3,
            ),
            (
                "invalid UTF-8 in a string",
                Method::POST,
                vec![key, protobuf],
                invalid_utf8,
                bad,
                PROTOBUF,
                3,
            ),
        ];
        for (name, method, headers, body, status, content_type, expected) in cases {
            let reply = send(method, "/v1/logs", &headers, body).await;
            assert_eq!(reply.status, status, "{name}");
            assert_eq!(reply.content_type, content_type, "{name}");
            if status == StatusCode::OK {
                let rejected =
                    match content_type {
                        PROTOBUF => {
                            ExportLogsServiceResponse::decode(&reply.body[..])
                                .unwrap()
                                .partial_success
                                .unwrap()
                                .rejected_log_records
                        }
                        _ => {
                            serde_json::from_slice::<Value>(&reply.body).unwrap()["partialSuccess"]
                                ["rejectedLogRecords"]
                                .as_i64()
                                .unwrap()
                        }
                    };
                assert_eq!(rejected, expected, "{name}");
            } else if content_type != "text/plain" {
                assert_eq!(rpc_code(&reply) as i64, expected, "{name}");
            }
            // Errors are fixed strings; none echoes the key.
            assert!(
                !String::from_utf8_lossy(&reply.body).contains(PROD),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn traces_and_unserved_paths() {
        let key = ("x-toudai-key", PROD);
        let json = (CONTENT_TYPE.as_str(), JSON);
        let span = |trace_id: &str| {
            json!({
                "traceId": trace_id,
                "spanId": "0102030405060708",
                "name": "s",
                "attributes": [
                    {"key": "a", "value": {"boolValue": true}},
                    {"key": "a", "value": {"boolValue": false}},
                ],
            })
        };
        let body = |span| {
            serde_json::to_vec(&json!({"resourceSpans": [{"scopeSpans": [{"spans": [span]}]}]}))
                .unwrap()
        };
        let reply = send(
            Method::POST,
            "/v1/traces",
            &[key, json],
            body(span("0102030405060708090a0b0c0d0e0f10")),
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK);
        let response: Value = serde_json::from_slice(&reply.body).unwrap();
        assert_eq!(response["partialSuccess"]["rejectedSpans"], 1);

        // OTLP/JSON trace IDs are hex; anything else is not a valid request.
        let reply = send(
            Method::POST,
            "/v1/traces",
            &[key, json],
            body(span("AQIDBA==")),
        )
        .await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST);

        let reply = send(Method::POST, "/v1/metrics", &[key, json], b"{}".to_vec()).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
    }

    /// A real listener with short timeouts, so a stalled client is seen to be
    /// cut off.
    async fn served(timeouts: Timeouts) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let pipeline = Arc::new(pipeline(None).unwrap());
        tokio::spawn(serve(listener, pipeline, timeouts, std::future::pending()));
        address
    }

    /// Everything the server sends until it closes the connection, which must
    /// happen well within `within`.
    async fn until_closed(stream: &mut tokio::net::TcpStream, within: Duration) -> String {
        use tokio::io::AsyncReadExt;
        let mut out = Vec::new();
        tokio::time::timeout(within, stream.read_to_end(&mut out))
            .await
            .expect("the server kept the connection open")
            .ok();
        String::from_utf8_lossy(&out).into_owned()
    }

    #[tokio::test]
    async fn stalled_clients_are_cut_off() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpStream;
        let short = Duration::from_millis(300);
        let address = served(Timeouts {
            header_read: short,
            body_read: short,
            ..Timeouts::default()
        })
        .await;
        let within = Duration::from_secs(5);

        // Headers never finished.
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"POST /v1/logs HTTP/1.1\r\nHost: x\r\n")
            .await
            .unwrap();
        until_closed(&mut stream, within).await;

        // A kept-alive connection that sends no next request.
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /v1/metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let reply = until_closed(&mut stream, within).await;
        assert!(reply.starts_with("HTTP/1.1 404"), "{reply}");

        // A body that stops arriving: refused once the body timeout passes,
        // with the request's own refusal, not left waiting.
        let mut stream = TcpStream::connect(address).await.unwrap();
        let head = format!(
            "POST /v1/logs HTTP/1.1\r\nHost: x\r\nX-Toudai-Key: {PROD}\r\n\
             Content-Type: application/json\r\nContent-Length: 100\r\n\r\n{{"
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        let reply = until_closed(&mut stream, within).await;
        assert!(reply.starts_with("HTTP/1.1 400"), "{reply}");
        assert!(reply.contains(BODY_TOO_LARGE.message), "{reply}");
    }

    /// On shutdown a request already in flight is still answered, and a
    /// client stalled in its headers holds shutdown only until the drain
    /// bound, not for its whole header timeout.
    #[tokio::test]
    async fn shutdown_answers_requests_in_flight_then_stops() {
        use tokio::io::AsyncWriteExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // ClickHouse is unreachable, so a storable request takes the whole
        // one-second budget before it is answered 503.
        let pipeline =
            Arc::new(crate::pipeline::testing::with_budget(None, Duration::from_secs(1)).unwrap());
        let timeouts = Timeouts {
            drain: Duration::from_secs(2),
            ..Timeouts::default()
        };
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve(listener, pipeline, timeouts, async {
            let _ = stopped.await;
        }));

        let mut stalled = tokio::net::TcpStream::connect(address).await.unwrap();
        stalled
            .write_all(b"POST /v1/logs HTTP/1.1\r\nHost: x\r\n")
            .await
            .unwrap();
        let in_flight = tokio::spawn(
            reqwest::Client::new()
                .post(format!("http://{address}/v1/logs"))
                .header("x-toudai-key", PROD)
                .header(CONTENT_TYPE, PROTOBUF)
                .body(storable_logs_protobuf())
                .send(),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = std::time::Instant::now();
        stop.send(()).unwrap();

        let reply = in_flight
            .await
            .unwrap()
            .expect("the request in flight was cut off");
        assert_eq!(reply.status(), StatusCode::SERVICE_UNAVAILABLE);
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("shutdown waited on the stalled client past the drain bound")
            .unwrap();
        // Not before the drain bound either: the stalled client was waited on.
        assert!(
            started.elapsed() >= Duration::from_millis(1500),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn health_and_counters_need_no_key() {
        let health = send(Method::GET, "/healthz", &[], vec![]).await;
        assert_eq!(health.status, StatusCode::OK);
        assert_eq!(&health.body[..], b"ok\n");

        // One refusal and one success, then the counters through the same
        // router: `send` builds a fresh pipeline per request, so this one
        // shares a router instead.
        let router = router(Arc::new(pipeline(None).unwrap()), Timeouts::default());
        let call = |method: Method, path: &str, headers: &[(&str, &str)], body: Vec<u8>| {
            let mut request = http::Request::builder().method(method).uri(path);
            for (name, value) in headers {
                request = request.header(*name, *value);
            }
            router
                .clone()
                .oneshot(request.body(Body::from(body)).unwrap())
        };
        call(
            Method::POST,
            "/v1/logs",
            &[(CONTENT_TYPE.as_str(), JSON)],
            b"{}".to_vec(),
        )
        .await
        .unwrap();
        let headers = [("x-toudai-key", PROD), (CONTENT_TYPE.as_str(), JSON)];
        call(Method::POST, "/v1/logs", &headers, b"{}".to_vec())
            .await
            .unwrap();
        let response = call(Method::GET, "/metrics", &[], vec![]).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let text = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(text.to_vec()).unwrap();
        for line in [
            "# TYPE toudai_ingest_requests_total counter",
            "toudai_ingest_requests_total{signal=\"logs\",outcome=\"accepted\"} 1",
            "toudai_ingest_requests_total{signal=\"logs\",outcome=\"unauthenticated\"} 1",
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "missing {line:?} in\n{text}"
            );
        }
        assert!(!text.contains(PROD));
    }

    #[tokio::test]
    async fn full_success_is_an_empty_response() {
        // A request with no records stores nothing and rejects nothing.
        for (content_type, expected) in [(JSON, &b"{}"[..]), (PROTOBUF, &b""[..])] {
            let body = match content_type {
                JSON => b"{}".to_vec(),
                _ => Vec::new(),
            };
            let headers = [
                ("x-toudai-key", PROD),
                (CONTENT_TYPE.as_str(), content_type),
            ];
            let reply = send(Method::POST, "/v1/logs", &headers, body).await;
            assert_eq!(reply.status, StatusCode::OK, "{content_type}");
            assert_eq!(&reply.body[..], expected, "{content_type}");
        }
    }
}
