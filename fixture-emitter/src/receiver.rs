//! A loopback OTLP receiver over gRPC and HTTP/protobuf that keeps every
//! request it accepts, so a run can be checked against what actually went on
//! the wire. It is a test sink, not a Collector: it acknowledges everything.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost::Message;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct Received {
    pub logs: Vec<ExportLogsServiceRequest>,
    pub traces: Vec<ExportTraceServiceRequest>,
    pub metrics: Vec<ExportMetricsServiceRequest>,
}

pub type Shared = Arc<Mutex<Received>>;

/// Signals the receiver refuses (gRPC `INVALID_ARGUMENT`, HTTP 400), so a test
/// can check that a rejected export is not reported as delivered.
#[derive(Clone, Copy, Default)]
pub struct Rejects {
    pub logs: bool,
    pub traces: bool,
    pub metrics: bool,
}

#[derive(Clone)]
struct Sink(Shared, Rejects);

fn refuse(signal: &str) -> Status {
    Status::invalid_argument(format!("{signal} rejected by the test receiver"))
}

#[tonic::async_trait]
impl LogsService for Sink {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        if self.1.logs {
            return Err(refuse("logs"));
        }
        self.0.lock().unwrap().logs.push(request.into_inner());
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl TraceService for Sink {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        if self.1.traces {
            return Err(refuse("traces"));
        }
        self.0.lock().unwrap().traces.push(request.into_inner());
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for Sink {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        if self.1.metrics {
            return Err(refuse("metrics"));
        }
        self.0.lock().unwrap().metrics.push(request.into_inner());
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

pub struct Receiver {
    pub grpc: SocketAddr,
    pub http: SocketAddr,
    pub shared: Shared,
    stop_grpc: Option<oneshot::Sender<()>>,
    stop_http: Arc<AtomicBool>,
}

impl Receiver {
    /// Bind both listeners on ephemeral loopback ports. The gRPC server runs
    /// on `handle`'s runtime; the HTTP listener runs on its own threads.
    pub fn start(handle: &tokio::runtime::Handle) -> Result<Receiver, String> {
        Self::start_rejecting(handle, Rejects::default())
    }

    pub fn start_rejecting(
        handle: &tokio::runtime::Handle,
        rejects: Rejects,
    ) -> Result<Receiver, String> {
        let shared: Shared = Arc::default();
        let sink = Sink(shared.clone(), rejects);

        let _enter = handle.enter();
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).map_err(|e| e.to_string())?;
        let grpc = incoming.local_addr().map_err(|e| e.to_string())?;
        let (stop_grpc, stopped) = oneshot::channel::<()>();
        handle.spawn(async move {
            let result = Server::builder()
                .add_service(LogsServiceServer::new(sink.clone()))
                .add_service(TraceServiceServer::new(sink.clone()))
                .add_service(MetricsServiceServer::new(sink))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = stopped.await;
                })
                .await;
            if let Err(e) = result {
                eprintln!("gRPC receiver stopped: {e}");
            }
        });

        let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        let http = listener.local_addr().map_err(|e| e.to_string())?;
        let stop_http = Arc::new(AtomicBool::new(false));
        let (flag, store) = (stop_http.clone(), shared.clone());
        thread::spawn(move || {
            for stream in listener.incoming() {
                if flag.load(Ordering::SeqCst) {
                    break;
                }
                if let Ok(stream) = stream {
                    let store = store.clone();
                    thread::spawn(move || {
                        if let Err(e) = serve_http(stream, &store, rejects) {
                            eprintln!("HTTP receiver connection: {e}");
                        }
                    });
                }
            }
        });

        Ok(Receiver {
            grpc,
            http,
            shared,
            stop_grpc: Some(stop_grpc),
            stop_http,
        })
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        if let Some(stop) = self.stop_grpc.take() {
            let _ = stop.send(());
        }
        self.stop_http.store(true, Ordering::SeqCst);
        // Wake the blocking accept so the listener thread can exit.
        let _ = TcpStream::connect(self.http);
    }
}

/// Serve OTLP/HTTP on one connection (keep-alive), accepting protobuf bodies
/// with a Content-Length or chunked framing.
fn serve_http(stream: TcpStream, store: &Shared, rejects: Rejects) -> Result<(), String> {
    let mut writer = stream.try_clone().map_err(|e| e.to_string())?;
    let mut reader = BufReader::new(stream);
    loop {
        let mut request_line = String::new();
        if reader
            .read_line(&mut request_line)
            .map_err(|e| e.to_string())?
            == 0
        {
            return Ok(());
        }
        let mut parts = request_line.split_whitespace();
        let (method, path) = (
            parts.next().unwrap_or_default().to_string(),
            parts.next().unwrap_or_default().to_string(),
        );
        let (mut length, mut chunked) = (0usize, false);
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).map_err(|e| e.to_string())?;
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                let (name, value) = (name.trim().to_ascii_lowercase(), value.trim());
                match name.as_str() {
                    "content-length" => length = value.parse().map_err(|_| "bad length")?,
                    "transfer-encoding" => chunked = value.eq_ignore_ascii_case("chunked"),
                    _ => {}
                }
            }
        }
        let body = if chunked {
            read_chunked(&mut reader)?
        } else {
            let mut body = vec![0u8; length];
            reader.read_exact(&mut body).map_err(|e| e.to_string())?;
            body
        };
        let status = if method == "POST" {
            ingest(&path, &body, store, rejects)
        } else {
            405
        };
        let response = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\n\r\n"
        );
        writer
            .write_all(response.as_bytes())
            .map_err(|e| e.to_string())?;
    }
}

fn read_chunked(reader: &mut BufReader<TcpStream>) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    loop {
        let mut size_line = String::new();
        reader
            .read_line(&mut size_line)
            .map_err(|e| e.to_string())?;
        let size = usize::from_str_radix(size_line.trim().split(';').next().unwrap_or("0"), 16)
            .map_err(|_| "bad chunk size")?;
        let mut chunk = vec![0u8; size];
        reader.read_exact(&mut chunk).map_err(|e| e.to_string())?;
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).map_err(|e| e.to_string())?;
        if size == 0 {
            return Ok(body);
        }
        body.extend_from_slice(&chunk);
    }
}

fn ingest(path: &str, body: &[u8], store: &Shared, rejects: Rejects) -> u16 {
    let refused = match path {
        "/v1/logs" => rejects.logs,
        "/v1/traces" => rejects.traces,
        "/v1/metrics" => rejects.metrics,
        _ => false,
    };
    if refused {
        return 400;
    }
    let mut received = store.lock().unwrap();
    match path {
        "/v1/logs" => match ExportLogsServiceRequest::decode(body) {
            Ok(r) => received.logs.push(r),
            Err(_) => return 400,
        },
        "/v1/traces" => match ExportTraceServiceRequest::decode(body) {
            Ok(r) => received.traces.push(r),
            Err(_) => return 400,
        },
        "/v1/metrics" => match ExportMetricsServiceRequest::decode(body) {
            Ok(r) => received.metrics.push(r),
            Err(_) => return 400,
        },
        _ => return 404,
    }
    200
}
