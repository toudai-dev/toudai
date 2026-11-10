//! The Toudai OTLP ingest service (RFD 0001 Decision 0003).
//!
//! Configuration is read from the environment, as for the query service:
//!
//! * `TOUDAI_WRITE_KEYS_FILE` (required): the write keys and their datasets,
//!   one `<key> [<dataset>...]` per line, the query service's key-file format.
//!   A placeholder for the key store, as read keys are.
//! * `TOUDAI_OTLP_GRPC_LISTEN` and `TOUDAI_OTLP_HTTP_LISTEN`: addresses for
//!   OTLP/gRPC and OTLP/HTTP (default `0.0.0.0:4317` and `0.0.0.0:4318`, the
//!   OTLP ports the Collector build listened on).
//! * `TOUDAI_CLICKHOUSE_URL`: ClickHouse HTTP interface (default
//!   `http://127.0.0.1:8123`), with `TOUDAI_CLICKHOUSE_USER` and
//!   `TOUDAI_CLICKHOUSE_PASSWORD` when it needs credentials.
//! * `TOUDAI_CLICKHOUSE_DATABASE`: the deployment database the schema was
//!   applied to (default `toudai`).
//! * `TOUDAI_DEFAULT_DATASET`: the dataset of a request whose header and key
//!   name none (default none, so such a request is a 400). It must be a valid
//!   dataset name and is still checked against the key's list.
//! * `TOUDAI_ROUTING_EPOCH`: stamped on every row (default 1).
//! * `TOUDAI_INGEST_MEMORY_BUDGET`: request memory held at once, in bytes or
//!   with a `KiB`, `MiB`, or `GiB` suffix (default `256MiB`). Each request is
//!   charged its bytes plus 4 KiB per record. A request that does not fit
//!   beside the others is answered 429 (HTTP) or `UNAVAILABLE` (gRPC) for the
//!   producer to retry; one that needs more than the whole budget is a 400 or
//!   `INVALID_ARGUMENT`, so the default admits at most about 60,000 records
//!   in one request (SDKs send 512, Collector exporters 8192). It must be at
//!   least the 20 MiB body limit, or the largest body could never be
//!   admitted, and below 4 GiB, the most one request's share can count.

use std::process::ExitCode;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use toudai::auth::Keys;
use toudai_ingest::pipeline::{Pipeline, Settings};
use toudai_ingest::writer::{Writer, WriterConfig};
use toudai_ingest::{grpc, http};

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// `TOUDAI_INGEST_MEMORY_BUDGET`: a byte count from the body limit to below
/// 4 GiB.
fn memory_budget(text: &str) -> Option<usize> {
    parse_bytes(text).filter(|&bytes| bytes >= http::MAX_BODY_BYTES && u32::try_from(bytes).is_ok())
}

/// A byte count, optionally with a binary unit: `268435456` or `256MiB`.
fn parse_bytes(text: &str) -> Option<usize> {
    let text = text.trim();
    let (digits, unit) = match text.find(|c: char| !c.is_ascii_digit()) {
        Some(i) => text.split_at(i),
        None => (text, ""),
    };
    let scale: usize = match unit.trim() {
        "" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        _ => return None,
    };
    digits.parse::<usize>().ok()?.checked_mul(scale)
}

async fn bind(variable: &str, default: &str) -> Result<TcpListener, String> {
    let address = env(variable).unwrap_or_else(|| default.into());
    TcpListener::bind(&address)
        .await
        .map_err(|e| format!("{variable}: cannot listen on {address}: {e}"))
}

async fn run() -> Result<(), String> {
    let keys_path = env("TOUDAI_WRITE_KEYS_FILE").ok_or(
        "TOUDAI_WRITE_KEYS_FILE is required: the file listing write keys and their datasets",
    )?;
    let keys = std::fs::read_to_string(&keys_path)
        .map_err(|e| format!("cannot read {keys_path}: {e}"))
        .and_then(|text| Keys::parse(&text).map_err(|e| format!("{keys_path}: {e}")))?;
    let routing_epoch = match env("TOUDAI_ROUTING_EPOCH") {
        None => Settings::default().routing_epoch,
        Some(value) => value
            .parse()
            .map_err(|_| "TOUDAI_ROUTING_EPOCH must be an integer from 0 to 65535")?,
    };

    let writer = Writer::new(
        WriterConfig {
            url: env("TOUDAI_CLICKHOUSE_URL").unwrap_or_else(|| "http://127.0.0.1:8123".into()),
            user: env("TOUDAI_CLICKHOUSE_USER"),
            password: env("TOUDAI_CLICKHOUSE_PASSWORD"),
            database: env("TOUDAI_CLICKHOUSE_DATABASE").unwrap_or_else(|| "toudai".into()),
        },
        Arc::default(),
    )
    .map_err(|e| format!("TOUDAI_CLICKHOUSE_DATABASE: {e}"))?;
    let memory_budget = match env("TOUDAI_INGEST_MEMORY_BUDGET") {
        None => Settings::default().memory_budget,
        Some(value) => memory_budget(&value).ok_or(
            "TOUDAI_INGEST_MEMORY_BUDGET must be from 20MiB to below 4GiB, such as 256MiB",
        )?,
    };
    let settings = Settings {
        default_dataset: env("TOUDAI_DEFAULT_DATASET"),
        routing_epoch,
        memory_budget,
        ..Settings::default()
    };
    let pipeline = Arc::new(
        Pipeline::new(keys, settings, writer)
            .map_err(|e| format!("TOUDAI_DEFAULT_DATASET: {e}"))?,
    );

    let grpc_listener = bind("TOUDAI_OTLP_GRPC_LISTEN", "0.0.0.0:4317").await?;
    let http_listener = bind("TOUDAI_OTLP_HTTP_LISTEN", "0.0.0.0:4318").await?;
    let grpc_address = grpc_listener.local_addr().map_err(|e| e.to_string())?;
    let http_address = http_listener.local_addr().map_err(|e| e.to_string())?;
    // Tests bind port 0 and read the chosen addresses from this line.
    eprintln!("toudai-ingest listening on grpc://{grpc_address} and http://{http_address}");

    // Ctrl-C or SIGTERM (what `docker stop` and Kubernetes send) stops both
    // servers accepting; each then waits for its requests in flight, so a
    // request already writing is answered rather than cut off.
    let mut terminate = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
    let (stop, stopped) = watch::channel(());
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
        let _ = stop.send(());
    });
    let shutdown = || {
        let mut stopped = stopped.clone();
        async move {
            let _ = stopped.changed().await;
        }
    };
    let timeouts = http::Timeouts::default();
    let grpc = async {
        grpc::serve(grpc_listener, pipeline.clone(), shutdown(), timeouts.drain)
            .await
            .map_err(|e| format!("OTLP/gRPC: {e}"))
    };
    let http = async {
        http::serve(http_listener, pipeline.clone(), timeouts, shutdown()).await;
        Ok(())
    };
    tokio::try_join!(grpc, http).map(|_| ())
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("toudai-ingest: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::memory_budget;

    #[test]
    fn parses_memory_budgets() {
        let cases = [
            ("268435456", Some(268_435_456)),
            ("256MiB", Some(256 << 20)),
            ("20 MiB", Some(20 << 20)),
            ("1GiB", Some(1 << 30)),
            ("4095MiB", Some(4095 << 20)),
            // A share of 4 GiB would not fit a permit count.
            ("4GiB", None),
            ("20480KiB", Some(20 << 20)),
            // Below the 20 MiB body limit.
            ("20479KiB", None),
            ("20971519", None),
            ("256MB", None),
            ("256mib", None),
            ("MiB", None),
            ("-1", None),
            ("99999999999999999999GiB", None),
        ];
        for (text, expected) in cases {
            assert_eq!(memory_budget(text), expected, "{text:?}");
        }
    }
}
