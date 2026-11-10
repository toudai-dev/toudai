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
    let settings = Settings {
        default_dataset: env("TOUDAI_DEFAULT_DATASET"),
        routing_epoch,
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
    let grpc = async {
        grpc::serve(grpc_listener, pipeline.clone(), shutdown())
            .await
            .map_err(|e| format!("OTLP/gRPC: {e}"))
    };
    let http = async {
        http::serve(
            http_listener,
            pipeline.clone(),
            http::Timeouts::default(),
            shutdown(),
        )
        .await;
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
