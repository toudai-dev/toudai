//! The Toudai query service.
//!
//! Configuration is read from the environment:
//!
//! * `TOUDAI_READ_KEYS_FILE` (required): the static read-key file, a placeholder
//!   for the tenant-policy registry (see `toudai::auth`).
//! * `TOUDAI_LISTEN`: address to serve on (default `127.0.0.1:8080`).
//! * `TOUDAI_CLICKHOUSE_URL`: ClickHouse HTTP interface (default
//!   `http://127.0.0.1:8123`), with `TOUDAI_CLICKHOUSE_USER` and
//!   `TOUDAI_CLICKHOUSE_PASSWORD` when it needs credentials.
//! * `TOUDAI_CLICKHOUSE_DATABASE`: the database holding the serving and
//!   canonical tables (default `toudai`).

use std::process::ExitCode;
use std::sync::Arc;

use toudai::api::{AppState, router};
use toudai::auth::ReadKeys;
use toudai::clickhouse::{ClickHouse, ClickHouseConfig};
use toudai::query::ServingTables;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

async fn run() -> Result<(), String> {
    let keys_path = env("TOUDAI_READ_KEYS_FILE")
        .ok_or("TOUDAI_READ_KEYS_FILE is required: the file mapping read keys to tenants")?;
    let keys = std::fs::read_to_string(&keys_path)
        .map_err(|e| format!("cannot read {keys_path}: {e}"))
        .and_then(|text| ReadKeys::parse(&text).map_err(|e| format!("{keys_path}: {e}")))?;

    let mut config = ClickHouseConfig::new(
        env("TOUDAI_CLICKHOUSE_URL").unwrap_or_else(|| "http://127.0.0.1:8123".into()),
    );
    config.user = env("TOUDAI_CLICKHOUSE_USER");
    config.password = env("TOUDAI_CLICKHOUSE_PASSWORD");
    let tables =
        ServingTables::new(&env("TOUDAI_CLICKHOUSE_DATABASE").unwrap_or_else(|| "toudai".into()))
            .map_err(|e| e.to_string())?;
    let state = Arc::new(AppState {
        clickhouse: ClickHouse::new(config).map_err(|e| e.to_string())?,
        tables,
        keys,
    });

    let address = env("TOUDAI_LISTEN").unwrap_or_else(|| "127.0.0.1:8080".into());
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(|e| format!("cannot listen on {address}: {e}"))?;
    let bound = listener.local_addr().map_err(|e| e.to_string())?;
    // Tests bind port 0 and read the chosen address from this line.
    eprintln!("toudai listening on http://{bound}");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("toudai: {error}");
            ExitCode::FAILURE
        }
    }
}
