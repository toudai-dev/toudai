//! ClickHouse HTTP client for generated statements.
//!
//! The statement text and its bound parameters travel separately: the SQL is the
//! request body and every parameter is a `param_<name>` URL argument that
//! ClickHouse parses against the `{name:Type}` placeholder. Nothing from a
//! request is ever interpolated into the SQL, so attribute keys, literals, and
//! the tenant scope cannot change a statement.

use std::fmt;
use std::time::Duration;

use serde_json::Value;

use crate::query::SqlStatement;

/// Settings that choose between failing and silently truncating a result or
/// its inputs when a limit is hit.
const OVERFLOW_MODES: [&str; 8] = [
    "result_overflow_mode",
    "read_overflow_mode",
    "group_by_overflow_mode",
    "sort_overflow_mode",
    "timeout_overflow_mode",
    "set_overflow_mode",
    "join_overflow_mode",
    "distinct_overflow_mode",
];

#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// Base URL of the HTTP interface, for example `http://127.0.0.1:8123`.
    /// Only plain HTTP is supported; terminate TLS in front of ClickHouse or
    /// keep it on a private network.
    pub url: String,
    pub user: Option<String>,
    pub password: Option<String>,
    /// Client-side deadline for one statement, also sent as the server-side
    /// `max_execution_time` so a cancelled request does not keep running.
    pub timeout: Duration,
}

impl ClickHouseConfig {
    pub fn new(url: impl Into<String>) -> ClickHouseConfig {
        ClickHouseConfig {
            url: url.into(),
            user: None,
            password: None,
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub enum ClickHouseError {
    /// The request did not complete: connection failure or deadline.
    Transport(String),
    /// ClickHouse answered with a non-success status; `body` is its error text.
    Status { status: u16, body: String },
    /// A response line was not a JSON object.
    Decode(String),
}

impl fmt::Display for ClickHouseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClickHouseError::Transport(detail) => write!(f, "clickhouse unreachable: {detail}"),
            ClickHouseError::Status { status, body } => {
                write!(f, "clickhouse returned HTTP {status}: {}", body.trim())
            }
            ClickHouseError::Decode(detail) => write!(f, "clickhouse response: {detail}"),
        }
    }
}

impl std::error::Error for ClickHouseError {}

#[derive(Debug, Clone)]
pub struct ClickHouse {
    http: reqwest::Client,
    config: ClickHouseConfig,
}

impl ClickHouse {
    pub fn new(config: ClickHouseConfig) -> Result<ClickHouse, ClickHouseError> {
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| ClickHouseError::Transport(e.to_string()))?;
        Ok(ClickHouse { http, config })
    }

    /// Execute a generated `SELECT ... FORMAT JSONEachRow` and return its rows.
    pub async fn query_rows(
        &self,
        statement: &SqlStatement,
    ) -> Result<Vec<Value>, ClickHouseError> {
        let mut args: Vec<(String, String)> = statement
            .params
            .iter()
            .map(|p| (format!("param_{}", p.name), p.wire_value()))
            .collect();
        // Buffer the whole result before replying so a failure mid-query is an
        // HTTP error status, not a truncated body behind a 200.
        args.push(("wait_end_of_query".into(), "1".into()));
        // Every result is complete or an error. A `break` or `any` overflow
        // policy (from the service user's profile, say) would return a valid
        // truncated answer behind a 200, which this service would report as an
        // exact one. If the profile forbids changing these settings ClickHouse
        // rejects the request, which also fails closed.
        for setting in OVERFLOW_MODES {
            args.push((setting.into(), "throw".into()));
        }
        args.push((
            "max_execution_time".into(),
            self.config.timeout.as_secs().max(1).to_string(),
        ));
        let mut request = self
            .http
            .post(&self.config.url)
            .query(&args)
            .body(statement.sql.clone());
        if let Some(user) = &self.config.user {
            request = request.basic_auth(user, self.config.password.as_deref());
        }
        let response = request
            .send()
            .await
            .map_err(|e| ClickHouseError::Transport(e.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| ClickHouseError::Transport(e.to_string()))?;
        if !status.is_success() {
            return Err(ClickHouseError::Status {
                status: status.as_u16(),
                body,
            });
        }
        body.lines()
            .filter(|line| !line.is_empty())
            .map(|line| {
                serde_json::from_str::<Value>(line)
                    .map_err(|e| ClickHouseError::Decode(format!("{e}: {line}")))
            })
            .collect()
    }

    /// Whether ClickHouse accepts a trivial authenticated statement.
    pub async fn ping(&self) -> Result<(), ClickHouseError> {
        let statement = SqlStatement {
            sql: "SELECT 1 AS ok FORMAT JSONEachRow".into(),
            params: vec![],
        };
        self.query_rows(&statement).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests;
