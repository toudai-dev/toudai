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

/// Overflow modes that choose between failing and silently truncating a result
/// or its inputs when a limit is hit.
const OVERFLOW_MODES: [&str; 10] = [
    "result_overflow_mode",
    "read_overflow_mode",
    "group_by_overflow_mode",
    "sort_overflow_mode",
    "timeout_overflow_mode",
    "set_overflow_mode",
    "join_overflow_mode",
    "distinct_overflow_mode",
    // Leaf limits are checked on their own settings, so the ordinary modes do
    // not override them.
    "read_overflow_mode_leaf",
    "timeout_overflow_mode_leaf",
];

/// Settings that a user profile (or the server defaults) could use to change
/// what a statement returns or how the reply is encoded without failing it:
/// an outer row cap or offset, a result filter, a rewritten select, sort, or
/// order, background execution that replies with an empty body, an output
/// format that overrides the statement's `FORMAT`, the query cache, the
/// empty-aggregate row, and the spelling of booleans. Pinned on every request so
/// the generated SQL alone decides the answer.
const NEUTRAL_SETTINGS: [(&str, &str); 15] = [
    ("limit", "0"),
    ("offset", "0"),
    ("page", "0"),
    // An empty `filter` is not "unset": 26.8 compiles it to `WHERE tuple()` and
    // fails every statement. `1` keeps every row.
    ("filter", "1"),
    ("select", ""),
    ("sort", ""),
    ("order", ""),
    ("additional_result_filter", ""),
    ("additional_table_filters", "{}"),
    ("run_query_in_background", "0"),
    ("output_format", "JSONEachRow"),
    // A cached answer to an identical request would hide an event that arrived
    // or a second payload that made it a conflict.
    ("use_query_cache", "0"),
    // On, an ungrouped aggregate over no rows returns no row instead of the
    // zero-count row the result layer expects.
    ("empty_result_for_aggregation_by_empty_set", "0"),
    // Bool parameters and `toString(Bool)` follow these; inverted, a true value
    // would be read and reported as false.
    ("bool_true_representation", "true"),
    ("bool_false_representation", "false"),
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
        // Every result is exactly what the generated SQL says or an error. A
        // `break` or `any` overflow policy, a profile-level cap, filter, or
        // output format (from the service user's profile, say) would return a
        // valid but different answer behind a 200, which this service would
        // report as an exact one. If a profile forbids changing these settings
        // ClickHouse rejects the request, which also fails closed.
        for setting in OVERFLOW_MODES {
            args.push((setting.into(), "throw".into()));
        }
        for (setting, value) in NEUTRAL_SETTINGS {
            args.push((setting.into(), value.into()));
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
