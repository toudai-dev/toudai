//! The HTTP API of the query service.
//!
//! * `POST /v1/query`: the request model of [`crate::query`] as JSON (see
//!   [`wire`]); the response is the rows plus [`crate::query::PlanDiagnostics`].
//! * `GET /v1/events/{dataset}/{event_id}`: the canonical payload of one event.
//! * `GET /healthz`: whether the service can reach ClickHouse.
//!
//! The query and event endpoints require `Authorization: Bearer <read key>`.
//! One deployment serves one customer (Decision 0002), so a valid key admits
//! the request and nothing scopes it to a customer. The dataset is named
//! explicitly, by the request body or the event path; a key restricted to a
//! list of datasets (see [`crate::auth`]) is refused any other dataset with
//! 403 before a statement runs.

pub mod wire;

use std::sync::Arc;

use axum::Router;
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequestParts, Path, State};
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, http};
use serde_json::{Value, json};

use crate::auth::{DatasetAccess, ReadKeys, bearer_token};
use crate::clickhouse::{ClickHouse, ClickHouseError};
use crate::events::{self, EventError};
use crate::query::{QueryError, ServingTables, interpret, plan};

use self::wire::QueryRequest;

pub struct AppState {
    pub clickhouse: ClickHouse,
    pub tables: ServingTables,
    pub keys: ReadKeys,
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/query", post(query))
        .route("/v1/events/{dataset}/{event_id}", get(event))
        .route("/healthz", get(healthz))
        .with_state(state)
}

/// A failed request: a stable `code` for clients and a human message.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> ApiError {
        ApiError {
            status,
            code,
            message: message.into(),
        }
    }

    /// A ClickHouse failure. The detail can name tables and settings, so it goes
    /// to the log and the client sees only the class of failure.
    fn clickhouse(error: ClickHouseError) -> ApiError {
        eprintln!("clickhouse request failed: {error}");
        match error {
            ClickHouseError::Transport(_) => ApiError::new(
                StatusCode::BAD_GATEWAY,
                "clickhouse_unavailable",
                "the storage backend is unreachable",
            ),
            _ => ApiError::new(
                StatusCode::BAD_GATEWAY,
                "clickhouse_error",
                "the storage backend failed the request",
            ),
        }
    }

    /// An error of the request itself, found before any statement ran.
    fn rejected(error: QueryError) -> ApiError {
        ApiError::new(StatusCode::BAD_REQUEST, error.code(), error.to_string())
    }

    /// An error found in the data the statement returned: the request is valid
    /// but its answer would break the typed-value contract.
    fn contract(error: QueryError) -> ApiError {
        match error {
            QueryError::MalformedResult { .. } => {
                eprintln!("{error}");
                ApiError::new(
                    StatusCode::BAD_GATEWAY,
                    error.code(),
                    "the storage backend returned an unexpected result",
                )
            }
            error => ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                error.code(),
                error.to_string(),
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error": { "code": self.code, "message": self.message } });
        let mut response = (self.status, Json(body)).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

/// The datasets a request's bearer key may read. Extracting it is what makes a
/// handler authenticated.
struct ReadKey(DatasetAccess);

impl ReadKey {
    /// Refuse a dataset outside the key's list (Decision 0002 D3).
    fn check(&self, dataset: &str) -> Result<(), ApiError> {
        if self.0.allows(dataset) {
            Ok(())
        } else {
            Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "dataset_not_allowed",
                "the read key may not read this dataset",
            ))
        }
    }
}

impl FromRequestParts<Arc<AppState>> for ReadKey {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<ReadKey, ApiError> {
        let unauthenticated = |message: &'static str| {
            ApiError::new(StatusCode::UNAUTHORIZED, "unauthenticated", message)
        };
        let header = parts
            .headers
            .get(AUTHORIZATION)
            .ok_or_else(|| unauthenticated("a bearer read key is required"))?;
        let key = header
            .to_str()
            .ok()
            .and_then(bearer_token)
            .ok_or_else(|| unauthenticated("the Authorization header is not a bearer token"))?;
        state
            .keys
            .access_for(key)
            .cloned()
            .map(ReadKey)
            .ok_or_else(|| unauthenticated("the read key is not recognised"))
    }
}

async fn query(
    State(state): State<Arc<AppState>>,
    key: ReadKey,
    body: Result<Json<QueryRequest>, JsonRejection>,
) -> Result<Json<Value>, ApiError> {
    let Json(request) = body.map_err(|rejection| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            rejection.body_text(),
        )
    })?;
    // Validate first, so a malformed request is a 400 whatever the key may read.
    let planned = plan(request.into_query(), &state.tables).map_err(ApiError::rejected)?;
    key.check(&planned.query.scope.dataset)?;
    let rows = state
        .clickhouse
        .query_rows(&planned.statement)
        .await
        .map_err(ApiError::clickhouse)?;
    let probe_rows = match &planned.non_finite_probe {
        Some(probe) => state
            .clickhouse
            .query_rows(probe)
            .await
            .map_err(ApiError::clickhouse)?,
        None => Vec::new(),
    };
    let result = interpret(&planned, &rows, &probe_rows).map_err(ApiError::contract)?;
    Ok(Json(result.to_json()))
}

async fn event(
    State(state): State<Arc<AppState>>,
    key: ReadKey,
    Path((dataset, event_id)): Path<(String, String)>,
) -> Result<Json<events::CanonicalEvent>, ApiError> {
    key.check(&dataset)?;
    let statement = events::lookup_statement(&state.tables, &dataset, &event_id);
    let rows = state
        .clickhouse
        .query_rows(&statement)
        .await
        .map_err(ApiError::clickhouse)?;
    match events::decode(&rows) {
        Ok(Some(event)) => Ok(Json(event)),
        Ok(None) => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "event_not_found",
            "no such event in this dataset",
        )),
        Err(error @ EventError::Conflict { .. }) => Err(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "event_conflict",
            error.to_string(),
        )),
        Err(error @ EventError::Malformed(_)) => {
            eprintln!("{error}");
            Err(ApiError::new(
                StatusCode::BAD_GATEWAY,
                "malformed_result",
                "the storage backend returned an unexpected result",
            ))
        }
    }
}

/// Unauthenticated, so a load balancer can probe it. It reports the service
/// degraded (503) while ClickHouse is unreachable or rejects the service's own
/// credentials, because no query or event lookup can succeed then.
async fn healthz(State(state): State<Arc<AppState>>) -> (http::StatusCode, Json<Value>) {
    match state.clickhouse.ping().await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({ "status": "ok", "clickhouse": "ok" })),
        ),
        Err(error) => {
            eprintln!("health check: {error}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "status": "degraded", "clickhouse": "unavailable" })),
            )
        }
    }
}

#[cfg(test)]
mod tests;
