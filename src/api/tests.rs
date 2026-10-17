//! Request handling that never reaches ClickHouse: authentication and request
//! rejection. The ClickHouse-backed paths are covered by `tests/e2e_api.rs`.

use reqwest::StatusCode;
use serde_json::{Value, json};

use super::*;
use crate::clickhouse::ClickHouseConfig;

/// Serve the API with a ClickHouse that refuses connections.
async fn serve() -> String {
    let state = Arc::new(AppState {
        clickhouse: ClickHouse::new(ClickHouseConfig::new("http://127.0.0.1:1")).unwrap(),
        tables: ServingTables::new("toudai").unwrap(),
        keys: ReadKeys::parse("key-a tenant-a\n").unwrap(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    url
}

fn valid_query() -> Value {
    json!({
        "dataset": "d", "signal": "logs",
        "time": {"start_ns": 1, "end_ns": 2},
        "aggregate": {"kind": "count"},
    })
}

async fn call(
    method: reqwest::Method,
    url: String,
    authorization: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Option<String>, Value) {
    let mut request = reqwest::Client::new().request(method, url);
    if let Some(value) = authorization {
        request = request.header("authorization", value);
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let challenge = response
        .headers()
        .get("www-authenticate")
        .map(|v| v.to_str().unwrap().to_owned());
    (status, challenge, response.json().await.unwrap())
}

#[tokio::test]
async fn requests_are_authenticated_before_anything_else() {
    let base = serve().await;
    let post = reqwest::Method::POST;
    let get = reqwest::Method::GET;
    let cases = [
        (post.clone(), "/v1/query", None),
        (post.clone(), "/v1/query", Some("Bearer wrong")),
        (post.clone(), "/v1/query", Some("Basic key-a")),
        (post.clone(), "/v1/query", Some("key-a")),
        (get.clone(), "/v1/events/d/e1", None),
        (get.clone(), "/v1/events/d/e1", Some("Bearer key-a-")),
    ];
    for (method, path, authorization) in cases {
        // An unparseable body must not be reported ahead of the credential.
        let body = (path == "/v1/query").then(|| json!("not a query"));
        let (status, challenge, body) =
            call(method, format!("{base}{path}"), authorization, body).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path} {authorization:?}");
        assert_eq!(challenge.as_deref(), Some("Bearer"));
        assert_eq!(body["error"]["code"], "unauthenticated");
    }
}

#[tokio::test]
async fn query_requests_are_rejected_before_any_statement_runs() {
    let base = serve().await;
    let mut inverted = valid_query();
    inverted["time"] = json!({"start_ns": 2, "end_ns": 2});
    let mut tenant = valid_query();
    tenant["tenant_id"] = json!("tenant-b");
    let mut unsupported = valid_query();
    unsupported["filters"] = json!([{
        "kind": "compare", "attr": {"namespace": "record", "key": "k"}, "op": "eq",
        "value": {"type": "double", "value": "NaN"},
    }]);
    let cases = [
        (inverted, "invalid_time_range"),
        (tenant, "invalid_request"),
        (unsupported, "unsupported_operation"),
        (json!({"dataset": "d"}), "invalid_request"),
    ];
    for (body, code) in cases {
        let (status, _, response) = call(
            reqwest::Method::POST,
            format!("{base}/v1/query"),
            Some("Bearer key-a"),
            Some(body.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(response["error"]["code"], code, "{body}");
    }
}

#[tokio::test]
async fn backend_outage_is_reported_without_detail() {
    let base = serve().await;
    let (status, _, body) = call(
        reqwest::Method::POST,
        format!("{base}/v1/query"),
        Some("Bearer key-a"),
        Some(valid_query()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["error"]["code"], "clickhouse_unavailable");
    assert!(!body.to_string().contains("127.0.0.1"));

    let (status, _, body) = call(reqwest::Method::GET, format!("{base}/healthz"), None, None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "degraded");
}
