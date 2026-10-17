use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;

use super::*;
use crate::query::Param;

#[derive(Debug, Default, Clone)]
struct Captured {
    query: String,
    authorization: Option<String>,
    body: String,
}

/// A stand-in ClickHouse that records the request and replies with `status`
/// and `reply`.
async fn serve(status: StatusCode, reply: &'static str) -> (String, Arc<Mutex<Captured>>) {
    let captured = Arc::new(Mutex::new(Captured::default()));
    let app = Router::new()
        .route(
            "/",
            post(
                move |State(captured): State<Arc<Mutex<Captured>>>,
                      RawQuery(query): RawQuery,
                      headers: HeaderMap,
                      body: String| async move {
                    *captured.lock().unwrap() = Captured {
                        query: query.unwrap_or_default(),
                        authorization: headers
                            .get("authorization")
                            .map(|v| v.to_str().unwrap().to_owned()),
                        body,
                    };
                    (status, reply)
                },
            ),
        )
        .with_state(captured.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, captured)
}

fn statement(params: &[(&str, &str)]) -> SqlStatement {
    SqlStatement {
        sql: "SELECT {p0:String} AS a FORMAT JSONEachRow".into(),
        params: params
            .iter()
            .map(|(name, value)| Param {
                name: (*name).into(),
                ch_type: "String",
                value: (*value).into(),
            })
            .collect(),
    }
}

#[tokio::test]
async fn parameters_travel_beside_the_sql_never_inside_it() {
    let (url, captured) = serve(StatusCode::OK, "{\"a\":\"x\"}\n{\"a\":\"y\"}\n").await;
    let mut config = ClickHouseConfig::new(url);
    config.user = Some("reader".into());
    config.password = Some("secret".into());
    let client = ClickHouse::new(config).unwrap();

    // A value built to break out of a quoted literal and to exercise the URL
    // and text-format escaping layers.
    let hostile = "a' OR 1=1 --\t\\ &x=1 %41+ é\n";
    let rows = client
        .query_rows(&statement(&[("p0", hostile)]))
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    let got = captured.lock().unwrap().clone();
    assert_eq!(got.body, "SELECT {p0:String} AS a FORMAT JSONEachRow");
    assert!(!got.body.contains("OR 1=1"));
    let args: Vec<(String, String)> = got
        .query
        .split('&')
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap();
            (name.to_owned(), value.to_owned())
        })
        .collect();
    let names: Vec<&str> = args.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(&names[..2], ["param_p0", "wait_end_of_query"]);
    assert!(names.contains(&"max_execution_time"));
    // No limit can turn into a truncated, successful answer.
    let overflow: Vec<_> = args
        .iter()
        .filter(|(name, _)| name.contains("_overflow_mode"))
        .collect();
    assert_eq!(overflow.len(), OVERFLOW_MODES.len());
    assert!(overflow.iter().all(|(_, value)| value == "throw"));
    // Profile settings cannot reshape the answer or its encoding.
    for (name, value) in [
        ("limit", "0"),
        ("offset", "0"),
        ("filter", "1"),
        ("additional_result_filter", ""),
        ("run_query_in_background", "0"),
        ("output_format", "JSONEachRow"),
        ("use_query_cache", "0"),
        ("empty_result_for_aggregation_by_empty_set", "0"),
        ("bool_true_representation", "true"),
        ("bool_false_representation", "false"),
    ] {
        assert!(args.iter().any(|(n, v)| n == name && v == value), "{name}");
    }
    // Operator limits stay in force: only their overflow policy is pinned.
    assert!(
        !names
            .iter()
            .any(|n| n.starts_with("max_") && *n != "max_execution_time")
    );
    // Leaf limits are separate settings that the ordinary modes do not override.
    for leaf in ["read_overflow_mode_leaf", "timeout_overflow_mode_leaf"] {
        assert!(overflow.iter().any(|(name, _)| name == leaf), "{leaf}");
    }
    // The URL value decodes back to the text-format escaped parameter.
    let decoded = url_decode(&args[0].1);
    assert_eq!(decoded, "a' OR 1=1 --\\t\\\\ &x=1 %41+ é\\n");
    // user "reader", password "secret".
    assert_eq!(
        got.authorization.as_deref(),
        Some("Basic cmVhZGVyOnNlY3JldA==")
    );
}

#[tokio::test]
async fn failures_are_distinguished() {
    let cases: [(StatusCode, &'static str, &str); 3] = [
        (StatusCode::OK, "not json\n", "decode"),
        (StatusCode::BAD_REQUEST, "Code: 62. DB::Exception", "status"),
        (StatusCode::OK, "", "empty"),
    ];
    for (status, reply, expected) in cases {
        let (url, _) = serve(status, reply).await;
        let client = ClickHouse::new(ClickHouseConfig::new(url)).unwrap();
        let got = client.query_rows(&statement(&[])).await;
        match (expected, got) {
            ("decode", Err(ClickHouseError::Decode(_))) => {}
            ("status", Err(ClickHouseError::Status { status: 400, body })) => {
                assert!(body.contains("DB::Exception"))
            }
            ("empty", Ok(rows)) => assert!(rows.is_empty()),
            (expected, got) => panic!("{expected}: {got:?}"),
        }
    }

    let unreachable = ClickHouse::new(ClickHouseConfig::new("http://127.0.0.1:1")).unwrap();
    assert!(matches!(
        unreachable.ping().await,
        Err(ClickHouseError::Transport(_))
    ));
}

/// Percent-decoding with `+` as space, the form-urlencoded convention.
fn url_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                out.push(u8::from_str_radix(&value[i + 1..i + 3], 16).unwrap());
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8(out).unwrap()
}
