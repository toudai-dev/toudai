//! End to end: the `toudai` binary over HTTP against a real ClickHouse server.
//!
//! The product DDL (`clickhouse/schema/*.sql`) is applied verbatim, the
//! Experiment 021 fixture is loaded (`tests/fixtures/021`), and the API is
//! exercised with independent expectations: the counts and sums the
//! `experiment021_*` tests derive from the fixture inventory, and the canonical
//! payloads and their SHA-256 written by `scripts/export-fixture021-canonical.py`.
//! See `server.rs` for how the ClickHouse server is started.

mod server;

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use reqwest::{Method, StatusCode};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use server::{ClickHouseServer, execute, product_ddl};

const WS_A: &str = "11111111-1111-4111-8111-111111111111";
const WS_B: &str = "22222222-2222-4222-8222-222222222222";
const KEY_A: &str = "test-key-workspace-a";
const KEY_B: &str = "test-key-workspace-b";
/// 2026-09-01T07:00:00.123456789Z, the fixture's event time.
const TS: i64 = 1_788_246_000_123_456_789;
const TS_TEXT: &str = "2026-09-01 07:00:00.123456789";

const LOG_ROWS: &str = include_str!("../fixtures/021/serving_logs.jsonl");
const SPAN_ROWS: &str = include_str!("../fixtures/021/serving_spans.jsonl");
const CANONICAL_ROWS: &str = include_str!("../fixtures/021/canonical_events.jsonl");

/// The attribute key and values that stress every escaping layer between the
/// request and ClickHouse: URL, text format, quoting, and non-ASCII.
/// Rows in the `bulk` dataset the truncation scenario reads.
const BULK_ROWS: u64 = 200_000;

const SPECIAL_KEY: &str = "k +%&=\t é\\'\"";
const SPECIAL_VALUE: &str = "v \\ ' \n";
const INJECTION_KEY: &str = "a' OR 1=1 --";

struct Service {
    child: Child,
    base: String,
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_service(clickhouse: &ClickHouseServer, user: &str) -> Service {
    let keys = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/e2e/read-keys.txt"
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_toudai"))
        .env("TOUDAI_LISTEN", "127.0.0.1:0")
        .env("TOUDAI_READ_KEYS_FILE", keys)
        .env("TOUDAI_CLICKHOUSE_URL", &clickhouse.url)
        .env("TOUDAI_CLICKHOUSE_USER", user)
        .env("TOUDAI_CLICKHOUSE_PASSWORD", server::PASSWORD)
        .stderr(Stdio::piped())
        .spawn()
        .expect("start toudai");
    let stderr = child.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            eprintln!("[toudai] {line}");
            if let Some(address) = line.strip_prefix("toudai listening on ") {
                let _ = sender.send(address.to_owned());
            }
        }
    });
    let base = receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("toudai did not report its address");
    Service { child, base }
}

struct Api {
    client: reqwest::Client,
    base: String,
}

impl Api {
    async fn send(
        &self,
        method: Method,
        url: reqwest::Url,
        key: Option<&str>,
        body: Option<&Value>,
    ) -> (StatusCode, String) {
        let mut request = self.client.request(method, url);
        if let Some(key) = key {
            request = request.bearer_auth(key);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.expect("api request");
        (response.status(), response.text().await.expect("api body"))
    }

    async fn query(&self, key: Option<&str>, body: &Value) -> (StatusCode, Value) {
        let url = format!("{}/v1/query", self.base).parse().unwrap();
        let (status, text) = self.send(Method::POST, url, key, Some(body)).await;
        (status, serde_json::from_str(&text).expect("json response"))
    }

    async fn event(
        &self,
        key: Option<&str>,
        dataset: &str,
        event_id: &str,
    ) -> (StatusCode, String) {
        // Path segments are percent-encoded, so an ID with `/` stays one segment.
        let mut url: reqwest::Url = format!("{}/v1/events", self.base).parse().unwrap();
        url.path_segments_mut().unwrap().extend([dataset, event_id]);
        self.send(Method::GET, url, key, None).await
    }
}

fn query_body(
    dataset: &str,
    signal: &str,
    filters: Value,
    group_by: Value,
    aggregate: Value,
) -> Value {
    json!({
        "dataset": dataset,
        "signal": signal,
        "time": {"start_ns": TS.to_string(), "end_ns": (TS + 1).to_string()},
        "filters": filters,
        "group_by": group_by,
        "aggregate": aggregate,
    })
}

fn logs(filters: Value, group_by: Value, aggregate: Value) -> Value {
    query_body("fixture", "logs", filters, group_by, aggregate)
}

fn metric() -> Value {
    json!({"namespace": "record", "key": "metric.value"})
}

fn int(value: &str) -> Value {
    json!({"type": "int", "value": value})
}

fn compare(op: &str, value: Value) -> Value {
    json!({"kind": "compare", "attr": metric(), "op": op, "value": value})
}

fn sum(attr: Value, value_type: &str) -> Value {
    json!({"kind": "sum", "attr": attr, "value_type": value_type})
}

fn count_json(value: u64) -> Value {
    json!({"type": "uint64", "value": value.to_string()})
}

async fn load(server: &ClickHouseServer) {
    for (name, sql) in product_ddl() {
        execute(server, &sql, None).await;
        eprintln!("applied {name}");
    }
    for (table, rows) in [
        ("serving_logs", LOG_ROWS),
        ("serving_spans", SPAN_ROWS),
        ("canonical_events", CANONICAL_ROWS),
    ] {
        let sql = format!("INSERT INTO toudai.{table} FORMAT JSONEachRow");
        execute(server, &sql, Some(rows)).await;
    }
    // Rows beyond the fixture: a workspace-B-only dataset to prove scope comes
    // from the key, and one event whose attribute names and values need every
    // escaping layer.
    let extra = [
        json!({"tenant_id": WS_B, "dataset": "b-only", "event_id": "b-1",
               "routing_epoch": 1, "timestamp": TS_TEXT}),
        json!({"tenant_id": WS_A, "dataset": "special", "event_id": "s-1",
               "routing_epoch": 1, "timestamp": TS_TEXT,
               "record_string": {SPECIAL_KEY: SPECIAL_VALUE},
               "record_int": {INJECTION_KEY: 5}}),
    ]
    .iter()
    .map(Value::to_string)
    .collect::<Vec<_>>()
    .join("\n");
    execute(
        server,
        "INSERT INTO toudai.serving_logs FORMAT JSONEachRow",
        Some(&extra),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn query_service_end_to_end() {
    let Some(clickhouse) = server::start().await else {
        return;
    };
    load(&clickhouse).await;
    let service = spawn_service(&clickhouse, server::USER);
    let api = Api {
        client: reqwest::Client::new(),
        base: service.base.clone(),
    };

    health_and_authentication(&api).await;
    query_results(&api).await;
    query_failures(&api).await;
    scope_and_escaping(&api).await;
    event_retrieval(&api).await;
    hostile_profiles_cannot_change_answers(&clickhouse, &api).await;
}

/// A user profile must not be able to change a successful answer. Each hostile
/// profile (see `server::HOSTILE_PROFILES`) is first shown to alter a direct
/// query; the service, run as that user, must then either answer exactly as it
/// does for the unrestricted user or fail without rows.
async fn hostile_profiles_cannot_change_answers(clickhouse: &ClickHouseServer, api: &Api) {
    // Enough rows in one dataset for a leaf read limit to bite (the fixture is a
    // single block).
    let bulk = format!(
        "INSERT INTO toudai.serving_logs (tenant_id, dataset, event_id, routing_epoch, timestamp) \
         SELECT '{WS_A}', 'bulk', toString(number), 1, toDateTime64('{TS_TEXT}', 9) \
         FROM numbers({BULK_ROWS})"
    );
    server::execute(clickhouse, &bulk, None).await;
    // An attribute filter forces row reads; a bare count is answered from the
    // primary index without reading any.
    let bulk_count = query_body(
        "bulk",
        "logs",
        json!([{"kind": "absent", "attr": metric()}]),
        json!([]),
        json!({"kind": "count"}),
    );
    let (status, response) = api.query(Some(KEY_A), &bulk_count).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["rows"][0]["events"], count_json(BULK_ROWS));
    let nine_groups = logs(ints_filter(), json!([metric()]), json!({"kind": "count"}));
    let (_, expected_groups) = api.query(Some(KEY_A), &nine_groups).await;
    assert_eq!(expected_groups["rows"].as_array().unwrap().len(), 9);

    let numbers = "SELECT number FROM numbers(5) FORMAT JSONEachRow";
    let leaf_probe = "SELECT count() AS n FROM toudai.serving_logs \
                      WHERE dataset = 'bulk' AND severity_number >= 0 FORMAT JSONEachRow";
    for (user, _) in server::HOSTILE_PROFILES {
        // Precondition: left alone, this user's profile really does change what
        // a plain query returns.
        let probe = if user == "h_leaf" {
            leaf_probe
        } else {
            numbers
        };
        let unrestricted = server::execute(clickhouse, probe, None).await;
        let direct = server::execute_as(clickhouse, user, probe, None).await;
        assert_ne!(direct, unrestricted, "{user} must alter a direct query");

        let service = spawn_service(clickhouse, user);
        let hostile = Api {
            client: reqwest::Client::new(),
            base: service.base.clone(),
        };
        let truncating = matches!(user, "h_result" | "h_leaf");
        let body = if user == "h_leaf" {
            &bulk_count
        } else {
            &nine_groups
        };
        let (status, response) = hostile.query(Some(KEY_A), body).await;
        if truncating {
            // Overflow limits are operator protections the service keeps; it
            // turns their `break` into an error.
            assert_eq!(status, StatusCode::BAD_GATEWAY, "{user}: {response}");
            assert_eq!(response["error"]["code"], "clickhouse_error", "{user}");
            assert!(
                response.get("rows").is_none(),
                "{user}: partial rows: {response}"
            );
        } else {
            assert_eq!(status, StatusCode::OK, "{user}: {response}");
            assert_eq!(response["rows"], expected_groups["rows"], "{user}");
        }

        // The event lookup (`LIMIT 1 BY ... LIMIT 2`) is exposed to the same
        // settings: a capped result must not hide a conflict or an event.
        if !truncating {
            let (status, text) = hostile.event(Some(KEY_A), "fixture", "conflict-3").await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{user}: {text}");
            let (status, text) = hostile.event(Some(KEY_A), "fixture", "typed-1").await;
            assert_eq!(status, StatusCode::OK, "{user}: {text}");
            let (status, _) = hostile
                .send(
                    Method::GET,
                    format!("{}/healthz", hostile.base).parse().unwrap(),
                    None,
                    None,
                )
                .await;
            assert_eq!(status, StatusCode::OK, "{user}");
        }
    }
}

async fn health_and_authentication(api: &Api) {
    let (status, text) = api
        .send(
            Method::GET,
            format!("{}/healthz", api.base).parse().unwrap(),
            None,
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{text}");
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap()["clickhouse"],
        "ok"
    );

    let body = logs(json!([]), json!([]), json!({"kind": "count"}));
    for key in [None, Some("wrong"), Some("test-key-workspace-")] {
        let (status, response) = api.query(key, &body).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{key:?}");
        assert_eq!(response["error"]["code"], "unauthenticated");
    }
    let (status, _) = api.event(None, "fixture", "typed-1").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// Contract results over HTTP. The expected values are the independent ones the
/// `experiment021_results` tests derive from the fixture inventory: 27 log
/// events per workspace, 12 of which hold an int at `metric.value`.
async fn query_results(api: &Api) {
    let ints = || json!([{"kind": "has_type", "attr": metric(), "value_type": "int"}]);
    let sum_int = || sum(metric(), "int");
    let count = |n: u64| count_json(n);

    // (name, body, expected events, expected value stats (contributing, sum))
    type Case = (&'static str, Value, u64, Option<(u64, Value)>);
    let cases: Vec<Case> = vec![
        (
            "count",
            logs(json!([]), json!([]), json!({"kind": "count"})),
            27,
            None,
        ),
        (
            "sum 9..=100 is exact",
            logs(
                json!([compare("ge", int("9")), compare("le", int("100"))]),
                json!([]),
                sum_int(),
            ),
            3,
            Some((3, int("119"))),
        ),
        (
            "2^53 + 1 survives both directions",
            logs(
                json!([compare("eq", int("9007199254740993"))]),
                json!([]),
                sum_int(),
            ),
            1,
            Some((1, int("9007199254740993"))),
        ),
        (
            "nothing contributes: the sum is null, not zero",
            logs(json!([compare("eq", int("12345"))]), json!([]), sum_int()),
            0,
            Some((0, Value::Null)),
        ),
        (
            "explicit empty is present and contributes nothing",
            logs(
                json!([]),
                json!([]),
                sum(json!({"namespace": "record", "key": "empty.value"}), "int"),
            ),
            27,
            Some((0, Value::Null)),
        ),
        (
            "finite doubles sum",
            logs(
                json!([
                    compare("gt", json!({"type": "double", "value": 0.5})),
                    compare("lt", json!({"type": "double", "value": 2}))
                ]),
                json!([]),
                sum(metric(), "double"),
            ),
            1,
            Some((1, json!({"type": "double", "value": 1.0}))),
        ),
        (
            "contributing values are counted apart from events",
            logs(
                json!([]),
                json!([]),
                json!({"kind": "count_values", "attr": metric(), "value_type": "int"}),
            ),
            27,
            Some((12, Value::Null)),
        ),
    ];
    for (name, body, events, stats) in cases {
        let (status, response) = api.query(Some(KEY_A), &body).await;
        assert_eq!(status, StatusCode::OK, "{name}: {response}");
        let rows = response["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1, "{name}: {response}");
        assert_eq!(rows[0]["events"], count(events), "{name}");
        if let Some((contributing, total)) = stats {
            assert_eq!(rows[0]["contributing"], count(contributing), "{name}");
            // `count_values` has no sum; every other aggregate here does.
            if name != "contributing values are counted apart from events" {
                assert_eq!(rows[0]["sum"], total, "{name}");
            }
        }
        assert_eq!(response["diagnostics"]["storage_path"], "raw", "{name}");
        assert_eq!(response["diagnostics"]["approximation"], "exact", "{name}");
        assert_eq!(response["diagnostics"]["table"], "serving_logs", "{name}");
    }

    // Diagnostics name what was read and which contract guards applied. The
    // comparison restricts the sum to ints, so no incompatible-type check runs.
    let (_, response) = api
        .query(
            Some(KEY_A),
            &logs(json!([compare("le", int("2"))]), json!([]), sum_int()),
        )
        .await;
    assert_eq!(
        response["diagnostics"]["guards"],
        json!([
            "type_matched_predicates",
            "key_membership_presence",
            "int128_sum",
            "contributing_counts"
        ])
    );
    let columns = response["diagnostics"]["columns"].as_array().unwrap();
    assert!(columns.contains(&json!("tenant_id")) && columns.contains(&json!("record_int")));

    // Typed groups: ints group by value text, distinct from strings and doubles.
    let (status, response) = api
        .query(
            Some(KEY_A),
            &logs(ints(), json!([metric()]), json!({"kind": "count"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let groups: Vec<(String, u64)> = response["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            assert_eq!(row["groups"][0]["type"], "int");
            (
                row["groups"][0]["value"].as_str().unwrap().to_owned(),
                row["events"]["value"].as_str().unwrap().parse().unwrap(),
            )
        })
        .collect();
    let expected: Vec<(String, u64)> = [
        ("-9223372036854775808", 1),
        ("0", 1),
        ("1", 3),
        ("10", 1),
        ("100", 1),
        ("2", 1),
        ("9", 1),
        ("9007199254740993", 1),
        ("9223372036854775807", 2),
    ]
    .map(|(v, n)| (v.to_owned(), n))
    .to_vec();
    assert_eq!(groups, expected);

    // Missing is its own group.
    let (_, response) = api
        .query(
            Some(KEY_A),
            &logs(
                json!([{"kind": "absent", "attr": metric()}]),
                json!([metric()]),
                json!({"kind": "count"}),
            ),
        )
        .await;
    assert_eq!(response["rows"][0]["groups"][0], json!({"type": "missing"}));
    assert_eq!(response["rows"][0]["events"], count_json(3));

    // Span durations: 9 spans, 4 observable (a valid zero included), 2 reversed.
    let mut spans = query_body(
        "fixture",
        "spans",
        json!([]),
        json!([]),
        json!({"kind": "span_duration"}),
    );
    spans["time"]["start_ns"] = json!("0");
    let (status, response) = api.query(Some(KEY_A), &spans).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let row = &response["rows"][0];
    assert_eq!(row["events"], count_json(9));
    assert_eq!(row["contributing"], count_json(4));
    assert_eq!(row["timing_errors"], count_json(2));
    assert_eq!(row["sum"], int("4010"));
    assert_eq!(response["diagnostics"]["table"], "serving_spans");

    // Double comparisons report the events a non-finite value left out, even
    // when nothing matched.
    let (status, response) = api
        .query(
            Some(KEY_A),
            &logs(
                json!([{"kind": "compare", "op": "lt",
                        "attr": {"namespace": "record", "key": "double.nan"},
                        "value": {"type": "double", "value": 1.0}}]),
                json!([metric()]),
                json!({"kind": "count"}),
            ),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["rows"], json!([]));
    assert_eq!(
        response["diagnostics"]["non_finite_excluded"],
        count_json(1)
    );
}

/// Contract violations are explicit errors, never wrapped, coerced, or partial.
async fn query_failures(api: &Api) {
    let double_nan = json!({"type": "double", "value": "NaN"});
    // (name, body, status, error code)
    let cases: Vec<(&str, Value, StatusCode, &str)> = vec![
        (
            "int total beyond Int64",
            logs(ints_filter(), json!([]), sum(metric(), "int")),
            StatusCode::UNPROCESSABLE_ENTITY,
            "integer_overflow",
        ),
        (
            "a string is not an int",
            logs(
                json!([]),
                json!([]),
                sum(json!({"namespace": "record", "key": "str.one"}), "int"),
            ),
            StatusCode::UNPROCESSABLE_ENTITY,
            "incompatible_type",
        ),
        (
            "bytes cannot be summed",
            logs(
                json!([]),
                json!([]),
                sum(json!({"namespace": "record", "key": "bytes.raw"}), "int"),
            ),
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_operation",
        ),
        (
            "non-finite floats cannot be summed",
            logs(
                json!([]),
                json!([]),
                sum(
                    json!({"namespace": "record", "key": "double.nan"}),
                    "double",
                ),
            ),
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_operation",
        ),
        (
            "bytes cannot be grouped",
            logs(json!([]), json!([metric()]), json!({"kind": "count"})),
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_operation",
        ),
        (
            "NaN cannot be compared",
            logs(
                json!([compare("eq", double_nan)]),
                json!([]),
                json!({"kind": "count"}),
            ),
            StatusCode::BAD_REQUEST,
            "unsupported_operation",
        ),
        (
            "ordering is undefined for bool",
            logs(
                json!([compare("lt", json!({"type": "bool", "value": true}))]),
                json!([]),
                json!({"kind": "count"}),
            ),
            StatusCode::BAD_REQUEST,
            "ordering_not_defined",
        ),
        (
            "an empty time range",
            {
                let mut body = logs(json!([]), json!([]), json!({"kind": "count"}));
                body["time"]["end_ns"] = body["time"]["start_ns"].clone();
                body
            },
            StatusCode::BAD_REQUEST,
            "invalid_time_range",
        ),
        (
            "span duration needs spans",
            logs(json!([]), json!([]), json!({"kind": "span_duration"})),
            StatusCode::BAD_REQUEST,
            "aggregate_requires_spans",
        ),
        (
            "a malformed request",
            json!({"dataset": "fixture"}),
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
    ];
    for (name, body, status, code) in cases {
        let (got, response) = api.query(Some(KEY_A), &body).await;
        assert_eq!(
            (got, response["error"]["code"].as_str()),
            (status, Some(code)),
            "{name}: {response}"
        );
        assert!(response.get("rows").is_none(), "{name}: no partial rows");
    }
}

fn ints_filter() -> Value {
    json!([{"kind": "has_type", "attr": metric(), "value_type": "int"}])
}

/// The tenant comes from the key alone, and request text never reaches the SQL.
async fn scope_and_escaping(api: &Api) {
    let count_of = |response: &Value| {
        response["rows"][0]["events"]["value"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let count = json!({"kind": "count"});

    // The same query under each key sees only that workspace's rows.
    for (dataset, a, b) in [
        ("fixture", "27", "27"),
        ("b-only", "0", "1"),
        ("special", "1", "0"),
    ] {
        for (key, expected) in [(KEY_A, a), (KEY_B, b)] {
            let body = query_body(dataset, "logs", json!([]), json!([]), count.clone());
            let (status, response) = api.query(Some(key), &body).await;
            assert_eq!(status, StatusCode::OK, "{response}");
            assert_eq!(count_of(&response), expected, "{dataset} as {key}");
        }
    }

    // A body that names a tenant is rejected, not obeyed or ignored.
    for field in ["tenant_id", "workspace_id"] {
        let mut body = query_body("b-only", "logs", json!([]), json!([]), count.clone());
        body[field] = json!(WS_B);
        let (status, response) = api.query(Some(KEY_A), &body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {response}");
        assert_eq!(response["error"]["code"], "invalid_request");
    }

    // Keys and literals that would break out of a quoted literal, a URL, or the
    // text format match exactly the one event that holds them.
    let special = |filters: Value| query_body("special", "logs", filters, json!([]), count.clone());
    let string_attr = |key: &str| json!({"namespace": "record", "key": key});
    let cases = [
        (
            "exists special key",
            json!([{"kind": "exists", "attr": string_attr(SPECIAL_KEY)}]),
            "1",
        ),
        (
            "special string value",
            json!([{"kind": "compare", "op": "eq", "attr": string_attr(SPECIAL_KEY),
                    "value": {"type": "string", "value": SPECIAL_VALUE}}]),
            "1",
        ),
        (
            "value differs by a trailing newline",
            json!([{"kind": "compare", "op": "eq", "attr": string_attr(SPECIAL_KEY),
                    "value": {"type": "string", "value": "v \\ ' "}}]),
            "0",
        ),
        (
            "injection-shaped key matches itself",
            json!([{"kind": "compare", "op": "eq", "attr": string_attr(INJECTION_KEY),
                    "value": int("5")}]),
            "1",
        ),
        (
            "injection-shaped key does not match a prefix",
            json!([{"kind": "exists", "attr": string_attr("a' OR 1=1")}]),
            "0",
        ),
        (
            "injection-shaped literal matches nothing",
            json!([{"kind": "compare", "op": "eq", "attr": string_attr(SPECIAL_KEY),
                    "value": {"type": "string", "value": "' OR 1=1 --"}}]),
            "0",
        ),
    ];
    for (name, filters, expected) in cases {
        let (status, response) = api.query(Some(KEY_A), &special(filters)).await;
        assert_eq!(status, StatusCode::OK, "{name}: {response}");
        assert_eq!(count_of(&response), expected, "{name}");
    }
}

#[derive(Deserialize)]
struct EventResponse {
    dataset: String,
    event_id: String,
    signal: String,
    timestamp_ns: String,
    schema_version: String,
    payload: Box<RawValue>,
    payload_sha256: String,
}

/// The fixture row for `(tenant, dataset, event_id)`, as the generator wrote it.
fn fixture_events(tenant: &str, dataset: &str, event_id: &str) -> Vec<Value> {
    CANONICAL_ROWS
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|row| {
            row["tenant_id"] == tenant && row["dataset"] == dataset && row["event_id"] == event_id
        })
        .collect()
}

async fn event_retrieval(api: &Api) {
    // (key, tenant, dataset, event id, signal): each must return the payload the
    // fixture stores for that exact (tenant, dataset, event_id).
    let found = [
        (KEY_A, WS_A, "fixture", "typed-1", "logs"),
        (KEY_B, WS_B, "fixture", "typed-1", "logs"),
        (KEY_A, WS_A, "other", "typed-1", "logs"),
        (KEY_A, WS_A, "fixture", "span-1", "spans"),
        (KEY_B, WS_B, "fixture", "only-b", "logs"),
        // Stored twice by an at-least-once replay: still one event.
        (KEY_A, WS_A, "fixture", "dup-1", "logs"),
        // A `/`, space, non-ASCII, `+`, and `%` in the ID stay one path segment.
        (KEY_A, WS_A, "fixture", "odd id/1 é+%", "logs"),
    ];
    let mut payloads = Vec::new();
    for (key, tenant, dataset, event_id, signal) in found {
        let (status, text) = api.event(Some(key), dataset, event_id).await;
        assert_eq!(status, StatusCode::OK, "{dataset}/{event_id}: {text}");
        let got: EventResponse = serde_json::from_str(&text).unwrap();
        let stored = &fixture_events(tenant, dataset, event_id)[0];
        assert_eq!(
            (got.dataset.as_str(), got.event_id.as_str()),
            (dataset, event_id)
        );
        assert_eq!(got.signal, signal);
        assert_eq!(got.timestamp_ns, TS.to_string());
        assert_eq!(got.schema_version, "1");
        // The stored text comes back byte for byte, so the hash computed here
        // from the response matches the one the fixture generator computed.
        assert_eq!(
            got.payload.get(),
            stored["payload"].as_str().unwrap(),
            "{dataset}/{event_id}"
        );
        assert_eq!(
            got.payload_sha256,
            stored["payload_sha256"].as_str().unwrap()
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(got.payload.get().as_bytes())),
            got.payload_sha256
        );
        payloads.push(got.payload.get().to_owned());
    }
    // Tenant and dataset are part of the identity: `typed-1` has three payloads.
    assert_eq!(
        [&payloads[0], &payloads[1], &payloads[2]]
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3
    );

    // (key, dataset, event id, status, error code)
    let missing = [
        // Another workspace's event is indistinguishable from an absent one.
        (
            KEY_A,
            "fixture",
            "only-b",
            StatusCode::NOT_FOUND,
            "event_not_found",
        ),
        (
            KEY_B,
            "other",
            "typed-1",
            StatusCode::NOT_FOUND,
            "event_not_found",
        ),
        (
            KEY_A,
            "nope",
            "typed-1",
            StatusCode::NOT_FOUND,
            "event_not_found",
        ),
        (
            KEY_A,
            "fixture",
            "typed-2",
            StatusCode::NOT_FOUND,
            "event_not_found",
        ),
        (
            KEY_A,
            "fixture",
            "TYPED-1",
            StatusCode::NOT_FOUND,
            "event_not_found",
        ),
        // Two different payloads under one identity are reported, not picked.
        (
            KEY_A,
            "fixture",
            "conflict-1",
            StatusCode::INTERNAL_SERVER_ERROR,
            "event_conflict",
        ),
        // Replays of one payload must not hide a different one.
        (
            KEY_A,
            "fixture",
            "conflict-3",
            StatusCode::INTERNAL_SERVER_ERROR,
            "event_conflict",
        ),
    ];
    for (key, dataset, event_id, status, code) in missing {
        let (got, text) = api.event(Some(key), dataset, event_id).await;
        let body: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            (got, body["error"]["code"].as_str()),
            (status, Some(code)),
            "{dataset}/{event_id}: {text}"
        );
    }
}
