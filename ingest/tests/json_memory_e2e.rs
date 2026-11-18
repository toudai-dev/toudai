//! Measure decoder allocation work, including freed temporary graphs, then store
//! the decoded request in a real ClickHouse and read its special value back.
#[allow(dead_code)]
#[path = "../../tests/e2e/server.rs"]
mod server;
#[allow(dead_code)]
mod support;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use tokio::time::Instant;
use toudai::auth::DatasetAccess;
use toudai_ingest::admit::admit_logs;
use toudai_ingest::auth::Scope;
use toudai_ingest::mapping::map_logs;
use toudai_ingest::writer::{Writer, WriterConfig};

thread_local! {
    // Only the synchronous decoder on this thread is measured; server/network
    // activity cannot change its high-water mark.
    static LIVE: Cell<Option<(isize, usize, usize)>> = const { Cell::new(None) };
}

fn allocated(delta: isize) {
    let _ = LIVE.try_with(|live| {
        if let Some((bytes, peak, total)) = live.get() {
            let bytes = bytes + delta;
            live.set(Some((
                bytes,
                peak.max(bytes.max(0) as usize),
                total + delta.max(0) as usize,
            )));
        }
    });
}

struct Measured;
// SAFETY: every allocation and deallocation is forwarded unchanged to System.
unsafe impl GlobalAlloc for Measured {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            allocated(layout.size() as isize);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        allocated(-(layout.size() as isize));
        unsafe { System.dealloc(ptr, layout) };
    }
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, old, size) };
        if !ptr.is_null() {
            allocated(size as isize - old.size() as isize);
        }
        ptr
    }
}

#[global_allocator]
static ALLOCATOR: Measured = Measured;

fn decode(body: &[u8]) -> (ExportLogsServiceRequest, usize) {
    LIVE.with(|live| live.set(Some((0, 0, 0))));
    let request = toudai_ingest::protojson::decode(body);
    let (_, peak, total) = LIVE.with(|live| live.replace(None).unwrap());
    eprintln!("decoder live peak={peak}, cumulative allocated bytes={total}");
    (request.expect("valid protojson"), total)
}

#[tokio::test]
async fn string_double_at_the_end_does_not_allocate_a_second_object_graph() {
    let Some(server) = server::start().await else {
        return;
    };
    let database = "toudai_ingest_json_memory";
    for (_, sql) in server::product_ddl() {
        server::apply_ddl(&server, &sql, database).await;
    }
    // The direct decoder ignores this subtree. A late string double must not
    // materialize all its empty objects just to normalize the known field.
    let body = |last: &str| {
        format!(
            "{{\"future\":[{}{{}}],\"resourceLogs\":[{{\"scopeLogs\":[{{\"logRecords\":[{{\"body\":{{\"doubleValue\":{last}}}}}]}}]}}]}}",
            "{},".repeat(8_192)
        )
    };
    let ordinary = body("0.25");
    let special = body("\"Infinity\"");
    let (_, ordinary_allocated) = decode(ordinary.as_bytes());
    let (request, special_allocated) = decode(special.as_bytes());
    eprintln!(
        "JSON bytes={}, ordinary allocated={ordinary_allocated}, string-double allocated={special_allocated}",
        special.len()
    );
    // Token buffering/patch offsets may cost a few body copies, but never a
    // full JSON tree in addition to the typed decoder's own allocations.
    assert!(
        special_allocated <= ordinary_allocated + 8 * special.len(),
        "second object graph: {special_allocated} vs {ordinary_allocated}"
    );

    let scope = Scope {
        dataset: "json-memory".into(),
        access: DatasetAccess::All,
    };
    let ids = admit_logs(&request, &scope).unwrap();
    let mapped = map_logs(&request, &scope.dataset, &ids, 1);
    let writer = Writer::new(
        WriterConfig {
            url: server.url.clone(),
            user: Some(server::USER.into()),
            password: Some(server::PASSWORD.into()),
            database: database.into(),
        },
        Arc::default(),
    )
    .unwrap();
    writer
        .write_logs(&mapped, Instant::now() + Duration::from_secs(10))
        .await
        .unwrap();
    let count = server::execute(&server, &format!(
        "SELECT count() FROM {database}.canonical_events AS c INNER JOIN {database}.serving_logs AS l USING (event_id) WHERE l.body = 'Infinity' AND position(c.payload, '\"inf\"') > 0"
    ), None).await;
    assert_eq!(
        count.trim(),
        "1",
        "decoded special value must reach both tables"
    );

    let service = support::spawn(
        &server,
        "tk_example_prod_0123456789abcdef json-memory\n",
        database,
        &[],
    );
    let client = reqwest::Client::new();
    for (body, status) in [
        (
            r#"{"resourceLogs":{"$serde_json::private::RawValue":"[]"}}"#,
            400,
        ),
        (
            r#"{"resourceLogs":null,"future":{"$serde_json::private::RawValue":0}}"#,
            400,
        ),
        (
            r#"{"resourceLogs":[],"future":{"$serde_json::private::RawValue":0}}"#,
            200,
        ),
    ] {
        let response = client
            .post(format!("{}/v1/logs", service.http))
            .header("Content-Type", "application/json")
            .header("X-Toudai-Key", "tk_example_prod_0123456789abcdef")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status, "{body}");
    }
}
