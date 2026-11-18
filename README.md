# Toudai

Toudai is a Rust-based, ClickHouse-backed telemetry service under active
design and benchmarking.

Architecture, decisions, implementation plans, and recorded experiments live in
the separate [canonical RFD repository (members)](https://github.com/toudai-dev/rfds).
Public readers can use the [historical RFD snapshot](https://github.com/toudai-dev/toudai/tree/ba8f98da50642b6f1212ca941845cf7100728bb9/rfd),
which preserves the designs, original task lists and evidence before migration,
but does not track later changes in the canonical repository.

```sh
make build
make test
make lint
```

## Query service

The `toudai` binary serves the typed query layer over HTTP in front of
ClickHouse.

Create the schema first with `clickhouse/apply-schema.sh` (see
`clickhouse/README.adoc`; the database defaults to `toudai`), then:

```sh
printf 'dev-read-key-0123456789\n' > read-keys.txt
TOUDAI_READ_KEYS_FILE=read-keys.txt \
TOUDAI_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  cargo run
```

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/query` | A query request (see `src/api/wire.rs`); returns rows plus `PlanDiagnostics`. |
| `GET /v1/events/{dataset}/{event_id}` | The canonical payload of one event from `canonical_events`. |
| `GET /v1/traces/{dataset}/{trace_id}?start_ns=&end_ns=` | One trace in an event-time window: its spans in waterfall order with their span events, and the logs and application events carrying its context (`&logs=false` leaves them out). See `src/traces.rs`. |
| `GET /healthz` | 200 while ClickHouse answers, 503 otherwise. No credential needed. |

A query names a dataset, a signal (`logs` or `spans`), a half-open time range
in Unix nanoseconds, filters, group keys, and one aggregate (`count`,
`count_values`, `sum`, or `span_duration`). With `time.bucket_ns` it is a time
series: buckets are half-open, start at `time.start_ns`, and each row reports
its `bucket_start_ns` (at most 1,000 buckets; a bucket without events has no
row). The `percentiles` and `span_duration_percentiles` aggregates return
exact nearest-rank percentiles, each a value that was received, under the same
typed rules as `sum`.

The data endpoints need `Authorization: Bearer <read key>`. One deployment
serves one customer
([historical Decision 0002](https://github.com/toudai-dev/toudai/blob/ba8f98da50642b6f1212ca941845cf7100728bb9/rfd/0001/decisions/0002-single-tenant-core.adoc)), so a key
admits the request, optionally restricted to a list of datasets: a dataset
outside the list is refused with 403. Every statement is scoped to the dataset
the request names. The static key file (`<key> [<dataset>...]` per line, no
datasets meaning all of them, read at startup; keys of at least 16 characters)
is a placeholder for the key store.

Configuration is environment-only: `TOUDAI_READ_KEYS_FILE` (required),
`TOUDAI_LISTEN` (`127.0.0.1:8080`), `TOUDAI_CLICKHOUSE_URL`
(`http://127.0.0.1:8123`, plain HTTP only), `TOUDAI_CLICKHOUSE_USER`,
`TOUDAI_CLICKHOUSE_PASSWORD`, and `TOUDAI_CLICKHOUSE_DATABASE` (`toudai`).

`cargo test` runs the end-to-end test in `tests/e2e/` against a real ClickHouse
server with the product DDL and the Experiment 021 fixture: a
`clickhouse/clickhouse-server` container when a Docker daemon is available,
otherwise the `clickhouse` binary from the Nix development shell
(`TOUDAI_E2E_RUNTIME=docker|native` forces one). It is skipped when neither is
present; `TOUDAI_REQUIRE_CLICKHOUSE=1` turns that into a failure.
