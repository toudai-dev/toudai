# Toudai

Toudai is a Rust-based, ClickHouse-backed telemetry service under active
design and benchmarking.

```sh
make build
make test
make lint
```

## Query service

The `toudai` binary serves the typed query layer over HTTP in front of
ClickHouse.

```sh
printf 'dev-read-key 11111111-1111-4111-8111-111111111111\n' > read-keys.txt
TOUDAI_READ_KEYS_FILE=read-keys.txt \
TOUDAI_CLICKHOUSE_URL=http://127.0.0.1:8123 \
  cargo run
```

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/query` | A query request (see `src/api/wire.rs`); returns rows plus `PlanDiagnostics`. |
| `GET /v1/events/{dataset}/{event_id}` | The canonical payload of one event from `canonical_events`. |
| `GET /healthz` | 200 while ClickHouse answers, 503 otherwise. No credential needed. |

Both data endpoints need `Authorization: Bearer <read key>`. The key selects the
tenant (workspace) every statement is scoped to; a request body that names a
tenant is rejected, and another tenant's event is reported as not found. The
static key file (`<key> <tenant_id>` per line, read at startup) is a placeholder
for the tenant-policy registry, which will own key resolution, rotation,
revocation, and dataset authorization.

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
