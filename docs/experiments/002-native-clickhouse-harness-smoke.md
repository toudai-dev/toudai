# Experiment 002: Native ClickHouse Harness Smoke Test

- **Status:** Completed
- **Date:** 2026-08-16
- **Data mutation:** Created and removed a temporary smoke directory on
  `fast-storage`; no source corpus data was changed

## Question

Can the Nix-pinned ClickHouse server run as an isolated unprivileged process with
all state on a caller-selected benchmark root, accept native client queries, and
stop cleanly?

## Environment

- ClickHouse: 26.3.15.4 LTS
- Temporary root: `/mnt/fast-storage/lighthouse-smoke`
- Native TCP port: 19000
- HTTP port: 18123
- Configuration: `benchmark/clickhouse/config.xml` and `users.xml`
- Lifecycle script: `benchmark/scripts/benchmark-clickhouse`

The final quota-limited child dataset was not available because ZFS
administration requires an interactive `sudo` password on this host. The smoke
directory was therefore created directly on the writable `fast-storage` pool
root and removed after validation.

On 2026-08-16 the child datasets were created and verified with the intended
properties. The smoke test was repeated successfully against the default
`/mnt/fast-storage/lighthouse` root.

## Method

The smoke command:

1. Created isolated data, temporary, user-files, schema, log, and run paths.
2. Started ClickHouse as a daemon with path and port overrides.
3. Waited for `SELECT 1` readiness.
4. Created a temporary MergeTree table.
5. Inserted three rows.
6. Queried row count and duration sum.
7. Dropped the smoke database.
8. Stopped ClickHouse and verified no process remained.

The lifecycle was then repeated with separate `start`, `status`, `client`, and
`stop` invocations.

## Commands

```bash
export LIGHTHOUSE_BENCH_ROOT=/mnt/fast-storage/lighthouse-smoke
benchmark/scripts/benchmark-clickhouse smoke

benchmark/scripts/benchmark-clickhouse start
benchmark/scripts/benchmark-clickhouse status
benchmark/scripts/benchmark-clickhouse client \
  --query "SELECT version(), getSetting('max_threads'), getSetting('max_memory_usage')"
benchmark/scripts/benchmark-clickhouse stop
```

## Results

| Measurement | Result |
|---|---|
| Readiness | Passed |
| ClickHouse version | 26.3.15.4 |
| Inserted rows | 3 |
| Queried duration sum | 44 |
| Configured max threads | 12 |
| Per-query memory limit | 42,949,672,960 bytes |
| Clean shutdown | Passed |
| Remaining ClickHouse processes | None |
| Temporary root size before cleanup | 204 KiB |
| Temporary root cleanup | Passed |
| Final SSD dataset smoke test | Passed |

The first attempted start failed because ClickHouse treats configuration
property overrides as positional arguments after a `--` separator. The
lifecycle script was corrected and the complete smoke test then passed.

## Interpretation

- Native ClickHouse can run without a system service or container and keep all
  benchmark state off the OS disk.
- The versioned configuration and lifecycle script supported the subsequent
  source-adapter calibration.
- This smoke test validates control flow and permissions only; it provides no
  storage, ingestion, or query-performance evidence.
- The final SSD and HDD benchmark datasets are available with 750 GiB and 4 TiB
  refquotas respectively.

## Follow-ups

- Add run manifests and system-table measurement collection.
