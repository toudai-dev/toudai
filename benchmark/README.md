# Lighthouse ClickHouse Benchmarks

The performance harness runs the Nix-pinned native ClickHouse server separately
from development services. It uses ports `19000` and `18123` and stores all
runtime state below `LIGHTHOUSE_BENCH_ROOT`.

## One-time storage setup

Create quota-limited ZFS child datasets:

```bash
scripts/benchmark-storage-setup
```

The command requires `sudo` because it creates and configures ZFS datasets. It
creates:

| Dataset | Mount | Refquota | Intended use |
|---|---|---:|---|
| `fast-storage/lighthouse` | `/mnt/fast-storage/lighthouse` | 750 GiB | Primary schema and query bakeoff |
| `storage/lighthouse-bench` | `/mnt/storage/lighthouse-bench` | 4 TiB | Later capacity and cold-tier tests |

Both datasets use `compression=off`, `atime=off`, and `recordsize=128K` for the
initial baseline. The setup script is idempotent and reasserts these properties.

## Server lifecycle

```bash
scripts/benchmark-clickhouse prepare
scripts/benchmark-clickhouse start
scripts/benchmark-clickhouse status
scripts/benchmark-clickhouse client --query 'SELECT version()'
scripts/benchmark-clickhouse stop
```

Run the lifecycle and write smoke test with:

```bash
scripts/benchmark-clickhouse smoke
```

## GCP Parquet calibration load

With the server running, load a bounded sample into the three schema candidates:

```bash
scripts/benchmark-clickhouse start
scripts/benchmark-load-gcp \
  /mnt/storage/lighthouse/2026-08-01/compact-0000.parquet 10000
```

The loader stages the Parquet JSON logical columns as strings so literal dotted
attribute keys are not interpreted as nested JSON paths. It refuses to append
to non-empty calibration tables. To deliberately replace a prior run:

```bash
LIGHTHOUSE_BENCH_RESET=1 scripts/benchmark-load-gcp \
  /mnt/storage/lighthouse/2026-08-01/compact-0000.parquet 10000
```

Omit the row limit, or pass `0`, to load the complete file. Do not use this
calibration loader for timed ingestion results yet: source staging and candidate
loads are separate, and run-manifest measurement capture is still pending.

Override the root or ports for an isolated run:

```bash
LIGHTHOUSE_BENCH_ROOT=/mnt/fast-storage/lighthouse/run-001 \
LIGHTHOUSE_CLICKHOUSE_TCP_PORT=19001 \
LIGHTHOUSE_CLICKHOUSE_HTTP_PORT=18124 \
scripts/benchmark-clickhouse smoke
```

Do not point `LIGHTHOUSE_BENCH_ROOT` at the Parquet source directory. Benchmark
results must record the ClickHouse version, configuration revision, dataset
properties, and effective environment overrides.
