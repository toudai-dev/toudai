# Lighthouse ClickHouse Benchmarks

The performance harness runs the Nix-pinned native ClickHouse server separately
from development services. It uses ports `19000` and `18123` and stores all
runtime state below `LIGHTHOUSE_BENCH_ROOT`.

## One-time storage setup

Create quota-limited ZFS child datasets:

```bash
benchmark/scripts/benchmark-storage-setup
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
benchmark/scripts/benchmark-clickhouse prepare
benchmark/scripts/benchmark-clickhouse start
benchmark/scripts/benchmark-clickhouse status
benchmark/scripts/benchmark-clickhouse client --query 'SELECT version()'
benchmark/scripts/benchmark-clickhouse stop
```

Set `LIGHTHOUSE_CLICKHOUSE_OVERFLOW_PATH` when a capacity experiment needs a
second local ClickHouse disk. Tables remain on the primary disk unless their
storage policy is explicitly changed to `benchmark_overflow` and parts are
moved to `os_overflow`.

Run the lifecycle and write smoke test with:

```bash
benchmark/scripts/benchmark-clickhouse smoke
```

## GCP Parquet calibration load

With the server running, load a bounded sample into the three schema candidates:

```bash
benchmark/scripts/benchmark-clickhouse start
benchmark/scripts/benchmark-load-gcp \
  /mnt/storage/lighthouse/2026-08-01/compact-0000.parquet 10000
```

The loader stages the Parquet JSON logical columns as strings so literal dotted
attribute keys are not interpreted as nested JSON paths. It refuses to append
to non-empty calibration tables. To deliberately replace a prior run:

```bash
LIGHTHOUSE_BENCH_RESET=1 benchmark/scripts/benchmark-load-gcp \
  /mnt/storage/lighthouse/2026-08-01/compact-0000.parquet 10000
```

Omit the row limit, or pass `0`, to load the complete file. Do not use this
calibration loader for timed ingestion results yet: source staging and candidate
loads are separate, and ingestion-specific run-manifest capture is still
pending.

## Query benchmark runs

Run the versioned query fixtures against all populated candidates:

```bash
nix develop --command benchmark/scripts/benchmark-clickhouse start
nix develop --command benchmark/scripts/benchmark-run-queries experiment-005-smoke
nix develop --command benchmark/scripts/benchmark-clickhouse stop
```

By default, each fixture runs three times in both `clickhouse-cold` and `warm`
modes. `clickhouse-cold` drops ClickHouse-managed caches but does not clear the
Linux page cache or ZFS ARC; the manifest records this limitation. `warm` runs
one unmeasured prewarm before its measured repetitions.

Useful overrides:

```bash
LIGHTHOUSE_BENCH_REPETITIONS=1 \
LIGHTHOUSE_BENCH_CACHE_MODES=warm \
LIGHTHOUSE_BENCH_CANDIDATES=map,typed,json \
nix develop --command benchmark/scripts/benchmark-run-queries experiment-005-order-b
```

Run artifacts are immutable directories below
`/mnt/fast-storage/lighthouse/runs` by default. Each contains the manifest,
rendered SQL, raw results, wall timings, ClickHouse query log, settings, active
parts, hardware details, and ZFS properties. Set
`LIGHTHOUSE_BENCH_RUNS_ROOT` to place artifacts elsewhere.

## Resumable day-scale load

Stage one natural Parquet day and load Map and native JSON in hourly windows:

```bash
nix develop --command benchmark/scripts/benchmark-clickhouse start

LIGHTHOUSE_BENCH_RESET=1 \
LIGHTHOUSE_BENCH_EXPECTED_ROWS=547356173 \
nix develop --command benchmark/scripts/benchmark-load-day \
  /mnt/storage/lighthouse/2026-08-05 \
  experiment-006-1tb
```

The initial invocation deliberately truncates all calibration tables. If it is
interrupted, rerun the same command without `LIGHTHOUSE_BENCH_RESET`; completed
files and hourly candidate windows are skipped after row-count and content
checksum validation. The loader deliberately stops if an interrupted insert
left a partial unit rather than silently duplicating or mutating benchmark data.
The loader stops before starting a new unit when free space is below 100 GiB and
records phase metrics, query logs, part growth, merges, settings, and the source
file inventory.

For an isolated harness validation, use a separate benchmark root and limit the
input without changing production defaults:

```bash
LIGHTHOUSE_BENCH_MAX_FILES=1 \
LIGHTHOUSE_BENCH_ROW_LIMIT_PER_FILE=10000 \
LIGHTHOUSE_BENCH_RESET=1 \
nix develop --command benchmark/scripts/benchmark-load-day \
  /mnt/storage/lighthouse/2026-08-05 \
  day-loader-smoke
```

Override the root or ports for an isolated run:

```bash
LIGHTHOUSE_BENCH_ROOT=/mnt/fast-storage/lighthouse/run-001 \
LIGHTHOUSE_CLICKHOUSE_TCP_PORT=19001 \
LIGHTHOUSE_CLICKHOUSE_HTTP_PORT=18124 \
benchmark/scripts/benchmark-clickhouse smoke
```

Do not point `LIGHTHOUSE_BENCH_ROOT` at the Parquet source directory. Benchmark
results must record the ClickHouse version, configuration revision, dataset
properties, and effective environment overrides.

## Sort-key comparison

Compare the current hour/service ordering with timestamp-first and
hour/timestamp alternatives over one natural day retained in
`events_map_hot`:

```bash
LIGHTHOUSE_BENCH_RESET=1 \
LIGHTHOUSE_BENCH_SOURCE_DAY=2026-08-05 \
LIGHTHOUSE_BENCH_EXPECTED_ROWS=548954103 \
benchmark/scripts/benchmark-run-sort-keys experiment-013-sort-keys-v1
```

The resumable loader creates three disposable candidate tables through 24
checksum-validated hourly inserts, then runs `OPTIMIZE FINAL` so every candidate
has one compacted part before measurement. It rotates candidate execution order
and compares 5-minute, 1-hour, 6-hour, and full-day latest-event, time-range,
service/severity, and dynamic-field fixtures. The ClickHouse-cold policy does
not clear the OS page cache or ZFS ARC.

## Distributed-cluster simulation

Load one total day across a previously initialized 1-, 2-, or 4-shard native
cluster, distributed by stable event identity:

```bash
benchmark/scripts/benchmark-load-distributed-day \
  2 experiment-014-distributed-day-v1-2s
```

Run the serial distributed query matrix against that retained topology:

```bash
benchmark/scripts/benchmark-run-distributed-queries \
  2 experiment-014-distributed-query-v1-2s
```

The query runner covers 5-minute, 1-hour, 6-hour, and full-day ranges for tail,
count, service, dynamic-field, rate, heatmap, and percentile queries. It saves
coordinator and leaf-local `EXPLAIN indexes = 1` plans, coordinator and leaf
query-log metrics, result hashes, and host evidence. ClickHouse-cold resets
ClickHouse-managed caches on every process but does not clear the Linux page
cache or shared ZFS ARC. The native processes share one host, so this validates
distributed semantics and fanout behavior rather than production scale-out
capacity.
