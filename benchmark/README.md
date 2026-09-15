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
capacity. New runs also sample cumulative CPU, block-device, memory, CPU
frequency, and load counters once per second under `evidence/host-samples` so
device utilization, queueing, iowait, and throttling can be calculated without
inferring them from ClickHouse latency.

Build the configured attribute indexes and five-minute aggregate states on each
leaf, then run the accelerated distributed query matrix:

```bash
LIGHTHOUSE_BENCH_RESET=1 \
benchmark/scripts/benchmark-run-distributed-accelerations \
  2 experiment-014-distributed-acceleration-v1-2s
```

The build runs each phase concurrently on all leaves, preserving per-leaf query
metrics and host samples. Accelerated exact results are checked against the
matching retained raw topology; heatmap and percentile results compare exact
sample-count and duration-sum anchors because histogram and t-digest byte
encodings can vary with distributed merge order.

## GCP cloud benchmark

Experiment 015's cost-free preparation lives under
[`benchmark/cloud/gcp`](cloud/gcp/README.adoc). Activate the committed Flox
environment to use the locked ClickHouse, OpenTofu, Packer, Google Cloud SDK,
and host-profiling tools:

```bash
flox activate
clickhouse local --version
tofu version
packer version
```

The OpenTofu network plan and Packer image template can be initialized and
validated without creating cloud resources. Do not apply a plan, build an
image, or upload a corpus until the Experiment 015 spend and data-handling gates
are explicitly approved.

Stage B's multi-host topology and immutable corpus inventory live under
`benchmark/cloud/gcp/stage-b`. Its loader streams Parquet objects through the
same normalized event transform used by the local experiments and inserts into
the coordinator's `Distributed` table:

```bash
LIGHTHOUSE_CLUSTER_COORDINATOR_HOST=10.142.0.10 \
LIGHTHOUSE_CLUSTER_LEAF_HOSTS=10.142.0.11,10.142.0.12 \
LIGHTHOUSE_BENCH_RUNS_ROOT="$HOME/lighthouse-stage-b/runs" \
LIGHTHOUSE_BENCH_REVISION="$(jj log -r @- --no-graph -T commit_id)" \
benchmark/scripts/benchmark-load-gcp-distributed \
  2 experiment-015-stage-b-load-v1-2s
```

The loader validates the remote object count and bytes before each day, checks
the resulting day by cumulative row count and normalized-event checksum, and
resumes at a validated day boundary. A partial source batch is refused instead
of being deleted, because a source directory can overlap adjacent event-time
partitions and only source provenance could bound the removal. Completed-day
markers and final run evidence are copied to the private experiment bucket.
`LIGHTHOUSE_GCP_CORPUS_INVENTORY` overrides the committed inventory path. Each
batch gate pays one checksum scan of the accumulated corpus; the pre-batch probe
is a metadata-only row count, so a run does not pay two full-table scans per
day. `benchmark/scripts/benchmark-load-gcp-distributed-test` exercises the
fresh-load, resume, recovery, and partial-batch paths against a local
single-node ClickHouse instance.

The existing distributed query and acceleration runners use the remote hosts
when given the compatible adapter. A Stage B run sets the exact load/run IDs,
5.69B-row expectation, end time, and range matrix explicitly:

```bash
export LIGHTHOUSE_DISTRIBUTED_CLUSTER_COMMAND="$PWD/benchmark/scripts/benchmark-clickhouse-remote-cluster"
export LIGHTHOUSE_BENCH_ROOT="$HOME/lighthouse-stage-b"
export LIGHTHOUSE_BENCH_RUNS_ROOT="$LIGHTHOUSE_BENCH_ROOT/runs"
export LIGHTHOUSE_DISTRIBUTED_LOAD_RUN=experiment-015-stage-b-load-v1-2s
export LIGHTHOUSE_BENCH_EXPECTED_ROWS=5689254359
export LIGHTHOUSE_DISTRIBUTED_DAY_END='2026-08-11 04:00:00'
export LIGHTHOUSE_DISTRIBUTED_RANGES=5m,1h,6h,24h,10d
export LIGHTHOUSE_DISTRIBUTED_HOST_SAMPLER=0
benchmark/scripts/benchmark-run-distributed-queries \
  2 experiment-015-stage-b-query-v1-2s
```

The cloud run disables the load-generator-only host sampler; startup evidence
and provider metrics must be collected for every coordinator and leaf instead.

Capture the topology's metered usage and price it with the current public
catalog before destroying it:

```bash
benchmark/scripts/benchmark-gcp-stage-b-cost \
  experiment-015-stage-b-v1-2s .tmp/experiment-015/stage-b/v1-2s/cost
```

The cost report reads the live instance inventory, the
`compute.googleapis.com/instance/uptime` metered series, and the Cloud Billing
Catalog SKUs for the shapes in use. It prices instance core, RAM, integrated
local SSD, and boot-disk hours, and it records the bucket's object bytes for
the shared storage that is not attributable to one topology. It is a
listed-price report for metered usage, not a metered invoice: without a billing
export the exact invoiced amount remains unverifiable.
