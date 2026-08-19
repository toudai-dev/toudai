# Experiment 007: Full-Day Map Query Baseline

- **Status:** In progress
- **Date:** 2026-08-19
- **Source mutation:** None

## Question

What interactive and exploratory query performance does the selected Map
candidate provide over one natural day (547,356,173 events), and which access
paths require metadata catalogs, promoted fields, projections, aggregates, or
query guardrails before testing sort-key alternatives?

## Dataset and Environment

- ClickHouse: 26.3.17.110 LTS
- Source day: `/mnt/storage/lighthouse/2026-08-05`
- Rows: 547,356,173
- Event-time range: 2026-08-05 04:00:00.000024–2026-08-06
  03:59:59.999999 UTC
- Provisional GCP-equivalent logical input: 1,039,976,728,700 bytes at 1,900
  bytes/event
- Map storage: 172.45 GiB in 24 active parts
- Host: AMD Ryzen 5 3600, 6 cores and 12 threads, 62 GiB RAM
- Storage: `fast-storage/lighthouse`, ZFS compression off, 128 KiB record size

The source and loaded table are the completed Map result from experiment 006.
That experiment's manifest and file inventory identify the 238 Parquet files
and their 115,853,143,188 physical bytes.

## Method

The run is Map-only and read-only. It starts with a single-repetition warm pilot
of bounded representative fixtures. The pilot is reviewed for elapsed time,
rows and bytes read, peak memory, selected marks, CPU time, and stable result
hashes before expanding the workload.

The controlled baseline then measures both modes used in experiment 005:

- `clickhouse-cold` drops ClickHouse mark, uncompressed, query,
  query-condition, and filesystem caches before each query. The pilot found
  that ClickHouse 26.3's query-condition cache remembers granules rejected by
  prior predicates. Linux page cache and ZFS ARC remain uncontrolled, so this
  is not a host-cold measurement.
- `warm` runs one unmeasured prewarm immediately before measured repetitions.

The existing fixtures cover recent tail, promoted filters, body search, rate
time series, dynamic low- and high-cardinality filters, dynamic conjunctions,
raw field discovery, duration heatmaps, and approximate percentiles by a
dynamic field. Raw field discovery is excluded from the first pilot because it
expands every event's Map keys; it will be measured independently with an
explicit five-minute execution limit and 8 GB memory limit. The result is used
to size a bounded metadata catalog rather than treated as a production query
path.

Parameters are fixed to values established by experiment 005 for the same GCP
logging corpus. This avoids hiding unmeasured full-table group-bys in benchmark
setup:

- service: `executor`
- severity: `ERROR`
- dynamic `type`: `event_lifecycle`
- dynamic `caller`: `executor-queue`
- dynamic `workspace_id`: `306e56de-c041-4b4d-9a62-d59e742bf58b`
- body term: `error`

Each run records its manifest, rendered SQL, raw results and SHA-256 hashes,
wall timings, complete ClickHouse query log, summarized query metrics, active
parts, merge activity, settings, repository revision, and host/storage state.

## Safety and Interpretation Limits

- The initial run is serial and does not represent dashboard concurrency.
- No query runs concurrently with ingestion in this baseline.
- Approximate histogram and t-digest output may vary with parallel aggregation;
  sample counts and sums remain the correctness anchors.
- Full-day exact or high-cardinality operations must have explicit scan,
  memory, result-cardinality, and runtime limits before they are added.
- Field-name and allowed-value discovery over raw events are diagnostic
  measurements, not the proposed production implementation.

## Planned Follow-ups

1. Run and review the bounded warm pilot.
2. Run controlled ClickHouse-cold and warm repetitions for safe fixtures.
3. Measure raw field discovery separately under explicit limits.
4. Add guarded high-cardinality group-by, bounded exact percentile,
   allowed-value discovery, and deterministic trace-access fixtures.
5. Add concurrent dashboard and exploration workloads, then ingestion under
   query load.
6. Use the evidence to specify bounded metadata catalogs, promoted fields, and
   aggregate states before comparing sort keys.

## Results

Pending.
