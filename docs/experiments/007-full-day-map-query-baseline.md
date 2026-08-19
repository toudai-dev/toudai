# Experiment 007: Full-Day Map Query Baseline

- **Status:** Completed
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

The following artifacts contain the complete evidence:

- `experiment-007-warm-pilot-a`: bounded promoted-column pilot
- `experiment-007-warm-pilot-b`: remaining non-expanding fixture pilot
- `experiment-007-map-baseline`: valid warm repetitions and a superseded cold
  run
- `experiment-007-map-cold-corrected`: cold repetitions with all ClickHouse
  caches, including the query-condition cache, reset before every query
- `experiment-007-map-field-discovery`: independently bounded field discovery

The first controlled cold run exposed a benchmark defect rather than a storage
result. ClickHouse 26.3 enables the query-condition cache by default. Repeated
dynamic predicates progressively selected fewer marks because the runner had
not reset that cache. Those cold measurements are superseded by
`experiment-007-map-cold-corrected`; the warm measurements intentionally retain
the cache and remain valid.

Values below are median ClickHouse duration across three repetitions. Read
volume is the median for the same repetitions, and peak memory is the maximum
across all six cold and warm measurements.

| Fixture | ClickHouse-cold | Warm | Cold read | Warm read | Peak memory |
|---|---:|---:|---:|---:|---:|
| Recent tail | 5.03 s | 4.92 s | 21.12 GiB | 21.12 GiB | 0.18 GiB |
| Service and severity | 1.62 s | 1.56 s | 6.21 GiB | 6.21 GiB | 0.02 GiB |
| Body search | 6.93 s | 6.28 s | 26.15 GiB | 24.96 GiB | 0.11 GiB |
| Rate by service | 5.19 s | 5.13 s | 9.18 GiB | 9.18 GiB | 0.09 GiB |
| Dynamic low-cardinality filter | 86.69 s | 11.75 s | 4.94 GiB | 3.18 GiB | 1.05 GiB |
| Dynamic high-cardinality filter | 83.75 s | 36.35 s | 10.24 GiB | 9.43 GiB | 1.26 GiB |
| Dynamic conjunction | 87.91 s | 13.00 s | 9.36 GiB | 4.49 GiB | 1.15 GiB |
| Duration heatmap | 84.19 s | 76.86 s | 5.57 GiB | 5.56 GiB | 1.38 GiB |
| Percentiles by caller | 95.04 s | 66.07 s | 8.56 GiB | 7.51 GiB | 0.72 GiB |

Field discovery completed in 28.19 seconds ClickHouse-cold and 27.81 seconds
warm. It scanned all 547,356,173 events, read 19.90 GiB, returned the 100 most
frequent keys, and used 0.26 GiB peak query memory. Its cold and warm results
matched byte-for-byte.

## Correctness

Exact query outputs were byte-stable across repetitions and cache modes. The
duration heatmap consistently selected 133,820,058 samples with a duration sum
of 581,514,702,978,425. Caller sample counts and duration sums were also stable
for grouped percentiles. Histogram boundaries and t-digest values varied
across parallel runs as expected for approximate aggregate functions.

## Interpretation

Map is operationally viable at the one-day, approximately 1 TB logical scale,
but raw Map extraction alone does not meet an interactive exploration goal for
arbitrary full-day aggregations on this host.

- Promoted-column filters and scans complete in 1.6–6.9 seconds serially. A
  one-day rate dashboard is feasible, but concurrency and tighter dashboard
  latency will require pre-aggregation.
- The current sort key does not make a full-day recent-tail query cheap: it
  reads 21.12 GiB to return 100 rows. Timestamp ordering is interrupted by the
  hour bucket and service, making this a priority for the sort-key comparison.
- Cold dynamic filters take 84–88 seconds. The query-condition cache reduces
  repeated predicates to 12–36 seconds, but an ephemeral cache cannot provide
  predictable latency for ad hoc values.
- Full-day heatmaps and grouped approximate percentiles take 77–95 seconds.
  They preserve full-resolution query semantics, but dashboards need aggregate
  states and exploratory scans need asynchronous execution or explicit cost
  guardrails.
- Raw field discovery is technically possible in 28 seconds, but scanning
  every event for autocomplete is unnecessary. A bounded, time-aware field and
  value catalog remains the appropriate production path.

This evidence supports continuing with Map as the base flexible representation
while adding measured acceleration paths. It does not support promoting every
dynamic key or replacing raw events with aggregates: raw Map events remain the
full-resolution source of truth, and promoted columns, metadata catalogs,
projections, and aggregate states are derived paths selected by workload.

## Decision and Next Experiment

Advance Map to sort-key and promoted-field testing. The next experiment should
compare the current ordering with alternatives that improve latest-event access
and representative dynamic filters without regressing tenant/time pruning or
ingestion behavior. Before finalizing the schema decision, add bounded
allowed-value discovery, exact-percentile, high-cardinality group-by, trace,
and concurrency fixtures from the representative inventory.
