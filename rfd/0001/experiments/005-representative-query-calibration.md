# Experiment 005: Representative Query Calibration

[RFD 0001](../README.adoc) · [Experiment index](README.md)

- **Status:** Completed
- **Date:** 2026-08-17
- **Source mutation:** None

## Question

Can the versioned query runner produce reproducible, correctness-checked
measurements across native JSON, Map, and typed-array candidates, and what
tradeoffs appear before the 1 TB bakeoff?

## Environment and Dataset

- ClickHouse: 26.3.17.110 LTS
- Query fixture implementation: `e1a2fc8b`
- Host: AMD Ryzen 5 3600, 6 cores and 12 threads, 62 GiB RAM
- Storage: `fast-storage/lighthouse`, ZFS compression off, 128 KiB record size
- Rows per candidate: 3,629,077 in one active part
- Source Parquet: 514,981,183 bytes
- Provisional GCP-equivalent logical input: 6,895,246,300 bytes at 1,900
  bytes/event
- Event-time range: 2026-08-01 04:01:32.225645–10:41:56.078300 UTC

The run manifest contains the source SHA-256, query parameters, repository
revision, ClickHouse settings, MergeTree settings, host and ZFS evidence, active
parts, raw results, wall timings, and the complete `system.query_log` records.

## Method

Ten initial fixtures cover recent tail, service/severity filtering, body search,
rate time series, dynamic low- and high-cardinality filters, dynamic
conjunctions, field discovery, duration heatmaps, and approximate percentiles by
an arbitrary dynamic field.

Two candidate orders were measured to expose order effects:

1. `experiment-005-order-a`: JSON, Map, typed arrays
2. `experiment-005-order-b`: typed arrays, Map, JSON

Each fixture ran three times per candidate in two modes:

- `clickhouse-cold` dropped ClickHouse mark, uncompressed, query, and filesystem
  caches before every measured query.
- `warm` ran an unmeasured prewarm immediately before three measured queries.

Neither mode clears the Linux page cache or ZFS ARC. The first mode is therefore
ClickHouse-cold, not host-cold. Order A exposed and led to a metadata-only fix in
revision capture: its manifest points at the then-empty working-copy child,
while its rendered SQL and evidence preserve the exact workload. Order B
correctly records query fixture revision `e1a2fc8b`.

## Correctness

Across both orders and both cache modes:

- all repetitions of tail, promoted filters, body search, rate, and dynamic
  filter fixtures were stable;
- those exact fixture outputs matched byte-for-byte across all candidates; and
- heatmap and percentile inputs had identical sample counts and duration sums.

Approximate histogram boundaries and t-digest estimates are intentionally not
required to match byte-for-byte because parallel approximate aggregation is not
deterministic. Map and typed-array field discovery matched. Native JSON field
counts differed where explicit nulls were normalized to absent paths, as already
recorded in experiments 003 and 004.

## Warm Query Results

Values are median ClickHouse query duration across three repetitions. Each cell
shows the medians from order A and order B; close pairs indicate limited order
sensitivity in these runs.

| Fixture | Native JSON | Map | Typed arrays |
|---|---:|---:|---:|
| Recent tail | 42 / 41 ms | 39 / 37 ms | 37 / 40 ms |
| Service and severity | 30 / 31 ms | 27 / 28 ms | 28 / 28 ms |
| Body search | 51 / 53 ms | 58 / 53 ms | 55 / 55 ms |
| Rate by service | 37 / 40 ms | 47 / 46 ms | 48 / 48 ms |
| Dynamic low-cardinality filter | 31 / 32 ms | 193 / 192 ms | 363 / 390 ms |
| Dynamic high-cardinality filter | 29 / 28 ms | 203 / 205 ms | 225 / 241 ms |
| Dynamic conjunction | 50 / 49 ms | 226 / 227 ms | 476 / 480 ms |
| Field discovery | 34,735 / 34,864 ms | 185 / 182 ms | 313 / 314 ms |
| Duration heatmap | 51 / 53 ms | 344 / 347 ms | 50 / 48 ms |
| Percentiles by caller | 60 / 58 ms | 321 / 323 ms | 288 / 289 ms |

ClickHouse-cold medians were close to warm medians for most fixtures. That is
consistent with the OS and ZFS caches remaining uncontrolled and means this run
does not establish host-cold latency.

## Resource Observations

- Native JSON field discovery reconstructed dynamic paths from every event. It
  read about 26.3 GiB, used approximately 5.0 GiB peak query memory, and consumed
  roughly 394 CPU-seconds per 35–37 second query.
- Map field discovery read about 129 MiB and completed in approximately 183–186
  ms. Typed arrays read about 232 MiB and completed in 313–318 ms.
- Native JSON known-path filters were 4–7× faster than Map and generally 8–12×
  faster than typed arrays for this data.
- Typed arrays matched native JSON on the integer duration heatmap, but scanning
  typed key/value arrays was expensive for string filters and grouped
  percentiles. The grouped percentile fixture read about 2.0 GiB from the typed
  table.
- Promoted-column and body queries were broadly equivalent across candidates,
  as expected because their physical access paths are the same.

## Interpretation

The runner is suitable for collecting reproducible query evidence, but this
dataset is still too small and cache-resident for a storage-format decision.

Native JSON exchanges expensive writes, merges, full-object reconstruction, and
explicit-null loss for excellent known dynamic-path reads. Map offers much
cheaper field discovery and lower write cost, with slower value extraction.
Typed arrays preserve values by type and are strong for direct numeric scans,
but linear key lookup and wide typed arrays penalize many string and grouped
queries.

Raw event scans should not implement production field discovery for any
candidate. A bounded, time-aware metadata catalog remains part of the proposed
architecture and would avoid native JSON's pathological path-reconstruction
query.

## Follow-ups

- Agree product latency, concurrency, ingestion, and storage-amplification
  thresholds before the 1 TB run.
- Add exact percentile, allowed-value discovery, high-cardinality guarded
  group-by, concurrent dashboard, and ingestion-under-query fixtures.
- Add a deterministic trace-bearing fixture and trace lookup queries.
- Define a practical host-cold method for ZFS, or explicitly scope acceptance
  tests to cache-resident and fresh-part conditions.
- Repeat the complete suite after loading the 1 TB natural subset.
