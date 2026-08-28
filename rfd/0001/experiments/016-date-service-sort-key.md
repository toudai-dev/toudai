# Experiment 016: Date/Service Sort-Key Comparison

[RFD 0001](../README.adoc) · [Experiment index](README.md)

- **Status:** Running
- **Started:** 2026-08-28T16:27:04Z
- **Source mutation:** None; the experiment creates one disposable candidate

## Question

What are the measured storage and query characteristics of placing
`toDate(timestamp)` and `service_name` before the full timestamp, rather than
inferring them from the fact that the table already uses daily partitions?

## Candidates

Date after the required tenant and dataset prefix:

```sql
PARTITION BY toDate(timestamp)
ORDER BY (
    tenant_id,
    dataset,
    toDate(timestamp),
    service_name,
    timestamp,
    event_id
)
```

Date as the first sorting expression:

```sql
PARTITION BY toDate(timestamp)
ORDER BY (
    toDate(timestamp),
    tenant_id,
    dataset,
    service_name,
    timestamp,
    event_id
)
```

Within a daily part, the date expression is constant, so both are expected to
have the same row ordering and service-first behavior. The date-leading variant
would not be acceptable if partitions could mix dates because it would place
time before the tenant-isolation prefix. Those expectations are hypotheses
being tested, not results.

## Method

1. Use the same 2026-08-05 source day as Experiment 013: 548,954,103 rows and
   approximately 1.04 TB of provisional logical input.
2. Load each candidate through 24 hourly windows, validating row count and event
   checksum after every insert.
3. Compact to one active part before measurement so part layout does not
   confound the comparison.
4. Run the same latest-tail, time-range count, service/severity, and dynamic
   `Map` filter fixtures over 5-minute, 1-hour, 6-hour, and 24-hour ranges.
5. Run three ClickHouse-cold and three warm repetitions and capture wall time,
   rows, bytes, marks, CPU, and memory.
6. Require every result hash to match the timestamp-first results from
   Experiment 013.
7. Preserve `EXPLAIN indexes = 1` for the representative full-day queries.

ClickHouse-cold resets ClickHouse-managed caches but not Linux page cache or ZFS
ARC. The run remains a single-host storage-layout test, not a distributed
capacity result.

## Guardrails

- Preserve the source, ten-day tables, Experiment 013 candidates, and immutable
  run artifacts.
- Preserve at least 100 GiB free on the benchmark SSD.
- Stop on a checksum mismatch, reference-result mismatch, partial window, or
  final part-count mismatch.

## Results

Pending.
