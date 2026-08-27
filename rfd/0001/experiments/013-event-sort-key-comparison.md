# Experiment 013: Event Sort-Key Comparison

[RFD 0001](../README.adoc) · [Experiment index](README.md)

- **Status:** Running
- **Started:** 2026-08-27T07:28:00Z
- **Source mutation:** None; the experiment creates disposable candidate tables

## Question

Given that every interactive event query must constrain tenant, dataset, and
time, should the normalized event table order the full timestamp immediately
after tenant and dataset instead of placing hour and service before timestamp?

## Candidates

All candidates retain daily partitions and identical columns, codecs, storage,
index granularity, source rows, and hourly part boundaries.

1. **Hour/service baseline:** `(tenant_id, dataset,
   toStartOfHour(timestamp), service_name, timestamp, event_id)`.
2. **Timestamp-first within tenant:** `(tenant_id, dataset, timestamp,
   event_id)`.
3. **Hour/timestamp compromise:** `(tenant_id, dataset,
   toStartOfHour(timestamp), timestamp, service_name, event_id)`.

Placing `toDate(timestamp)` first is not a candidate because the table is
already partitioned by that expression; it is constant within each candidate
part. Placing timestamp before tenant is also excluded because the product
contract requires tenant and dataset equality and must not interleave every
tenant in the primary query range.

## Dataset and capacity boundary

Use the natural 2026-08-05 partition from `events_map_hot`: 548,954,103 rows,
approximately 1.04 TB of provisional logical input, and 103.96 GiB in the
current physical layout.

The host has 765 GiB free before this experiment. Three additional ten-day
copies would require roughly 3 TiB and cannot fit. Three one-day candidates are
expected to consume approximately 312 GiB, leaving more than the 100 GiB safety
guardrail. This experiment can select the local ordering mechanics but cannot
alone prove ten-day part counts, cluster fanout, or long-retention merge cost.

## Method

1. Copy the same source day into each candidate through 24 serial hourly
   inserts.
2. Stop candidate-table merges before the first insert and require all 24
   active parts to remain level zero. This prevents merge timing or part layout
   from confounding the sort-order comparison.
3. Validate each candidate window against source row counts and a checksum over
   event identity, timestamp, service, severity, and body.
4. Test 5-minute, 1-hour, 6-hour, and full-day ranges for:
   - latest 100 events ordered by timestamp;
   - exact time-range count and event checksum;
   - the dominant service/severity predicate; and
   - `attributes['type'] = 'event_lifecycle'` as a raw dynamic predicate.
5. Run three ClickHouse-cold and three warm repetitions. Rotate candidate order
   across repetitions so each occupies every execution position once.
6. Require byte-identical results across all three layouts and record latency,
   rows and bytes read, marks, CPU, memory, build cost, compression, and parts.

ClickHouse-cold resets ClickHouse-managed caches but does not evict Linux page
cache or ZFS ARC, so it is not a host-cold measurement.

## Interpretation rules

- Prefer `(tenant_id, dataset, timestamp, event_id)` if it materially improves
  latest-event and narrow time-bound queries without unacceptable regression on
  service filtering or compression.
- Retain an hour bucket only if measured coarse-range pruning or compression
  justifies it.
- Do not retain `service_name` ahead of timestamp merely because one promoted
  predicate benefits; dashboards already have aggregate-state acceleration and
  selective service access can use a projection or bounded secondary path.
- Do not extrapolate full-day scans to narrow time ranges or infer ten-day
  retained performance linearly.

## Guardrails

- Preserve `events_map_hot`, `event_payloads`, and Experiment 012 acceleration
  tables unchanged.
- Preserve at least 100 GiB free on the benchmark SSD.
- Stop on a partial window, checksum mismatch, merged candidate part, or source
  row-count mismatch.
- Resume candidate merges on every normal or failed harness exit.

## Results

The controlled run is active at
`/mnt/fast-storage/lighthouse/runs/experiment-013-sort-keys-v1`.
