# Experiment 012: Ten-Day Acceleration Validation

[RFD 0001](../README.adoc) · [Experiment index](README.md)

- **Status:** Running
- **Started:** 2026-08-26T03:48:03Z
- **Source mutation:** None; the experiment creates disposable derived tables

## Question

Do the bounded tenant acceleration paths selected at one day preserve their
storage efficiency, build feasibility, correctness anchors, and interactive
latency over the completed 5,689,254,359-row, approximately 10.81 TB logical
corpus?

## Hypothesis

The generic two-field attribute index should grow roughly with field prevalence
and remain a small percentage of the 1,000.85 GiB normalized event table.
Five-minute count and duration aggregate states should remain much smaller than
the raw corpus and answer dashboard-shaped queries in interactive time.

The experiment does not assume linear build time or query latency. Part count,
merge work, cache behavior, and high-cardinality value distribution may change
at ten days.

## Candidates

Rebuild the same manually selected accelerations from Experiment 008 against
`benchmark.events_map_hot`:

1. Generic string-attribute rows for configured `type` and `workspace_id`
   fields.
2. Five-minute event counts by `service_name`.
3. Five-minute duration count, sum, adaptive histogram, and t-digest states for
   the global distribution and configured `caller` dimension.

These are schema-stable shared tables keyed by tenant, dataset, field or
dimension name, and value. They do not create tenant-specific ClickHouse
columns and do not index every discovered field.

## Method

1. Record the source table, row count, parts, storage, host state, ClickHouse
   settings, and repository revision.
2. Drop only the three disposable acceleration tables from Experiment 008 and
   recreate them on the benchmark SSD policy.
3. Build each selected path serially while recording elapsed time, rows and
   bytes written, CPU, memory, and part growth.
4. Run raw and accelerated versions of two dynamic equality filters, rate by
   service, duration heatmap, and percentiles by caller.
5. Run three repetitions under ClickHouse-cold and warm cache policies.
6. Require byte equality for exact filter and rate outputs. For approximate
   aggregates, require identical sample counts and duration sums and report
   histogram or t-digest variation separately.

The selected fixture values match Experiment 011:

- `type = event_lifecycle`;
- `workspace_id = 7b10263c-4daa-44e4-9dde-d15c404b23e2`.

## Guardrails

- Preserve the raw hot-event and exact payload tables unchanged.
- Preserve at least 100 GiB free on the SSD dataset.
- Build and query serially; do not overlap external benchmark workloads.
- Stop on a partial build, correctness mismatch, unexpected source row count,
  or storage guardrail violation.
- Do not infer sustainable ingestion maintenance from asynchronous backfill.
- Keep activation manual; this experiment does not justify automatic tenant
  acceleration policy.

## Acceptance Criteria

- Every source query uses the ten-day `events_map_hot` table and records the
  expected 5,689,254,359 rows.
- All derived paths complete without changing source-table counts or checksums.
- Exact outputs match raw execution; approximate anchors match.
- Total derived storage and build resources are measured and remain within host
  guardrails.
- Accelerated medians and raw fallbacks are reported independently for both
  cache modes.

## Results

The controlled run is active at
`/mnt/fast-storage/lighthouse/runs/experiment-012-ten-day-acceleration-v1`.
