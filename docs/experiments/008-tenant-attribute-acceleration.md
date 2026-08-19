# Experiment 008: Tenant Attribute Acceleration

- **Status:** Planned
- **Date:** 2026-08-19
- **Source mutation:** None

## Question

Can schema-stable, tenant-scoped derived tables make selected dynamic filters
and dashboard aggregations interactive without altering or sampling the
full-resolution Map event table, and what storage and build cost does that
acceleration require?

## Candidates

The experiment builds three derived tables over the completed 547,356,173-row
Map dataset:

1. A generic string attribute index containing only configured `type` and
   `workspace_id` fields. Tenant-specific configuration changes inserted rows,
   not ClickHouse columns.
2. A five-minute event-count rollup by the configured `service_name` dimension.
3. A five-minute duration-state rollup for the global duration distribution and
   configured `caller` dimension. It stores count, sum, histogram, and t-digest
   states.

These represent manual selections by an acceleration registry. They do not
assume that every Map key should be indexed or aggregated.

## Measurements

- Build duration, rows written, peak memory, and CPU time for each derived path.
- Physical bytes and amplification relative to `events_map`.
- ClickHouse-cold and warm latency, rows and bytes read, CPU, and peak memory.
- Exact output equality for filters and rates.
- Equal sample counts and sums for approximate heatmaps and percentiles.
- The effect of low versus high cardinality on index storage and lookup cost.

The experiment initially runs one cold and one warm repetition as a safety
pilot. Controlled repetitions follow only after build size and query resource
use are reviewed.

## Guardrails

- Keep at least 100 GiB free on the benchmark dataset.
- Build only the explicitly selected fields and dimensions.
- Keep the raw Map table unchanged and available for every fallback query.
- Do not run ingestion concurrently in this first acceleration experiment.
- Treat aggregate output as an approximate execution path; exact mode continues
  to query raw events.

## Results

Pending.
