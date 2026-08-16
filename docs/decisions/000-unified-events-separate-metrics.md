# Decision 000: Unify Event Storage and Separate Native Metrics

- **Status:** Accepted
- **Date:** 2026-08-16

## Context

Lighthouse should let users investigate telemetry at a higher level than the
traditional logs/traces distinction. A trace span is a structured event with
correlation and parent-child fields; a correlated log is another event in the
same timeline. Users need to filter, group, aggregate, calculate heatmaps and
percentiles, and move between raw events and trace rendering without crossing
physical signal silos.

The system must retain full-resolution events without ingestion sampling while
also supporting dashboards. Accelerators must not determine which dimensions
remain queryable.

Native metrics have different semantics: series identity, temporality,
counters, gauges, histograms, stale markers, and Prometheus/OpenTelemetry query
behavior. Honeycomb's current public model similarly treats logs and spans as
structured events while keeping native metrics in dedicated metrics datasets.

The initial Lighthouse proof of concept used separate logs and metrics tables.
Its broad per-attribute log indexes amplified 62.4M source events into 3.73B
string-index rows and still produced unsafe metadata scans. A unified event
table must not imply an unbounded inverted index over every event field.

## Decision

1. Store logs, spans, and arbitrary structured application events in one raw
   physical `events` table with a common envelope.
2. Represent event kind as semantics used for validation and rendering, not as
   a physical storage boundary.
3. Store trace correlation and parent-child fields on the common event row.
   Querying a trace can therefore return both spans and correlated logs.
4. Keep native Prometheus and OpenTelemetry metrics in signal-specific physical
   tables.
5. Retain accepted raw events at full resolution without ingestion sampling.
6. Use bounded projections, metadata catalogs, aggregate states, caches, and
   planner-selected materializations to accelerate known workloads.
7. Preserve raw fallback for queryable fields that are not represented by an
   accelerator.
8. Support approximate interactive aggregates and guarded exact aggregates,
   clearly identifying the execution mode. Approximation does not remove raw
   values.

## Alternatives Considered

### Separate logs and spans

This permits signal-specific sort keys and narrower rows, as used by ClickStack.
It was not selected because cross-event exploration and trace-correlated logs
are core product behavior rather than a UI join between independent products.
The benchmark must still prove that a unified sparse row remains performant.

### Put logs, spans, and native metrics in one table

This maximizes physical uniformity but mixes incompatible metric payloads and
query semantics into the event model. It would produce sparse rows and make
series-oriented access paths harder without improving event exploration.

### Keep only raw JSON

This preserves source fidelity but makes common routing, time, trace, and
service predicates repeatedly parse or extract dynamic paths. Common envelope
fields will be normalized into physical columns while long-tail attributes
remain flexible.

### Materialize every event attribute into an inverted index

This can accelerate selective equality lookups but has unbounded row and storage
amplification. Proof-of-concept staging evidence showed that it also fails broad
field discovery and low-selectivity queries. Only measured, bounded indexes and
catalogs are allowed.

### Sample events at ingestion

Sampling controls cost but removes individual events and unanticipated detail.
It conflicts with the full-resolution product requirement and is not selected.

## Evidence

- [Experiment 000: GCP Parquet corpus profile](../experiments/000-gcp-parquet-corpus-profile.md)
- [Unified Event Storage and Scale Validation plan](../plans/000-unified_event_storage.org)
- [Honeycomb: Traces, Metrics, and Logs](https://docs.honeycomb.io/get-started/honeycomb/traces-metrics-logs)
- [Honeycomb Telemetry Pipeline](https://www.honeycomb.io/blog/introducing-honeycomb-telemetry-pipeline)
- Proof-of-concept performance evidence in
  `~/workspace/inngest/lighthouse/docs/plans/019-performance_tuning.org`

## Consequences

### Benefits

- One query model can aggregate logs, spans, and wide application events.
- Trace rendering becomes a view over correlated event rows.
- Event-derived statistics retain access to every original event dimension.
- Native metrics remain efficient and Prometheus-compatible.

### Costs and risks

- The common event table contains fields unused by some event kinds.
- One primary sort key cannot optimize both time-scoped exploration and direct
  trace-ID lookup; a bounded secondary trace path is required.
- Arbitrary dynamic fields will not all have equal latency.
- Full-resolution retention transfers cost control to compression, tiering,
  query planning, and workload isolation.
- The schema must prevent derived tables from recreating the proof of concept's
  index amplification.

### Required follow-up

- Benchmark JSON, Map, and typed-array attributes at 1 TB.
- Benchmark a trace projection against a narrow derived trace table.
- Define dashboard aggregate states and raw fallback semantics.
- Validate the selected schema at 10 TB and 100 TB before production sizing.

## Revisit Conditions

Revisit the physical unification of logs and spans if the controlled 1 TB and
10 TB experiments show that, under the same storage budget, a unified table
cannot meet agreed ingestion or query thresholds while separate tables can. Any
replacement must preserve the unified query contract and correlated-event user
experience.
