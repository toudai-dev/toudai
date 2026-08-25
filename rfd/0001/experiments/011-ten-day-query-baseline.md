# Experiment 011: Ten-Day Query Baseline

[RFD 0001](../README.adoc) · [Experiment index](README.md)

- **Status:** Running
- **Date:** 2026-08-25
- **Source mutation:** None

## Question

How do representative raw exploration queries and exact payload lookup behave
over the completed 5,689,254,359-row, approximately 10.81 TB logical corpus,
before adding ten-day tenant accelerations?

## Method

Retain the completed Experiment 010 tables unchanged:

- `events_map_hot` contains normalized query-serving events on striped SSD;
- `event_payloads` contains exact payloads on bulk ZFS.

Reuse the versioned query fixtures and query-log capture from the one-day Map
baseline. Run a one-repetition warm safety pilot over the four common fixtures,
then run controlled ClickHouse-cold and warm repetitions. ClickHouse-cold drops
ClickHouse caches but does not claim to evict the OS page cache or ZFS ARC.

The controlled raw suite will cover recent tail, promoted filters, body search,
rate, dynamic equality and conjunction, bounded field discovery, heatmap, and
percentiles. Exact payload lookup will use a deterministic tenant, dataset,
timestamp, service, and event identity and record result hashes and query-log
metrics.

## Guardrails

- Run serially and read-only.
- Preserve at least 100 GiB free on both storage tiers.
- Record result hashes for correctness across repetitions.
- Bound field discovery, result groups, memory, and execution time.
- Stop the pilot before controlled repetitions if a query threatens host
  availability or reveals an invalid fixture assumption.
- Treat the Experiment 010 patch-version boundary as an ingestion limitation;
  all Experiment 011 queries run on ClickHouse 26.3.17.110.

## Results

Pending.
