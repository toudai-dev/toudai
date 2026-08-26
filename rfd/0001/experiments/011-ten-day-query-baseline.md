# Experiment 011: Ten-Day Query Baseline

[RFD 0001](../README.adoc) · [Experiment index](README.md)

- **Status:** Completed
- **Started:** 2026-08-25T17:20:15Z
- **Completed:** 2026-08-26T02:09:25Z
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

The controlled run `experiment-011-ten-day-raw-controlled-v1` completed all 60
measured queries: ten fixtures, two cache modes, and three repetitions. Ten
additional warm prequeries were unmeasured. The run used ClickHouse
26.3.17.110 at repository revision `2741ecd51b8f` and took 8 hours 49 minutes.

The artifact is preserved at
`/mnt/fast-storage/lighthouse/runs/experiment-011-ten-day-raw-controlled-v1`.

### Raw query latency

The table reports median client wall time over three serial repetitions. The
ClickHouse-cold mode resets ClickHouse-managed caches only; it is not a host-cold
measurement.

| Fixture | ClickHouse-cold | Warm | Warm median rows read | Warm peak memory |
|---|---:|---:|---:|---:|
| Recent tail | 60.7 s | 58.2 s | 5.69B | 1.30 GiB |
| Service and severity | 12.0 s | 12.1 s | 1.23B | 0.08 GiB |
| Body search | 75.1 s | 74.6 s | 5.29B | 0.37 GiB |
| Rate by service | 54.9 s | 55.3 s | 5.69B | 0.26 GiB |
| Dynamic low-cardinality equality | 927.5 s | 412.2 s | 1.71B warm | 1.96 GiB |
| Dynamic high-cardinality equality | 979.6 s | 484.1 s | 1.85B warm | 2.10 GiB |
| Dynamic conjunction | 950.4 s | 416.4 s | 1.70B warm | 2.15 GiB |
| Field discovery | 280.2 s | 284.1 s | 5.69B | 0.43 GiB |
| Duration heatmap | 1007.2 s | 990.8 s | 5.51B warm | 5.80 GiB |
| Percentiles by caller | 960.3 s | 726.4 s | 4.15B warm | 4.12 GiB |

Warm query-condition caching approximately halved the three selective dynamic
Map predicate medians by reducing rows read, but those queries still took 6.9
to 8.1 minutes. It did not materially accelerate broad body, rate, field
discovery, or heatmap scans. The schema-stable service/severity predicate was
the fastest raw query at approximately 12 seconds, but it still read 1.23B
rows. None of these ten-day raw paths meet an interactive sub-second or
two-second product target.

### Result stability

Fixtures 01 through 08 produced byte-identical output hashes across all six
measured repetitions. Heatmap and percentile hashes differed on every run even
though their exact sample counts and duration sums remained stable.

This is expected approximation behavior, not changing source data:

- `histogram(20)` builds adaptive buckets whose parallel merge order changes
  bucket boundaries and weights materially;
- `quantilesTDigest` is merge-order dependent, although the largest observed
  relative spread among returned quantiles was 0.40% cold and 0.66% warm.

Dashboard heatmaps therefore need fixed compatible buckets or a mergeable
sketch with an explicit error contract. Approximate percentile correctness
should be checked with numeric tolerances and disclosed error semantics rather
than byte hashes. Exact count, sum, and raw-event fixtures should continue to
require stable hashes.

### Exact payload lookup

A deterministic lookup selected the final retained event using tenant, dataset,
timestamp, service, and event identity, then queried the 708.48 GiB bulk payload
table. All six result hashes matched.

| Mode | Client wall time | ClickHouse query time | Rows read | Bytes read |
|---|---:|---:|---:|---:|
| First ClickHouse-cold lookup | 919 ms | 852 ms | 2,938 | 10.45 MB |
| Later ClickHouse-cold lookups | 71–73 ms | 12 ms | 2,938 | 10.45 MB |
| Warm lookups | 62–71 ms | 6–11 ms | 2,938 | 10.45 MB |

The first post-cache-reset bulk lookup is slower than the one-day experiment's
351 ms result, but subsequent and warm retrieval remains well below one second.
The result supports bulk payload storage for explicit exact-event drill-down;
it does not make bulk payload scans suitable for ordinary exploration.

## Interpretation

The ten-day result supports the proposed split serving model but rejects raw
full-range Map scans as the normal interactive path. Rapid exploration needs:

- time and schema-stable physical pruning before dynamic extraction;
- bounded per-tenant indexes for selective long-tail predicates;
- metadata catalogs instead of raw field-discovery scans;
- aggregate states with fixed merge semantics for dashboards, heatmaps, and
  percentiles; and
- raw fallback with cost estimates, limits, cancellation, and asynchronous
  execution for expensive exact work.

The result does not establish mixed-concurrency isolation, restart behavior,
query performance during merges, trace lookup, cold-tier aggregate routing, or
distributed fanout. Those remain separate Experiment 011 follow-ups or later
experiments.
