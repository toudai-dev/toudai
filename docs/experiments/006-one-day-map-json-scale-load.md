# Experiment 006: One-day Map versus JSON scale load

- **Status:** Running
- **Started:** 2026-08-17T01:54:44Z

## Question

Can the Map and native JSON event layouts ingest and retain one natural,
contiguous approximately 1 TB GCP-equivalent day on the local NVMe while
preserving enough headroom for ClickHouse merges, and how do their ingestion
cost, merge behavior, and physical size compare at that scale?

## Hypothesis

Based on Experiment 005, Map should ingest materially faster, require less peak
memory, merge faster, and occupy less disk than native JSON. Native JSON should
retain its advantage for filters on known dynamic paths. This load does not by
itself decide the representation; the representative query suite must run on
the resulting data before that decision.

## Environment

- ClickHouse: Nix-pinned `26.3.17.110`
- ClickHouse data: `/mnt/fast-storage/lighthouse`, a 750 GiB-refquota ZFS child
  dataset on the Samsung SSD 980 NVMe
- Source data: `/mnt/storage/lighthouse/2026-08-05` on the HDD-backed ZFS pool
- Storage dataset properties: compression off, atime off, 128 KiB records
- Candidate order: Map, then native JSON
- Minimum free-space guardrail: 100 GiB, checked before each load unit

The run manifest and evidence directory will capture the exact repository
revision, host details, settings, parts, merges, and storage state.

## Dataset

| Property | Value |
|---|---:|
| Parquet files | 238 |
| Parquet physical bytes | 115,853,143,188 (107.90 GiB) |
| Rows | 547,356,173 |
| Provisional GCP bytes/event | 1,900 |
| GCP-equivalent logical bytes | 1,039,976,728,700 (~1.04 TB) |

This is a natural day rather than a repeated or time-shifted fixture. The
1,900-byte calibration remains provisional until the exact GCP billing metric
and period behind the reported monthly volume are verified.

## Capacity preflight

Linear projection from the 3,629,077-row Experiment 005 calibration:

| Table | Projected active size |
|---|---:|
| Staged source | 183.4 GiB |
| Map | 108.9 GiB |
| Native JSON | 125.5 GiB |
| **Total** | **417.9 GiB** |

The projection leaves about 328 GiB under the 750 GiB quota before temporary
merge amplification. A single full-candidate rewrite would still fit in that
margin, but the loader does not request `OPTIMIZE FINAL`; free space and active
merges must be monitored during the run. Linear projection is not a guarantee
because attribute cardinality and compression may differ across days.

## Method

1. Inventory every Parquet file and obtain exact footer row counts.
2. Stage files independently into `benchmark.gcp_source`; record a completion
   marker only after ClickHouse reports and stores the expected row count.
3. Derive natural event-time hours from the normalized source view.
4. Load Map and then JSON one hour at a time.
5. Validate every candidate hour using both row count and a checksum over event
   ID, event timestamp, and raw source content.
6. Capture query-log ingestion duration and peak memory per file/hour, plus
   cumulative part and disk growth after every candidate hour.
7. Stop rather than mutate data if interruption leaves a partial file or hour.
   A rerun skips completed units and recovers a completed-but-unmarked unit only
   after row-count and checksum validation.
8. Do not force final merges. Record naturally active parts and merges after the
   load, then run the scale query suite as a separate experiment phase.

The phase timings are ingestion-under-normal-background-merge measurements. The
source is staged first, so they do not include Parquet read or source
normalization time in candidate insertion latency.

## Command

```bash
nix develop --command scripts/benchmark-clickhouse start

LIGHTHOUSE_BENCH_EXPECTED_ROWS=547356173 \
LIGHTHOUSE_BENCH_CANDIDATES=map,json \
LIGHTHOUSE_BENCH_RESET=1 \
nix develop --command scripts/benchmark-load-day \
  /mnt/storage/lighthouse/2026-08-05 \
  experiment-006-2026-08-05-map-json
```

After interruption, use the same command without
`LIGHTHOUSE_BENCH_RESET=1`.

## Results

Pending.

Planned immutable artifacts:

```text
/mnt/fast-storage/lighthouse/runs/experiment-006-2026-08-05-map-json
```

## Interpretation

Pending.

## Follow-ups

- Run the representative query suite against both scale candidates.
- Compare natural merge convergence and storage after a controlled quiet
  period without forcing a final merge.
- Decide whether candidate query advantages justify JSON's expected ingestion
  and storage cost.
