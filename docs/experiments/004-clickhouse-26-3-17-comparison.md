# Experiment 004: ClickHouse 26.3.17 Patch Comparison

- **Status:** Completed
- **Date:** 2026-08-16
- **Source mutation:** None

## Question

Does updating the Nix-pinned ClickHouse LTS package from 26.3.15.4 to
26.3.17.110 materially change the initial native JSON, Map, or typed-array
calibration results?

## Change Under Test

- Baseline ClickHouse: 26.3.15.4 LTS
- Comparison ClickHouse: 26.3.17.110 LTS
- Nix update commit: `61cd7663`
- Comparison Nixpkgs revision: `e5bdc4a41d4c072fe1e3787eaa0320a384741d44`

The ambient development shell still resolved ClickHouse 26.3.15.4. Every server
and client command for this run therefore executed explicitly through
`nix develop`. The server reported 26.3.17.110 before the load began.

## Fixed Conditions

- Source: all 3,629,077 rows in
  `/mnt/storage/lighthouse/2026-08-01/compact-0000.parquet`
- Candidate schema and source adapter: unchanged from experiment 003
- CPU: AMD Ryzen 5 3600, 6 cores and 12 threads
- Memory limit: 40 GiB per query
- Storage: `fast-storage/lighthouse`, ZFS compression off, 128 KiB record size
- ClickHouse JSON serialization: v3
- Merged JSON shared-data serialization: `advanced`
- Level-zero JSON shared-data serialization: `map_with_buckets`

The tables were truncated, the source was staged once, and the candidates were
loaded sequentially in JSON, Map, typed-array order. Each table was then forced
to one active part with `OPTIMIZE FINAL`. Durations and peak query memory came
from `system.query_log`; storage came from the final active part.

## Correctness

All candidates again produced:

- 3,629,077 rows;
- zero promoted-service mismatches;
- duration sum 5,343,709,954,117; and
- common-field checksum 8,077,539,968,839,479,491.

Native JSON still omitted explicit null paths for all 659,314 source rows with
at least one top-level null attribute. The patch does not change that semantic
limitation.

## Results

Negative deltas mean the updated version used less time or storage.

| Candidate | Insert 26.3.15 | Insert 26.3.17 | Delta | Merge 26.3.15 | Merge 26.3.17 | Delta | Storage 26.3.15 | Storage 26.3.17 | Delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Native JSON | 229.238 s | 228.429 s | -0.35% | 425.942 s | 416.605 s | -2.19% | 895,001,985 B | 893,531,129 B | -0.16% |
| Map | 43.179 s | 46.384 s | +7.42% | 18.920 s | 18.012 s | -4.80% | 775,363,865 B | 775,358,431 B | -0.00% |
| Typed arrays | 44.784 s | 47.940 s | +7.05% | 18.203 s | 18.149 s | -0.30% | 797,066,333 B | 797,225,641 B | +0.02% |

Peak query memory was effectively unchanged at the precision reported here:
2.44 GiB for native JSON, 1.17 GiB for Map, and 1.28 GiB for typed arrays. The
complete staging and three-candidate loader took 367.617 seconds versus 363.761
seconds in the baseline run, but this combined wall time is not an accepted
ingestion metric.

## Interpretation

This patch update does not materially change the initial conclusion. Native
JSON remains approximately five times slower to insert, more than twenty times
slower to merge, and about 15% larger than Map for this file and configuration.

The single-run improvements in JSON merge time and Map merge time are too small
to distinguish from cache, merge scheduling, CPU-frequency, and other run-order
effects. The slower Map and typed-array inserts likewise require repeated,
order-rotated measurements before being called a regression.

The controlled benchmark should use 26.3.17.110 going forward because it is the
current pinned LTS patch, but upgrading alone does not resolve the JSON write and
merge cost observed in experiment 003.
