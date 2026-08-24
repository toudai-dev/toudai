# Experiment 010: Ten-Day Split-Storage Load

- **Status:** In progress
- **Date:** 2026-08-19

## Question

Can one ClickHouse node retain approximately ten logical terabytes of natural
events by keeping normalized Map events on striped SSD and exact raw payloads
on bulk ZFS, while maintaining resumable ingestion and sufficient SSD headroom
for tenant acceleration?

## Environment

The benchmark-only `fast-storage` pool was recreated, with explicit approval,
from a two-disk mirror as a two-vdev stripe:

- Samsung 870 QVO 1 TB
- Samsung 870 EVO 1 TB
- ZFS usable size: 1.81 TiB
- Compression: off
- Record size: 128 KiB
- `atime`: off
- Autotrim: on
- Redundancy: none; either SSD failure loses the benchmark pool

All prior experiment evidence was copied to
`storage/lighthouse-bench/preserved-before-stripe-2026-08-19` and 1,115 regular
files were checksum-verified before pool destruction.

## Method

Load the first ten natural day directories from `/mnt/storage/lighthouse`.
Only one day is staged at a time in `gcp_source` on bulk storage. Each staged day
is transformed in hourly windows into:

- normalized `events_map_hot` on striped SSD; and
- exact `event_payloads` on bulk ZFS.

The loader records file, window, row, query-log, storage, and free-space
evidence and is resumable at file and hourly target boundaries. It truncates
the decoded source stage after both targets for a day are complete.

## Results

In progress. Seven natural days are complete and checksum-validated:

| Measurement | Current result |
|---|---:|
| Completed source rows | 4,221,491,105 |
| GCP-equivalent logical bytes | 8,020,515,756,500 (~8.02 TB) |
| Normalized hot bytes | 786,923,624,252 (732.88 GiB) |
| Exact payload bytes | 559,292,133,452 (520.88 GiB) |

The byte measurements are cumulative after Day 7 and may decrease as background
merges finish. Day 8 is not included until both target tables pass every window
check and the daily summary is durable.

### Recovery evidence

The run has preserved validated boundaries through three interruptions:

1. After Day 1, ClickHouse's large-table safety limit rejected staging-table
   truncation. The loader now supplies the explicit size override and resumed
   without reloading the validated targets.
2. After Day 5, staging-table truncation completed server-side but the original
   loader did not preserve the client exit reason. The loader now records its
   phase, command, line, exit code, and combined output on failure.
3. During Day 8, replacement of the benchmark executor killed ClickHouse and the
   loader without allowing the shell failure trap to run. Sixteen hot windows
   and fifteen payload windows had durable validation markers. The interrupted
   payload insert contained 2,468,087 unvalidated rows; they were removed with
   a bounded lightweight delete before replay.

The third recovery exposed two loader defects. Additional July source data had
changed the first ten lexically sorted directories, so resume initially failed
its immutable configuration check. Resume now reads the persisted day list from
the run configuration. A subsequent attempt incorrectly treated the active Day
8 staging table as leaked data from an earlier completed day and truncated only
that rebuildable staging copy. Completed days now skip cleanup, and the first
incomplete day distinguishes missing active staging from a previous day's
untruncated stage using its file markers.

Day 8 source staging is currently being rebuilt. Its validated hot and payload
windows remain intact and will be rechecked before loading continues. The
original process used ClickHouse 26.3.15.4; recovery after the executor
replacement uses the Nix-updated 26.3.17.110. Query and throughput comparisons
must therefore identify the patch-version boundary.
