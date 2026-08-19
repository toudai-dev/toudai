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

Pending.
