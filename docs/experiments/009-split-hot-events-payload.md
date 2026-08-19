# Experiment 009: Split Hot Events and Raw Payloads

- **Status:** Planned
- **Date:** 2026-08-19
- **Source mutation:** None

## Question

Can the exact original payload move to bulk storage while normalized Map events
remain on SSD, preserving full-resolution reconstruction and materially reducing
the hot-tier footprint before the ten-day benchmark?

## Method

Build two derived copies of the completed 547,356,173-row day:

1. `events_map_hot` contains every normalized event column except `source_raw`
   and remains on the benchmark SSD policy.
2. `event_payloads` contains tenant, dataset, timestamp, service, event ID, and
   exact raw payload on the bulk archive policy. The small service column keeps
   its ordering aligned with the ingestion-friendly hot table.

Record build time, CPU, memory, physical bytes, row and checksum equality, and
exact payload lookup behavior. Preserve all prior experiment artifacts on bulk
storage before recreating the SSD pool.

## Acceptance Criteria

- Both derived tables contain exactly 547,356,173 rows.
- Event identity and normalized-value checksums match `events_map`.
- Raw payload checksums and deterministic lookup output match byte-for-byte.
- The hot table is small enough that ten equivalent days plus measured
  acceleration remain below 65% of the planned striped SSD pool.

## Results

Pending.
