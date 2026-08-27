# Experiment Records

[RFD 0001](../README.adoc)

Experiment records preserve enough context to reproduce and interpret a result.
Use a zero-padded sequence and descriptive name, for example
`001-json-vs-map-1tb.md`.

Each record should contain:

1. **Status and date** — planned, running, completed, invalidated, or superseded.
2. **Question** — the specific uncertainty being tested.
3. **Hypothesis** — optional, but written before inspecting results.
4. **Environment** — hardware, ClickHouse version/settings, storage, caches, and
   relevant repository revision.
5. **Dataset** — source, time range, transformation revision, rows, logical input
   bytes, and expected limitations.
6. **Method** — schema revision, load procedure, query fixture IDs, repetitions,
   and cold/warm cache policy.
7. **Commands** — runnable commands or links to versioned scripts.
8. **Results** — raw measurements and artifact locations, not only conclusions.
9. **Interpretation** — what the results support and what they do not prove.
10. **Follow-ups** — unresolved questions and the decision records that consume
    the evidence.

Do not overwrite prior measurements when rerunning an experiment under different
conditions. Add a new run or record and link the relationship.

## Index

| ID | Status | Experiment |
|---|---|---|
| 000 | Completed | [GCP Parquet corpus profile](000-gcp-parquet-corpus-profile.md) |
| 001 | Completed | [Local benchmark host profile](001-local-benchmark-host-profile.md) |
| 002 | Completed | [Native ClickHouse harness smoke test](002-native-clickhouse-harness-smoke.md) |
| 003 | In progress | [GCP Parquet source adapter calibration](003-gcp-parquet-source-adapter.md) |
| 004 | Completed | [ClickHouse 26.3.17 patch comparison](004-clickhouse-26-3-17-comparison.md) |
| 005 | Completed | [Representative query calibration](005-representative-query-calibration.md) |
| 006 | Completed | [One-day Map versus JSON scale load](006-one-day-map-json-scale-load.md) |
| 007 | Completed | [Full-day Map query baseline](007-full-day-map-query-baseline.md) |
| 008 | Completed | [Tenant attribute acceleration](008-tenant-attribute-acceleration.md) |
| 009 | Completed | [Split hot events and raw payloads](009-split-hot-events-payload.md) |
| 010 | Completed | [Ten-day split-storage load](010-ten-day-split-storage-load.md) |
| 011 | Completed | [Ten-day query baseline](011-ten-day-query-baseline.md) |
| 012 | Completed | [Ten-day acceleration validation](012-ten-day-acceleration-validation.md) |
| 013 | Planned | [Event sort-key comparison](013-event-sort-key-comparison.md) |
