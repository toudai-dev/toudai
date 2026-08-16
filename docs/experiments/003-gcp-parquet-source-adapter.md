# Experiment 003: GCP Parquet Source Adapter Calibration

- **Status:** In progress
- **Started:** 2026-08-16
- **Source mutation:** None

## Question

What deterministic transformation should the schema bakeoff use to turn the
GCP-derived Parquet rows into the common event envelope without changing
attribute key semantics between candidates?

## Initial Input

The first calibration uses
`/mnt/storage/lighthouse/2026-08-01/compact-0000.parquet`:

| Measurement | Result |
|---|---:|
| Rows | 3,629,077 |
| Event-time range | 2026-08-01 04:01:32.225645–10:41:56.078300 UTC |
| Uncompressed `raw_entry` strings | 12.09 GiB |
| Populated trace IDs | 0 |
| Populated span IDs | 0 |

This file can calibrate log and wide-event ingestion, but it cannot validate
trace lookup or span-specific columns.

## Dotted-Key Finding

`resource_labels` and `labels` are Parquet JSON logical columns. Their source
objects contain flat OpenTelemetry-style keys such as `service.name` and
`k8s.pod.uid`. They also contain both `app` and
`app.kubernetes.io/managed-by` in the same object.

Allowing ClickHouse to infer these columns as `Nullable(JSON)` treats dots as
path separators. The resulting display makes the flat `app` key appear beside
a nested `app` object, and the original key boundary is no longer directly
queryable. Enabling `json_type_escape_dots_in_keys` at that point is too late
because the Parquet decoder has already constructed the JSON value.

Reading the Parquet JSON logical columns explicitly as `Nullable(String)`, then
casting them to `JSON` with `json_type_escape_dots_in_keys=1`, preserves the
keys. ClickHouse exposes the escaped dynamic paths as, for example,
`service%2Ename` and `app%2Ekubernetes%2Eio/managed-by`.

The native JSON candidate must therefore use an explicit source schema and
perform the string-to-JSON cast itself. Direct inferred Parquet-to-JSON loading
is not a valid candidate because it changes source semantics.

## Candidate-Neutral Rules Established So Far

1. Every candidate reads the same file list and row set.
2. Parquet JSON logical columns are first read as strings.
3. Attribute keys remain literal keys; dots are not interpreted as nesting.
4. Source `timestamp` and `receive_timestamp` retain microsecond precision from
   this corpus. The adapter must not imply nanosecond precision that is absent.
5. `raw_entry` remains available while retention sensitivity is measured, but
   storage results must report it separately because it duplicates normalized
   fields from the original GCP entry.
6. The adapter derives stable IDs and promoted fields identically for all three
   attribute representations.

## Still To Define Before Loading

- Exact common-envelope mapping for GCP resource labels, log labels,
  `json_payload`, `text_payload`, and `proto_payload`.
- Deterministic event-ID fallback when `insert_id` is empty.
- Representation of nested objects and arrays in the Map and typed-array
  candidates.
- Logical input-byte accounting for this transformed row set.
- A trace-bearing input fixture for correctness tests that this file cannot
  supply.

## Interpretation

Source decoding is part of the benchmark contract, not an incidental loader
detail. Without the explicit string read, the native JSON candidate would be
benchmarked against altered attributes while Map and typed-array candidates
could preserve the original flat keys.

The next step is to finalize the common-envelope mapping and encode it as a
versioned transformation before writing any candidate table.
