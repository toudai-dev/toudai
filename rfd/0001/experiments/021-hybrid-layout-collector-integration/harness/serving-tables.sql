-- Experiment 021 provisional hybrid serving + canonical layout. {{DB}} is
-- substituted by run.sh (lh021). The layout is PROVISIONAL for this PoC trial:
-- it follows Experiment 020's minimal native typed record layout and Experiment
-- 009's separate canonical payload, and it is not a retained-scale-qualified
-- production DDL.
--
-- Serving rows and canonical payloads are separate tables (Experiment 009).
-- There is no cross-table atomicity; the owned exporter writes canonical first
-- and reports per-destination outcomes (see the experiment report).
--
-- Record attributes carry native typed value Maps plus a key-only `other` map
-- for the canonical-only types (bytes, array, map) and explicit empty.
-- Resource and scope attributes keep the stock-compatible string value Map plus
-- a companion type Map (Experiment 020). Spans keep the span scope-attribute
-- namespace in its own string + type Maps instead of dropping it the way the
-- stock traces schema does.

DROP TABLE IF EXISTS {{DB}}.canonical_events;
CREATE TABLE {{DB}}.canonical_events (
    workspace_id String,
    dataset LowCardinality(String),
    signal LowCardinality(String),
    event_id String,
    ts DateTime64(9),
    schema_version LowCardinality(String),
    payload String,
    payload_sha256 String
) ENGINE = MergeTree ORDER BY (workspace_id, dataset, signal, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.serving_logs;
CREATE TABLE {{DB}}.serving_logs (
    workspace_id String,
    dataset LowCardinality(String),
    event_id String,
    ts DateTime64(9),
    observed_ts DateTime64(9),
    kind LowCardinality(String),
    service_name String,
    severity_text LowCardinality(String),
    severity_number Int32,
    body String,
    trace_id String,
    span_id String,
    event_name String,
    resource Map(LowCardinality(String), String),
    scope Map(LowCardinality(String), String),
    resource_types Map(LowCardinality(String), LowCardinality(String)),
    scope_types Map(LowCardinality(String), LowCardinality(String)),
    record_int Map(LowCardinality(String), Int64),
    record_double Map(LowCardinality(String), Float64),
    record_string Map(LowCardinality(String), String),
    record_bool Map(LowCardinality(String), Bool),
    record_other Map(LowCardinality(String), LowCardinality(String)),
    payload_sha256 String
) ENGINE = MergeTree ORDER BY (workspace_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.serving_spans;
CREATE TABLE {{DB}}.serving_spans (
    workspace_id String,
    dataset LowCardinality(String),
    event_id String,
    ts DateTime64(9),
    kind LowCardinality(String),
    service_name String,
    trace_id String,
    span_id String,
    parent_span_id String,
    span_name String,
    span_kind LowCardinality(String),
    status_code Int32,
    -- Explicit: a malformed negative span is rejected by the mapping component,
    -- and missing timing is NULL rather than a UInt64 underflow (Experiment 018
    -- measured 18446744073709550616 for a span whose end precedes its start).
    duration_ns Nullable(Int64),
    resource Map(LowCardinality(String), String),
    resource_types Map(LowCardinality(String), LowCardinality(String)),
    -- A span's instrumentation scope namespace is named `span_scope` here, as in
    -- Experiments 019/020; the layout keeps one column for it instead of a
    -- duplicated `scope` column.
    span_scope Map(LowCardinality(String), String),
    span_scope_types Map(LowCardinality(String), LowCardinality(String)),
    span_int Map(LowCardinality(String), Int64),
    span_double Map(LowCardinality(String), Float64),
    span_string Map(LowCardinality(String), String),
    span_bool Map(LowCardinality(String), Bool),
    span_other Map(LowCardinality(String), LowCardinality(String)),
    payload_sha256 String
) ENGINE = MergeTree ORDER BY (workspace_id, dataset, ts, event_id);
