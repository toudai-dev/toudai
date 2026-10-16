-- Decision 0001 D1/D2/D4: serving rows for spans. Same key, partition, epoch, and
-- identifier rules as serving_logs.
CREATE TABLE IF NOT EXISTS lighthouse.serving_spans
(
    tenant_id String,
    dataset LowCardinality(String),
    event_id String,
    routing_epoch UInt16,
    -- Both supplied timestamps are preserved: `timestamp` is the span start and
    -- `end_ts` is the supplied end (0 when OTLP carries no timing), so a span
    -- whose end precedes its start stays searchable by its real timestamps.
    timestamp DateTime64(9),
    end_ts DateTime64(9),
    kind LowCardinality(String),
    service_name String,
    trace_id String,
    span_id String,
    parent_span_id String,
    span_name String,
    span_kind LowCardinality(String),
    status_code Int32,
    -- Derived duration, NULL whenever it is not observable: an unset OTLP
    -- timestamp (0; proto3 fixed64 has no presence, so an explicit 0 is
    -- indistinguishable) on either side, or a reversed span (both set, end <
    -- start). Missing timing takes precedence over the reversed classification:
    -- a missing start must not fabricate a duration from epoch 0 and a missing
    -- end must not be read as end < start. A valid zero duration stays 0. Never
    -- a zero substitute and never an unsigned underflow (Experiment 018
    -- measured 18446744073709550616 for a span whose end precedes its start).
    duration_ns Nullable(Int64),
    -- Machine-readable timing diagnostic: 'ok' (observable duration, including
    -- a valid zero), 'missing' (start or end unset), or 'end_before_start'
    -- (data error; the span is preserved with an unavailable duration).
    span_timing_status LowCardinality(String),
    resource Map(LowCardinality(String), String),
    resource_types Map(LowCardinality(String), LowCardinality(String)),
    -- The instrumentation scope namespace of a span is `span_scope`; there is no
    -- duplicated `scope` column.
    span_scope Map(LowCardinality(String), String),
    span_scope_types Map(LowCardinality(String), LowCardinality(String)),
    span_int Map(LowCardinality(String), Int64),
    span_double Map(LowCardinality(String), Float64),
    span_string Map(LowCardinality(String), String),
    span_bool Map(LowCardinality(String), Bool),
    span_other Map(LowCardinality(String), LowCardinality(String)),
    payload_sha256 String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, timestamp, event_id);
