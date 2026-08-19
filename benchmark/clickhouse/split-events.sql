CREATE TABLE IF NOT EXISTS benchmark.events_map_hot
(
    tenant_id UInt64,
    dataset LowCardinality(String),
    timestamp DateTime64(6, 'UTC'),
    observed_at DateTime64(6, 'UTC'),
    event_id String,
    event_kind Enum8('event' = 1, 'log' = 2, 'span' = 3),
    event_name LowCardinality(String),
    body String,
    service_name LowCardinality(String),
    environment LowCardinality(String),
    severity LowCardinality(String),
    status_code LowCardinality(String),
    trace_id String,
    span_id String,
    parent_span_id String,
    span_kind LowCardinality(String),
    duration_ns UInt64,
    resource Map(LowCardinality(String), String),
    scope Map(LowCardinality(String), String),
    attributes Map(LowCardinality(String), String)
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, toStartOfHour(timestamp), service_name, timestamp, event_id)
SETTINGS storage_policy = 'benchmark_overflow';
