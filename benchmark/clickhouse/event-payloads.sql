CREATE TABLE IF NOT EXISTS benchmark.event_payloads
(
    tenant_id UInt64,
    dataset LowCardinality(String),
    timestamp DateTime64(6, 'UTC'),
    service_name LowCardinality(String),
    event_id String,
    source_raw String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, toStartOfHour(timestamp), service_name, timestamp, event_id)
SETTINGS storage_policy = 'benchmark_archive';
