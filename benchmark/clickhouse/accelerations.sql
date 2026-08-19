CREATE TABLE IF NOT EXISTS benchmark.accel_attribute_string
(
    tenant_id UInt64,
    dataset LowCardinality(String),
    field_name LowCardinality(String),
    field_value String,
    timestamp DateTime64(6, 'UTC'),
    event_id String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, field_name, field_value, timestamp, event_id)
SETTINGS storage_policy = 'benchmark_overflow';

CREATE TABLE IF NOT EXISTS benchmark.accel_event_count_5m
(
    tenant_id UInt64,
    dataset LowCardinality(String),
    bucket DateTime('UTC'),
    dimension_name LowCardinality(String),
    dimension_value String,
    events UInt64
)
ENGINE = SummingMergeTree(events)
PARTITION BY toDate(bucket)
ORDER BY (tenant_id, dataset, dimension_name, dimension_value, bucket)
SETTINGS storage_policy = 'benchmark_overflow';

CREATE TABLE IF NOT EXISTS benchmark.accel_duration_5m
(
    tenant_id UInt64,
    dataset LowCardinality(String),
    bucket DateTime('UTC'),
    dimension_name LowCardinality(String),
    dimension_value String,
    samples AggregateFunction(count),
    duration_sum AggregateFunction(sum, Float64),
    duration_quantiles AggregateFunction(quantilesTDigest(0.5, 0.9, 0.95, 0.99), Float64),
    duration_histogram AggregateFunction(histogram(20), Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (tenant_id, dataset, dimension_name, dimension_value, bucket)
SETTINGS storage_policy = 'benchmark_overflow';
