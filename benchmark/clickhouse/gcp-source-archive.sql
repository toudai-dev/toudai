CREATE TABLE IF NOT EXISTS benchmark.gcp_source
(
    timestamp Nullable(DateTime64(6, 'UTC')),
    timestamp_unix_nanos Nullable(Int64),
    receive_timestamp Nullable(DateTime64(6, 'UTC')),
    receive_timestamp_unix_nanos Nullable(Int64),
    severity Nullable(String),
    severity_number Nullable(Int64),
    log_name Nullable(String),
    resource_type Nullable(String),
    resource_labels Nullable(String),
    labels Nullable(String),
    insert_id Nullable(String),
    trace Nullable(String),
    span_id Nullable(String),
    trace_sampled Nullable(Bool),
    raw_entry Nullable(String),
    source_file LowCardinality(String) DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (coalesce(timestamp, toDateTime64(0, 6, 'UTC')), coalesce(insert_id, ''))
SETTINGS storage_policy = 'benchmark_archive';
