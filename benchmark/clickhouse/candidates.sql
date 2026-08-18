CREATE DATABASE IF NOT EXISTS benchmark;

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
ORDER BY (coalesce(timestamp, toDateTime64(0, 6, 'UTC')), coalesce(insert_id, ''));

-- Existing calibration roots predate resumable multi-file staging.
ALTER TABLE benchmark.gcp_source
    ADD COLUMN IF NOT EXISTS source_file LowCardinality(String) DEFAULT '';

CREATE OR REPLACE VIEW benchmark.gcp_events AS
WITH
    assumeNotNull(raw_entry) AS source_raw,
    JSONExtractRaw(source_raw, 'json_payload') AS json_payload,
    JSONExtractRaw(source_raw, 'proto_payload') AS proto_payload,
    if(json_payload NOT IN ('', 'null', '{}'), json_payload,
       if(proto_payload NOT IN ('', 'null', '{}'), proto_payload, '{}')) AS attributes_json,
    JSONExtractKeysAndValuesRaw(attributes_json) AS attribute_pairs,
    arrayMap(pair -> (
        pair.1,
        JSONType(pair.2),
        if(JSONType(pair.2) IN ('Object', 'Array', 'Null'), pair.2, JSON_VALUE(pair.2, '$'))
    ), attribute_pairs) AS attribute_entries,
    JSONExtract(assumeNotNull(labels), 'Map(String, String)') AS label_map,
    JSONExtract(assumeNotNull(resource_labels), 'Map(String, String)') AS gcp_resource_map,
    mapConcat(
        label_map,
        map('gcp.resource.type', assumeNotNull(resource_type)),
        mapFromArrays(
            arrayMap(key -> concat('gcp.resource.', key), mapKeys(gcp_resource_map)),
            mapValues(gcp_resource_map)
        )
    ) AS resource_map
SELECT
    toUInt64(1) AS tenant_id,
    CAST('gcp-logging' AS LowCardinality(String)) AS dataset,
    coalesce(timestamp, toDateTime64(fromUnixTimestamp64Nano(timestamp_unix_nanos, 'UTC'), 6, 'UTC')) AS event_timestamp,
    coalesce(receive_timestamp, toDateTime64(fromUnixTimestamp64Nano(receive_timestamp_unix_nanos, 'UTC'), 6, 'UTC')) AS observed_at,
    if(notEmpty(assumeNotNull(insert_id)), assumeNotNull(insert_id),
       hex(sipHash128(timestamp_unix_nanos, assumeNotNull(log_name), source_raw))) AS event_id,
    CAST('log' AS Enum8('event' = 1, 'log' = 2, 'span' = 3)) AS event_kind,
    coalesce(nullIf(JSONExtractString(attributes_json, 'event'), ''), assumeNotNull(log_name)) AS event_name,
    coalesce(
        nullIf(JSONExtractString(json_payload, 'message'), ''),
        nullIf(JSONExtractString(source_raw, 'text_payload'), ''),
        ''
    ) AS body,
    CAST(JSONExtractString(assumeNotNull(labels), 'service.name') AS LowCardinality(String)) AS service_name,
    CAST(coalesce(
        nullIf(JSONExtractString(assumeNotNull(labels), 'service.namespace'), ''),
        JSONExtractString(assumeNotNull(resource_labels), 'namespace')
    ) AS LowCardinality(String)) AS environment,
    CAST(assumeNotNull(severity) AS LowCardinality(String)) AS severity,
    CAST('' AS LowCardinality(String)) AS status_code,
    arrayElement(splitByChar('/', assumeNotNull(trace)), -1) AS trace_id,
    assumeNotNull(span_id) AS span_id,
    '' AS parent_span_id,
    CAST('' AS LowCardinality(String)) AS span_kind,
    toUInt64(0) AS duration_ns,
    resource_map,
    '{}' AS scope_json,
    attributes_json,
    attribute_entries,
    source_raw,
    source_file
FROM benchmark.gcp_source;

-- ClickHouse JSON normalizes explicit null attributes to absent paths. The raw
-- source remains available for exact reconstruction while this tradeoff is measured.
CREATE TABLE IF NOT EXISTS benchmark.events_json
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
    resource JSON(max_dynamic_paths = 256),
    scope JSON(max_dynamic_paths = 32),
    attributes JSON(max_dynamic_paths = 2048),
    source_raw String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, toStartOfHour(timestamp), service_name, timestamp, event_id);

CREATE TABLE IF NOT EXISTS benchmark.events_map
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
    attributes Map(LowCardinality(String), String),
    source_raw String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, toStartOfHour(timestamp), service_name, timestamp, event_id);

CREATE TABLE IF NOT EXISTS benchmark.events_typed
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
    resource Nested(key LowCardinality(String), value String),
    scope Nested(key LowCardinality(String), value String),
    attribute_string Nested(key LowCardinality(String), value String),
    attribute_int Nested(key LowCardinality(String), value Int64),
    attribute_uint Nested(key LowCardinality(String), value UInt64),
    attribute_float Nested(key LowCardinality(String), value Float64),
    attribute_bool Nested(key LowCardinality(String), value Bool),
    attribute_json Nested(key LowCardinality(String), value String),
    source_raw String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, toStartOfHour(timestamp), service_name, timestamp, event_id);
