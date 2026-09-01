CREATE TABLE IF NOT EXISTS benchmark.sort_hour_service
AS benchmark.events_map_hot
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, toStartOfHour(timestamp), service_name, timestamp, event_id)
SETTINGS storage_policy = 'benchmark_overflow', index_granularity = 8192;

CREATE TABLE IF NOT EXISTS benchmark.sort_time
AS benchmark.events_map_hot
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, timestamp, event_id)
SETTINGS storage_policy = 'benchmark_overflow', index_granularity = 8192;

CREATE TABLE IF NOT EXISTS benchmark.sort_hour_time
AS benchmark.events_map_hot
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, toStartOfHour(timestamp), timestamp, service_name, event_id)
SETTINGS storage_policy = 'benchmark_overflow', index_granularity = 8192;

CREATE TABLE IF NOT EXISTS benchmark.sort_date_service
AS benchmark.events_map_hot
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, toDate(timestamp), service_name, timestamp, event_id)
SETTINGS storage_policy = 'benchmark_overflow', index_granularity = 8192;

CREATE TABLE IF NOT EXISTS benchmark.sort_date_first_service
AS benchmark.events_map_hot
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (toDate(timestamp), tenant_id, dataset, service_name, timestamp, event_id)
SETTINGS storage_policy = 'benchmark_overflow', index_granularity = 8192;
