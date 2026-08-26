INSERT INTO benchmark.accel_event_count_5m
SELECT
    tenant_id,
    dataset,
    toStartOfFiveMinutes(timestamp) AS bucket,
    'service_name' AS dimension_name,
    service_name AS dimension_value,
    count() AS events
FROM {{table}}
GROUP BY tenant_id, dataset, bucket, dimension_value
SETTINGS max_memory_usage = 16000000000;
