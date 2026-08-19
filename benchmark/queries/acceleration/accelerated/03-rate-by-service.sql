SELECT bucket, dimension_value AS service_name, sum(events) AS events
FROM benchmark.accel_event_count_5m
WHERE tenant_id = 1
  AND dataset = 'gcp-logging'
  AND dimension_name = 'service_name'
  AND bucket BETWEEN toStartOfFiveMinutes({start:DateTime64(6, 'UTC')}) AND {end:DateTime64(6, 'UTC')}
GROUP BY bucket, service_name
ORDER BY bucket, service_name
FORMAT JSONEachRow;
