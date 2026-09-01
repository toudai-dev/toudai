SELECT toStartOfFiveMinutes(timestamp) AS bucket,
       service_name,
       count() AS events
FROM {{table}}
WHERE tenant_id = 1
  AND dataset = 'gcp-logging'
  AND timestamp >= {start:DateTime64(6, 'UTC')}
  AND timestamp < {end:DateTime64(6, 'UTC')}
GROUP BY bucket, service_name
ORDER BY bucket, service_name
FORMAT JSONEachRow;
