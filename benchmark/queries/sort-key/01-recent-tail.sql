SELECT count() AS result_rows,
       sum(cityHash64(timestamp, event_id, service_name, severity, body)) AS checksum
FROM
(
    SELECT timestamp, event_id, service_name, severity, body
    FROM {{table}}
    WHERE tenant_id = 1
      AND dataset = 'gcp-logging'
      AND timestamp >= {start:DateTime64(6, 'UTC')}
      AND timestamp < {end:DateTime64(6, 'UTC')}
    ORDER BY timestamp DESC, event_id DESC
    LIMIT 100
)
FORMAT JSONEachRow;
