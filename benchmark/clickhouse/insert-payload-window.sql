INSERT INTO benchmark.event_payloads
SELECT tenant_id, dataset, timestamp, event_id, source_raw
FROM benchmark.events_map
WHERE timestamp >= {window_start:DateTime64(6, 'UTC')}
  AND timestamp < {window_end:DateTime64(6, 'UTC')};
