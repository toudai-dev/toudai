INSERT INTO benchmark.event_payloads
SELECT tenant_id, dataset, event_timestamp, service_name, event_id, source_raw
FROM benchmark.gcp_events
WHERE event_timestamp >= {window_start:DateTime64(6, 'UTC')}
  AND event_timestamp < {window_end:DateTime64(6, 'UTC')};
