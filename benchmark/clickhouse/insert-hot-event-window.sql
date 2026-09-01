INSERT INTO benchmark.events_map_hot
SELECT
    tenant_id, dataset, timestamp, observed_at, event_id, event_kind, event_name,
    body, service_name, environment, severity, status_code, trace_id, span_id,
    parent_span_id, span_kind, duration_ns, resource, scope, attributes
FROM benchmark.events_map
WHERE timestamp >= {window_start:DateTime64(6, 'UTC')}
  AND timestamp < {window_end:DateTime64(6, 'UTC')};
