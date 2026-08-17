SET json_type_escape_dots_in_keys = 1;

INSERT INTO benchmark.events_json
SELECT
    tenant_id, dataset, event_timestamp, observed_at, event_id, event_kind,
    event_name, body, service_name, environment, severity, status_code, trace_id,
    span_id, parent_span_id, span_kind, duration_ns,
    CAST(toJSONString(resource_map) AS JSON),
    CAST(scope_json AS JSON),
    CAST(attributes_json AS JSON),
    source_raw
FROM benchmark.gcp_events
WHERE event_timestamp >= {window_start:DateTime64(6, 'UTC')}
  AND event_timestamp < {window_end:DateTime64(6, 'UTC')};
