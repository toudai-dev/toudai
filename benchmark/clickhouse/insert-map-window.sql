INSERT INTO benchmark.events_map
SELECT
    tenant_id, dataset, event_timestamp, observed_at, event_id, event_kind,
    event_name, body, service_name, environment, severity, status_code, trace_id,
    span_id, parent_span_id, span_kind, duration_ns,
    resource_map,
    CAST(map() AS Map(LowCardinality(String), String)),
    mapFromArrays(
        arrayMap(entry -> entry.1, attribute_entries),
        arrayMap(entry -> entry.3, attribute_entries)
    ),
    source_raw
FROM benchmark.gcp_events
WHERE event_timestamp >= {window_start:DateTime64(6, 'UTC')}
  AND event_timestamp < {window_end:DateTime64(6, 'UTC')};
