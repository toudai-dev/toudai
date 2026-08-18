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
FROM benchmark.gcp_events;

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
FROM benchmark.gcp_events;

INSERT INTO benchmark.events_typed
WITH
    arrayFilter(entry -> entry.2 = 'String', attribute_entries) AS strings,
    arrayFilter(entry -> entry.2 = 'Int64', attribute_entries) AS ints,
    arrayFilter(entry -> entry.2 = 'UInt64', attribute_entries) AS uints,
    arrayFilter(entry -> entry.2 = 'Double', attribute_entries) AS floats,
    arrayFilter(entry -> entry.2 = 'Bool', attribute_entries) AS bools,
    arrayFilter(entry -> entry.2 IN ('Object', 'Array', 'Null'), attribute_entries) AS json_values
SELECT
    tenant_id, dataset, event_timestamp, observed_at, event_id, event_kind,
    event_name, body, service_name, environment, severity, status_code, trace_id,
    span_id, parent_span_id, span_kind, duration_ns,
    mapKeys(resource_map), mapValues(resource_map),
    CAST([] AS Array(LowCardinality(String))), CAST([] AS Array(String)),
    arrayMap(entry -> entry.1, strings), arrayMap(entry -> entry.3, strings),
    arrayMap(entry -> entry.1, ints), arrayMap(entry -> toInt64(entry.3), ints),
    arrayMap(entry -> entry.1, uints), arrayMap(entry -> toUInt64(entry.3), uints),
    arrayMap(entry -> entry.1, floats), arrayMap(entry -> toFloat64(entry.3), floats),
    arrayMap(entry -> entry.1, bools), arrayMap(entry -> toBool(entry.3), bools),
    arrayMap(entry -> entry.1, json_values), arrayMap(entry -> entry.3, json_values),
    source_raw
FROM benchmark.gcp_events;
