WITH
    assumeNotNull(raw_entry) AS source_raw,
    JSONExtractRaw(source_raw, 'json_payload') AS json_payload,
    JSONExtractRaw(source_raw, 'proto_payload') AS proto_payload,
    if(json_payload NOT IN ('', 'null', '{}'), json_payload,
       if(proto_payload NOT IN ('', 'null', '{}'), proto_payload, '{}')) AS attributes_json,
    JSONExtractKeysAndValuesRaw(attributes_json) AS attribute_pairs,
    arrayMap(pair -> (
        pair.1,
        JSONType(pair.2),
        if(JSONType(pair.2) IN ('Object', 'Array', 'Null'), pair.2, JSON_VALUE(pair.2, '$'))
    ), attribute_pairs) AS attribute_entries,
    JSONExtract(assumeNotNull(labels), 'Map(String, String)') AS label_map,
    JSONExtract(assumeNotNull(resource_labels), 'Map(String, String)') AS gcp_resource_map,
    mapConcat(
        label_map,
        map('gcp.resource.type', assumeNotNull(resource_type)),
        mapFromArrays(
            arrayMap(key -> concat('gcp.resource.', key), mapKeys(gcp_resource_map)),
            mapValues(gcp_resource_map)
        )
    ) AS resource_map
SELECT
    toUInt64(1) AS tenant_id,
    CAST('gcp-logging' AS LowCardinality(String)) AS dataset,
    coalesce(timestamp, toDateTime64(fromUnixTimestamp64Nano(timestamp_unix_nanos, 'UTC'), 6, 'UTC')) AS timestamp,
    coalesce(receive_timestamp, toDateTime64(fromUnixTimestamp64Nano(receive_timestamp_unix_nanos, 'UTC'), 6, 'UTC')) AS observed_at,
    if(notEmpty(assumeNotNull(insert_id)), assumeNotNull(insert_id),
       hex(sipHash128(timestamp_unix_nanos, assumeNotNull(log_name), source_raw))) AS event_id,
    CAST('log' AS Enum8('event' = 1, 'log' = 2, 'span' = 3)) AS event_kind,
    coalesce(nullIf(JSONExtractString(attributes_json, 'event'), ''), assumeNotNull(log_name)) AS event_name,
    coalesce(
        nullIf(JSONExtractString(json_payload, 'message'), ''),
        nullIf(JSONExtractString(source_raw, 'text_payload'), ''),
        ''
    ) AS body,
    CAST(JSONExtractString(assumeNotNull(labels), 'service.name') AS LowCardinality(String)) AS service_name,
    CAST(coalesce(
        nullIf(JSONExtractString(assumeNotNull(labels), 'service.namespace'), ''),
        JSONExtractString(assumeNotNull(resource_labels), 'namespace')
    ) AS LowCardinality(String)) AS environment,
    CAST(assumeNotNull(severity) AS LowCardinality(String)) AS severity,
    CAST('' AS LowCardinality(String)) AS status_code,
    arrayElement(splitByChar('/', assumeNotNull(trace)), -1) AS trace_id,
    assumeNotNull(span_id) AS span_id,
    '' AS parent_span_id,
    CAST('' AS LowCardinality(String)) AS span_kind,
    toUInt64(0) AS duration_ns,
    resource_map AS resource,
    CAST(map() AS Map(LowCardinality(String), String)) AS scope,
    mapFromArrays(
        arrayMap(entry -> entry.1, attribute_entries),
        arrayMap(entry -> entry.3, attribute_entries)
    ) AS attributes
FROM table
FORMAT Native;
