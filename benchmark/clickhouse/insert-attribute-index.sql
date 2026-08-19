INSERT INTO benchmark.accel_attribute_string
SELECT
    tenant_id,
    dataset,
    {field_name:String} AS field_name,
    attributes[{field_name:String}] AS field_value,
    timestamp,
    event_id
FROM benchmark.events_map
WHERE notEmpty(field_value)
SETTINGS max_memory_usage = 16000000000;
