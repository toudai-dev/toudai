SELECT count() AS events, sum(cityHash64(event_id)) AS checksum
FROM benchmark.accel_attribute_string
WHERE tenant_id = 1
  AND dataset = 'gcp-logging'
  AND field_name = 'type'
  AND field_value = {dynamic_low_value:String}
  AND timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
FORMAT JSONEachRow;
