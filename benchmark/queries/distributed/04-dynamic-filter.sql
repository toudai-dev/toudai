SELECT count() AS events, sum(cityHash64(event_id)) AS checksum
FROM {{table}}
WHERE tenant_id = 1
  AND dataset = 'gcp-logging'
  AND timestamp >= {start:DateTime64(6, 'UTC')}
  AND timestamp < {end:DateTime64(6, 'UTC')}
  AND attributes['type'] = {dynamic_value:String}
FORMAT JSONEachRow;
