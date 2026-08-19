SELECT count() AS events, sum(cityHash64(event_id)) AS checksum
FROM benchmark.events_map
WHERE tenant_id = 1
  AND dataset = 'gcp-logging'
  AND timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
  AND attributes['workspace_id'] = {dynamic_high_value:String}
FORMAT JSONEachRow;
