SELECT
    count() AS events,
    sum(cityHash64(event_id)) AS checksum
FROM {{table}}
WHERE tenant_id = 1
  AND dataset = 'gcp-logging'
  AND timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
  AND positionCaseInsensitive(body, {body_term:String}) > 0
FORMAT JSONEachRow;
