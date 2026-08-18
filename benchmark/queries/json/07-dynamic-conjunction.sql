SELECT count() AS events, sum(cityHash64(event_id)) AS checksum
FROM {{table}}
WHERE timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
  AND toString(attributes.type) = {dynamic_low_value:String}
  AND toString(attributes.caller) = {caller:String}
FORMAT JSONEachRow;
