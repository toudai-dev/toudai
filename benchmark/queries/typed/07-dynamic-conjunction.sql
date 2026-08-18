SELECT count() AS events, sum(cityHash64(event_id)) AS checksum
FROM {{table}}
WHERE timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
  AND arrayElement(attribute_string.value, indexOf(attribute_string.key, 'type')) = {dynamic_low_value:String}
  AND arrayElement(attribute_string.value, indexOf(attribute_string.key, 'caller')) = {caller:String}
FORMAT JSONEachRow;
