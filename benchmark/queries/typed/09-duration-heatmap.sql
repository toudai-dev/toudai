WITH indexOf(attribute_int.key, 'duration') AS duration_index
SELECT count() AS samples,
       sum(toFloat64(arrayElement(attribute_int.value, duration_index))) AS duration_sum,
       histogram(20)(toFloat64(arrayElement(attribute_int.value, duration_index))) AS buckets
FROM {{table}}
WHERE timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
  AND duration_index > 0
FORMAT JSONEachRow;
