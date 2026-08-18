WITH
    indexOf(attribute_string.key, 'caller') AS caller_index,
    indexOf(attribute_int.key, 'duration') AS duration_index,
    arrayElement(attribute_string.value, caller_index) AS caller,
    toFloat64(arrayElement(attribute_int.value, duration_index)) AS duration
SELECT caller, count() AS samples, sum(duration) AS duration_sum,
       quantilesTDigest(0.5, 0.9, 0.95, 0.99)(duration) AS quantiles
FROM {{table}}
WHERE timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
  AND caller_index > 0
  AND duration_index > 0
GROUP BY caller
ORDER BY samples DESC, caller
LIMIT 100
FORMAT JSONEachRow;
