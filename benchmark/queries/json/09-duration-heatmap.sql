WITH toFloat64OrNull(toString(attributes.duration)) AS duration
SELECT count() AS samples, sum(duration) AS duration_sum, histogram(20)(duration) AS buckets
FROM {{table}}
WHERE timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
  AND duration IS NOT NULL
FORMAT JSONEachRow;
