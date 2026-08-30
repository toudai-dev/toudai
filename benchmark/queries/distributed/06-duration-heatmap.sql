WITH toFloat64OrNull(attributes['duration']) AS duration
SELECT count() AS samples,
       sum(duration) AS duration_sum,
       histogram(20)(duration) AS buckets
FROM {{table}}
WHERE tenant_id = 1
  AND dataset = 'gcp-logging'
  AND timestamp >= {start:DateTime64(6, 'UTC')}
  AND timestamp < {end:DateTime64(6, 'UTC')}
  AND duration IS NOT NULL
FORMAT JSONEachRow;
