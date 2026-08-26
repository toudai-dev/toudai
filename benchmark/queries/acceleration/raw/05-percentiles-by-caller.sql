WITH
    attributes['caller'] AS caller,
    toFloat64OrNull(attributes['duration']) AS duration
SELECT caller, count() AS samples, sum(duration) AS duration_sum,
       quantilesTDigest(0.5, 0.9, 0.95, 0.99)(duration) AS quantiles
FROM {{table}}
WHERE tenant_id = 1
  AND dataset = 'gcp-logging'
  AND timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
  AND notEmpty(caller)
  AND duration IS NOT NULL
GROUP BY caller
ORDER BY samples DESC, caller
LIMIT 100
FORMAT JSONEachRow;
