SELECT
    dimension_value AS caller,
    countMerge(samples) AS samples,
    sumMerge(duration_sum) AS duration_sum,
    quantilesTDigestMerge(0.5, 0.9, 0.95, 0.99)(duration_quantiles) AS quantiles
FROM benchmark.accel_duration_5m
WHERE tenant_id = 1
  AND dataset = 'gcp-logging'
  AND dimension_name = 'caller'
  AND bucket BETWEEN toStartOfFiveMinutes({start:DateTime64(6, 'UTC')}) AND {end:DateTime64(6, 'UTC')}
GROUP BY caller
ORDER BY samples DESC, caller
LIMIT 100
FORMAT JSONEachRow;
