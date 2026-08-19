SELECT
    countMerge(samples) AS samples,
    sumMerge(duration_sum) AS duration_sum,
    histogramMerge(20)(duration_histogram) AS buckets
FROM benchmark.accel_duration_5m
WHERE tenant_id = 1
  AND dataset = 'gcp-logging'
  AND dimension_name = 'all'
  AND bucket BETWEEN toStartOfFiveMinutes({start:DateTime64(6, 'UTC')}) AND {end:DateTime64(6, 'UTC')}
FORMAT JSONEachRow;
