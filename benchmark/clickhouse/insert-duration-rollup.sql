INSERT INTO benchmark.accel_duration_5m
WITH
    toFloat64OrNull(attributes['duration']) AS duration,
    attributes['caller'] AS caller
SELECT
    tenant_id,
    dataset,
    toStartOfFiveMinutes(timestamp) AS bucket,
    tupleElement(dimension, 1) AS dimension_name,
    tupleElement(dimension, 2) AS dimension_value,
    countState() AS samples,
    sumState(assumeNotNull(duration)) AS duration_sum,
    quantilesTDigestState(0.5, 0.9, 0.95, 0.99)(assumeNotNull(duration)) AS duration_quantiles,
    histogramState(20)(assumeNotNull(duration)) AS duration_histogram
FROM {{table}}
ARRAY JOIN if(notEmpty(caller), [('all', ''), ('caller', caller)], [('all', '')]) AS dimension
WHERE duration IS NOT NULL
GROUP BY tenant_id, dataset, bucket, dimension_name, dimension_value
SETTINGS max_memory_usage = 16000000000;
