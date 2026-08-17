SELECT path, count() AS events
FROM {{table}}
ARRAY JOIN arrayConcat(
    attribute_string.key,
    attribute_int.key,
    attribute_uint.key,
    attribute_float.key,
    attribute_bool.key,
    attribute_json.key
) AS path
WHERE timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
GROUP BY path
ORDER BY events DESC, path
LIMIT 100
FORMAT JSONEachRow;
