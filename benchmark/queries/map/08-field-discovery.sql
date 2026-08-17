SELECT path, count() AS events
FROM {{table}}
ARRAY JOIN mapKeys(attributes) AS path
WHERE timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
GROUP BY path
ORDER BY events DESC, path
LIMIT 100
FORMAT JSONEachRow;
