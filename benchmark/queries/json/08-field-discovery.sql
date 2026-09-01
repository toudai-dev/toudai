SELECT path, count() AS events
FROM {{table}}
ARRAY JOIN JSONAllPaths(attributes) AS path
WHERE timestamp BETWEEN {start:DateTime64(6, 'UTC')} AND {end:DateTime64(6, 'UTC')}
GROUP BY path
ORDER BY events DESC, path
LIMIT 100
SETTINGS max_execution_time = 300, max_memory_usage = 8000000000
FORMAT JSONEachRow;
