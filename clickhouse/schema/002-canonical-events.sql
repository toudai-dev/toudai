-- Decision 0001 D3: the canonical payload table. It lives on the S3-backed
-- `lighthouse_payload` storage policy (clickhouse/config.d/lighthouse-storage.xml):
-- new parts land on the local `cache` volume and the TTL moves a partition to the
-- `object` volume once it is older than the window below. The window is a
-- placeholder for the ten-day retained-scale load to size against the cache
-- volume; it is not a retention limit (the TTL only moves data, it never deletes).
--
-- Lookup is by (tenant_id, dataset, event_id), so the sort key leads with those
-- columns instead of the serving sort key: a payload fetch should find its row
-- by primary-key search without knowing the event time. Daily partitions by event
-- time are what the TTL and tiering act on.
--
-- `tenant_id` holds the globally unique workspace ID, never an account ID.
-- Identifiers stay String (D2).
CREATE TABLE IF NOT EXISTS lighthouse.canonical_events
(
    tenant_id String,
    dataset LowCardinality(String),
    signal LowCardinality(String),
    event_id String,
    timestamp DateTime64(9),
    schema_version LowCardinality(String),
    payload String,
    payload_sha256 String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (tenant_id, dataset, event_id)
TTL toDateTime(timestamp) + INTERVAL 2 DAY TO VOLUME 'object'
SETTINGS storage_policy = 'lighthouse_payload';
