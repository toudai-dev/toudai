-- Decision 0001 D3: the canonical payload table. It lives on the S3-backed
-- `toudai_payload` storage policy (clickhouse/config.d/toudai-storage.xml):
-- new parts land on the local `cache` volume and the TTL moves a partition to the
-- `object` volume once it is older than the window below. The window is a
-- placeholder for the ten-day retained-scale load to size against the cache
-- volume; it is not a retention limit (the TTL only moves data, it never deletes).
--
-- Decision 0002 D2: lookup is by (dataset, event_id), so the sort key is those
-- columns instead of the serving sort key: a payload fetch should find its row
-- by primary-key search without knowing the event time. Daily partitions by event
-- time are what the TTL and tiering act on. There is no tenant column: one
-- database holds one deployment's data, and `dataset` is a partition of it, not
-- an isolation boundary. Identifiers stay String (Decision 0001 D2).
CREATE TABLE IF NOT EXISTS {database:Identifier}.canonical_events
(
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
ORDER BY (dataset, event_id)
TTL toDateTime(timestamp) + INTERVAL 2 DAY TO VOLUME 'object'
SETTINGS storage_policy = 'toudai_payload';
