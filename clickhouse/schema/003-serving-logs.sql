-- Decision 0001 D1/D2/D4: serving rows for logs.
--
-- Sort key (dataset, timestamp, event_id) (Decision 0002 D2: no tenant column)
-- and daily partitions by event time (D2). Identifiers stay String.
-- `routing_epoch` is the placement epoch the writer stamped on the row (D4); the
-- planner filters on it inside each shard.
--
-- Record attributes are native typed value Maps plus a key-only `record_other`
-- map for the canonical-only kinds (bytes, array, map) and explicit empty.
-- Resource and scope attributes are one string Map plus a companion type Map
-- (Experiment 020).
CREATE TABLE IF NOT EXISTS {database:Identifier}.serving_logs
(
    dataset LowCardinality(String),
    event_id String,
    routing_epoch UInt16,
    timestamp DateTime64(9),
    observed_ts DateTime64(9),
    kind LowCardinality(String),
    service_name String,
    severity_text LowCardinality(String),
    severity_number Int32,
    body String,
    trace_id String,
    span_id String,
    event_name String,
    resource Map(LowCardinality(String), String),
    scope Map(LowCardinality(String), String),
    resource_types Map(LowCardinality(String), LowCardinality(String)),
    scope_types Map(LowCardinality(String), LowCardinality(String)),
    record_int Map(LowCardinality(String), Int64),
    record_double Map(LowCardinality(String), Float64),
    record_string Map(LowCardinality(String), String),
    record_bool Map(LowCardinality(String), Bool),
    record_other Map(LowCardinality(String), LowCardinality(String)),
    payload_sha256 String
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (dataset, timestamp, event_id);
