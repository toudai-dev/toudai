-- Experiment 019 Lighthouse-managed schema for the create_schema:false probes.
-- The leading columns are exactly the ones the pinned stock exporter names in
-- its INSERT templates (v0.161.0 logs_insert.sql / traces_insert.sql); the
-- trailing managed columns are the additions the exporter does not know about.
CREATE DATABASE IF NOT EXISTS lh019;

DROP TABLE IF EXISTS lh019.managed_logs;
CREATE TABLE lh019.managed_logs (
    Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    TraceFlags UInt8,
    SeverityText LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber UInt8,
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    Body String CODEC(ZSTD(1)),
    ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    EventName String CODEC(ZSTD(1)),
    -- Managed additions: the exporter never inserts these. PayloadDefault shows
    -- an extra ordinary column receiving its DEFAULT; PayloadM and AttrTypes
    -- show Map-carried values projected into queryable columns.
    PayloadDefault String DEFAULT '' COMMENT 'extra ordinary column with DEFAULT; exporter omits it',
    PayloadM String MATERIALIZED LogAttributes['lighthouse.payload'] COMMENT 'Map-carried payload projected by ClickHouse',
    AttrTypes Map(String, String) MATERIALIZED JSONExtract(LogAttributes['lighthouse.types'], 'Map(String, String)') COMMENT 'companion type Map parsed from a Map-carried JSON value'
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)
SETTINGS index_granularity = 8192;

-- A payload column with no DEFAULT and no MATERIALIZED expression. The stock
-- INSERT never names it, so ClickHouse fills the type default (empty string)
-- instead of rejecting the row: the payload is silently absent.
DROP TABLE IF EXISTS lh019.managed_logs_payload;
CREATE TABLE lh019.managed_logs_payload (
    Timestamp DateTime64(9),
    TraceId String,
    SpanId String,
    TraceFlags UInt8,
    SeverityText LowCardinality(String),
    SeverityNumber UInt8,
    ServiceName LowCardinality(String),
    Body String,
    ResourceSchemaUrl LowCardinality(String),
    ResourceAttributes Map(LowCardinality(String), String),
    ScopeSchemaUrl LowCardinality(String),
    ScopeName String,
    ScopeVersion LowCardinality(String),
    ScopeAttributes Map(LowCardinality(String), String),
    LogAttributes Map(LowCardinality(String), String),
    Payload String
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY Timestamp
SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS lh019.managed_traces;
CREATE TABLE lh019.managed_traces (
    Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    ParentSpanId String CODEC(ZSTD(1)),
    TraceState String CODEC(ZSTD(1)),
    SpanName LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Duration UInt64 CODEC(ZSTD(1)),
    StatusCode LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage String CODEC(ZSTD(1)),
    `Events.Timestamp` Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Events.Name` Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `Events.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Links.TraceId` Array(String) CODEC(ZSTD(1)),
    `Links.SpanId` Array(String) CODEC(ZSTD(1)),
    `Links.TraceState` Array(String) CODEC(ZSTD(1)),
    `Links.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    -- The stock traces INSERT never names ScopeAttributes, so this managed
    -- column stays at its default even though the received spans carry scope
    -- attributes.
    ScopeAttributes Map(String, String) DEFAULT map() COMMENT 'span scope attributes; stock exporter cannot fill this',
    PayloadM String MATERIALIZED SpanAttributes['lighthouse.payload']
) ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
SETTINGS index_granularity = 8192;
