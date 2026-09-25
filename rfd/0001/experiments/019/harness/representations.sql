-- Experiment 019 candidate representations, one table per storage approach.
-- {{DB}} is substituted by run.sh (lh019 for the semantic fixture, lh019_cost
-- for the cost corpus). The envelope columns are shared so only the attribute
-- representation differs between candidates.
--
-- rep_stock_map      stock exporter Map values, no type information (baseline)
-- rep_payload_only   the same stock Map plus a canonical payload column; shows
--                    that preserving the payload does not repair Map semantics
-- rep_type_map       companion type Maps + a canonical payload column (the
--                    representation a direct custom write would produce)
-- rep_tagged_map     one Map whose values carry an inline type tag
-- rep_map_carried    the stock mechanism: payload and types travel inside the
--                    known Map columns and MATERIALIZED columns project them

DROP TABLE IF EXISTS {{DB}}.rep_stock_map;
CREATE TABLE {{DB}}.rep_stock_map (
    event_id String,
    kind LowCardinality(String),
    ts DateTime64(9),
    tenant_id String,
    dataset LowCardinality(String),
    resource Map(LowCardinality(String), String),
    scope Map(LowCardinality(String), String),
    record Map(LowCardinality(String), String),
    body String
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.rep_payload_only;
CREATE TABLE {{DB}}.rep_payload_only (
    event_id String,
    kind LowCardinality(String),
    ts DateTime64(9),
    tenant_id String,
    dataset LowCardinality(String),
    resource Map(LowCardinality(String), String),
    scope Map(LowCardinality(String), String),
    record Map(LowCardinality(String), String),
    body String,
    payload String
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.rep_type_map;
CREATE TABLE {{DB}}.rep_type_map (
    event_id String,
    kind LowCardinality(String),
    ts DateTime64(9),
    tenant_id String,
    dataset LowCardinality(String),
    resource Map(LowCardinality(String), String),
    scope Map(LowCardinality(String), String),
    record Map(LowCardinality(String), String),
    resource_types Map(LowCardinality(String), LowCardinality(String)),
    scope_types Map(LowCardinality(String), LowCardinality(String)),
    record_types Map(LowCardinality(String), LowCardinality(String)),
    body String,
    payload String
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.rep_tagged_map;
CREATE TABLE {{DB}}.rep_tagged_map (
    event_id String,
    kind LowCardinality(String),
    ts DateTime64(9),
    tenant_id String,
    dataset LowCardinality(String),
    resource Map(LowCardinality(String), String),
    scope Map(LowCardinality(String), String),
    record Map(LowCardinality(String), String),
    body String,
    payload String
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.rep_map_carried;
CREATE TABLE {{DB}}.rep_map_carried (
    event_id String,
    kind LowCardinality(String),
    ts DateTime64(9),
    tenant_id String,
    dataset LowCardinality(String),
    resource Map(LowCardinality(String), String),
    scope Map(LowCardinality(String), String),
    record Map(LowCardinality(String), String),
    body String,
    -- Projections of values a mapping processor would carry inside the known
    -- Map columns. The payload and the type JSON are still stored in the Map,
    -- so these columns duplicate data.
    payload String MATERIALIZED record['lighthouse.payload'],
    resource_types Map(String, String) MATERIALIZED JSONExtract(resource['lighthouse.types'], 'Map(String, String)'),
    scope_types Map(String, String) MATERIALIZED JSONExtract(scope['lighthouse.types'], 'Map(String, String)'),
    record_types Map(String, String) MATERIALIZED JSONExtract(record['lighthouse.types'], 'Map(String, String)')
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

-- Span scope attributes: the stock traces table has no ScopeAttributes column
-- in either mode, so this table exists only to show that a direct typed write
-- can keep the span scope namespace distinct from span attributes.
DROP TABLE IF EXISTS {{DB}}.rep_span_scope;
CREATE TABLE {{DB}}.rep_span_scope (
    event_id String,
    tenant_id String,
    dataset LowCardinality(String),
    ts DateTime64(9),
    span Map(LowCardinality(String), String),
    span_types Map(LowCardinality(String), LowCardinality(String)),
    span_scope Map(LowCardinality(String), String),
    span_scope_types Map(LowCardinality(String), LowCardinality(String))
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);
