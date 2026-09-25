-- Experiment 020 candidate representations. {{DB}} is substituted by run.sh
-- (lh020 for the semantic fixture, lh020_cost for the cost corpora).
--
-- rep_companion        Experiment 019's companion-type baseline: one string
--                      value Map per namespace plus a per-key type Map.
-- rep_native_typed     per-type native value Maps plus a full per-key type
--                      sidecar (presence/type for every key, including the
--                      canonical-only types).
-- rep_native_minimal   per-type native value Maps plus a key-only `other` Map
--                      that carries only the canonical-only types (bytes,
--                      array, map) and explicit empty. Presence/type for
--                      scalar keys is derived from the typed value Maps.
-- rep_wrong_*          deliberately wrong layouts used only to show that the
--                      assertions fail for plausible mistakes; same shape as
--                      rep_native_typed. They are never a cost candidate.
--
-- All variants share the envelope columns, the same ORDER BY, the same default
-- codec, and a canonical payload column, so the representation is the only
-- difference between them.

DROP TABLE IF EXISTS {{DB}}.rep_companion;
CREATE TABLE {{DB}}.rep_companion (
    event_id String,
    kind LowCardinality(String),
    ts DateTime64(9),
    tenant_id String,
    dataset LowCardinality(String),
    body String,
    resource Map(LowCardinality(String), String),
    scope Map(LowCardinality(String), String),
    record Map(LowCardinality(String), String),
    resource_types Map(LowCardinality(String), LowCardinality(String)),
    scope_types Map(LowCardinality(String), LowCardinality(String)),
    record_types Map(LowCardinality(String), LowCardinality(String)),
    payload String
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.rep_native_typed;
CREATE TABLE {{DB}}.rep_native_typed (
    event_id String,
    kind LowCardinality(String),
    ts DateTime64(9),
    tenant_id String,
    dataset LowCardinality(String),
    body String,
    resource_int Map(LowCardinality(String), Int64),
    resource_double Map(LowCardinality(String), Float64),
    resource_string Map(LowCardinality(String), String),
    resource_bool Map(LowCardinality(String), Bool),
    resource_types Map(LowCardinality(String), LowCardinality(String)),
    scope_int Map(LowCardinality(String), Int64),
    scope_double Map(LowCardinality(String), Float64),
    scope_string Map(LowCardinality(String), String),
    scope_bool Map(LowCardinality(String), Bool),
    scope_types Map(LowCardinality(String), LowCardinality(String)),
    record_int Map(LowCardinality(String), Int64),
    record_double Map(LowCardinality(String), Float64),
    record_string Map(LowCardinality(String), String),
    record_bool Map(LowCardinality(String), Bool),
    record_types Map(LowCardinality(String), LowCardinality(String)),
    payload String
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.rep_native_minimal;
CREATE TABLE {{DB}}.rep_native_minimal (
    event_id String,
    kind LowCardinality(String),
    ts DateTime64(9),
    tenant_id String,
    dataset LowCardinality(String),
    body String,
    resource_int Map(LowCardinality(String), Int64),
    resource_double Map(LowCardinality(String), Float64),
    resource_string Map(LowCardinality(String), String),
    resource_bool Map(LowCardinality(String), Bool),
    resource_other Map(LowCardinality(String), LowCardinality(String)),
    scope_int Map(LowCardinality(String), Int64),
    scope_double Map(LowCardinality(String), Float64),
    scope_string Map(LowCardinality(String), String),
    scope_bool Map(LowCardinality(String), Bool),
    scope_other Map(LowCardinality(String), LowCardinality(String)),
    record_int Map(LowCardinality(String), Int64),
    record_double Map(LowCardinality(String), Float64),
    record_string Map(LowCardinality(String), String),
    record_bool Map(LowCardinality(String), Bool),
    record_other Map(LowCardinality(String), LowCardinality(String)),
    payload String
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.rep_wrong_float_only;
CREATE TABLE {{DB}}.rep_wrong_float_only (
    event_id String,
    kind LowCardinality(String),
    ts DateTime64(9),
    tenant_id String,
    dataset LowCardinality(String),
    body String,
    resource_int Map(LowCardinality(String), Int64),
    resource_double Map(LowCardinality(String), Float64),
    resource_string Map(LowCardinality(String), String),
    resource_bool Map(LowCardinality(String), Bool),
    resource_types Map(LowCardinality(String), LowCardinality(String)),
    scope_int Map(LowCardinality(String), Int64),
    scope_double Map(LowCardinality(String), Float64),
    scope_string Map(LowCardinality(String), String),
    scope_bool Map(LowCardinality(String), Bool),
    scope_types Map(LowCardinality(String), LowCardinality(String)),
    record_int Map(LowCardinality(String), Int64),
    record_double Map(LowCardinality(String), Float64),
    record_string Map(LowCardinality(String), String),
    record_bool Map(LowCardinality(String), Bool),
    record_types Map(LowCardinality(String), LowCardinality(String)),
    payload String
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.rep_wrong_double_presence;
CREATE TABLE {{DB}}.rep_wrong_double_presence (
    event_id String,
    kind LowCardinality(String),
    ts DateTime64(9),
    tenant_id String,
    dataset LowCardinality(String),
    body String,
    resource_int Map(LowCardinality(String), Int64),
    resource_double Map(LowCardinality(String), Float64),
    resource_string Map(LowCardinality(String), String),
    resource_bool Map(LowCardinality(String), Bool),
    resource_types Map(LowCardinality(String), LowCardinality(String)),
    scope_int Map(LowCardinality(String), Int64),
    scope_double Map(LowCardinality(String), Float64),
    scope_string Map(LowCardinality(String), String),
    scope_bool Map(LowCardinality(String), Bool),
    scope_types Map(LowCardinality(String), LowCardinality(String)),
    record_int Map(LowCardinality(String), Int64),
    record_double Map(LowCardinality(String), Float64),
    record_string Map(LowCardinality(String), String),
    record_bool Map(LowCardinality(String), Bool),
    record_types Map(LowCardinality(String), LowCardinality(String)),
    payload String
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

-- Span tables keep the span attribute namespace and the span scope-attribute
-- namespace separate (Experiment 019 measured that the stock traces schema has
-- no scope-attributes column at all).
DROP TABLE IF EXISTS {{DB}}.rep_span_companion;
CREATE TABLE {{DB}}.rep_span_companion (
    event_id String,
    tenant_id String,
    dataset LowCardinality(String),
    ts DateTime64(9),
    span_name String,
    span Map(LowCardinality(String), String),
    span_types Map(LowCardinality(String), LowCardinality(String)),
    span_scope Map(LowCardinality(String), String),
    span_scope_types Map(LowCardinality(String), LowCardinality(String))
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.rep_span_native_typed;
CREATE TABLE {{DB}}.rep_span_native_typed (
    event_id String,
    tenant_id String,
    dataset LowCardinality(String),
    ts DateTime64(9),
    span_name String,
    span_int Map(LowCardinality(String), Int64),
    span_double Map(LowCardinality(String), Float64),
    span_string Map(LowCardinality(String), String),
    span_bool Map(LowCardinality(String), Bool),
    span_types Map(LowCardinality(String), LowCardinality(String)),
    span_scope_int Map(LowCardinality(String), Int64),
    span_scope_double Map(LowCardinality(String), Float64),
    span_scope_string Map(LowCardinality(String), String),
    span_scope_bool Map(LowCardinality(String), Bool),
    span_scope_types Map(LowCardinality(String), LowCardinality(String))
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);

DROP TABLE IF EXISTS {{DB}}.rep_span_native_minimal;
CREATE TABLE {{DB}}.rep_span_native_minimal (
    event_id String,
    tenant_id String,
    dataset LowCardinality(String),
    ts DateTime64(9),
    span_name String,
    span_int Map(LowCardinality(String), Int64),
    span_double Map(LowCardinality(String), Float64),
    span_string Map(LowCardinality(String), String),
    span_bool Map(LowCardinality(String), Bool),
    span_other Map(LowCardinality(String), LowCardinality(String)),
    span_scope_int Map(LowCardinality(String), Int64),
    span_scope_double Map(LowCardinality(String), Float64),
    span_scope_string Map(LowCardinality(String), String),
    span_scope_bool Map(LowCardinality(String), Bool),
    span_scope_other Map(LowCardinality(String), LowCardinality(String))
) ENGINE = MergeTree ORDER BY (tenant_id, dataset, ts, event_id);
