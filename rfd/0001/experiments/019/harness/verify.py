#!/usr/bin/env python3
"""Assertions and measurements for Experiment 019.

Phases:

  stock-map    stock Map-mode exporter output (DDL, serving strings, duplicates)
  stock-json   stock json:true exporter output (ClickHouse JSON columns)
  managed      create_schema:false managed-schema probes
  transform    stock-processor gap probes
  identity     trusted workspace ordering probe
  semantics    candidate representations against independently computed values
  cost         storage and execution cost on the larger corpus (measurement)
  files        file-only observations (emits, metrics, logs)

Semantic acceptance checks and characterization checks are both reported; the
`kind` field says which one a check is. The query prototype (`prototype_*`)
implements the harness-side contract rules (unsupported-operation rejection,
integer-overflow rejection); those are prototype behavior, not ClickHouse
guarantees.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

import requests

import fixture
import model

CH = "http://127.0.0.1:18124/"


def ch(db, sql, fmt="TSV", timeout=180):
    resp = requests.post(CH, params={"database": db, "default_format": fmt},
                         data=sql.encode(), timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"clickhouse error {resp.status_code}: {resp.text[:400]}\nSQL: {sql[:300]}")
    # Only the trailing newline is stripped: a TSV row may end in empty fields.
    return resp.text.rstrip("\n")


def scalar(db, sql):
    return ch(db, sql)


def rows(db, sql):
    text = ch(db, sql)
    return [line.split("\t") for line in text.splitlines() if line]


class UnsupportedOperation(Exception):
    """Harness query-prototype rejection, not a ClickHouse error."""


def prototype_compare(type_name, value_text):
    """The contract defers comparison/aggregation on these values; the harness
    prototype rejects them explicitly instead of coercing."""
    if type_name in ("bytes", "array", "map"):
        raise UnsupportedOperation(f"comparison on {type_name} is unsupported")
    if type_name == "double" and value_text in ("NaN", "Infinity", "-Infinity"):
        raise UnsupportedOperation("comparison on non-finite double is unsupported")
    return "ok"


def prototype_int_aggregate(values):
    """Integer aggregates reject overflow rather than wrap (contract rule)."""
    total = sum(values)
    if total > fixture.INT64_MAX or total < fixture.INT64_MIN:
        raise UnsupportedOperation("integer aggregate overflow")
    return total


# ---------------------------------------------------------------------------
# Expected values computed from the typed inventory
# ---------------------------------------------------------------------------

def metric_rows():
    out = []
    for event in fixture.EVENTS:
        for key, type_name, value in event["record"]:
            if key == "metric.value":
                out.append((event["event_id"], type_name, value))
    return out


def expected_metric_groups():
    groups = {}
    for _, type_name, value in metric_rows():
        key = (type_name, model.stock_string(type_name, value))
        groups[key] = groups.get(key, 0) + 1
    return sorted(groups.items())


def expected_int_order():
    pairs = [(value, event_id) for event_id, type_name, value in metric_rows() if type_name == "int"]
    return [event_id for _, event_id in sorted(pairs)]


def expected_lexical_order():
    pairs = [(model.stock_string("int", value), event_id)
             for event_id, type_name, value in metric_rows() if type_name == "int"]
    return [event_id for _, event_id in sorted(pairs)]


def expected_type_counts():
    counts = {}
    for _, type_name, _ in metric_rows():
        counts[type_name] = counts.get(type_name, 0) + 1
    return sorted(counts.items())


def expected_stock_string_count(text):
    """Rows whose stock string equals `text`, regardless of the received type."""
    return len([1 for _, type_name, value in metric_rows()
                if model.stock_string(type_name, value) == text])


def expected_type_value_count(type_name, text):
    return len([1 for _, t, v in metric_rows()
                if t == type_name and model.stock_string(t, v) == text])


# ---------------------------------------------------------------------------
# Representation dialects
# ---------------------------------------------------------------------------

TAG_TO_TYPE = ("multiIf(splitByChar(':', {v})[1] = 'i', 'int', "
               "splitByChar(':', {v})[1] = 'd', 'double', "
               "splitByChar(':', {v})[1] = 's', 'string', "
               "splitByChar(':', {v})[1] = 'b', 'bool', "
               "splitByChar(':', {v})[1] = 'e', 'empty', "
               "splitByChar(':', {v})[1] = 'y', 'bytes', "
               "splitByChar(':', {v})[1] = 'a', 'array', "
               "splitByChar(':', {v})[1] = 'm', 'map', '?')")


def companion_dialect(table, types_col="record_types", resource_types_col="resource_types"):
    def value(key):
        return f"record['{key}']"

    def type_of(key):
        return f"{types_col}['{key}']"

    def resource_value(key):
        return f"resource['{key}']"

    def resource_type_of(key):
        return f"{resource_types_col}['{key}']"

    return {"name": table, "table": table, "value": value, "type_of": type_of,
            "resource_value": resource_value, "resource_type_of": resource_type_of, "typed": True}


def tagged_dialect(table):
    def decode(expr):
        return f"arrayStringConcat(arraySlice(splitByChar(':', {expr}), 2), ':')"

    def value(key):
        return decode(f"record['{key}']")

    def type_of(key):
        return TAG_TO_TYPE.format(v=f"record['{key}']")

    def resource_value(key):
        return decode(f"resource['{key}']")

    def resource_type_of(key):
        return TAG_TO_TYPE.format(v=f"resource['{key}']")

    return {"name": table, "table": table, "value": value, "type_of": type_of,
            "resource_value": resource_value, "resource_type_of": resource_type_of, "typed": True}


def stock_dialect(table):
    def value(key):
        return f"record['{key}']"

    def type_of(key):
        return "''"

    return {"name": table, "table": table, "value": value, "type_of": type_of,
            "resource_value": value, "resource_type_of": type_of, "typed": False}


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

class Checks:
    def __init__(self, capture=False):
        self.outcomes = []
        self.failures = 0
        self.capture = capture

    def check(self, name, observed, expected, kind="characterization", note=""):
        ok = str(observed) == str(expected) if expected is not None else not str(observed).startswith("ERROR")
        if expected is None:
            kind = "measurement"
        if not ok and not self.capture:
            self.failures += 1
        self.outcomes.append({
            "name": name, "kind": kind, "expected": expected, "observed": observed,
            "ok": ok, "note": note,
        })
        return observed

    def sql(self, name, db, sql, expected, kind="characterization", note=""):
        try:
            observed = scalar(db, sql)
        except Exception as exc:  # keep the failure visible in the record
            observed = f"ERROR: {exc}"
        return self.check(name, observed, expected, kind=kind, note=note)

    def info(self, name, observed, note=""):
        return self.check(name, observed, None, note=note)


def decode_canonical(node):
    """Independent decoder for the canonical payload (separate from the encoder)."""
    t = node["t"]
    if t == "empty":
        return ("empty", None)
    if t == "string":
        return ("string", node["v"])
    if t == "bool":
        return ("bool", bool(node["v"]))
    if t == "int":
        return ("int", int(node["v"]))
    if t == "double":
        text = node["v"]
        special = {"nan": float("nan"), "inf": float("inf"), "-inf": float("-inf")}
        if text in special:
            return ("double", special[text])
        return ("double", -0.0 if text == "-0.0" else float(text))
    if t == "bytes":
        import base64
        return ("bytes", base64.b64decode(node["v"]))
    if t == "array":
        return ("array", [decode_canonical(item)[1] for item in node["v"]])
    if t == "map":
        return ("map", {k: decode_canonical(v)[1] for k, v in node["v"].items()})
    raise ValueError(f"unknown canonical type {t}")


def _values_equal(type_name, a, b):
    if type_name == "double":
        if isinstance(a, float) and isinstance(b, float):
            if a != a and b != b:  # NaN
                return True
            if a == 0 and b == 0:
                return math.copysign(1.0, a) == math.copysign(1.0, b)
            return a == b
    return a == b


def phase_canonical(c: Checks, results: Path):
    """Lossless canonical round-trip and canonical/serving equivalence."""
    payload_path = results / "canonical-payloads.jsonl"
    if not payload_path.exists():
        c.check("canonical.payloads_present", "missing", "present")
        return
    payloads = {}
    for line in payload_path.read_text().splitlines():
        if line.strip():
            record = json.loads(line)
            payloads[record["event_id"]] = json.loads(record["payload"])
    c.check("canonical.payload_count", len(payloads), str(len(fixture.EVENTS) - 1),
            kind="semantic", note="the duplicate-key record is rejected by the mapping prototype")
    c.check("canonical.rejected_event_absent", "dup-1" in payloads, False, kind="semantic")
    roundtrip_ok = True
    first_mismatch = ""
    for event in fixture.EVENTS:
        if event["event_id"] not in payloads:
            continue
        doc = payloads[event["event_id"]]
        for namespace in ("resource", "scope", "record"):
            decoded = {k: decode_canonical(v) for k, v in doc[namespace].items()}
            expected = {k: (t, v) for k, t, v in event[namespace]}
            if set(decoded) != set(expected):
                roundtrip_ok = False
                first_mismatch = first_mismatch or f"{event['event_id']}.{namespace} keys"
                continue
            for key, (type_name, value) in expected.items():
                got_type, got_value = decoded[key]
                if got_type != type_name or not _values_equal(type_name, got_value, value):
                    roundtrip_ok = False
                    first_mismatch = first_mismatch or f"{event['event_id']}.{namespace}.{key}"
    c.check("canonical.lossless_roundtrip", "ok" if roundtrip_ok else f"mismatch: {first_mismatch}", "ok",
            kind="semantic", note="decoded payload compared with the typed fixture inventory")
    # Canonical versus serving: the serving Map must equal the stock string of
    # each decoded canonical value, with the same key sets.
    serving = ch("lh019", "SELECT event_id, record, record_types FROM rep_type_map FORMAT JSONEachRow")
    equivalent = True
    detail = ""
    for line in serving.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        doc = payloads.get(row["event_id"])
        if doc is None:
            continue
        decoded = {k: decode_canonical(v) for k, v in doc["record"].items()}
        if set(decoded) != set(row["record"]):
            equivalent = False
            detail = detail or f"{row['event_id']} key set"
            continue
        for key, (type_name, value) in decoded.items():
            if model.stock_string(type_name, value) != row["record"][key]:
                equivalent = False
                detail = detail or f"{row['event_id']}.{key} value"
            if row["record_types"].get(key) != type_name:
                equivalent = False
                detail = detail or f"{row['event_id']}.{key} type"
    c.check("canonical.serving_equivalence", "ok" if equivalent else f"mismatch: {detail}", "ok",
            kind="semantic", note="canonical payload values/types versus the companion-type serving Map")


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def phase_stock_map(c: Checks):
    c.sql("stock_map.log_rows", "stock_map", "SELECT count() FROM otel_logs", "28")
    c.sql("stock_map.trace_rows", "stock_map", "SELECT count() FROM otel_traces", "3")
    # The stock Map stores every attribute as a string; these pin the exact
    # serving strings the representation comparison reuses.
    c.sql("stock_map.int_one", "stock_map", "SELECT LogAttributes['int.one'] FROM otel_logs WHERE Body='typed-log-1'", "1")
    c.sql("stock_map.double_one", "stock_map", "SELECT LogAttributes['double.one'] FROM otel_logs WHERE Body='typed-log-1'", "1")
    c.sql("stock_map.str_one", "stock_map", "SELECT LogAttributes['str.one'] FROM otel_logs WHERE Body='typed-log-1'", "1")
    c.sql("stock_map.int_double_string_collide", "stock_map",
          "SELECT LogAttributes['int.one'] = LogAttributes['double.one'] AND "
          "LogAttributes['str.one'] = LogAttributes['double.one'] FROM otel_logs WHERE Body='typed-log-1'", "1")
    c.sql("stock_map.big_int_exact", "stock_map", "SELECT LogAttributes['int.big'] FROM otel_logs WHERE Body='typed-log-1'", "9007199254740993")
    c.sql("stock_map.min_int_exact", "stock_map", "SELECT LogAttributes['int.min'] FROM otel_logs WHERE Body='typed-log-1'", "-9223372036854775808")
    c.sql("stock_map.nan_text", "stock_map", "SELECT LogAttributes['double.nan'] FROM otel_logs WHERE Body='typed-log-1'", "NaN")
    c.sql("stock_map.posinf_text", "stock_map", "SELECT LogAttributes['double.posinf'] FROM otel_logs WHERE Body='typed-log-1'", "Infinity")
    c.sql("stock_map.negzero_text", "stock_map", "SELECT LogAttributes['zero.float'] FROM otel_logs WHERE Body='typed-log-1'", "-0")
    c.sql("stock_map.bytes_base64", "stock_map", "SELECT LogAttributes['bytes.raw'] FROM otel_logs WHERE Body='typed-log-1'", "AAH+/w==")
    c.sql("stock_map.array_json_text", "stock_map", "SELECT LogAttributes['array.mixed'] FROM otel_logs WHERE Body='typed-log-1'", '["a",1,true]')
    c.sql("stock_map.map_json_text", "stock_map", "SELECT LogAttributes['map.nested'] FROM otel_logs WHERE Body='typed-log-1'", '{"nested.bool":false,"nested.int":2}')
    c.sql("stock_map.json_like_string_collides_with_map", "stock_map",
          "SELECT LogAttributes['str.json_like'] = LogAttributes['map.nested'] FROM otel_logs WHERE Body='typed-log-1'", "1")
    c.sql("stock_map.empty_and_empty_string_both_empty", "stock_map",
          "SELECT LogAttributes['empty.value'] = LogAttributes['str.empty'] FROM otel_logs WHERE Body='typed-log-1'", "1")
    c.sql("stock_map.empty_present", "stock_map", "SELECT mapContains(LogAttributes,'empty.value') FROM otel_logs WHERE Body='typed-log-1'", "1")
    c.sql("stock_map.missing_absent", "stock_map", "SELECT mapContains(LogAttributes,'missing.key') FROM otel_logs WHERE Body='typed-log-1'", "0")
    c.sql("stock_map.namespace_collide", "stock_map",
          "SELECT ResourceAttributes['res.collide'], LogAttributes['collide'] FROM otel_logs WHERE Body='typed-log-1'",
          "resource-value\trecord-value")
    c.sql("stock_map.case_sensitive", "stock_map",
          "SELECT LogAttributes['Foo'], ResourceAttributes['Foo'], ResourceAttributes['foo'] FROM otel_logs WHERE Body='typed-log-1'",
          "record-upper\tresource-upper\tresource-lower")
    # Duplicate keys: the stock Map keeps both copies; nothing rejects them.
    c.sql("stock_map.duplicate_lookup_first", "stock_map", "SELECT LogAttributes['dup'] FROM otel_logs WHERE Body='dup-1'", "first")
    c.sql("stock_map.duplicate_both_stored", "stock_map",
          "SELECT countEqual(mapKeys(LogAttributes),'dup') FROM otel_logs WHERE Body='dup-1'", "2")
    # A typed filter cannot be expressed on the stock Map; this is the
    # string-collision baseline the typed representations must beat.
    c.sql("stock_map.untyped_filter_collides", "stock_map",
          "SELECT count() FROM otel_logs WHERE LogAttributes['metric.value'] = '1'",
          str(expected_stock_string_count("1")),
          note="int 1, double 1.0, and string \"1\" rows all match the same string")
    c.sql("stock_map.untyped_lexical_order", "stock_map",
          "SELECT arrayStringConcat(groupArray(event_id), ',') FROM (SELECT Body AS event_id FROM otel_logs "
          "WHERE LogAttributes['metric.value'] IN ('9','10','100') ORDER BY LogAttributes['metric.value'])",
          "order-10,order-100,order-9", kind="characterization",
          note="lexical order over the stock Map strings differs from numeric order")
    # Trace-specific gaps.
    c.sql("stock_map.traces_no_scope_attributes_column", "stock_map",
          "SELECT count() FROM system.columns WHERE database='stock_map' AND table='otel_traces' AND name='ScopeAttributes'", "0")
    c.sql("stock_map.negative_duration_underflow", "stock_map",
          "SELECT Duration FROM otel_traces WHERE SpanName='span/negative-duration'", "18446744073709550616")
    c.sql("stock_map.span_scope_attr_only_in_span_attributes", "stock_map",
          "SELECT SpanAttributes['span.scope.attr'] FROM otel_traces WHERE SpanName='span/scope-attrs'", "present",
          note="the scope-namespace value scope.span.attr is dropped; this is the record attribute")
    c.sql("stock_map.trace_scope_attr_dropped", "stock_map",
          "SELECT count() FROM otel_traces WHERE SpanAttributes['scope.span.attr'] != ''", "0")


def phase_stock_json(c: Checks):
    c.sql("stock_json.log_rows_safe", "stock_json", "SELECT count() FROM otel_logs", "25")
    c.sql("stock_json.nan_row_absent", "stock_json", "SELECT count() FROM otel_logs WHERE Body='nan'", "0")
    c.sql("stock_json.dynamic_type_int_one", "stock_json",
          "SELECT dynamicType(LogAttributes.`int.one`) FROM otel_logs WHERE Body='typed-log-1'", "Int64")
    c.sql("stock_json.dynamic_type_double_one_is_int64", "stock_json",
          "SELECT dynamicType(LogAttributes.`double.one`) FROM otel_logs WHERE Body='typed-log-1'", "Int64",
          note="Go json.Marshal writes float64(1) as 1, so JSON mode collides int 1 and double 1.0")
    c.sql("stock_json.dynamic_type_str_one", "stock_json",
          "SELECT dynamicType(LogAttributes.`str.one`) FROM otel_logs WHERE Body='typed-log-1'", "String")
    c.sql("stock_json.int_double_collide", "stock_json",
          "SELECT LogAttributes.`int.one` = LogAttributes.`double.one` FROM otel_logs WHERE Body='typed-log-1'", "1")
    try:
        observed = scalar("stock_json", "SELECT LogAttributes.`str.one` = LogAttributes.`double.one` FROM otel_logs WHERE Body='typed-log-1'")
    except Exception as exc:
        observed = "ERROR: NO_COMMON_TYPE" if "NO_COMMON_TYPE" in str(exc) else f"ERROR: {exc}"
    c.check("stock_json.string_number_comparison_errors", observed, "ERROR: NO_COMMON_TYPE",
            kind="characterization", note="JSON mode is type-strict at the SQL level instead of coercing")
    c.sql("stock_json.big_int_exact", "stock_json",
          "SELECT LogAttributes.`int.big` FROM otel_logs WHERE Body='typed-log-1'", "9007199254740993")
    c.sql("stock_json.min_int_exact", "stock_json",
          "SELECT LogAttributes.`int.min` FROM otel_logs WHERE Body='typed-log-1'", "-9223372036854775808")
    c.sql("stock_json.empty_is_null", "stock_json",
          "SELECT isNull(LogAttributes.`empty.value`) FROM otel_logs WHERE Body='typed-log-1'", "1")
    c.sql("stock_json.empty_key_present", "stock_json",
          "SELECT has(LogAttributesKeys, 'empty.value') FROM otel_logs WHERE Body='typed-log-1'", "1")
    c.sql("stock_json.empty_string_distinct", "stock_json",
          "SELECT isNull(LogAttributes.`str.empty`), LogAttributes.`str.empty` = '' FROM otel_logs WHERE Body='typed-log-1'", "0\t1")
    c.sql("stock_json.bytes_base64_string", "stock_json",
          "SELECT LogAttributes.`bytes.raw`, dynamicType(LogAttributes.`bytes.raw`) FROM otel_logs WHERE Body='typed-log-1'",
          "AAH+/w==\tString")
    c.sql("stock_json.array_structured", "stock_json",
          "SELECT dynamicType(LogAttributes.`array.mixed`) FROM otel_logs WHERE Body='typed-log-1'", "Array(Dynamic)")
    c.sql("stock_json.zero_float_sign_lost", "stock_json",
          "SELECT LogAttributes.`zero.float`, dynamicType(LogAttributes.`zero.float`) FROM otel_logs WHERE Body='typed-log-1'",
          "0\tInt64")
    c.sql("stock_json.duplicate_last_wins", "stock_json",
          "SELECT LogAttributes.`dup` FROM otel_logs WHERE Body='dup-1'", "second")
    c.sql("stock_json.traces_no_scope_attributes_column", "stock_json",
          "SELECT count() FROM system.columns WHERE database='stock_json' AND table='otel_traces' AND name='ScopeAttributes'", "0")
    c.sql("stock_json.negative_duration_underflow", "stock_json",
          "SELECT Duration FROM otel_traces WHERE SpanName='span/negative-duration'", "18446744073709550616")


def phase_managed(c: Checks, results: Path):
    c.sql("managed.log_rows", "lh019", "SELECT count() FROM managed_logs", "28")
    c.sql("managed.trace_rows", "lh019", "SELECT count() FROM managed_traces", "3")
    c.sql("managed.extra_default_column_empty", "lh019",
          "SELECT PayloadDefault FROM managed_logs WHERE Body='typed-log-1'", "")
    c.sql("managed.materialized_payload_projection", "lh019",
          "SELECT PayloadM FROM managed_logs WHERE Body='typed-log-1'", "payload-marker")
    c.sql("managed.materialized_type_map", "lh019",
          "SELECT AttrTypes['int.one'], AttrTypes['double.one'] FROM managed_logs WHERE Body='typed-log-1'",
          "int\tdouble",
          kind="semantic",
          note="a Map-carried type JSON can be projected into a queryable Map by MATERIALIZED")
    c.sql("managed.plain_payload_column_rows", "lh019",
          "SELECT count() FROM managed_logs_payload", "25")
    c.sql("managed.plain_payload_column_is_type_default", "lh019",
          "SELECT DISTINCT Payload FROM managed_logs_payload", "",
          note="the stock INSERT never names the payload column; ClickHouse fills the type default")
    c.sql("managed.traces_scope_attributes_present_but_empty", "lh019",
          "SELECT ScopeAttributes FROM managed_traces WHERE SpanName='span/scope-attrs'", "{}",
          note="the stock traces INSERT never names ScopeAttributes even when the managed table has it")
    c.sql("managed.traces_materialized_payload", "lh019",
          "SELECT PayloadM FROM managed_traces WHERE SpanName='span/typed'", "payload-marker")
    # The actual INSERT statement is the direct evidence for the fixed column list.
    try:
        insert_sql = scalar("lh019", "SELECT query FROM system.query_log WHERE type='QueryStart' "
                                     "AND query LIKE 'INSERT INTO lh019.managed_logs %' "
                                     "ORDER BY event_time DESC LIMIT 1")
    except Exception as exc:
        insert_sql = f"ERROR: {exc}"
    (results / "managed-insert-statement.txt").write_text(insert_sql + "\n")
    c.check("managed.insert_names_only_stock_columns",
            "payloaddefault" not in insert_sql.lower() and "payloadm" not in insert_sql.lower()
            and "attrtypes" not in insert_sql.lower() and "logattributes" in insert_sql.lower(),
            True, kind="characterization",
            note="captured from system.query_log; full statement in managed-insert-statement.txt")
    c.info("managed.insert_statement", insert_sql.replace("\n", " ")[:300])


def phase_transform(c: Checks, results: Path):
    types_log = (results / "transform-types.log").read_text() if (results / "transform-types.log").exists() else ""
    c.check("transform.types_expression_not_expressible",
            "invalid input text" in types_log and "? " in types_log, True,
            kind="characterization",
            note="OTTL has no conditional operator for a per-key type name")
    received = results / "transform-stringify-received-logs.jsonl"
    if received.exists():
        rec = json.loads(received.read_text().splitlines()[0])
        attrs = None
        for rl in rec.get("resourceLogs", []):
            for sl in rl.get("scopeLogs", []):
                for r in sl.get("logRecords", []):
                    candidate = {kv["key"]: kv["value"] for kv in r.get("attributes", [])}
                    if "int.one" in candidate:
                        attrs = candidate
        attrs = attrs or {}
        c.check("transform.stringify_all_collapses_types",
                json.dumps({k: attrs.get(k) for k in ("int.one", "double.one", "str.one")}, sort_keys=True),
                json.dumps({"double.one": {"stringValue": "1"}, "int.one": {"stringValue": "1"},
                            "str.one": {"stringValue": "1"}}, sort_keys=True),
                kind="characterization",
                note="the closest stock expression stores the same string for int 1, double 1.0, and string \"1\"")
    else:
        c.check("transform.stringify_all_file_present", "missing", "present")


def phase_identity(c: Checks, results: Path):
    c.sql("identity.trusted_workspace_stored", "lh019",
          "SELECT ResourceAttributes['lighthouse.workspace_id'] FROM managed_logs WHERE Body='identity-ws-a'",
          fixture.WS_A)
    c.sql("identity.forged_workspace_absent", "lh019",
          "SELECT count() FROM managed_logs WHERE ResourceAttributes['lighthouse.workspace_id'] = "
          f"'{fixture.WS_FORGED}'", "0")
    quarantine = results / "quarantine-logs.jsonl"
    observed = "no-records"
    if quarantine.exists():
        count = sum(len(sl.get("logRecords", []))
                    for line in quarantine.read_text().splitlines() if line.strip()
                    for rl in json.loads(line).get("resourceLogs", [])
                    for sl in rl.get("scopeLogs", []))
        observed = "no-records" if count == 0 else f"{count}-records"
    c.check("identity.quarantine_empty", observed, "no-records")


def semantics_checks(c: Checks, dialect):
    table = dialect["table"]
    value = dialect["value"]
    type_of = dialect["type_of"]
    typed = dialect["typed"]
    prefix = f"semantics.{dialect['name']}"

    if typed:
        observed = ch("lh019", f"SELECT {type_of('metric.value')} AS t, count() FROM {table} "
                               f"WHERE mapContains(record,'metric.value') GROUP BY t ORDER BY t")
        expected = "\n".join(f"{t}\t{n}" for t, n in expected_type_counts())
        c.check(f"{prefix}.type_counts", observed, expected, kind="semantic")
        c.sql(f"{prefix}.typed_filter_int", "lh019",
              f"SELECT count() FROM {table} WHERE {type_of('metric.value')}='int' AND toInt64({value('metric.value')}) = 1",
              str(expected_type_value_count("int", "1")), kind="semantic")
        c.sql(f"{prefix}.typed_filter_double", "lh019",
              f"SELECT count() FROM {table} WHERE {type_of('metric.value')}='double' AND toFloat64({value('metric.value')}) = 1",
              str(expected_type_value_count("double", "1")), kind="semantic")
        c.sql(f"{prefix}.typed_filter_string", "lh019",
              f"SELECT count() FROM {table} WHERE {type_of('metric.value')}='string' AND {value('metric.value')} = '1'",
              str(expected_type_value_count("string", "1")), kind="semantic")
        c.sql(f"{prefix}.typed_filter_excludes_other_types", "lh019",
              f"SELECT count() FROM {table} WHERE mapContains(record,'metric.value') "
              f"AND {type_of('metric.value')}='int' AND toInt64({value('metric.value')}) = 1 "
              f"AND event_id IN ('group-double-1','group-string-1')", "0", kind="semantic")
        observed = ch("lh019", f"SELECT {type_of('metric.value')} AS t, {value('metric.value')} AS v, count() "
                               f"FROM {table} WHERE mapContains(record,'metric.value') GROUP BY t, v ORDER BY t, v")
        expected = "\n".join(f"{t}\t{v}\t{n}" for (t, v), n in expected_metric_groups())
        c.check(f"{prefix}.group_by_type_value", observed, expected, kind="semantic")
        c.sql(f"{prefix}.group_by_literals", "lh019",
              f"SELECT sumIf(n, t='int' AND v='1'), sumIf(n, t='double' AND v='NaN'), "
              f"sumIf(n, t='string' AND v=''), sumIf(n, t='empty' AND v=''), "
              f"sumIf(n, t='bytes' AND v='AAH+/w=='), sumIf(n, t='bool' AND v='false') "
              f"FROM (SELECT {type_of('metric.value')} AS t, {value('metric.value')} AS v, count() AS n "
              f"FROM {table} WHERE mapContains(record,'metric.value') GROUP BY t, v)",
              "3\t1\t1\t1\t1\t1", kind="semantic",
              note="literal expectations authored from the fixture inventory, independent of the Python conversion helper")
        c.sql(f"{prefix}.presence_empty", "lh019",
              f"SELECT {type_of('metric.value')} FROM {table} WHERE event_id='group-empty'", "empty", kind="semantic")
        c.sql(f"{prefix}.presence_empty_string", "lh019",
              f"SELECT {type_of('metric.value')}, {value('metric.value')} FROM {table} WHERE event_id='group-empty-string'",
              "string\t", kind="semantic")
        c.sql(f"{prefix}.presence_missing", "lh019",
              f"SELECT count() FROM {table} WHERE event_id='group-missing' AND mapContains(record,'metric.value')",
              "0", kind="semantic")
        c.sql(f"{prefix}.bool_false", "lh019",
              f"SELECT count() FROM {table} WHERE {type_of('metric.value')}='bool' AND {value('metric.value')}='false'",
              "1", kind="semantic")
        c.sql(f"{prefix}.int_zero", "lh019",
              f"SELECT count() FROM {table} WHERE {type_of('metric.value')}='int' AND toInt64({value('metric.value')})=0",
              "1", kind="semantic")
        c.sql(f"{prefix}.big_int_exact", "lh019",
              f"SELECT {value('metric.value')} FROM {table} WHERE event_id='big-int'", "9007199254740993", kind="semantic")
        c.sql(f"{prefix}.min_int_exact", "lh019",
              f"SELECT {value('metric.value')} FROM {table} WHERE event_id='min-int'", "-9223372036854775808", kind="semantic")
        observed = ch("lh019", f"SELECT event_id FROM {table} WHERE {type_of('metric.value')}='int' "
                               f"ORDER BY toInt64({value('metric.value')}), event_id")
        c.check(f"{prefix}.numeric_order", observed, "\n".join(expected_int_order()), kind="semantic")
        observed = ch("lh019", f"SELECT event_id FROM {table} WHERE {type_of('metric.value')}='int' "
                               f"ORDER BY {value('metric.value')}, event_id")
        c.check(f"{prefix}.lexical_order", observed, "\n".join(expected_lexical_order()), kind="characterization",
                note="lexical order differs from numeric order; the typed path uses the numeric one")
        c.sql(f"{prefix}.nonfinite_type_carried", "lh019",
              f"SELECT {type_of('metric.value')}, isNaN(toFloat64OrNull({value('metric.value')})) FROM {table} WHERE event_id='nan'",
              "double\t1", kind="semantic")
        c.sql(f"{prefix}.bytes_type_carried", "lh019",
              f"SELECT {type_of('metric.value')} FROM {table} WHERE event_id='bytes-row'", "bytes", kind="semantic")
        c.sql(f"{prefix}.nested_type_carried", "lh019",
              f"SELECT {type_of('metric.value')} FROM {table} WHERE event_id='nested-row'", "map", kind="semantic")
        c.sql(f"{prefix}.namespace_collide", "lh019",
              f"SELECT {dialect['resource_value']('res.collide')}, {value('collide')} FROM {table} WHERE event_id='typed-1'",
              "resource-value\trecord-value", kind="semantic")
        c.sql(f"{prefix}.json_like_type_distinct", "lh019",
              f"SELECT {type_of('str.json_like')}, {type_of('map.nested')} FROM {table} WHERE event_id='typed-1'",
              "string\tmap", kind="semantic")
        c.sql(f"{prefix}.case_sensitive", "lh019",
              f"SELECT {value('Foo')}, {dialect['resource_value']('Foo')}, {dialect['resource_value']('foo')} "
              f"FROM {table} WHERE event_id='typed-1'",
              "record-upper\tresource-upper\tresource-lower", kind="semantic")
        c.sql(f"{prefix}.duplicate_rejected_row_absent", "lh019",
              f"SELECT count() FROM {table} WHERE event_id='dup-1'", "0", kind="semantic",
              note="the harness mapping prototype rejects duplicate keys; the stock path stored them")
        # Aggregates: exact small sum, measured ClickHouse overflow behavior,
        # and the prototype's overflow rejection.
        c.sql(f"{prefix}.sum_small_exact", "lh019",
              f"SELECT sum(toInt128({value('metric.value')})) FROM {table} "
              f"WHERE {type_of('metric.value')}='int' AND event_id IN ('sum-small-a','sum-small-b')",
              "3", kind="semantic")
        c.sql(f"{prefix}.sum_int64_wraps_in_clickhouse", "lh019",
              f"SELECT sum(toInt64({value('metric.value')})) FROM {table} "
              f"WHERE {type_of('metric.value')}='int' AND event_id IN ('sum-a','sum-b','sum-c')",
              "-1", kind="characterization",
              note="measured ClickHouse behavior: sum(Int64) wraps; the prototype must guard it")
        c.sql(f"{prefix}.sum_int128_wide", "lh019",
              f"SELECT sum(toInt128({value('metric.value')})) FROM {table} "
              f"WHERE {type_of('metric.value')}='int' AND event_id IN ('sum-a','sum-b','sum-c')",
              "18446744073709551615", kind="characterization",
              note="Int128 accumulation keeps the true total so the prototype can range-check it")
        c.sql(f"{prefix}.aggregate_incompatible_type_count", "lh019",
              f"SELECT count() FROM {table} WHERE mapContains(record,'metric.value') AND {type_of('metric.value')} != 'int'",
              str(len([1 for _, t, _ in metric_rows() if t != "int"])), kind="semantic",
              note="the prototype must error when these rows are not excluded by a type filter")
        c.sql(f"{prefix}.aggregate_contributing_counts", "lh019",
              f"SELECT countIf(mapContains(record,'metric.value')), countIf({type_of('metric.value')}='int') FROM {table}",
              f"{len(metric_rows())}\t{len([1 for _, t, _ in metric_rows() if t == 'int'])}", kind="semantic",
              note="present rows versus contributing int rows; missing values are not zero")
    else:
        c.sql(f"{prefix}.untyped_filter_collides", "lh019",
              f"SELECT count() FROM {table} WHERE {value('metric.value')} = '1'",
              str(expected_stock_string_count("1")), kind="characterization",
              note="without type information the string Map cannot answer the typed filter")
        c.sql(f"{prefix}.no_type_information", "lh019",
              f"SELECT {type_of('metric.value')} FROM {table} WHERE event_id='group-int-1'", "", kind="characterization")


def phase_stock_oracle(c: Checks, results: Path):
    """Independent oracle: the pinned stock exporter's stored values.

    The representation mappings are built with `model.stock_string`; comparing
    them only with expectations produced by the same helper would be circular.
    This phase compares both the canonical payload decode and the representation
    serving values with the rows the real Collector run wrote into
    `stock_map.otel_logs`.
    """
    try:
        stock_text = ch("stock_map", "SELECT Body, ResourceAttributes, ScopeAttributes, LogAttributes "
                                     "FROM otel_logs FORMAT JSONEachRow")
    except Exception as exc:
        c.check("oracle.stock_table_available", f"ERROR: {exc}", "present")
        return
    # JSONEachRow keeps the selected column names; the stock table uses capitals.
    stock = {}
    for line in stock_text.splitlines():
        if line.strip():
            row = json.loads(line)
            stock[row["Body"]] = row
    body_by_event = {event["event_id"]: event["body"][1] for event in fixture.EVENTS}
    rep_text = ch("lh019", "SELECT event_id, resource, scope, record FROM rep_type_map FORMAT JSONEachRow")
    rep = {}
    for line in rep_text.splitlines():
        if line.strip():
            row = json.loads(line)
            rep[row["event_id"]] = row
    matched = 0
    mismatches = []
    for event_id, row in rep.items():
        body = body_by_event.get(event_id)
        source = stock.get(body)
        if source is None:
            mismatches.append(f"{event_id}: no stock row")
            continue
        matched += 1
        for namespace, column in (("resource", "ResourceAttributes"), ("scope", "ScopeAttributes"),
                                  ("record", "LogAttributes")):
            if row[namespace] != source[column]:
                mismatches.append(f"{event_id}.{namespace}")
    c.check("oracle.rep_serving_matches_stock_exporter",
            "ok" if not mismatches else f"mismatch: {mismatches[0]}", "ok",
            kind="semantic",
            note="rep_type_map values compared with the rows the pinned exporter stored")
    c.check("oracle.rep_rows_matched", str(matched), str(len(rep)), kind="semantic")

    payload_path = results / "canonical-payloads.jsonl"
    if not payload_path.exists():
        c.check("oracle.canonical_strings_match_stock_exporter", "missing payloads", "ok")
        return
    payloads = {}
    for line in payload_path.read_text().splitlines():
        if line.strip():
            record = json.loads(line)
            payloads[record["event_id"]] = json.loads(record["payload"])
    conversion_mismatches = []
    for event_id, doc in payloads.items():
        source = stock.get(body_by_event.get(event_id))
        if source is None:
            conversion_mismatches.append(f"{event_id}: no stock row")
            continue
        for namespace, column in (("resource", "ResourceAttributes"), ("scope", "ScopeAttributes"),
                                  ("record", "LogAttributes")):
            for key, node in doc[namespace].items():
                type_name, value = decode_canonical(node)
                if model.stock_string(type_name, value) != source[column].get(key):
                    conversion_mismatches.append(f"{event_id}.{namespace}.{key}")
    c.check("oracle.canonical_strings_match_stock_exporter",
            "ok" if not conversion_mismatches else f"mismatch: {conversion_mismatches[0]}", "ok",
            kind="semantic",
            note="model.stock_string output for each decoded canonical value compared with the exporter's stored string")


def phase_semantics(c: Checks, results: Path):
    try:
        _phase_semantics(c, results)
    except Exception as exc:
        # A crash must be visible as a failed check, not as a silently missing
        # phase: the earlier outcomes are kept.
        c.check("semantics.error", f"ERROR: {type(exc).__name__}: {exc}", "ok")


def _phase_semantics(c: Checks, results: Path):
    phase_stock_oracle(c, results)
    for dialect in (companion_dialect("rep_type_map"),
                    tagged_dialect("rep_tagged_map"),
                    companion_dialect("rep_map_carried"),
                    stock_dialect("rep_stock_map"),
                    stock_dialect("rep_payload_only")):
        semantics_checks(c, dialect)
    # Payload presence does not repair the string Map: the same untyped filter
    # still collides, while the payload extraction can answer correctly.
    c.sql("semantics.payload_only.untyped_filter_still_collides", "lh019",
          "SELECT count() FROM rep_payload_only WHERE record['metric.value'] = '1'",
          str(expected_stock_string_count("1")),
          kind="characterization", note="a preserved payload does not change Map query results")
    c.sql("semantics.payload_only.payload_extraction_typed", "lh019",
          "SELECT count() FROM rep_payload_only WHERE "
          "JSONExtract(payload, 'record', 'metric.value', 't', 'String') = 'int' AND "
          "JSONExtract(payload, 'record', 'metric.value', 'v', 'String') = '1'",
          str(expected_type_value_count("int", "1")),
          kind="characterization", note="the payload can be parsed at query time, at extraction cost")
    c.sql("semantics.type_map.serving_values_are_stock_strings", "lh019",
          "SELECT record['int.one'], record['double.one'], record['str.one'] FROM rep_type_map "
          "WHERE event_id='typed-1'", "1\t1\t1", kind="semantic",
          note="companion-type serving values are the stock strings; only type info is added")
    c.sql("semantics.tagged.value_encoding_roundtrip", "lh019",
          "SELECT record['int.one'], record['double.one'], record['str.one'], record['empty.value'], "
          "record['bytes.raw'] FROM rep_tagged_map WHERE event_id='typed-1'",
          "i:1\td:1\ts:1\te:\ty:AAH+/w==", kind="semantic")
    c.sql("semantics.map_carried.payload_projection_matches", "lh019",
          "SELECT count() FROM rep_map_carried WHERE payload = record['lighthouse.payload']", "27", kind="semantic")
    # Span scope attributes: the representation keeps them distinct.
    c.sql("semantics.span_scope.namespace_separated", "lh019",
          "SELECT span_types['span.scope.attr'], span_scope_types['scope.span.attr'], span_scope['scope.span.attr'] "
          "FROM rep_span_scope", "string\tstring\ts", kind="semantic")
    # Prototype rejections (harness behavior, not ClickHouse guarantees).
    for event_id, operation in (("nan", "compare"), ("bytes-row", "compare"), ("nested-row", "compare")):
        event = next(e for e in fixture.EVENTS if e["event_id"] == event_id)
        key, type_name, value = event["record"][0]
        value_text = model.stock_string(type_name, value)
        try:
            prototype_compare(type_name, value_text)
            observed = "allowed"
        except UnsupportedOperation as exc:
            observed = f"rejected: {exc}"
        c.check(f"prototype.{event_id}.unsupported_operation_rejected", observed.split(":")[0], "rejected",
                kind="prototype", note="harness query-prototype behavior, not a ClickHouse guarantee")
    try:
        prototype_int_aggregate([fixture.INT64_MAX, fixture.INT64_MAX, 1])
        observed = "wrapped"
    except UnsupportedOperation:
        observed = "rejected"
    c.check("prototype.aggregate_overflow_rejected", observed, "rejected", kind="prototype",
            note="harness query-prototype behavior, not a ClickHouse guarantee")
    phase_canonical(c, results)


def phase_cost(c: Checks, results: Path):
    """Measure storage and execution cost; a few sanity assertions only."""
    tables = ["rep_stock_map", "rep_payload_only", "rep_type_map", "rep_tagged_map", "rep_map_carried"]
    # Merge parts so per-column bytes compare the same data layout instead of
    # async-insert part boundaries. This is a 50k-row micro-measurement.
    for table in tables:
        ch("lh019_cost", f"OPTIMIZE TABLE {table} FINAL", timeout=300)
    storage = []
    for table in tables:
        total = scalar("lh019_cost", f"SELECT sum(bytes_on_disk) FROM system.parts "
                                     f"WHERE database='lh019_cost' AND table='{table}' AND active")
        cols = rows("lh019_cost", f"SELECT name, data_compressed_bytes, data_uncompressed_bytes "
                                  f"FROM system.columns WHERE database='lh019_cost' AND table='{table}' "
                                  f"ORDER BY name")
        storage.append({"table": table, "bytes_on_disk": int(total or 0),
                        "columns": [{"name": n, "compressed": int(c_), "uncompressed": int(u)}
                                    for n, c_, u in cols]})
        c.sql(f"cost.{table}.row_count", "lh019_cost", f"SELECT count() FROM {table}", "50000")
    (results / "cost-storage.jsonl").write_text("".join(json.dumps(s) + "\n" for s in storage))

    queries = {
        "stock_map.untyped_filter": ("rep_stock_map", "SELECT count() FROM rep_stock_map WHERE record['int.one'] = '5'"),
        "payload_only.extract_filter": ("rep_payload_only",
                                        "SELECT count() FROM rep_payload_only WHERE "
                                        "JSONExtract(payload, 'record', 'int.one', 't', 'String') = 'int' AND "
                                        "JSONExtract(payload, 'record', 'int.one', 'v', 'String') = '5'"),
        "type_map.typed_filter": ("rep_type_map",
                                  "SELECT count() FROM rep_type_map WHERE record_types['int.one'] = 'int' "
                                  "AND toInt64(record['int.one']) = 5"),
        "type_map.group_by_type": ("rep_type_map",
                                   "SELECT record_types['int.one'], count() FROM rep_type_map "
                                   "WHERE mapContains(record,'int.one') GROUP BY 1"),
        "type_map.numeric_order": ("rep_type_map",
                                   "SELECT toInt64(record['int.one']) FROM rep_type_map "
                                   "WHERE record_types['int.one'] = 'int' ORDER BY 1 DESC LIMIT 5"),
        "type_map.aggregate": ("rep_type_map",
                               "SELECT sum(toInt128(record['int.one'])) FROM rep_type_map "
                               "WHERE record_types['int.one'] = 'int'"),
        "tagged_map.typed_filter": ("rep_tagged_map",
                                    "SELECT count() FROM rep_tagged_map WHERE "
                                    "splitByChar(':', record['int.one'])[1] = 'i' "
                                    "AND toInt64(arrayStringConcat(arraySlice(splitByChar(':', record['int.one']), 2), ':')) = 5"),
        "tagged_map.group_by_type": ("rep_tagged_map",
                                     "SELECT splitByChar(':', record['int.one'])[1], count() FROM rep_tagged_map "
                                     "WHERE mapContains(record,'int.one') GROUP BY 1"),
        "tagged_map.aggregate": ("rep_tagged_map",
                                 "SELECT sum(toInt128(arrayStringConcat(arraySlice(splitByChar(':', record['int.one']), 2), ':'))) "
                                 "FROM rep_tagged_map WHERE splitByChar(':', record['int.one'])[1] = 'i'"),
        "map_carried.typed_filter": ("rep_map_carried",
                                     "SELECT count() FROM rep_map_carried WHERE record_types['int.one'] = 'int' "
                                     "AND toInt64(record['int.one']) = 5"),
    }
    measurements = []
    for name, (table, sql) in queries.items():
        marked = f"/* q19-{name} */ {sql}"
        # Run twice; the second (warm) execution is the recorded one.
        for _ in range(2):
            ch("lh019_cost", marked)
        text = ""
        for _ in range(20):
            # Prefix match on the marker so the query_log reader's own text
            # (which contains the marker in its LIKE pattern) is never matched.
            text = ch("lh019_cost", "SELECT query_duration_ms, read_rows, read_bytes, result_rows "
                                    f"FROM system.query_log WHERE type='QueryFinish' AND query LIKE '/* q19-{name} */%' "
                                    "ORDER BY event_time DESC LIMIT 1")
            if text:
                break
            time.sleep(0.5)
        parts = text.split("\t") if text else ["", "", "", ""]
        measurements.append({"query": name, "table": table,
                             "duration_ms": float(parts[0] or 0), "read_rows": int(parts[1] or 0),
                             "read_bytes": int(parts[2] or 0), "result_rows": int(parts[3] or 0)})
    (results / "cost-queries.jsonl").write_text("".join(json.dumps(m) + "\n" for m in measurements))
    for m in measurements:
        c.info(f"cost.query.{m['query']}",
               f"{m['duration_ms']}ms rows={m['read_rows']} bytes={m['read_bytes']}")
    by_table = {s["table"]: s for s in storage}
    def column_bytes(table, column):
        for col in by_table[table]["columns"]:
            if col["name"] == column:
                return col["compressed"]
        return 0
    c.check("cost.map_carried.payload_in_map_and_materialized",
            column_bytes("rep_map_carried", "payload") > 0
            and column_bytes("rep_map_carried", "record") > column_bytes("rep_stock_map", "record"),
            True, kind="measurement",
            note="Map-carried payload bloats the Map and the MATERIALIZED column duplicates it")
    c.check("cost.type_map_overhead_positive",
            column_bytes("rep_type_map", "record_types") > 0, True, kind="measurement")
    c.check("cost.tagged_overhead_positive",
            column_bytes("rep_tagged_map", "record") > column_bytes("rep_stock_map", "record"),
            True, kind="measurement")


def phase_files(c: Checks, results: Path):
    for label, path in (("stock_map", "emit-stock-map-logs.jsonl"),
                        ("stock_json_nan", "emit-stock-json-logs-nan.jsonl"),
                        ("stock_json_safe", "emit-stock-json-logs-safe.jsonl")):
        p = results / path
        if p.exists():
            rec = json.loads(p.read_text().splitlines()[0])
            c.check(f"files.{label}.producer_status", rec["status"], 200)
    metrics = results / "metrics-stock-json.txt"
    if metrics.exists():
        values = {}
        for line in metrics.read_text().splitlines():
            if line.startswith("#") or not line.strip():
                continue
            name = line.split("{", 1)[0].split(" ", 1)[0]
            values.setdefault(name, line.rsplit(" ", 1)[-1])
        c.check("files.stock_json.nan_send_failed_records",
                values.get("otelcol_exporter_send_failed_log_records", "absent"), "28",
                kind="characterization", note="the whole NaN-bearing request failed, not one record")
    log = results / "stock-json.log"
    if log.exists():
        text = log.read_text()
        c.check("files.stock_json.nan_marshal_error",
                "failed to marshal json log attributes: json: unsupported value: NaN" in text, True,
                kind="characterization")
        c.check("files.stock_json.nan_dropped_items", "dropped_items\": 28" in text, True,
                kind="characterization")


# ---------------------------------------------------------------------------

def run_phase(phase, results, capture=False):
    c = Checks(capture=capture)
    if phase == "stock-map":
        phase_stock_map(c)
    elif phase == "stock-json":
        phase_stock_json(c)
    elif phase == "managed":
        phase_managed(c, results)
    elif phase == "transform":
        phase_transform(c, results)
    elif phase == "identity":
        phase_identity(c, results)
    elif phase == "semantics":
        phase_semantics(c, results)
    elif phase == "cost":
        phase_cost(c, results)
    elif phase == "files":
        phase_files(c, results)
    else:
        raise SystemExit(f"unknown phase {phase}")
    return c


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--results", default="results")
    parser.add_argument("--capture", action="store_true", help="record observations without failing")
    parser.add_argument("--out")
    args = parser.parse_args(argv)

    results = Path(args.results)
    c = run_phase(args.phase, results, capture=args.capture)
    for item in c.outcomes:
        mark = "PASS" if item["ok"] else ("CAPTURE" if args.capture else "FAIL")
        print(f"[{mark}] ({item['kind']}) {item['name']}: expected={item['expected']!r} observed={item['observed']!r}")
    if args.out:
        (results / args.out).write_text("".join(json.dumps(item) + "\n" for item in c.outcomes))
    total = len(c.outcomes)
    print(f"\n{args.phase}: {total - c.failures}/{total} checks passed"
          + (" (capture mode)" if args.capture else ""))
    return 1 if c.failures else 0


if __name__ == "__main__":
    sys.exit(main())
