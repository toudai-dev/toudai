#!/usr/bin/env python3
"""Assertions and measurements for Experiment 020 (native typed Map comparison).

Phases:

  capabilities  pinned ClickHouse 26.8.2.7 type/behavior probes (characterization)
  semantics     companion baseline, native typed with a full type sidecar, and
                native typed with minimal canonical-only bookkeeping, checked
                against expectations computed from the typed inventory
  negative      deliberately wrong layouts; shows the semantics assertions are
                discriminating and that ClickHouse cannot detect the mistake
  cost          storage and warm execution cost for both corpora (measurement)
  files         imported-source hashes and evidence-file presence

Expected values are computed from the Experiment 019 typed inventory in
`fixture020.py`, or authored as literals from that inventory. They are never
computed by the mapping code under test (`mapping020.py` / the reused
Experiment 019 `model.py` encoding), which is why this module does not import
either.

`prototype_*` checks implement contract rules in the harness query prototype
(unsupported-operation rejection, integer-overflow rejection). They are not
ClickHouse guarantees.
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import sys
import time
from pathlib import Path

import requests

import fixture020

CH = "http://127.0.0.1:18125/"

# Pinned sha256 of the imported Experiment 019 sources. A change to the shared
# fixture or encoding must fail this experiment instead of silently changing
# what it compared.
EXPECTED_SOURCE_HASHES = {
    "fixture.py": "375934344f9afe55ddd27de9f9b37ef2842bf5d6d92b472300c4baa9aff2be86",
    "model.py": "dcd4e2a3c70053f58a749dc414489ffd90437e317e8fbabc1f793f3f6a474a7d",
    "represent.py": "419aa638439f37690a4764805b4384f368703694c68898bd0d6fbb7b4bcfc2f8",
}

SCALARS = ("int", "double", "string", "bool")
VARIANTS = ("companion", "native_typed", "native_minimal")
COST_TABLES = {
    "companion": "rep_companion",
    "native_typed": "rep_native_typed",
    "native_minimal": "rep_native_minimal",
}
COST_QUERIES = {
    "base": ("int_filter", "string_filter", "group_by_type", "numeric_order", "sum_typed", "presence",
             "canonical_bytes_payload"),
    "diverse": ("int_filter", "string_filter", "group_by_type", "numeric_order", "sum_typed", "presence"),
}
COST_ROWS = 50_000
# One database per corpus so the two corpora never share tables (and therefore
# never share parts or storage measurements).
COST_DBS = {"base": "lh020_cost_base", "diverse": "lh020_cost_diverse"}


def ch(db, sql, fmt="TSV", timeout=300):
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
    if total > fixture020.fixture019.INT64_MAX or total < fixture020.fixture019.INT64_MIN:
        raise UnsupportedOperation("integer aggregate overflow")
    return total


# ---------------------------------------------------------------------------
# Expectations computed from the typed inventory
# ---------------------------------------------------------------------------

def stored_events():
    return [event for event in fixture020.EVENTS if not event["duplicate"]]


def metric_rows():
    out = []
    for event in stored_events():
        for key, type_name, value in event["record"]:
            if key == "metric.value":
                out.append((event["event_id"], type_name, value))
    return out


def expected_type_counts():
    counts = {}
    for _, type_name, _ in metric_rows():
        counts[type_name] = counts.get(type_name, 0) + 1
    return sorted(counts.items())


def expected_type_value_count(type_name, value):
    return len([1 for _, t, v in metric_rows() if t == type_name and v == value])


def expected_int_order():
    pairs = [(value, event_id) for event_id, type_name, value in metric_rows() if type_name == "int"]
    return [event_id for _, event_id in sorted(pairs)]


def expected_lexical_order():
    pairs = [(str(value), event_id) for event_id, type_name, value in metric_rows() if type_name == "int"]
    return [event_id for _, event_id in sorted(pairs)]


def expected_int_sum():
    return sum(value for _, type_name, value in metric_rows() if type_name == "int")


def expected_present_count():
    return len(metric_rows())


def expected_int_count():
    return len([1 for _, type_name, _ in metric_rows() if type_name == "int"])


def expected_default_matches(type_name, default):
    """Rows a lookup of `default` matches: absent key plus an explicit default value.

    This is the missing-Map-lookup default behavior: `record_int['metric.value']`
    is 0 for every row that has no integer `metric.value`, so a filter without a
    presence guard silently matches those rows.
    """
    matches = 0
    for event in stored_events():
        values = [value for key, t, value in event["record"] if key == "metric.value" and t == type_name]
        if not values or values[0] == default:
            matches += 1
    return matches


def wrapped_int64(value):
    """Two's-complement Int64 wrap of an integer total (measured behavior)."""
    return ((value + 2**63) % 2**64) - 2**63


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


# ---------------------------------------------------------------------------
# Independent canonical decode (separate from the encoder used by the mapping)
# ---------------------------------------------------------------------------

def decode_canonical(node):
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
        return ("bytes", base64.b64decode(node["v"]))
    if t == "array":
        return ("array", [decode_canonical(item)[1] for item in node["v"]])
    if t == "map":
        return ("map", {key: decode_canonical(value)[1] for key, value in node["v"].items()})
    raise ValueError(f"unknown canonical type {t}")


def _special_double(value):
    if isinstance(value, str):
        return {"nan": float("nan"), "inf": float("inf"), "-inf": float("-inf")}.get(value, float("nan"))
    return value


def _parse_json_int(text):
    """Preserve negative zero: JSON writes Float64 -0.0 as `-0`, which Python's
    default int parser would collapse to integer 0 and lose the sign of."""
    return -0.0 if text == "-0" else int(text)


def _values_equal(type_name, a, b):
    if type_name == "double":
        a, b = float(_special_double(a)), float(_special_double(b))
        if a != a and b != b:  # NaN
            return True
        if a == 0 and b == 0:
            return math.copysign(1.0, a) == math.copysign(1.0, b)
        return a == b
    if type_name == "int":
        return int(a) == int(b)
    if type_name == "bool":
        return bool(a) == bool(b)
    return a == b


def load_payloads(results: Path) -> dict:
    path = results / "canonical-payloads.jsonl"
    payloads = {}
    if not path.exists():
        return payloads
    for line in path.read_text().splitlines():
        if line.strip():
            record = json.loads(line)
            payloads[record["event_id"]] = json.loads(record["payload"])
    return payloads


# ---------------------------------------------------------------------------
# Capabilities
# ---------------------------------------------------------------------------

def cap(c, name, table, create, insert, select_sql, expected, raw=False):
    db = "lh020_caps"
    try:
        if table:
            ch(db, f"DROP TABLE IF EXISTS {table}")
            ch(db, create)
        if insert:
            ch(db, insert)
        observed = ch(db, select_sql, fmt="JSONEachRow") if raw else scalar(db, select_sql)
    except Exception as exc:
        c.check(f"capabilities.{name}", f"ERROR: {str(exc)[:160]}", expected, kind="characterization")
        return
    c.check(f"capabilities.{name}", observed, expected, kind="characterization")


def phase_capabilities(c: Checks, results: Path):
    ch("default", "DROP DATABASE IF EXISTS lh020_caps")
    ch("default", "CREATE DATABASE lh020_caps")
    probes = [
        ("map_string_int64", "cap_int64", "CREATE TABLE cap_int64 (m Map(String, Int64)) ENGINE=Memory",
         'INSERT INTO cap_int64 FORMAT JSONEachRow\n{"m":{"a":1}}',
         "SELECT m['a'], toTypeName(m['a']) FROM cap_int64", "1\tInt64"),
        ("map_string_float64", "cap_f64", "CREATE TABLE cap_f64 (m Map(String, Float64)) ENGINE=Memory",
         'INSERT INTO cap_f64 FORMAT JSONEachRow\n{"m":{"a":1.5}}',
         "SELECT m['a'], toTypeName(m['a']) FROM cap_f64", "1.5\tFloat64"),
        ("map_string_string", "cap_str", "CREATE TABLE cap_str (m Map(String, String)) ENGINE=Memory",
         'INSERT INTO cap_str FORMAT JSONEachRow\n{"m":{"a":"s"}}',
         "SELECT m['a'], toTypeName(m['a']) FROM cap_str", "s\tString"),
        ("map_string_bool", "cap_bool", "CREATE TABLE cap_bool (m Map(String, Bool)) ENGINE=Memory",
         'INSERT INTO cap_bool FORMAT JSONEachRow\n{"m":{"t":true,"f":false}}',
         "SELECT m['t'], m['f'], toString(m['f']), toTypeName(m['f']) FROM cap_bool",
         "true\tfalse\tfalse\tBool"),
        ("map_string_uint8", "cap_u8", "CREATE TABLE cap_u8 (m Map(String, UInt8)) ENGINE=Memory",
         'INSERT INTO cap_u8 FORMAT JSONEachRow\n{"m":{"a":1}}',
         "SELECT m['a'], toTypeName(m['a']) FROM cap_u8", "1\tUInt8"),
        ("map_lc_string_int64", "cap_lc_int", "CREATE TABLE cap_lc_int (m Map(LowCardinality(String), Int64)) ENGINE=Memory",
         'INSERT INTO cap_lc_int FORMAT JSONEachRow\n{"m":{"a":1}}',
         "SELECT m['a'], toTypeName(m['a']) FROM cap_lc_int", "1\tInt64"),
        ("map_lc_string_float64", "cap_lc_f64", "CREATE TABLE cap_lc_f64 (m Map(LowCardinality(String), Float64)) ENGINE=Memory",
         'INSERT INTO cap_lc_f64 FORMAT JSONEachRow\n{"m":{"a":1.5}}',
         "SELECT m['a'], toTypeName(m['a']) FROM cap_lc_f64", "1.5\tFloat64"),
        ("map_lc_string_bool", "cap_lc_bool", "CREATE TABLE cap_lc_bool (m Map(LowCardinality(String), Bool)) ENGINE=Memory",
         'INSERT INTO cap_lc_bool FORMAT JSONEachRow\n{"m":{"a":true}}',
         "SELECT m['a'], toTypeName(m['a']) FROM cap_lc_bool", "true\tBool"),
        ("map_lc_string_string", "cap_lc_str", "CREATE TABLE cap_lc_str (m Map(LowCardinality(String), String)) ENGINE=Memory",
         'INSERT INTO cap_lc_str FORMAT JSONEachRow\n{"m":{"a":"s"}}',
         "SELECT m['a'], toTypeName(m['a']) FROM cap_lc_str", "s\tString"),
        ("map_lc_value_not_preserved", "cap_lc_lc",
         "CREATE TABLE cap_lc_lc (m Map(LowCardinality(String), LowCardinality(String))) ENGINE=Memory",
         'INSERT INTO cap_lc_lc FORMAT JSONEachRow\n{"m":{"a":"s"}}',
         "SELECT m['a'], toTypeName(m['a']) FROM cap_lc_lc", "s\tString"),
        ("map_lc_string_enum8", "cap_enum", "CREATE TABLE cap_enum (m Map(LowCardinality(String), Enum8('s' = 1, 'i' = 2))) ENGINE=Memory",
         "INSERT INTO cap_enum FORMAT JSONEachRow\n{\"m\":{\"k\":\"i\"}}",
         "SELECT m['k'] FROM cap_enum", "i"),
        ("map_lc_string_int128", "cap_i128", "CREATE TABLE cap_i128 (m Map(LowCardinality(String), Int128)) ENGINE=Memory",
         'INSERT INTO cap_i128 FORMAT JSONEachRow\n{"m":{"a":1}}',
         "SELECT m['a'], toTypeName(m['a']) FROM cap_i128", "1\tInt128"),
        ("map_nullable_float64", "cap_nullable",
         "CREATE TABLE cap_nullable (m Map(String, Nullable(Float64))) ENGINE=Memory",
         "INSERT INTO cap_nullable VALUES (map('a', CAST(NULL, 'Nullable(Float64)'), 'b', 1.5))",
         "SELECT mapContains(m,'a'), isNull(m['a']), m['b'] FROM cap_nullable", "1\t1\t1.5"),
        ("json_each_row_nonfinite_doubles", "cap_nonfinite",
         "CREATE TABLE cap_nonfinite (m Map(String, Float64)) ENGINE=Memory",
         'INSERT INTO cap_nonfinite FORMAT JSONEachRow\n{"m":{"nan":NaN,"inf":Infinity,"ninf":-Infinity,"half":1.5}}',
         "SELECT isNaN(m['nan']), isInfinite(m['inf']), m['ninf'], m['half'], toString(m['nan']) FROM cap_nonfinite",
         "1\t1\t-inf\t1.5\tnan"),
        ("missing_key_default_int", None, None, None,
         "SELECT map('a',1)['b'], mapContains(map('a',1),'b')", "0\t0"),
        ("missing_key_default_string", None, None, None,
         "SELECT map('a','x')['b'], mapContains(map('a','x'),'b')", "\t0"),
        ("missing_key_default_bool", None, None, None,
         "SELECT map('a',true)['b']", "false"),
        ("missing_key_default_float", None, None, None,
         "SELECT map('a',1.5)['b']", "0"),
        ("duplicate_keys_stored", None, None, None,
         "SELECT length(mapKeys(map('a',1,'a',2))), map('a',1,'a',2)['a']", "2\t1"),
        ("sum_int64_wraps", None, None, None,
         "SELECT sum(x) FROM (SELECT arrayJoin([9223372036854775807, 9223372036854775807, 1])::Int64 AS x)",
         "-1"),
        ("sum_int128_wide", None, None, None,
         "SELECT sum(toInt128(x)) FROM (SELECT arrayJoin([9223372036854775807, 9223372036854775807, 1])::Int64 AS x)",
         "18446744073709551615"),
        ("negative_zero", None, None, None,
         "SELECT toString(toFloat64(-0.0)), toFloat64(-0.0) = 0", "-0\t1"),
        ("bool_false_not_int_zero", None, None, None,
         "SELECT map('b',false)['b'], map('i',0)['i']", "false\t0"),
        ("json_quote_denormals", None, None, None,
         "SELECT toFloat64(nan) AS n, toFloat64(-0.0) AS z SETTINGS output_format_json_quote_denormals=1 FORMAT JSONEachRow",
         '{"n":"nan","z":-0}'),
    ]
    for name, table, create, insert, select_sql, expected in probes:
        cap(c, name, table, create, insert, select_sql, expected, raw=select_sql.endswith("FORMAT JSONEachRow"))


# ---------------------------------------------------------------------------
# Semantics
# ---------------------------------------------------------------------------

def companion_checks(c: Checks):
    table = "rep_companion"
    prefix = "semantics.companion"
    c.sql(f"{prefix}.row_count", "lh020", f"SELECT count() FROM {table}", "27", kind="semantic")
    c.sql(f"{prefix}.untyped_filter_collides", "lh020",
          f"SELECT count() FROM {table} WHERE record['metric.value'] = '1'", "5",
          kind="characterization",
          note="int 1, double 1.0, and string \"1\" all match the same string; "
               "Experiment 019 measured this same collision")
    c.sql(f"{prefix}.serving_values", "lh020",
          f"SELECT record['int.one'], record['double.one'], record['str.one'] FROM {table} WHERE event_id='typed-1'",
          "1\t1\t1", kind="semantic")
    c.sql(f"{prefix}.type_map_carried", "lh020",
          f"SELECT record_types['int.one'], record_types['double.one'], record_types['str.one'] "
          f"FROM {table} WHERE event_id='typed-1'", "int\tdouble\tstring", kind="semantic")
    c.sql(f"{prefix}.presence_semantics", "lh020",
          f"SELECT record['empty.value'] = record['str.empty'], mapContains(record,'empty.value'), "
          f"mapContains(record,'missing.key') FROM {table} WHERE event_id='typed-1'", "1\t1\t0",
          kind="characterization", note="empty and empty string collide in the value Map")
    c.sql(f"{prefix}.typed_filter_with_type_map", "lh020",
          f"SELECT count() FROM {table} WHERE record_types['metric.value']='int' "
          f"AND toInt64(record['metric.value']) = 1", str(expected_type_value_count("int", 1)), kind="semantic")
    c.sql(f"{prefix}.presence_count", "lh020",
          f"SELECT countIf(mapContains(record_types,'metric.value')) FROM {table}",
          str(expected_present_count()), kind="semantic")
    c.sql(f"{prefix}.aggregate_contributing_counts", "lh020",
          f"SELECT countIf(mapContains(record_types,'metric.value')), "
          f"countIf(record_types['metric.value']='int') FROM {table}",
          f"{expected_present_count()}\t{expected_int_count()}", kind="semantic")
    c.sql(f"{prefix}.sum_int64_wraps", "lh020",
          f"SELECT sum(toInt64(record['metric.value'])) FROM {table} "
          f"WHERE record_types['metric.value']='int' AND event_id IN ('sum-a','sum-b','sum-c')", "-1",
          kind="characterization", note="measured ClickHouse behavior: sum(Int64) wraps")
    c.sql(f"{prefix}.duplicate_rejected_row_absent", "lh020",
          f"SELECT count() FROM {table} WHERE event_id='dup-1'", "0", kind="semantic")


def native_typed_checks(c: Checks, table: str, results: Path):
    prefix = "semantics.native_typed"
    c.sql(f"{prefix}.row_count", "lh020", f"SELECT count() FROM {table}", "27", kind="semantic")
    observed = ch("lh020", f"SELECT record_types['metric.value'] AS t, count() FROM {table} "
                           f"WHERE mapContains(record_types,'metric.value') GROUP BY t ORDER BY t")
    c.check(f"{prefix}.type_counts", observed,
            "\n".join(f"{t}\t{n}" for t, n in expected_type_counts()), kind="semantic")
    c.sql(f"{prefix}.typed_filter_int", "lh020",
          f"SELECT count() FROM {table} WHERE record_types['metric.value']='int' "
          f"AND record_int['metric.value'] = 1", str(expected_type_value_count("int", 1)), kind="semantic")
    c.sql(f"{prefix}.typed_filter_double", "lh020",
          f"SELECT count() FROM {table} WHERE record_types['metric.value']='double' "
          f"AND record_double['metric.value'] = 1", str(expected_type_value_count("double", 1.0)), kind="semantic")
    c.sql(f"{prefix}.typed_filter_string", "lh020",
          f"SELECT count() FROM {table} WHERE record_types['metric.value']='string' "
          f"AND record_string['metric.value'] = '1'", str(expected_type_value_count("string", "1")), kind="semantic")
    c.sql(f"{prefix}.typed_filter_bool_false", "lh020",
          f"SELECT count() FROM {table} WHERE record_types['metric.value']='bool' "
          f"AND record_bool['metric.value'] = false", "1", kind="semantic")
    c.sql(f"{prefix}.typed_filter_excludes_other_types", "lh020",
          f"SELECT count() FROM {table} WHERE record_types['metric.value']='int' "
          f"AND record_int['metric.value'] = 1 AND event_id IN ('group-double-1','group-string-1')",
          "0", kind="semantic")
    c.sql(f"{prefix}.group_by_literals", "lh020",
          "SELECT sumIf(n, t='int' AND vi=1), sumIf(n, t='double' AND vd=1), "
          "sumIf(n, t='string' AND vs='1'), sumIf(n, t='bool' AND vb=false), "
          "sumIf(n, t='empty'), sumIf(n, t='string' AND vs=''), "
          "sumIf(n, t='double' AND isNaN(vd)), sumIf(n, t='bytes'), sumIf(n, t='map') FROM ("
          f"SELECT record_types['metric.value'] AS t, record_int['metric.value'] AS vi, "
          f"record_double['metric.value'] AS vd, record_string['metric.value'] AS vs, "
          f"record_bool['metric.value'] AS vb, count() AS n FROM {table} "
          "WHERE mapContains(record_types,'metric.value') GROUP BY t, vi, vd, vs, vb)",
          "3\t1\t1\t1\t1\t1\t1\t1\t1", kind="semantic",
          note="literal expectations authored from the typed inventory, independent of the mapping code")
    c.sql(f"{prefix}.presence_missing", "lh020",
          f"SELECT count() FROM {table} WHERE event_id='group-missing' "
          f"AND mapContains(record_types,'metric.value')", "0", kind="semantic")
    c.sql(f"{prefix}.presence_empty", "lh020",
          f"SELECT record_types['metric.value'], mapContains(record_int,'metric.value') "
          f"FROM {table} WHERE event_id='group-empty'", "empty\t0", kind="semantic")
    c.sql(f"{prefix}.presence_empty_string", "lh020",
          f"SELECT record_types['metric.value'], record_string['metric.value'] "
          f"FROM {table} WHERE event_id='group-empty-string'", "string\t", kind="semantic")
    c.sql(f"{prefix}.presence_nonfinite_supported", "lh020",
          f"SELECT record_types['metric.value'], mapContains(record_double,'metric.value') "
          f"FROM {table} WHERE event_id='nan'", "double\t1", kind="semantic",
          note="presence and type inspection of a non-finite double is supported; "
               "only its value comparison/aggregation is unsupported")
    c.sql(f"{prefix}.canonical_only_has_no_typed_value", "lh020",
          f"SELECT record_types['bytes.raw'], "
          f"mapContains(record_int,'bytes.raw') OR mapContains(record_double,'bytes.raw') "
          f"OR mapContains(record_string,'bytes.raw') OR mapContains(record_bool,'bytes.raw') "
          f"FROM {table} WHERE event_id='typed-1'", "bytes\t0", kind="semantic")
    c.sql(f"{prefix}.nested_only_has_no_typed_value", "lh020",
          f"SELECT record_types['map.nested'], mapContains(record_string,'map.nested') "
          f"FROM {table} WHERE event_id='typed-1'", "map\t0", kind="semantic")
    c.sql(f"{prefix}.canonical_only_types_distinct", "lh020",
          f"SELECT record_types['array.mixed'], record_types['map.nested'], record_types['empty.value'] "
          f"FROM {table} WHERE event_id='typed-1'", "array\tmap\tempty", kind="semantic",
          note="array, map, and explicit empty keep distinct type identity")
    c.sql(f"{prefix}.missing_lookup_default_int", "lh020",
          f"SELECT count() FROM {table} WHERE record_int['metric.value'] = 0",
          str(expected_default_matches("int", 0)), kind="semantic",
          note="a missing key lookup returns the type default; without a presence guard "
               "the filter also matches every row that has no integer value")
    c.sql(f"{prefix}.missing_lookup_default_string", "lh020",
          f"SELECT count() FROM {table} WHERE record_string['metric.value'] = ''",
          str(expected_default_matches("string", "")), kind="semantic",
          note="same default behavior for the string Map")
    c.sql(f"{prefix}.presence_guard_fixes_default", "lh020",
          f"SELECT count() FROM {table} WHERE mapContains(record_int,'metric.value') "
          f"AND record_int['metric.value'] = 0", "1", kind="semantic")
    c.sql(f"{prefix}.int_zero_typed", "lh020",
          f"SELECT count() FROM {table} WHERE record_types['metric.value']='int' "
          f"AND record_int['metric.value'] = 0", "1", kind="semantic")
    c.sql(f"{prefix}.big_int_exact", "lh020",
          f"SELECT record_int['metric.value'] FROM {table} WHERE event_id='big-int'",
          "9007199254740993", kind="semantic")
    c.sql(f"{prefix}.min_int_exact", "lh020",
          f"SELECT record_int['metric.value'] FROM {table} WHERE event_id='min-int'",
          "-9223372036854775808", kind="semantic")
    observed = ch("lh020", f"SELECT event_id FROM {table} WHERE record_types['metric.value']='int' "
                           f"ORDER BY record_int['metric.value'], event_id")
    c.check(f"{prefix}.numeric_order", observed, "\n".join(expected_int_order()), kind="semantic")
    observed = ch("lh020", f"SELECT event_id FROM {table} WHERE record_types['metric.value']='int' "
                           f"ORDER BY toString(record_int['metric.value']), event_id")
    c.check(f"{prefix}.lexical_order", observed, "\n".join(expected_lexical_order()),
            kind="characterization", note="lexical order differs from numeric order; the typed path uses the numeric one")
    c.sql(f"{prefix}.nonfinite_values_carried", "lh020",
          f"SELECT toString(record_double['metric.value']) FROM {table} "
          f"WHERE event_id IN ('nan','posinf','neginf') ORDER BY event_id", "nan\n-inf\ninf", kind="semantic")
    c.sql(f"{prefix}.negative_zero_preserved", "lh020",
          f"SELECT toString(record_double['metric.value']), record_double['metric.value'] = 0 "
          f"FROM {table} WHERE event_id='negzero'", "-0\t1", kind="semantic")
    c.sql(f"{prefix}.namespace_collide", "lh020",
          f"SELECT resource_string['res.collide'], record_string['collide'] FROM {table} WHERE event_id='typed-1'",
          "resource-value\trecord-value", kind="semantic")
    c.sql(f"{prefix}.case_sensitive", "lh020",
          f"SELECT record_string['Foo'], resource_string['Foo'], resource_string['foo'] "
          f"FROM {table} WHERE event_id='typed-1'", "record-upper\tresource-upper\tresource-lower", kind="semantic")
    c.sql(f"{prefix}.json_like_type_distinct", "lh020",
          f"SELECT record_types['str.json_like'], record_types['map.nested'] FROM {table} WHERE event_id='typed-1'",
          "string\tmap", kind="semantic")
    c.sql(f"{prefix}.type_change_across_rows", "lh020",
          f"SELECT countIf(record_types['metric.value']='int'), countIf(record_types['metric.value']='double'), "
          f"countIf(record_types['metric.value']='string') FROM {table}", "12\t5\t3", kind="semantic")
    c.sql(f"{prefix}.duplicate_rejected_row_absent", "lh020",
          f"SELECT count() FROM {table} WHERE event_id='dup-1'", "0", kind="semantic")
    c.sql(f"{prefix}.sum_small_exact", "lh020",
          f"SELECT sum(record_int['metric.value']) FROM {table} WHERE record_types['metric.value']='int' "
          f"AND event_id IN ('sum-small-a','sum-small-b')", "3", kind="semantic")
    c.sql(f"{prefix}.sum_int64_wraps_in_clickhouse", "lh020",
          f"SELECT sum(record_int['metric.value']) FROM {table} WHERE record_types['metric.value']='int' "
          f"AND event_id IN ('sum-a','sum-b','sum-c')", "-1", kind="characterization",
          note="native Int64 columns do not make sum(Int64) safe; it still wraps")
    c.sql(f"{prefix}.sum_int128_wide", "lh020",
          f"SELECT sum(toInt128(record_int['metric.value'])) FROM {table} WHERE record_types['metric.value']='int' "
          f"AND event_id IN ('sum-a','sum-b','sum-c')", "18446744073709551615", kind="characterization",
          note="Int128 accumulation keeps the true total so the prototype can range-check it")
    c.sql(f"{prefix}.aggregate_incompatible_type_count", "lh020",
          f"SELECT count() FROM {table} WHERE mapContains(record_types,'metric.value') "
          f"AND record_types['metric.value'] != 'int'",
          str(expected_present_count() - expected_int_count()), kind="semantic",
          note="the prototype must error when these rows are not excluded by a type filter")
    c.sql(f"{prefix}.aggregate_contributing_counts", "lh020",
          f"SELECT countIf(mapContains(record_types,'metric.value')), "
          f"countIf(record_types['metric.value']='int') FROM {table}",
          f"{expected_present_count()}\t{expected_int_count()}", kind="semantic",
          note="present rows versus contributing int rows; missing values are not zero")
    c.sql(f"{prefix}.unguarded_sum_includes_defaults", "lh020",
          f"SELECT sum(toInt128(record_int['metric.value'])) FROM {table}", str(expected_int_sum()),
          kind="characterization",
          note="without a type/presence guard the aggregate silently includes every row "
               "whose key is absent or another type as the default 0")
    c.sql(f"{prefix}.unguarded_sum_int64_wraps", "lh020",
          f"SELECT sum(record_int['metric.value']) FROM {table}", str(wrapped_int64(expected_int_sum())),
          kind="characterization",
          note="the fixture's integer total exceeds Int64, so the plain Int64 aggregate wraps "
               "even though the column type is native")
    serving_equivalence(c, table, "full", prefix, results)


def native_minimal_checks(c: Checks, table: str, results: Path):
    prefix = "semantics.native_minimal"
    presence = ("(mapContains(record_int,'metric.value') OR mapContains(record_double,'metric.value') "
                "OR mapContains(record_string,'metric.value') OR mapContains(record_bool,'metric.value') "
                "OR mapContains(record_other,'metric.value'))")
    derived_type = ("multiIf(mapContains(record_int,'metric.value'),'int', "
                    "mapContains(record_double,'metric.value'),'double', "
                    "mapContains(record_string,'metric.value'),'string', "
                    "mapContains(record_bool,'metric.value'),'bool', "
                    "mapContains(record_other,'metric.value'), record_other['metric.value'], '')")
    c.sql(f"{prefix}.row_count", "lh020", f"SELECT count() FROM {table}", "27", kind="semantic")
    observed = ch("lh020", f"SELECT {derived_type} AS t, count() FROM {table} WHERE {presence} GROUP BY t ORDER BY t")
    c.check(f"{prefix}.type_counts", observed,
            "\n".join(f"{t}\t{n}" for t, n in expected_type_counts()), kind="semantic",
            note="the canonical-only map carries the type for bytes/array/map/empty keys, so type "
                 "identity is preserved without a full per-key sidecar")
    c.sql(f"{prefix}.typed_filter_int", "lh020",
          f"SELECT count() FROM {table} WHERE mapContains(record_int,'metric.value') "
          f"AND record_int['metric.value'] = 1", str(expected_type_value_count("int", 1)), kind="semantic")
    c.sql(f"{prefix}.typed_filter_double", "lh020",
          f"SELECT count() FROM {table} WHERE mapContains(record_double,'metric.value') "
          f"AND record_double['metric.value'] = 1", str(expected_type_value_count("double", 1.0)), kind="semantic")
    c.sql(f"{prefix}.typed_filter_string", "lh020",
          f"SELECT count() FROM {table} WHERE mapContains(record_string,'metric.value') "
          f"AND record_string['metric.value'] = '1'", str(expected_type_value_count("string", "1")), kind="semantic")
    c.sql(f"{prefix}.group_by_literals", "lh020",
          "SELECT sumIf(n, t='int' AND vi=1), sumIf(n, t='double' AND vd=1), "
          "sumIf(n, t='string' AND vs='1'), sumIf(n, t='bool' AND vb=false), "
          "sumIf(n, t='empty'), sumIf(n, t='string' AND vs=''), "
          "sumIf(n, t='double' AND isNaN(vd)), sumIf(n, t='bytes'), sumIf(n, t='map') FROM ("
          f"SELECT {derived_type} AS t, record_int['metric.value'] AS vi, "
          f"record_double['metric.value'] AS vd, record_string['metric.value'] AS vs, "
          f"record_bool['metric.value'] AS vb, count() AS n FROM {table} "
          f"WHERE {presence} GROUP BY t, vi, vd, vs, vb)",
          "3\t1\t1\t1\t1\t1\t1\t1\t1", kind="semantic")
    c.sql(f"{prefix}.presence_count", "lh020", f"SELECT countIf({presence}) FROM {table}",
          str(expected_present_count()), kind="semantic",
          note="same answer as the sidecar and companion layouts: 24 present rows")
    c.sql(f"{prefix}.presence_missing", "lh020",
          f"SELECT count() FROM {table} WHERE event_id='group-missing' AND {presence}", "0", kind="semantic")
    c.sql(f"{prefix}.presence_empty", "lh020",
          f"SELECT record_other['metric.value'], mapContains(record_other,'metric.value') "
          f"FROM {table} WHERE event_id='group-empty'", "empty\t1", kind="semantic")
    c.sql(f"{prefix}.presence_empty_string", "lh020",
          f"SELECT {derived_type}, record_string['metric.value'], mapContains(record_other,'metric.value') "
          f"FROM {table} WHERE event_id='group-empty-string'", "string\t\t0", kind="semantic",
          note="empty string is a string value; it is not in the canonical-only map")
    c.sql(f"{prefix}.canonical_only_presence_bookkept", "lh020",
          f"SELECT record_other['bytes.raw'], mapContains(record_other,'bytes.raw') "
          f"FROM {table} WHERE event_id='typed-1'", "bytes\t1", kind="semantic",
          note="minimal bookkeeping still answers presence/type for bytes, array, and map keys")
    c.sql(f"{prefix}.nested_presence_bookkept", "lh020",
          f"SELECT record_other['map.nested'], mapContains(record_other,'map.nested') "
          f"FROM {table} WHERE event_id='typed-1'", "map\t1", kind="semantic")
    c.sql(f"{prefix}.canonical_only_types_distinct", "lh020",
          f"SELECT record_other['array.mixed'], record_other['map.nested'], record_other['empty.value'] "
          f"FROM {table} WHERE event_id='typed-1'", "array\tmap\tempty", kind="semantic",
          note="array, map, and explicit empty keep distinct type identity")
    c.sql(f"{prefix}.presence_nonfinite_supported", "lh020",
          f"SELECT {derived_type}, mapContains(record_double,'metric.value') FROM {table} WHERE event_id='nan'",
          "double\t1", kind="semantic")
    c.sql(f"{prefix}.missing_lookup_default_int", "lh020",
          f"SELECT count() FROM {table} WHERE record_int['metric.value'] = 0",
          str(expected_default_matches("int", 0)), kind="semantic")
    c.sql(f"{prefix}.missing_lookup_default_string", "lh020",
          f"SELECT count() FROM {table} WHERE record_string['metric.value'] = ''",
          str(expected_default_matches("string", "")), kind="semantic")
    c.sql(f"{prefix}.presence_guard_fixes_default", "lh020",
          f"SELECT count() FROM {table} WHERE mapContains(record_int,'metric.value') "
          f"AND record_int['metric.value'] = 0", "1", kind="semantic")
    c.sql(f"{prefix}.big_int_exact", "lh020",
          f"SELECT record_int['metric.value'] FROM {table} WHERE event_id='big-int'",
          "9007199254740993", kind="semantic")
    c.sql(f"{prefix}.min_int_exact", "lh020",
          f"SELECT record_int['metric.value'] FROM {table} WHERE event_id='min-int'",
          "-9223372036854775808", kind="semantic")
    observed = ch("lh020", f"SELECT event_id FROM {table} WHERE mapContains(record_int,'metric.value') "
                           f"ORDER BY record_int['metric.value'], event_id")
    c.check(f"{prefix}.numeric_order", observed, "\n".join(expected_int_order()), kind="semantic")
    c.sql(f"{prefix}.namespace_collide", "lh020",
          f"SELECT resource_string['res.collide'], record_string['collide'] FROM {table} WHERE event_id='typed-1'",
          "resource-value\trecord-value", kind="semantic")
    c.sql(f"{prefix}.duplicate_rejected_row_absent", "lh020",
          f"SELECT count() FROM {table} WHERE event_id='dup-1'", "0", kind="semantic")
    c.sql(f"{prefix}.sum_small_exact", "lh020",
          f"SELECT sum(record_int['metric.value']) FROM {table} WHERE mapContains(record_int,'metric.value') "
          f"AND event_id IN ('sum-small-a','sum-small-b')", "3", kind="semantic")
    c.sql(f"{prefix}.sum_int64_wraps_in_clickhouse", "lh020",
          f"SELECT sum(record_int['metric.value']) FROM {table} WHERE mapContains(record_int,'metric.value') "
          f"AND event_id IN ('sum-a','sum-b','sum-c')", "-1", kind="characterization")
    c.sql(f"{prefix}.aggregate_contributing_counts", "lh020",
          f"SELECT countIf({presence}), countIf({derived_type}='int') FROM {table}",
          f"{expected_present_count()}\t{expected_int_count()}", kind="semantic")
    c.sql(f"{prefix}.type_change_across_rows", "lh020",
          f"SELECT countIf(mapContains(record_int,'metric.value')), "
          f"countIf(mapContains(record_double,'metric.value')), "
          f"countIf(mapContains(record_string,'metric.value')) FROM {table}", "12\t5\t3", kind="semantic")
    serving_equivalence(c, table, "minimal", prefix, results)


def serving_equivalence(c: Checks, table: str, bookkeeping: str, prefix: str, results: Path):
    """Serving Maps must equal the decoded canonical values, key by key."""
    payloads = load_payloads(results)
    if not payloads:
        c.check(f"{prefix}.serving_equivalence", "missing payloads", "ok", kind="semantic")
        return
    columns = ["event_id"]
    for namespace in ("resource", "scope", "record"):
        columns += [f"{namespace}_{t}" for t in SCALARS]
        columns.append(f"{namespace}_{'types' if bookkeeping == 'full' else 'other'}")
    text = ch("lh020", f"SELECT {', '.join(columns)} FROM {table} "
                       f"SETTINGS output_format_json_quote_denormals=1 FORMAT JSONEachRow")
    mismatches = []
    checked = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        row = json.loads(line, parse_int=_parse_json_int)
        doc = payloads.get(row["event_id"])
        if doc is None:
            mismatches.append(f"{row['event_id']}: no payload")
            continue
        checked += 1
        for namespace in ("resource", "scope", "record"):
            decoded = {key: decode_canonical(value) for key, value in doc[namespace].items()}
            if bookkeeping == "full":
                types = row[f"{namespace}_types"]
            else:
                types = {}
                for key in decoded:
                    found = ""
                    for type_name in SCALARS:
                        if key in row[f"{namespace}_{type_name}"]:
                            found = type_name
                    if not found and key in row[f"{namespace}_other"]:
                        found = row[f"{namespace}_other"][key]
                    types[key] = found
            if set(types) != set(decoded):
                mismatches.append(f"{row['event_id']}.{namespace} key set")
                continue
            for key, (type_name, value) in decoded.items():
                if types.get(key) != type_name:
                    mismatches.append(f"{row['event_id']}.{namespace}.{key} type")
                    continue
                if type_name in SCALARS:
                    got = row[f"{namespace}_{type_name}"].get(key)
                    if not _values_equal(type_name, got, value):
                        mismatches.append(f"{row['event_id']}.{namespace}.{key} value")
                    for other in SCALARS:
                        if other != type_name and key in row[f"{namespace}_{other}"]:
                            mismatches.append(f"{row['event_id']}.{namespace}.{key} duplicate map")
                else:
                    for other in SCALARS:
                        if key in row[f"{namespace}_{other}"]:
                            mismatches.append(f"{row['event_id']}.{namespace}.{key} typed value for canonical-only type")
                    if bookkeeping == "minimal" and row[f"{namespace}_other"].get(key) != type_name:
                        mismatches.append(f"{row['event_id']}.{namespace}.{key} other map")
    c.check(f"{prefix}.serving_equivalence",
            "ok" if not mismatches else f"mismatch: {mismatches[0]}", "ok", kind="semantic",
            note=f"decoded canonical values compared with the serving Maps for {checked} rows")
    c.check(f"{prefix}.serving_rows_checked", str(checked), str(len(payloads)), kind="semantic")


def span_checks(c: Checks):
    c.sql("semantics.span_scope.companion_namespace_separated", "lh020",
          "SELECT span_types['span.scope.attr'], span_scope_types['scope.span.attr'], "
          "span_scope['scope.span.attr'] FROM rep_span_companion WHERE event_id='span-scope'",
          "string\tstring\ts", kind="semantic")
    c.sql("semantics.span_scope.native_typed_namespace_separated", "lh020",
          "SELECT span_types['span.scope.attr'], span_scope_types['scope.span.attr'], "
          "span_scope_string['scope.span.attr'] FROM rep_span_native_typed WHERE event_id='span-scope'",
          "string\tstring\ts", kind="semantic")
    c.sql("semantics.span_scope.native_minimal_namespace_separated", "lh020",
          "SELECT span_string['span.scope.attr'], span_scope_string['scope.span.attr'], "
          "mapContains(span_other,'scope.span.attr'), mapContains(span_scope_other,'span.scope.attr') "
          "FROM rep_span_native_minimal WHERE event_id='span-scope'",
          "present\ts\t0\t0", kind="semantic")
    c.sql("semantics.span_scope.native_typed_scope_int", "lh020",
          "SELECT span_scope_types['scope.span.int'], span_scope_int['scope.span.int'] "
          "FROM rep_span_native_typed WHERE event_id='span-scope'", "int\t11", kind="semantic")
    c.sql("semantics.span_scope.native_typed_namespaces_disjoint", "lh020",
          "SELECT mapContains(span_scope_types,'collide'), mapContains(span_types,'scope.attr') "
          "FROM rep_span_native_typed WHERE event_id='span-1'", "0\t0", kind="semantic",
          note="span scope attributes do not leak into the span attribute namespace and vice versa")


def prototype_checks(c: Checks):
    """Harness query-prototype contract rules.

    Presence/type inspection is supported; value comparison and aggregation on
    bytes, arrays, maps, and non-finite doubles are rejected by the prototype,
    and integer aggregates reject overflow instead of wrapping. These are
    harness rules, not ClickHouse guarantees.
    """
    cases = (
        ("nan", "double", "NaN"),
        ("bytes-row", "bytes", "AAH+/w=="),
        ("nested-row", "map", '{"nested.int":1}'),
        ("typed-1", "array", '["a",1,true]'),
    )
    for event_id, type_name, value_text in cases:
        try:
            prototype_compare(type_name, value_text)
            observed = "allowed"
        except UnsupportedOperation as exc:
            observed = f"rejected: {exc}"
        c.check(f"prototype.{event_id}.value_comparison_rejected", observed.split(":")[0], "rejected",
                kind="prototype", note="harness query-prototype behavior, not a ClickHouse guarantee")
    c.check("prototype.finite_scalar_comparison_allowed", prototype_compare("int", "1"), "ok",
            kind="prototype", note="presence/type inspection and finite scalar comparisons are supported")
    try:
        prototype_int_aggregate([fixture020.fixture019.INT64_MAX, fixture020.fixture019.INT64_MAX, 1])
        observed = "wrapped"
    except UnsupportedOperation:
        observed = "rejected"
    c.check("prototype.aggregate_overflow_rejected", observed, "rejected", kind="prototype",
            note="harness query-prototype behavior, not a ClickHouse guarantee")
    c.check("prototype.aggregate_in_range_allowed", prototype_int_aggregate([1, 2]), 3, kind="prototype")


def phase_rejections(c: Checks, results: Path):
    path = results / "mapping-rejections.jsonl"
    if not path.exists():
        c.check("semantics.mapping.rejections_present", "missing", "present", kind="semantic")
        return
    items = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    stored = [item for item in items if "event_id" in item]
    crafted = {item["case"]: item["reason"] for item in items if "case" in item}
    c.check("semantics.mapping.duplicate_event_rejected",
            "ok" if any(item["event_id"] == "dup-1" and "duplicate attribute key 'dup'" in item["reason"]
                        for item in stored) else "missing", "ok", kind="semantic")
    expected_fragments = {
        "duplicate-same-type": "duplicate attribute key",
        "duplicate-different-type": "duplicate attribute key",
        "cross-map-conflict": "more than one typed value map",
        "sidecar-type-mismatch": "does not match its typed value map",
        "canonical-only-also-typed": "also has a typed value",
        "missing-sidecar-entry": "no sidecar type",
        "minimal-bookkeeping-scalar": "canonical-only types",
    }
    for case, fragment in expected_fragments.items():
        reason = crafted.get(case, "missing")
        c.check(f"semantics.mapping.{case}", "ok" if fragment in reason else reason, "ok", kind="semantic",
                note="harness mapping-boundary enforcement; ClickHouse has no unique-key constraint")
    c.check("semantics.mapping.no_unrejected_cases",
            "ok" if "NOT-REJECTED" not in crafted.values() else "NOT-REJECTED", "ok", kind="semantic")
    for variant in VARIANTS:
        c.sql(f"semantics.mapping.dup_absent.{variant}", "lh020",
              f"SELECT count() FROM rep_{variant} WHERE event_id='dup-1'", "0", kind="semantic")


def phase_canonical(c: Checks, results: Path):
    payloads = load_payloads(results)
    c.check("canonical.payload_count", len(payloads), str(len(stored_events())), kind="semantic",
            note="the duplicate-key record is rejected by the mapping prototype")
    ok = True
    detail = ""
    for event in stored_events():
        doc = payloads.get(event["event_id"])
        if doc is None:
            ok, detail = False, detail or f"{event['event_id']} missing"
            continue
        for namespace in ("resource", "scope", "record"):
            decoded = {key: decode_canonical(value) for key, value in doc[namespace].items()}
            expected = {key: (type_name, value) for key, type_name, value in event[namespace]}
            if set(decoded) != set(expected):
                ok, detail = False, detail or f"{event['event_id']}.{namespace} keys"
                continue
            for key, (type_name, value) in expected.items():
                got_type, got_value = decoded[key]
                if got_type != type_name or not _values_equal(type_name, got_value, value):
                    ok, detail = False, detail or f"{event['event_id']}.{namespace}.{key}"
    c.check("canonical.lossless_roundtrip", "ok" if ok else f"mismatch: {detail}", "ok", kind="semantic",
            note="decoded payload compared with the typed fixture inventory by an independent decoder")


def phase_semantics(c: Checks, results: Path, candidate=None):
    try:
        if candidate:
            # Test-only mode: run the native typed checks against a candidate
            # table. The wrong-layout test expects this to fail.
            native_typed_checks(c, candidate, results)
            return
        companion_checks(c)
        native_typed_checks(c, "rep_native_typed", results)
        native_minimal_checks(c, "rep_native_minimal", results)
        span_checks(c)
        prototype_checks(c)
        phase_canonical(c, results)
        phase_rejections(c, results)
    except Exception as exc:
        c.check("semantics.error", f"ERROR: {type(exc).__name__}: {exc}", "ok", kind="semantic")


# ---------------------------------------------------------------------------
# Negative: deliberately wrong layouts
# ---------------------------------------------------------------------------

def phase_negative(c: Checks, results: Path):
    c.sql("negative.correct_layout_answers_3", "lh020",
          "SELECT count() FROM rep_native_typed WHERE record_types['metric.value']='int' "
          "AND record_int['metric.value'] = 1", "3", kind="semantic",
          note="the expectation the wrong-layout comparisons are measured against")
    c.sql("negative.float_only.typed_int_filter_finds_nothing", "lh020",
          "SELECT count() FROM rep_wrong_float_only WHERE record_types['metric.value']='int' "
          "AND record_int['metric.value'] = 1", "0", kind="semantic",
          note="a collapsed layout cannot answer the typed int filter at all")
    c.sql("negative.float_only.float_collision", "lh020",
          "SELECT count() FROM rep_wrong_float_only WHERE record_double['metric.value'] = 1", "4", kind="semantic",
          note="int 1 rows (group-int-1, sum-c, sum-small-a) and double 1.0 collide in the Float64 "
               "map; the correct layout returns one double match")
    c.sql("negative.float_only.correct_layout_double_matches", "lh020",
          "SELECT count() FROM rep_native_typed WHERE record_double['metric.value'] = 1", "1", kind="semantic",
          note="the same query on the correct layout answers 1")
    c.sql("negative.float_only.big_int_rounded", "lh020",
          "SELECT record_double['metric.value'] FROM rep_wrong_float_only WHERE event_id='big-int'",
          "9007199254740992", kind="semantic",
          note="9007199254740993 is not representable in Float64")
    c.sql("negative.float_only.int64_min_rounded", "lh020",
          "SELECT record_double['metric.value'] FROM rep_wrong_float_only WHERE event_id='min-int'",
          "-9223372036854776000", kind="semantic", note="Int64 minimum is rounded by Float64")
    c.sql("negative.double_presence.union_counts_twice", "lh020",
          "SELECT countIf(mapContains(record_int,'int.one')) + countIf(mapContains(record_string,'int.one')) "
          "FROM rep_wrong_double_presence WHERE event_id='typed-1'", "2", kind="semantic",
          note="one key written into two value maps: a union presence query double-counts and "
               "ClickHouse cannot detect it")
    c.sql("negative.double_presence.sidecar_records_one_type", "lh020",
          "SELECT record_types['int.one'] FROM rep_wrong_double_presence WHERE event_id='typed-1'",
          "int", kind="semantic", note="the sidecar still records one type while two value maps hold the key")
    observed = scalar("lh020", "SELECT count() FROM rep_wrong_float_only "
                               "WHERE record_types['metric.value']='int' AND record_int['metric.value'] = 1")
    c.check("negative.assertion_sensitivity.typed_filter_mismatch", observed != "3", True, kind="semantic",
            note="the semantics check expects 3 and the wrong layout produces a mismatch, so the "
                 "assertion is discriminating; test-run-failure-modes.sh runs the check set against "
                 "this table and asserts a nonzero exit")


# ---------------------------------------------------------------------------
# Cost
# ---------------------------------------------------------------------------

def _presence(variant, namespace, key):
    if variant in ("companion", "native_typed"):
        return f"mapContains({namespace}_types, '{key}')"
    return "(" + " OR ".join(f"mapContains({namespace}_{t}, '{key}')" for t in SCALARS + ("other",)) + ")"


def _type_expr(variant, namespace, key):
    if variant in ("companion", "native_typed"):
        return f"{namespace}_types['{key}']"
    return ("multiIf(" + ", ".join(f"mapContains({namespace}_{t}, '{key}'), '{t}'" for t in SCALARS)
            + f", mapContains({namespace}_other, '{key}'), {namespace}_other['{key}'], '')")


def _value_expr(variant, namespace, key, type_name):
    if variant == "companion":
        expr = f"{namespace}['{key}']"
        # OrNull: a key absent from the string Map yields '' and a plain cast
        # would raise on rows the type guard excludes but the engine still reads.
        if type_name == "int":
            return f"toInt64OrNull({expr})"
        if type_name == "double":
            return f"toFloat64OrNull({expr})"
        return expr
    return f"{namespace}_{type_name}['{key}']"


def cost_query(corpus, variant, name):
    table = COST_TABLES[variant]
    if corpus == "base":
        keys = {
            "int_filter": ("int.one", "int", "5"),
            "string_filter": ("str.one", "string", "'s5'"),
            "group_by_type": ("int.one", None, None),
            "numeric_order": ("int.one", "int", None),
            "sum_typed": ("int.one", "int", None),
            "presence": ("int.one", None, None),
            "canonical_bytes_payload": ("bytes.raw", "bytes", None),
        }
    else:
        keys = {
            "int_filter": ("http.status_code", "int", "200"),
            "string_filter": ("http.method", "string", "'GET'"),
            "group_by_type": ("attr.mixed", None, None),
            "numeric_order": ("duration_ms", "double", None),
            "sum_typed": ("retry.count", "int", None),
            "presence": ("attr.mixed", None, None),
        }
    key, type_name, literal = keys[name]
    presence = _presence(variant, "record", key)
    type_expr = _type_expr(variant, "record", key)
    value = _value_expr(variant, "record", key, type_name) if type_name else None
    if name == "canonical_bytes_payload":
        # Canonical-only values live in the payload in every layout; this reads
        # the payload column to show the retrieval cost that stays unchanged.
        return (f"SELECT JSONExtract(payload, 'record', 'bytes.raw', 'v', 'String') FROM {table} "
                f"WHERE event_id='cost-0'")
    if name in ("int_filter", "string_filter"):
        return (f"SELECT count() FROM {table} WHERE {presence} AND {type_expr}='{type_name}' "
                f"AND {value}={literal}")
    if name == "group_by_type":
        return f"SELECT {type_expr} AS t, count() FROM {table} WHERE {presence} GROUP BY t ORDER BY t"
    if name == "numeric_order":
        return (f"SELECT {value} FROM {table} WHERE {presence} AND {type_expr}='{type_name}' "
                f"ORDER BY 1 DESC LIMIT 5")
    if name == "sum_typed":
        return (f"SELECT sum(toInt128({value})) FROM {table} WHERE {presence} "
                f"AND {type_expr}='{type_name}'")
    if name == "presence":
        return f"SELECT countIf({presence}) FROM {table}"
    raise SystemExit(f"unknown cost query {name}")


def measure(db, marker, sql):
    """Run a query twice and read the warm execution from system.query_log."""
    marked = f"/* {marker} */ {sql}"
    result_text = ""
    for _ in range(2):
        result_text = ch(db, marked)
    text = ""
    for _ in range(20):
        text = ch(db, "SELECT query_duration_ms, read_rows, read_bytes, result_rows, "
                      "ProfileEvents['OSCPUVirtualTimeMicroseconds'], memory_usage "
                      "FROM system.query_log WHERE type='QueryFinish' "
                      f"AND query LIKE '/* {marker} */%' ORDER BY event_time DESC LIMIT 1")
        if text:
            break
        time.sleep(0.5)
    parts = text.split("\t") if text else ["", "", "", "", "", ""]
    return {
        "marker": marker,
        "result": result_text,
        "duration_ms": float(parts[0] or 0),
        "read_rows": int(parts[1] or 0),
        "read_bytes": int(parts[2] or 0),
        "result_rows": int(parts[3] or 0),
        "cpu_us": int(parts[4] or 0),
        "memory_bytes": int(parts[5] or 0),
    }


def phase_cost(c: Checks, results: Path):
    storage = []
    measurements = []
    for corpus in fixture020.CORPORA:
        db = COST_DBS[corpus]
        for variant in VARIANTS:
            table = COST_TABLES[variant]
            ch(db, f"OPTIMIZE TABLE {table} FINAL")
            total = scalar(db, f"SELECT sum(bytes_on_disk) FROM system.parts "
                               f"WHERE database='{db}' AND table='{table}' AND active")
            cols = rows(db, f"SELECT name, data_compressed_bytes, data_uncompressed_bytes "
                            f"FROM system.columns WHERE database='{db}' AND table='{table}' "
                            f"ORDER BY name")
            storage.append({
                "corpus": corpus, "variant": variant, "table": table,
                "bytes_on_disk": int(total or 0),
                "columns": [{"name": name, "compressed": int(compressed), "uncompressed": int(uncompressed)}
                            for name, compressed, uncompressed in cols],
            })
            c.sql(f"cost.{corpus}.{variant}.row_count", db, f"SELECT count() FROM {table}",
                  str(COST_ROWS))
        for name in COST_QUERIES[corpus]:
            observed = {}
            for variant in VARIANTS:
                marker = f"q020-{corpus}-{name}-{variant}"
                measurement = measure(db, marker, cost_query(corpus, variant, name))
                measurement["corpus"] = corpus
                measurement["variant"] = variant
                measurement["query"] = name
                measurements.append(measurement)
                observed[variant] = measurement["result"]
                c.info(f"cost.{corpus}.{variant}.{name}",
                       f"{measurement['duration_ms']}ms rows={measurement['read_rows']} "
                       f"bytes={measurement['read_bytes']} result={measurement['result'][:60]!r}")
            values = list(observed.values())
            same = len(set(values)) == 1 and values[0] not in ("", "0")
            c.check(f"cost.{corpus}.same_answer.{name}", "ok" if same else f"mismatch: {values}", "ok",
                    kind="semantic",
                    note="companion, full-sidecar, and minimal-bookkeeping layouts must answer the "
                         "same question with the same result")
    (results / "cost-storage.jsonl").write_text("".join(json.dumps(item) + "\n" for item in storage))
    (results / "cost-queries.jsonl").write_text("".join(json.dumps(item) + "\n" for item in measurements))

    by_table = {(item["corpus"], item["variant"]): item for item in storage}

    def column_bytes(corpus, variant, column):
        for col in by_table[(corpus, variant)]["columns"]:
            if col["name"] == column:
                return col["compressed"]
        return 0

    for corpus in fixture020.CORPORA:
        c.check(f"cost.{corpus}.companion.record_bytes_positive",
                column_bytes(corpus, "companion", "record") > 0, True, kind="measurement")
        c.check(f"cost.{corpus}.native_typed.record_int_bytes_positive",
                column_bytes(corpus, "native_typed", "record_int") > 0, True, kind="measurement")
        c.check(f"cost.{corpus}.native_typed.record_types_bytes_positive",
                column_bytes(corpus, "native_typed", "record_types") > 0, True, kind="measurement")
        c.check(f"cost.{corpus}.native_minimal.record_other_bytes_positive",
                column_bytes(corpus, "native_minimal", "record_other") > 0, True, kind="measurement")
        c.check(f"cost.{corpus}.native_minimal.record_types_absent",
                column_bytes(corpus, "native_minimal", "record_types"), 0, kind="measurement",
                note="the minimal layout has no full per-key type sidecar")
    inserts = {}
    insert_path = results / "cost-insert.jsonl"
    if insert_path.exists():
        for line in insert_path.read_text().splitlines():
            if line.strip():
                item = json.loads(line)
                inserts[(item["corpus"], item["variant"])] = item
    for corpus in fixture020.CORPORA:
        for variant in VARIANTS:
            item = inserts.get((corpus, variant))
            c.check(f"cost.{corpus}.{variant}.insert_recorded",
                    "ok" if item and item["rows"] == COST_ROWS else str(item), "ok", kind="measurement",
                    note="mapping and insert wall time recorded by the mapping prototype")
            if item:
                c.info(f"cost.{corpus}.{variant}.insert_work",
                       f"mapping={item['mapping_seconds']}s insert={item['insert_seconds']}s "
                       f"rows/s={item['rows_per_second']}")


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

def phase_files(c: Checks, results: Path):
    hashes = fixture020.source_hashes()
    for name, expected in EXPECTED_SOURCE_HASHES.items():
        c.check(f"files.imported_source_hash.{name}", hashes.get(name), expected, kind="semantic",
                note="the imported Experiment 019 fixture/encoding must not change under this experiment")
    for name in ("checks-capabilities.jsonl", "mapping-rejections.jsonl", "canonical-payloads.jsonl",
                 "cost-storage.jsonl", "cost-queries.jsonl", "cost-insert.jsonl"):
        path = results / name
        c.check(f"files.{name}.present", path.exists() and path.stat().st_size > 0, True, kind="semantic")


# ---------------------------------------------------------------------------

def run_phase(phase, results, capture=False, candidate=None):
    c = Checks(capture=capture)
    try:
        if phase == "capabilities":
            phase_capabilities(c, results)
        elif phase == "semantics":
            phase_semantics(c, results, candidate=candidate)
        elif phase == "negative":
            phase_negative(c, results)
        elif phase == "cost":
            phase_cost(c, results)
        elif phase == "files":
            phase_files(c, results)
        else:
            raise SystemExit(f"unknown phase {phase}")
    except Exception as exc:
        # A crash must be visible as a failed check and a nonzero exit, not as a
        # silently missing phase.
        c.check(f"{phase}.error", f"ERROR: {type(exc).__name__}: {exc}", "ok")
    return c


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--results", default="results")
    parser.add_argument("--candidate", help="test-only: run the native typed checks against this table")
    parser.add_argument("--capture", action="store_true", help="record observations without failing")
    parser.add_argument("--out")
    args = parser.parse_args(argv)

    results = Path(args.results)
    # Test-only hook (harness/test-run-failure-modes.sh): raise an unexpected
    # exception in the named phase so the harness failure path is exercised.
    crash_phase = os.environ.get("LH020_TEST_CRASH_PHASE")
    if crash_phase and args.phase == crash_phase:
        raise RuntimeError("injected test crash (LH020_TEST_CRASH_PHASE)")
    c = run_phase(args.phase, results, capture=args.capture, candidate=args.candidate)
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
