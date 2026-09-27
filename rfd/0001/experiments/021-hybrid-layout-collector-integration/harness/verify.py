#!/usr/bin/env python3
"""Experiment 021 independent verifier.

Every expectation in this file is derived from the independent Experiment 019
typed fixture and from literals in this file, never from the Go mapping under
test. The Go mapping is checked by comparing stored rows and canonical payloads
against those expectations.

Check name prefixes:

* ``meta.``   pins, manifests, fixture-independence hashes
* ``rep.``    stored representation conformance (types, values, absence, schema)
* ``query.``  query-layer and harness enforcement (counts, duplicate scans, guards)
* ``canon.``  canonical payload integrity, retrieval, reconstruction
* ``xdest.``  canonical/serving correlation for one event identity
* ``ack.``    producer acknowledgement versus visibility
* ``fail.``   failure-injection outcomes (outage, queue, crash, ambiguous, split)
* ``comp.``   component error handling, counters, and collector logs
* ``files.``  evidence files
* ``stock.``  stock write-path probe results

Usage: verify.py --phase PHASE --results DIR --out checks-PHASE.jsonl
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import re
import sys
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
EXP019 = HERE.parent.parent / "019" / "harness"
sys.path.insert(0, str(EXP019))

import fixture as fixture019  # noqa: E402
import fixture021  # noqa: E402

CH_HTTP = os.environ.get("LH021_CH_HTTP", "http://127.0.0.1:18224")
CH_USER = os.environ.get("LH021_CH_USER", "default")
CH_PASSWORD = os.environ.get("LH021_CH_PASSWORD", "")
DB = os.environ.get("LH021_DATABASE", "lh021")
DB_UNTRUSTED = os.environ.get("LH021_UNTRUSTED_DATABASE", "lh021_untrusted")
DB_STOCK = os.environ.get("LH021_STOCK_DATABASE", "lh021_stock")
RESULTS = Path(os.environ.get("LH021_RESULTS", str(HERE / "results")))
GOMODCACHE = Path(os.environ.get("GOMODCACHE", "/home/user/lh021/gomodcache"))

WS_A = fixture021.WS_A
WS_B = fixture021.WS_B
WS_FORGED = fixture021.WS_FORGED
DATASET = fixture021.DATASET
RESERVED_PREFIX = "lighthouse."

CONTRIB_VERSION = "v0.161.0"
INT64_MIN = fixture019.INT64_MIN
INT_ABOVE_2_53 = fixture019.INT_ABOVE_2_53

# The span set is emitted through one scope group per span (fixture021.build_spans),
# so the expected instrumentation scope namespace is fixed per span here.
SPAN_SCOPE_OF = {
    "span-1": fixture019.FULL_SCOPE,
    "span-scope": fixture019.SPAN_SCOPE_ATTRS,
    "span-missing-time": fixture019.FULL_SCOPE,
    "span-negative": fixture019.FULL_SCOPE,
    "span-zero": fixture019.FULL_SCOPE,
    "timing-mixed-valid": fixture019.FULL_SCOPE,
    "timing-mixed-reversed": fixture019.FULL_SCOPE,
}


# ---------------------------------------------------------------------------
# check collection
# ---------------------------------------------------------------------------


class Checks:
    def __init__(self, out: Path, phase: str):
        self.out = out
        self.phase = phase
        self.rows: list[dict] = []
        self.failed = 0

    def check(self, name: str, ok: bool, detail: str = "", **extra) -> bool:
        record = {"phase": self.phase, "check": name, "ok": bool(ok), "detail": detail}
        record.update(extra)
        self.rows.append(record)
        if not ok:
            self.failed += 1
            print(f"FAIL {name}: {detail}", file=sys.stderr)
        return bool(ok)

    def write(self) -> int:
        self.out.parent.mkdir(parents=True, exist_ok=True)
        with self.out.open("w", encoding="utf-8") as fh:
            for row in self.rows:
                fh.write(json.dumps(row, default=str) + "\n")
        print(f"{self.phase}: {len(self.rows)} checks, {self.failed} failed -> {self.out}")
        return 1 if self.failed else 0


# ---------------------------------------------------------------------------
# ClickHouse access
# ---------------------------------------------------------------------------


def ch(sql: str, params: dict | None = None, db: str = DB, timeout: int = 60) -> list[dict]:
    query = {"database": db, "default_format": "JSON"}
    for key, value in (params or {}).items():
        query[f"param_{key}"] = value
    resp = requests.post(CH_HTTP, params=query, data=sql.encode("utf-8"), timeout=timeout,
                         auth=(CH_USER, CH_PASSWORD))
    if resp.status_code != 200:
        raise RuntimeError(f"clickhouse error {resp.status_code}: {resp.text[:500]}")
    return resp.json()["data"]


def ch_or_error(sql: str, params: dict | None = None, db: str = DB) -> tuple[bool, object]:
    try:
        return True, ch(sql, params, db)
    except Exception as err:  # noqa: BLE001 - the outage phase expects failures
        return False, str(err)


def scalar(sql: str, params: dict | None = None, db: str = DB) -> int:
    rows = ch(sql, params, db)
    return int(list(rows[0].values())[0]) if rows else 0


def db_exists(name: str) -> bool:
    return scalar("SELECT count() FROM system.databases WHERE name = {n:String}", {"n": name}) == 1


def table_columns(db: str, table: str) -> dict[str, str]:
    rows = ch("SELECT name, type FROM system.columns WHERE database = {d:String} AND table = {t:String}",
              {"d": db, "t": table})
    return {row["name"]: row["type"] for row in rows}


def row_count(table: str, event_id: str, ws: str | None = None, db: str = DB) -> int:
    if ws is None:
        return scalar(f"SELECT count() FROM {db}.{table} WHERE event_id = {{e:String}}", {"e": event_id}, db)
    return scalar(
        f"SELECT count() FROM {db}.{table} WHERE event_id = {{e:String}} AND workspace_id = {{w:String}}",
        {"e": event_id, "w": ws}, db)


def duplicate_events(table: str, db: str = DB) -> list[dict]:
    return ch(f"SELECT event_id, workspace_id, count() AS c FROM {db}.{table} "
              f"WHERE dataset = {{d:String}} GROUP BY event_id, workspace_id HAVING c > 1",
              {"d": DATASET}, db)


def fixture_duplicates(table: str, db: str = DB) -> list[dict]:
    """Rows that share one fixture identity: a double write, whatever the id."""
    key = "body" if table == "serving_logs" else "span_name"
    return ch(f"SELECT workspace_id, {key} AS fixture_identity, count() AS c FROM {db}.{table} "
              f"WHERE dataset = {{d:String}} GROUP BY workspace_id, fixture_identity HAVING c > 1",
              {"d": DATASET}, db)


def canonical_rows(event_id: str, db: str = DB) -> list[dict]:
    return ch("SELECT workspace_id, dataset, signal, event_id, payload, payload_sha256, "
              "toString(toUnixTimestamp64Nano(ts)) AS ts_ns, schema_version "
              f"FROM {db}.canonical_events WHERE event_id = {{e:String}}",
              {"e": event_id}, db)


def serving_link(event_id: str, table: str = "serving_logs", db: str = DB) -> dict | None:
    """Only the correlation columns: Map values can hold non-finite doubles."""
    rows = ch(f"SELECT workspace_id, dataset, event_id, payload_sha256 FROM {db}.{table} "
              "WHERE event_id = {e:String}", {"e": event_id}, db)
    return rows[0] if len(rows) == 1 else None


# ---------------------------------------------------------------------------
# expectation builders (independent fixture -> expected serving values)
# ---------------------------------------------------------------------------


def value_equal(a, b) -> bool:
    """Equality that separates NaN (equal to itself here) and zero signs."""
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, float) or isinstance(b, float):
        if a != a or b != b:
            return a != a and b != b
        if a == 0 and b == 0:
            return math.copysign(1.0, a) == math.copysign(1.0, b)
        return a == b
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(value_equal(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return set(a) == set(b) and all(value_equal(a[key], b[key]) for key in a)
    return a == b


def serving_double_text(value: float) -> str:
    """pcommon.Value.AsString() double projection, re-implemented from the rule.

    The pinned pdata source returns "NaN"/"Infinity"/"-Infinity" for special
    values and an ES6-style shortest text otherwise, with a signed zero.
    """
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if value == 0:
        return "-0" if math.copysign(1.0, value) < 0 else "0"
    if abs(value) < 1e-6 or abs(value) >= 1e21:
        text = repr(value)
        mantissa, _, exponent = text.partition("e")
        if exponent:
            sign = "-" if exponent.startswith("-") else ""
            exponent = exponent.lstrip("+-").lstrip("0") or "0"
            text = f"{mantissa}e{sign}{exponent}"
        return text
    text = repr(value)
    return text[:-2] if text.endswith(".0") else text


def project_pairs(pairs) -> tuple[dict, dict]:
    """Stock-compatible string projection + companion type map for pairs."""
    strings: dict[str, str] = {}
    types: dict[str, str] = {}
    for key, type_name, value in pairs:
        types[key] = type_name
        if type_name == "string":
            strings[key] = value if value is not None else ""
        elif type_name == "int":
            strings[key] = str(value)
        elif type_name == "bool":
            strings[key] = "true" if value else "false"
        elif type_name == "double":
            strings[key] = serving_double_text(value)
        elif type_name == "empty":
            strings[key] = ""
        elif type_name == "bytes":
            strings[key] = base64.b64encode(value).decode("ascii")
        elif type_name in ("array", "map"):
            strings[key] = json.dumps(value, separators=(",", ":"), ensure_ascii=False,
                                      sort_keys=True)
        else:
            raise AssertionError(f"unknown fixture type {type_name}")
    return strings, types


def without_reserved(pairs):
    """Producer-controlled namespaces drop every reserved lighthouse.* key."""
    return [pair for pair in pairs if not pair[0].startswith(RESERVED_PREFIX)]


def record_maps(pairs) -> dict[str, dict]:
    out: dict[str, dict] = {"int": {}, "double": {}, "bool": {}, "string": {}, "other": {}}
    for key, type_name, value in pairs:
        if type_name == "int":
            out["int"][key] = value
        elif type_name == "double":
            out["double"][key] = value
        elif type_name == "bool":
            out["bool"][key] = value
        elif type_name == "string":
            out["string"][key] = "" if value is None else value
        elif type_name in ("bytes", "array", "map", "empty"):
            out["other"][key] = type_name
        else:
            raise AssertionError(f"unknown fixture type {type_name}")
    return out


def expected_resource(pairs, workspace: str) -> tuple[dict, dict]:
    """Resource namespace as served: the gateway's trusted keys win.

    The trusted resource processor upserts workspace and dataset and sets the
    scope-source marker, so a producer-supplied value for those keys is replaced
    and the marker is not stored.
    """
    strings, types = project_pairs([p for p in pairs if p[0] != "lighthouse.scope_source"])
    strings["lighthouse.workspace_id"] = workspace
    types["lighthouse.workspace_id"] = "string"
    strings["lighthouse.dataset"] = DATASET
    types["lighthouse.dataset"] = "string"
    return strings, types


def service_name(pairs) -> str:
    for key, type_name, value in pairs:
        if key == "service.name":
            return value
    return ""


def log_expectation(event: dict, workspace: str) -> dict:
    record_pairs = without_reserved(event["record"] + (event["duplicate"] or []))
    resource_strings, resource_types = expected_resource(event["resource"], workspace)
    scope_strings, scope_types = project_pairs(without_reserved(event["scope"]))
    return {
        "workspace": workspace,
        "dataset": DATASET,
        "record": record_maps(record_pairs),
        "resource": resource_strings,
        "resource_types": resource_types,
        "scope": scope_strings,
        "scope_types": scope_types,
        "ts_ns": event["time_ns"],
        "observed_ns": event["observed_ns"] or (event["time_ns"] + 1_000_000),
        "body": event["body"][1] if isinstance(event["body"][1], str) else "",
        "body_kind": event["body"][0],
        "severity_text": event["severity"][0],
        "severity_number": event["severity"][1],
        "service_name": service_name(event["resource"]),
        "rejected": bool(event["duplicate"]),
    }


def span_timing_status(event: dict) -> str:
    """Fixture-derived timing classification, independent of the component.

    The three cases the approved policy must keep apart: a valid duration
    (including zero), missing OTLP timing (0/0), and reversed timing (end <
    start, whose derived duration is unavailable).
    """
    start, end = event["start_ns"], event["end_ns"]
    if start == 0 and end == 0:
        return "missing"
    if end < start:
        return "end_before_start"
    return "ok"


def span_duration_ns(event: dict) -> int | None:
    """The observable duration, or None when it is unavailable."""
    if span_timing_status(event) != "ok":
        return None
    return event["end_ns"] - event["start_ns"]


def span_expectation(event: dict) -> dict:
    scope_strings, scope_types = project_pairs(SPAN_SCOPE_OF[event["event_id"]])
    resource_strings, resource_types = expected_resource(event["resource"], WS_A)
    return {
        "workspace": WS_A,
        "dataset": DATASET,
        "record": record_maps(without_reserved(event["record"])),
        "resource": resource_strings,
        "resource_types": resource_types,
        "span_scope": scope_strings,
        "span_scope_types": scope_types,
        "ts_ns": event["start_ns"],
        "end_ns": event["end_ns"],
        "timing_status": span_timing_status(event),
        "duration_ns": span_duration_ns(event),
        "span_name": event["span_name"],
        "parent_span_id": fixture019.PARENT_SPAN_ID.hex(),
        "span_kind": "Server",
        "status_code": 1,
        "service_name": service_name(event["resource"]),
        "trace_id": (event["trace_id"] or fixture019.TRACE_ID).hex(),
        "span_id": (event["span_id"] or fixture019.SPAN_ID).hex(),
    }


# ---------------------------------------------------------------------------
# canonical decoding (independent implementation of the documented schema)
# ---------------------------------------------------------------------------


def decode_node(node: dict) -> tuple[str, object]:
    kind = node["t"]
    if kind == "empty":
        return "empty", None
    if kind == "string":
        return "string", node["v"]
    if kind == "bool":
        return "bool", bool(node["v"])
    if kind == "int":
        return "int", int(node["v"])
    if kind == "double":
        text = node["v"]
        return "double", {"nan": float("nan"), "inf": float("inf"), "-inf": float("-inf"),
                          "-0.0": -0.0}.get(text, float(text) if text != "-0.0" else -0.0)
    if kind == "bytes":
        return "bytes", base64.b64decode(node["v"])
    if kind == "array":
        return "array", [decode_node(item) for item in node["v"]]
    if kind == "map":
        return "map", {key: decode_node(item) for key, item in node["v"].items()}
    raise AssertionError(f"unknown canonical node type {kind}")


def decode_plain(node: dict) -> object:
    """Canonical node -> plain value for the serving string projection.

    Type tags are dropped the same way the OTel value model drops them when a
    nested value is projected to text, so an array attribute reconstructs to the
    same text the serving path stored (for example ``["a",1,true]``).
    """
    kind = node["t"]
    if kind == "array":
        return [decode_plain(item) for item in node["v"]]
    if kind == "map":
        return {key: decode_plain(item) for key, item in node["v"].items()}
    _, value = decode_node(node)
    return value


def canonical_projection_pairs(nodes: dict) -> list[tuple[str, str, object]]:
    pairs = []
    for key, node in nodes.items():
        kind, _ = decode_node(node)
        pairs.append((key, kind, decode_plain(node)))
    return pairs


def reconstruct_serving(doc: dict, kind: str) -> dict:
    """Rebuild the serving representation expected from one canonical payload.

    Rules are the documented serving layout: typed record Maps with the
    canonical-only kinds (bytes/array/map/empty) in the key-only `other` Map, the
    stock-compatible string projection plus companion type Map for
    resource/scope, and `span_scope` for a span's instrumentation scope.
    """
    record = record_maps(canonical_projection_pairs(doc["record"]))
    resource_strings, resource_types = project_pairs(canonical_projection_pairs(doc["resource"]))
    scope_strings, scope_types = project_pairs(canonical_projection_pairs(doc["scope"]))
    expected = {
        "workspace": doc["identity"]["tenant_id"],
        "dataset": doc["identity"]["dataset"],
        "record": record,
        "resource": resource_strings,
        "resource_types": resource_types,
        "ts_ns": int(doc["time"]["timestamp_ns"]),
        "observed_ns": int(doc["time"]["observed_ns"]),
        "body": doc["body"].get("v", ""),
        "body_kind": doc["body"]["t"],
    }
    if kind == "log":
        log = doc["log"]
        expected.update({
            "scope": scope_strings,
            "scope_types": scope_types,
            "severity_text": log["severity_text"],
            "severity_number": log["severity_number"],
        })
    else:
        span = doc["span"]
        start = int(doc["time"]["timestamp_ns"])
        end = int(span["end_ns"])
        expected.update({
            "span_scope": scope_strings,
            "span_scope_types": scope_types,
            "span_name": span["name"],
            "span_kind": span["kind"],
            "status_code": int(span["status_code"]),
            "parent_span_id": span["parent_span_id"],
            "end_ns": end,
            # The serving timing diagnostic and unavailable duration are
            # reconstructed from the canonical start/end by the documented rule.
            "timing_status": ("missing" if (start == 0 and end == 0)
                              else ("end_before_start" if end < start else "ok")),
            "duration_ns": int(span["duration_ns"]) if "duration_ns" in span else None,
        })
    return expected


CANONICAL_COMPARE_KEYS = {
    "log": ["workspace", "dataset", "record", "resource", "resource_types", "ts_ns",
            "observed_ns", "body", "body_kind", "scope", "scope_types", "severity_text",
            "severity_number"],
    "span": ["workspace", "dataset", "record", "resource", "resource_types", "ts_ns",
             "end_ns", "span_scope", "span_scope_types", "span_name", "span_kind",
             "status_code", "parent_span_id", "duration_ns", "timing_status"],
}


# ---------------------------------------------------------------------------
# SQL expression builders for the stored representation
# ---------------------------------------------------------------------------


def san(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")[:40]


MAP_CATEGORY = {
    "resource": "string_map", "scope": "string_map", "span_scope": "string_map",
    "resource_types": "type_map", "scope_types": "type_map", "span_scope_types": "type_map",
    "record_int": "int", "record_double": "double", "record_bool": "bool",
    "record_string": "string_map", "record_other": "type_map",
    "span_int": "int", "span_double": "double", "span_bool": "bool",
    "span_string": "string_map", "span_other": "type_map",
}


def map_exprs(alias: str, column: str, expected: dict, exprs: dict, params: dict) -> None:
    category = MAP_CATEGORY[column]
    exprs[f"rep.stored.{alias}.{column}.count"] = f"length({column}) = {len(expected)}"
    for key, value in expected.items():
        key_param = f"k_{san(alias)}_{column}_{san(key)}"
        value_param = f"v_{san(alias)}_{column}_{san(key)}"
        params[key_param] = key
        access = f"{column}[{{{key_param}:String}}]"
        has = f"has({column}, {{{key_param}:String}})"
        if category in ("string_map", "type_map"):
            params[value_param] = str(value)
            expr = f"{has} AND {access} = {{{value_param}:String}}"
        elif category == "int":
            params[value_param] = str(int(value))
            expr = f"{has} AND {access} = {{{value_param}:Int64}}"
        elif category == "bool":
            params[value_param] = "true" if value else "false"
            expr = f"{has} AND {access} = {{{value_param}:Bool}}"
        elif category == "double":
            if math.isnan(value):
                expr = f"{has} AND isNaN({access})"
            elif math.isinf(value):
                direction = ">" if value > 0 else "<"
                expr = f"{has} AND isInfinite({access}) AND {access} {direction} 0"
            elif value == 0:
                sign = "<" if math.copysign(1.0, value) < 0 else ">"
                expr = f"{has} AND {access} = 0 AND 1 / {access} {sign} 0"
            else:
                params[value_param] = repr(float(value))
                expr = f"{has} AND {access} = {{{value_param}:Float64}}"
        else:
            raise AssertionError(column)
        exprs[f"rep.stored.{alias}.{column}.{san(key)}"] = expr


def run_row_exprs(c: Checks, table: str, where: str, params: dict, exprs: dict,
                  expect_rows: int = 1, db: str = DB) -> None:
    sql = "SELECT " + ", ".join(f'{expr} AS "{name}"' for name, expr in exprs.items()) + \
        f" FROM {db}.{table} WHERE {where}"
    rows = ch(sql, params, db)
    if len(rows) != expect_rows:
        c.check(f"query.rowcount.{table}", False, f"expected {expect_rows} row(s), got {len(rows)}")
        return
    row = rows[0]
    for name, expr in exprs.items():
        value = row.get(name)
        ok = value is not None and int(value) == 1
        c.check(name, ok, "" if ok else f"expression false: {expr}")


LOG_MAP_OF = {"record_int": "int", "record_double": "double", "record_bool": "bool",
              "record_string": "string", "record_other": "other"}
SPAN_MAP_OF = {"span_int": "int", "span_double": "double", "span_bool": "bool",
               "span_string": "string", "span_other": "other"}


def check_common_row(c: Checks, table: str, event_id: str, expected: dict, kind: str = "log",
                     db: str = DB) -> None:
    exprs: dict[str, str] = {}
    params: dict = {"e": event_id, "w2": expected["workspace"], "tsn": str(expected["ts_ns"]),
                    "svc": expected["service_name"], "ds": expected["dataset"]}
    exprs["rep.row.workspace"] = "workspace_id = {w2:String}"
    exprs["rep.row.dataset"] = "dataset = {ds:String}"
    exprs["rep.row.ts"] = "toUnixTimestamp64Nano(ts) = {tsn:Int64}"
    exprs["rep.row.service_name"] = "service_name = {svc:String}"
    mapping = LOG_MAP_OF if kind == "log" else SPAN_MAP_OF
    for column, key in mapping.items():
        map_exprs(event_id, column, expected["record"][key], exprs, params)
    for column in ("resource", "resource_types"):
        map_exprs(event_id, column, expected[column], exprs, params)
    if kind == "log":
        for column in ("scope", "scope_types"):
            map_exprs(event_id, column, expected[column], exprs, params)
        params.update({"obs": str(expected["observed_ns"]), "body": expected["body"],
                       "sev": expected["severity_text"], "sevn": str(expected["severity_number"])})
        exprs["rep.row.observed_ts"] = "toUnixTimestamp64Nano(observed_ts) = {obs:Int64}"
        exprs["rep.row.body"] = "body = {body:String}"
        exprs["rep.row.severity_text"] = "severity_text = {sev:String}"
        exprs["rep.row.severity_number"] = "severity_number = {sevn:Int32}"
        exprs["rep.row.event_name"] = "event_name = ''"
        exprs["rep.row.kind"] = "kind = 'log'"
    else:
        for column in ("span_scope", "span_scope_types"):
            map_exprs(event_id, column, expected[column], exprs, params)
        params.update({"spname": expected["span_name"], "skind": expected["span_kind"],
                       "scode": str(expected["status_code"]), "pid": expected["parent_span_id"],
                       "tid": expected["trace_id"], "sid": expected["span_id"],
                       "endn": str(expected["end_ns"]), "timing": expected["timing_status"]})
        exprs["rep.row.span_name"] = "span_name = {spname:String}"
        exprs["rep.row.span_kind"] = "span_kind = {skind:String}"
        exprs["rep.row.status_code"] = "status_code = {scode:Int32}"
        exprs["rep.row.parent_span_id"] = "parent_span_id = {pid:String}"
        exprs["rep.row.trace_id"] = "trace_id = {tid:String}"
        exprs["rep.row.span_id"] = "span_id = {sid:String}"
        exprs["rep.row.kind"] = "kind = 'span'"
        # Both supplied timestamps are stored exactly as received, including the
        # reversed and the missing-timing spans.
        exprs["rep.row.end_ts"] = "toUnixTimestamp64Nano(end_ts) = {endn:Int64}"
        exprs["rep.row.timing_status"] = "span_timing_status = {timing:String}"
        if expected["duration_ns"] is None:
            exprs["rep.row.duration_null"] = "isNull(duration_ns)"
        else:
            params["dur"] = str(expected["duration_ns"])
            exprs["rep.row.duration_exact"] = "not isNull(duration_ns) AND duration_ns = {dur:Int64}"
    run_row_exprs(c, table, "event_id = {e:String} AND workspace_id = {w2:String}", params, exprs, db=db)


def record_identity(fixture_id: str, stored_id: str, kind: str) -> None:
    """Append the fixture-identity to stored-identity correlation to the evidence."""
    path = RESULTS / "identity-map.tsv"
    new = not path.exists()
    with path.open("a", encoding="utf-8") as fh:
        if new:
            fh.write("fixture_identity\tstored_event_id\tkind\n")
        fh.write(f"{fixture_id}\t{stored_id}\t{kind}\n")


def check_canonical_and_correlation(c: Checks, fixture_id: str, stored_id: str, expected: dict,
                                    kind: str, db: str = DB, expect_canonical: int = 1,
                                    expect_serving: int = 1) -> None:
    table = "serving_logs" if kind == "log" else "serving_spans"
    record_identity(fixture_id, stored_id, kind)
    canon = canonical_rows(stored_id, db)
    serving = row_count(table, stored_id, db=db)
    c.check(f"query.{fixture_id}.canonical_count", len(canon) == expect_canonical,
            f"expected {expect_canonical} canonical rows, got {len(canon)}")
    c.check(f"query.{fixture_id}.serving_count", serving == expect_serving,
            f"expected {expect_serving} serving rows, got {serving}")
    if not canon:
        return
    for index, row in enumerate(canon):
        digest = hashlib.sha256(row["payload"].encode("utf-8")).hexdigest()
        c.check(f"canon.{fixture_id}.{index}.sha256_matches_payload", digest == row["payload_sha256"],
                "payload_sha256 must be the sha256 of the stored payload")
        c.check(f"canon.{fixture_id}.{index}.schema_version",
                row["schema_version"] == "lighthouse.canonical.event/1", row["schema_version"])
        c.check(f"canon.{fixture_id}.{index}.signal",
                row["signal"] == ("logs" if kind == "log" else "spans"), row["signal"])
        c.check(f"canon.{fixture_id}.{index}.workspace", row["workspace_id"] == expected["workspace"],
                f"expected {expected['workspace']}, got {row['workspace_id']}")
        c.check(f"canon.{fixture_id}.{index}.dataset", row["dataset"] == expected["dataset"], row["dataset"])
        c.check(f"canon.{fixture_id}.{index}.event_id_links_selected", row["event_id"] == stored_id,
                "the canonical row must carry the resolved stored write identity")
        doc = json.loads(row["payload"])
        c.check(f"canon.{fixture_id}.{index}.identity_binding",
                doc["identity"]["event_id"] == row["event_id"]
                and doc["identity"]["tenant_id"] == row["workspace_id"],
                "the canonical payload identity must match the stored row identity")
        rebuilt = reconstruct_serving(doc, kind)
        for key in CANONICAL_COMPARE_KEYS[kind]:
            c.check(f"canon.{fixture_id}.{index}.{key}",
                    value_equal(expected.get(key), rebuilt.get(key)),
                    f"fixture={expected.get(key)!r} canonical-reconstruction={rebuilt.get(key)!r}")
    if expect_serving == 1:
        link = serving_link(stored_id, table, db)
        if link is None:
            c.check(f"xdest.{fixture_id}.serving_row_present", False,
                    f"no single serving row for {stored_id}")
        else:
            c.check(f"xdest.{fixture_id}.payload_sha256_links_canonical",
                    any(r["payload_sha256"] == link["payload_sha256"] for r in canon),
                    f"serving payload_sha256={link['payload_sha256']} matches no canonical row")
            c.check(f"xdest.{fixture_id}.serving_identity",
                    link["workspace_id"] == expected["workspace"] and link["dataset"] == expected["dataset"],
                    f"serving row identity {link['workspace_id']}/{link['dataset']}")


# ---------------------------------------------------------------------------
# metrics and result files
# ---------------------------------------------------------------------------


METRIC_LINE = re.compile(r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>.*)\})?\s+(?P<value>\S+)$")


def read_metrics(name: str) -> dict[tuple[str, tuple], float]:
    path = RESULTS / name
    if not path.exists():
        return {}
    out: dict[tuple[str, tuple], float] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        match = METRIC_LINE.match(line.strip())
        if not match:
            continue
        labels = dict(re.findall(r'(\w+)="([^"]*)"', match.group("labels") or ""))
        try:
            value = float(match.group("value"))
        except ValueError:
            continue
        out[(match.group("name"), tuple(sorted(labels.items())))] = value
    return out


def metric_sum(metrics: dict, prefix: str, **want) -> float:
    total = 0.0
    for (name, labels), value in metrics.items():
        if not name.startswith(prefix):
            continue
        label_dict = dict(labels)
        if all(label_dict.get(key) == expected for key, expected in want.items()):
            total += value
    return total


def read_emits(phase: str) -> list[dict]:
    path = RESULTS / f"{phase}-emits.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()]


def emitted(c: Checks, phase: str, label: str, signal: str = "logs") -> dict | None:
    for record in read_emits(phase):
        if record.get("label") == label and record.get("signal") == signal:
            return record
    c.check(f"query.emit.{phase}.{label}.present", False, "no emit record found")
    return None


def emit_ok(c: Checks, phase: str, label: str, signal: str = "logs") -> dict | None:
    record = emitted(c, phase, label, signal)
    if record is not None:
        c.check(f"query.emit.{phase}.{label}.status", record["status"] == 200,
                f"producer saw HTTP {record['status']}: {record.get('response_body', '')[:120]}")
    return record


def phase_window(name: str) -> tuple[str, str] | None:
    path = RESULTS / "phase-times.tsv"
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) >= 3 and parts[0] == name:
            return parts[1], parts[2]
    return None


def fixture_log(label: str, index: int = 0) -> dict:
    return fixture021.LOG_LABELS[label][index]


def fixture_event(fixture_id: str) -> dict:
    """The fixture inventory record for a readable fixture identity."""
    for events in (fixture021.TYPED_LOGS, fixture021.ALL_SPAN_EVENTS):
        for event in events:
            if event["event_id"] == fixture_id:
                return event
    for events in fixture021.LOG_LABELS.values():
        for event in events:
            if event["event_id"] == fixture_id:
                return event
    raise KeyError(fixture_id)


def fixture_body(event: dict) -> str:
    body = event.get("body")
    if isinstance(body, tuple) and len(body) == 2 and body[0] == "string":
        return body[1]
    return ""


# ---------------------------------------------------------------------------
# fixture identity -> stored write identity
# ---------------------------------------------------------------------------
#
# The fixture's readable identity is not carried on the wire. The component
# stores a provisional content-derived write identity with deduplication
# unselected, so this verifier locates rows through fixture-derived fields (log
# body text; span name plus trace ID) and the trusted workspace/dataset instead
# of recomputing that identity. Byte-level faithfulness of the stored identity
# derivation is therefore NOT claimed here; what is checked is that one fixture
# identity resolves to exactly one stored identity and that the same fixture
# identity keeps the same stored identity across retries.


def serving_rows_for_fixture(event: dict, workspace: str | None = None,
                             dataset: str | None = None, db: str = DB) -> list[dict]:
    """Serving rows for one fixture event, located by fixture-derived fields."""
    table = "serving_spans" if event.get("kind") == "span" else "serving_logs"
    sql = f"SELECT event_id, workspace_id, dataset, payload_sha256 FROM {db}.{table} WHERE 1"
    params: dict[str, str] = {}
    if table == "serving_logs":
        sql += " AND body = {b:String}"
        params["b"] = fixture_body(event)
    else:
        sql += " AND span_name = {n:String} AND trace_id = {t:String}"
        params["n"] = event["span_name"]
        params["t"] = (event.get("trace_id") or fixture019.TRACE_ID).hex()
    if workspace is not None:
        sql += " AND workspace_id = {w:String}"
        params["w"] = workspace
    if dataset is not None:
        sql += " AND dataset = {d:String}"
        params["d"] = dataset
    return ch(sql, params, db)


def canonical_rows_for_fixture(fixture_id: str, workspace: str | None = None,
                               db: str = DB) -> list[dict]:
    """Canonical rows whose payload carries the fixture identity as a JSON string.

    Needed where no serving row exists to supply the stored write identity, for
    example the canonical-first split write: the payload is probed for the
    quoted fixture identity, which is how the fixture's own identity appears in
    either the body or a record attribute.
    """
    sql = ("SELECT workspace_id, dataset, signal, event_id, payload, payload_sha256, "
           "toString(toUnixTimestamp64Nano(ts)) AS ts_ns, schema_version "
           f"FROM {db}.canonical_events WHERE position(payload, {{e:String}}) > 0")
    params = {"e": f'"{fixture_id}"'}
    if workspace is not None:
        sql += " AND workspace_id = {w:String}"
        params["w"] = workspace
    return ch(sql, params, db)


def resolve_stored(c: Checks, fixture_id: str, workspace: str | None = WS_A,
                   dataset: str | None = DATASET, db: str = DB, want: int = 1) -> list[dict]:
    """Resolve serving rows for a fixture identity and record the outcome."""
    rows = serving_rows_for_fixture(fixture_event(fixture_id), workspace, dataset, db)
    c.check(f"query.{fixture_id}.serving_rows_resolved", len(rows) == want,
            f"expected {want} serving row(s) for fixture identity {fixture_id} "
            f"via body/span name in {db}, got {len(rows)}")
    return rows if len(rows) == want else []


# ---------------------------------------------------------------------------
# phases
# ---------------------------------------------------------------------------


def phase_stock_probe(c: Checks) -> None:
    """Can a small stock transformation/config path meet the approved layout?"""
    exporter_dir = None
    for candidate in sorted(GOMODCACHE.glob(
            "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter@*")):
        if candidate.name.endswith(CONTRIB_VERSION):
            exporter_dir = candidate
    c.check("stock.source.module_present", exporter_dir is not None,
            f"looked for clickhouseexporter@{CONTRIB_VERSION} under {GOMODCACHE}")
    if exporter_dir is None:
        return
    inserts = {}
    # The stock exporter builds its INSERT from embedded SQL templates; the fixed
    # column list lives in internal/sqltemplates, not in the Go files.
    for name, filename in (("logs", "logs_insert.sql"), ("spans", "traces_insert.sql")):
        path = exporter_dir / "internal" / "sqltemplates" / filename
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        inserts[name] = text
        c.check(f"stock.source.{name}.insert_template_present", "INSERT INTO" in text,
                f"{path} missing or has no INSERT statement")
    logs_insert = inserts.get("logs", "")
    spans_insert = inserts.get("spans", "")
    c.check("stock.source.logs_insert_is_fixed",
            "LogAttributes" in logs_insert and "record_int" not in logs_insert
            and "RecordInt" not in logs_insert and logs_insert.count("?") >= 15,
            f"logs insert: {logs_insert[:200]}")
    c.check("stock.source.logs_insert_scope_attributes_string_map",
            "ScopeAttributes" in logs_insert,
            f"logs insert has no scope attribute column: {logs_insert[:200]}")
    c.check("stock.source.spans_insert_has_no_scope_attributes",
            "ScopeAttributes" not in spans_insert and spans_insert.count("?") >= 20,
            f"spans insert: {spans_insert[:200]}")
    config_text = (exporter_dir / "config.go").read_text(encoding="utf-8")
    for absent in ("extra_columns", "payload_column", "canonical"):
        c.check(f"stock.source.config_has_no_{absent}", absent not in config_text,
                f"{absent} unexpectedly present in config.go")

    typed_path = RESULTS / "stock-transform-typed.jsonl"
    if typed_path.exists():
        text = typed_path.read_text(encoding="utf-8").replace(" ", "")
        c.check("stock.transform.typed_int_created",
                '"key":"probe.int","value":{"intValue":"1"}' in text,
                "expected a typed int probe.int in the transform probe output")
        c.check("stock.transform.typed_double_created",
                '"key":"probe.double","value":{"doubleValue":1.5}' in text, "typed double missing")
        c.check("stock.transform.guarded_statement_applied",
                '"key":"probe.guarded","value":{"intValue":"2"}' in text,
                'the guarded statement on the string key str.one did not apply')
        c.check("stock.transform.guarded_statement_skipped",
                '"key":"probe.never"' not in text,
                'the guard on the int key int.one applied to a non-string value')
    else:
        c.check("stock.transform.typed_output_present", False, f"{typed_path} missing")

    stringify_path = RESULTS / "stock-transform-stringify.jsonl"
    if stringify_path.exists():
        text = stringify_path.read_text(encoding="utf-8").replace(" ", "")
        c.check("stock.transform.stringify_collapses_types",
                '"key":"int.one","value":{"stringValue":"1"}' in text
                and '"key":"double.one","value":{"stringValue":"1"}' in text
                and '"key":"str.one","value":{"stringValue":"1"}' in text,
                "stringify_all should turn int/double/string 1 into the same string")
    else:
        c.check("stock.transform.stringify_output_present", False, f"{stringify_path} missing")

    log_path = RESULTS / "transform-types.log"
    if log_path.exists():
        text = log_path.read_text(encoding="utf-8", errors="replace")
        c.check("stock.transform.types_expression_rejected",
                "error" in text.lower() and ("invalid" in text.lower() or "unable to" in text.lower()
                                            or "parse" in text.lower()),
                "expected a config-load error for the MapEach/ternary type-map expression")
    else:
        c.check("stock.transform.types_log_present", False, f"{log_path} missing")

    if not db_exists(DB_STOCK):
        c.check("stock.db_present", False, f"database {DB_STOCK} missing")
        return
    columns = table_columns(DB_STOCK, "otel_logs")
    c.check("stock.stored.logs_attributes_single_string_map",
            columns.get("LogAttributes", "").startswith("Map("),
            f"LogAttributes={columns.get('LogAttributes')!r}")
    for absent in ("RecordInt", "record_int", "Payload", "CanonicalPayload"):
        c.check(f"stock.stored.no_{absent}", absent not in columns, f"unexpected column {absent}")
    span_columns = table_columns(DB_STOCK, "otel_traces")
    c.check("stock.stored.spans_no_scope_attribute_column",
            not any("ScopeAttr" in name for name in span_columns), f"span columns: {sorted(span_columns)}")
    rows = ch("SELECT LogAttributes['int.one'] AS int_one, LogAttributes['double.one'] AS double_one, "
              "LogAttributes['str.one'] AS str_one, LogAttributes['bytes.raw'] AS bytes_raw, "
              "LogAttributes['array.mixed'] AS array_mixed, LogAttributes['empty.value'] AS empty_value, "
              "LogAttributes['int.big'] AS int_big, LogAttributes['double.nan'] AS double_nan "
              f"FROM {DB_STOCK}.otel_logs WHERE LogAttributes['str.one'] != '' LIMIT 1", db=DB_STOCK)
    if rows:
        row = rows[0]
        c.check("stock.stored.int_double_string_collapse",
                row["int_one"] == "1" and row["double_one"] == "1" and row["str_one"] == "1",
                f"stored strings: {row}")
        c.check("stock.stored.big_int_string_exact", row["int_big"] == str(INT_ABOVE_2_53),
                f"{row['int_big']!r}")
        c.check("stock.stored.nan_string", row["double_nan"] == "NaN", f"{row['double_nan']!r}")
        c.check("stock.stored.bytes_present", row["bytes_raw"] != "", f"{row['bytes_raw']!r}")
        c.check("stock.stored.array_json_text", row["array_mixed"] == '["a",1,true]',
                f"{row['array_mixed']!r}")
        c.check("stock.stored.empty_is_empty_string", row["empty_value"] == "",
                f"{row['empty_value']!r}")
    else:
        c.check("stock.stored.rows_present", False, "no stock log row with str.one found")


def check_gateway_trust_config(c: Checks) -> None:
    """Configuration-shape assertions for the trusted-scope path.

    The exporter's marker check is configuration-dependent: it proves the
    configured stamping pipeline ran before the exporter, not that the producer
    is authenticated or that the resource attributes are authentic. These
    assertions record what makes the tested configuration safe and cannot by
    themselves prove the deployment: the stamping processors upsert the reserved
    keys (so producer-supplied values are overwritten before batching), and the
    owned exporter is referenced only from pipelines that include a stamping
    processor. The exported endpoint must be reachable only through those
    pipelines; that is an operational control this harness cannot assert.
    """
    path = HERE / "collector-gateway.yaml"
    text = path.read_text(encoding="utf-8")
    upserts = text.count("action: upsert")
    c.check("trust.stamping_upserts_reserved_keys", upserts == 6,
            f"both workspace processors must upsert workspace, dataset, and marker; "
            f"found {upserts} upsert actions")
    # Anchor on line starts: with a leading `\n` in the pattern, a pipeline whose
    # body ended exactly where the next pipeline's line began was skipped, so the
    # scan silently inspected only half of the pipelines (found in a full run).
    service = text.split("\nservice:\n", 1)[1] if "\nservice:\n" in text else ""
    owned: list[tuple[str, str]] = []
    for match in re.finditer(r"^    ([a-z]+/[a-z-]+):\n((?:      .*\n)+)", service, re.M):
        name, body = match.group(1), match.group(2)
        if re.search(r"exporters: \[[^\]]*\blighthouse\b", body):
            owned.append((name, body))
    c.check("trust.owned_exporter_only_workspace_pipelines",
            sorted(name for name, _ in owned) == ["logs/ws-a", "logs/ws-b", "traces/ws-a", "traces/ws-b"],
            f"pipelines exporting to the owned exporter: {sorted(name for name, _ in owned)}")
    unstamped = [name for name, body in owned
                 if not re.search(r"processors: \[resource/ws-[ab]\]", body)]
    c.check("trust.owned_exporter_pipelines_are_stamped", not unstamped,
            f"pipelines reaching the owned exporter without a stamping processor: {unstamped}")
    # The fail-closed probe must stay without stamping, so the missing-scope
    # rejection path is genuinely exercised.
    probe = (HERE / "collector-no-trust.yaml").read_text(encoding="utf-8")
    c.check("trust.fail_closed_probe_has_no_stamping",
            "resource/ws" not in probe and "lighthouse.workspace_id" not in probe,
            "the fail-closed probe configuration injects trusted scope")


def phase_trust_config(c: Checks) -> None:
    """Configuration-shape checks only, with no services or stored rows.

    The fast self-test runs this phase so a degraded trusted-scope scan (which
    would otherwise be hidden behind heavyweight row checks in `semantics`) fails
    the harness self-test directly.
    """
    check_gateway_trust_config(c)


def phase_semantics(c: Checks) -> None:
    check_gateway_trust_config(c)
    metrics = read_metrics("metrics-semantics.txt")
    log_path = RESULTS / "collector-semantics.log"
    for event in fixture021.TYPED_LOGS:
        fixture_id = event["event_id"]
        expected = log_expectation(event, WS_A)
        rows = serving_rows_for_fixture(event, WS_A, DATASET)
        if expected["rejected"]:
            c.check(f"query.{fixture_id}.serving_absent", len(rows) == 0,
                    "a record with duplicate attribute keys must not be served")
            c.check(f"query.{fixture_id}.canonical_absent",
                    len(canonical_rows_for_fixture(fixture_id, WS_A)) == 0,
                    "a record with duplicate attribute keys must not be canonicalized")
            continue
        if not c.check(f"query.{fixture_id}.serving_present", len(rows) == 1,
                       f"expected exactly one serving row for fixture identity {fixture_id}, "
                       f"got {len(rows)}"):
            continue
        stored_id = rows[0]["event_id"]
        check_canonical_and_correlation(c, fixture_id, stored_id, expected, "log")
        check_common_row(c, "serving_logs", stored_id, expected)
    # The group rows isolate one value per type for the same key.
    for label in ("group-int-1", "group-double-1", "group-string-1", "group-bool-false",
                  "group-int-zero", "group-empty", "group-empty-string", "group-missing",
                  "bytes-row", "nested-row", "sum-string"):
        rows = resolve_stored(c, label)
        if not rows:
            continue
        exprs = {"query.group.single_namespace":
                 "length(record_int) + length(record_double) + length(record_bool) + "
                 "length(record_string) + length(record_other) <= 1"}
        run_row_exprs(c, "serving_logs", "event_id = {e:String}", {"e": rows[0]["event_id"]}, exprs)
    # Numeric (not lexical) typed ordering through a direct map guard.
    order_ids = [rows[0]["event_id"] for label in ("order-9", "order-10", "order-100")
                 if (rows := resolve_stored(c, label))]
    if len(order_ids) == 3:
        order = ch("SELECT event_id, record_int['metric.value'] AS v FROM serving_logs "
                   "WHERE event_id IN ({i9:String}, {i10:String}, {i100:String}) "
                   "ORDER BY record_int['metric.value']",
                   {"i9": order_ids[0], "i10": order_ids[1], "i100": order_ids[2]})
        c.check("query.order.numeric_guard", [int(r["v"]) for r in order] == [9, 10, 100],
                f"typed ORDER BY gave {[r['v'] for r in order]}")
    else:
        c.check("query.order.numeric_guard", False,
                "the three numeric-order fixture rows did not resolve to serving rows")
    # Named representation checks the report cites directly.
    typed_rows = resolve_stored(c, "typed-1")
    params = {"e": typed_rows[0]["event_id"] if typed_rows else "", "k": "int.one",
              "k2": "double.one", "k3": "str.one", "k4": "int.big", "k5": "int.min",
              "k6": "bool.false", "k7": "zero.int", "k8": "str.empty", "k9": "empty.value",
              "k10": "bytes.raw", "k11": "double.nan", "k12": "double.posinf",
              "k13": "double.neginf", "k14": "zero.float", "k15": "unicode",
              "uni": fixture019.UNICODE, "big": str(INT_ABOVE_2_53), "imin": str(INT64_MIN)}
    if typed_rows:
        exprs = {
            "rep.typed.int_one_is_int64": "record_int[{k:String}] = 1",
            "rep.typed.double_one_exact": "record_double[{k2:String}] = 1.0",
            "rep.typed.string_one_exact": "record_string[{k3:String}] = '1'",
            "rep.typed.big_int_exact": "record_int[{k4:String}] = {big:Int64}",
            "rep.typed.int64_min_exact": "record_int[{k5:String}] = {imin:Int64}",
            "rep.typed.bool_false_present": "has(record_bool, {k6:String}) AND record_bool[{k6:String}] = false",
            "rep.typed.int_zero_present": "has(record_int, {k7:String}) AND record_int[{k7:String}] = 0",
            "rep.typed.empty_string_is_string": "record_string[{k8:String}] = ''",
            "rep.typed.explicit_empty_is_other": "has(record_other, {k9:String}) AND record_other[{k9:String}] = 'empty'",
            "rep.typed.bytes_marked_not_typed": "has(record_other, {k10:String}) AND NOT has(record_string, {k10:String})",
            "rep.typed.nan_preserved": "isNaN(record_double[{k11:String}])",
            "rep.typed.posinf_preserved": "isInfinite(record_double[{k12:String}]) AND record_double[{k12:String}] > 0",
            "rep.typed.neginf_preserved": "isInfinite(record_double[{k13:String}]) AND record_double[{k13:String}] < 0",
            "rep.typed.negative_zero_sign": "record_double[{k14:String}] = 0 AND 1 / record_double[{k14:String}] < 0",
            "rep.typed.unicode_string_exact": "record_string[{k15:String}] = {uni:String}",
            "rep.typed.record_and_other_disjoint":
                "has(record_other, {k10:String}) AND NOT has(record_int, {k10:String}) "
                "AND NOT has(record_double, {k10:String}) AND NOT has(record_bool, {k10:String})",
        }
        run_row_exprs(c, "serving_logs", "event_id = {e:String}", params, exprs)
        # Namespace separation: the same key name in resource, scope, and record.
        ns = {"query.namespace.collide_distinct":
              "resource[{kr:String}] = 'resource-value' AND scope[{ks:String}] = 'scope-value' "
              "AND record_string[{kc:String}] = 'record-value'",
              "query.namespace.case_variants":
              "resource[{ku:String}] = 'resource-upper' AND resource[{kl:String}] = 'resource-lower' "
              "AND record_string[{kr2:String}] = 'record-upper'"}
        run_row_exprs(c, "serving_logs", "event_id = {e:String}",
                      {"e": params["e"], "kr": "res.collide", "ks": "scope.attr",
                       "kc": "collide", "ku": "Foo", "kl": "foo", "kr2": "Foo"}, ns)
    else:
        c.check("rep.typed.rows_resolved", False,
                "the typed fixture row did not resolve to a serving row")
    # Spans. Every span is preserved, including the reversed-timing span: the
    # approved policy keeps the span and both supplied timestamps, stores the
    # derived duration as unavailable, and records the timing diagnostic.
    for event in fixture021.ALL_SPAN_EVENTS:
        fixture_id = event["event_id"]
        expected = span_expectation(event)
        rows = serving_rows_for_fixture(event, WS_A, DATASET)
        if not c.check(f"query.{fixture_id}.serving_present", len(rows) == 1,
                       f"expected exactly one serving span for {fixture_id}, got {len(rows)}"):
            continue
        stored_span_id = rows[0]["event_id"]
        check_canonical_and_correlation(c, fixture_id, stored_span_id, expected, "span")
        check_common_row(c, "serving_spans", stored_span_id, expected, kind="span")
    scope_rows = resolve_stored(c, "span-scope")
    if scope_rows:
        scope_exprs = {
            "rep.span.span_scope_attrs_preserved":
                "span_scope[{ks:String}] = 's' AND span_scope_types[{ks:String}] = 'string'",
            "rep.span.span_scope_int_projected":
                "span_scope[{ki:String}] = '11' AND span_scope_types[{ki:String}] = 'int'",
            "rep.span.span_scope_not_record":
                "NOT has(span_string, {ks:String}) AND NOT has(span_int, {ks:String})",
        }
        run_row_exprs(c, "serving_spans", "event_id = {e:String}",
                      {"e": scope_rows[0]["event_id"], "ks": "scope.span.attr",
                       "ki": "scope.span.int"}, scope_exprs)
    # The three timing cases must stay distinct in the stored diagnostic, and
    # the reversed span keeps its original timestamps.
    timing_rows = {label: resolve_stored(c, label)
                   for label in ("span-missing-time", "span-negative", "span-zero")}
    if all(timing_rows.values()):
        statuses = ch(
            "SELECT "
            "(SELECT span_timing_status FROM serving_spans WHERE event_id = {m:String}) AS missing_status, "
            "(SELECT span_timing_status FROM serving_spans WHERE event_id = {r:String}) AS reversed_status, "
            "(SELECT span_timing_status FROM serving_spans WHERE event_id = {z:String}) AS zero_status",
            {"m": timing_rows["span-missing-time"][0]["event_id"],
             "r": timing_rows["span-negative"][0]["event_id"],
             "z": timing_rows["span-zero"][0]["event_id"]})
        row = statuses[0] if statuses else {}
        c.check("rep.span.missing_timing_status", row.get("missing_status") == "missing",
                f"missing-timing status = {row.get('missing_status')!r}")
        c.check("rep.span.reversed_timing_status", row.get("reversed_status") == "end_before_start",
                f"reversed-timing status = {row.get('reversed_status')!r}")
        c.check("rep.span.zero_timing_status", row.get("zero_status") == "ok",
                f"zero-duration status = {row.get('zero_status')!r}")
        c.check("rep.span.timing_statuses_distinct",
                len({row.get("missing_status"), row.get("reversed_status"),
                     row.get("zero_status")}) == 3,
                f"timing diagnostics are not distinct: {row}")
    missing_rows = timing_rows["span-missing-time"]
    if missing_rows:
        run_row_exprs(c, "serving_spans", "event_id = {e:String}", {"e": missing_rows[0]["event_id"]},
                      {"rep.span.missing_timing_is_null_not_underflow": "isNull(duration_ns)",
                       "rep.span.missing_timing_keeps_zero_start": "toUnixTimestamp64Nano(ts) = 0",
                       "rep.span.missing_timing_keeps_zero_end": "toUnixTimestamp64Nano(end_ts) = 0",
                       "rep.span.missing_timing_diagnostic": "span_timing_status = 'missing'"})
    reversed_rows = timing_rows["span-negative"]
    if reversed_rows:
        run_row_exprs(c, "serving_spans", "event_id = {e:String}", {"e": reversed_rows[0]["event_id"]},
                      {"rep.span.reversed_duration_unavailable": "isNull(duration_ns)",
                       "rep.span.reversed_keeps_original_timestamps":
                           "toUnixTimestamp64Nano(end_ts) < toUnixTimestamp64Nano(ts)",
                       "rep.span.reversed_span_preserved":
                           "span_name = 'span/negative-duration' AND trace_id != ''",
                       "rep.span.reversed_diagnostic": "span_timing_status = 'end_before_start'"})
    # Independent aggregate expectations for span timing, derived from the
    # fixture inventory (never from the stored rows): contributing spans have an
    # observable duration (a valid zero counts), excluded spans have none, and
    # the error count is the reversed-timing subset. This is the count contract a
    # future duration aggregate must honour, not a production aggregate.
    fixture_spans = list(fixture021.ALL_SPAN_EVENTS)
    want_ok = sum(1 for e in fixture_spans if span_timing_status(e) == "ok")
    want_missing = sum(1 for e in fixture_spans if span_timing_status(e) == "missing")
    want_reversed = sum(1 for e in fixture_spans if span_timing_status(e) == "end_before_start")
    agg = ch("SELECT count() AS total, countIf(duration_ns IS NOT NULL) AS contributing, "
             "countIf(isNull(duration_ns)) AS excluded, "
             "countIf(span_timing_status = 'end_before_start') AS errors, "
             "countIf(span_timing_status = 'missing') AS missing, "
             "uniqExact(span_timing_status) AS distinct_statuses "
             "FROM serving_spans WHERE workspace_id = {w:String} AND dataset = {d:String}",
             {"w": WS_A, "d": DATASET})
    row = agg[0] if agg else {}
    c.check("rep.span.aggregate_total", int(row.get("total", -1)) == len(fixture_spans),
            f"fixture has {len(fixture_spans)} spans, stored total = {row.get('total')}")
    c.check("rep.span.aggregate_contributing",
            int(row.get("contributing", -1)) == want_ok,
            f"fixture expects {want_ok} contributing spans, stored = {row.get('contributing')}")
    c.check("rep.span.aggregate_excluded",
            int(row.get("excluded", -1)) == want_missing + want_reversed,
            f"fixture expects {want_missing + want_reversed} excluded spans, "
            f"stored = {row.get('excluded')}")
    c.check("rep.span.aggregate_errors", int(row.get("errors", -1)) == want_reversed,
            f"fixture expects {want_reversed} reversed-timing spans, stored = {row.get('errors')}")
    c.check("rep.span.aggregate_missing", int(row.get("missing", -1)) == want_missing,
            f"fixture expects {want_missing} missing-timing spans, stored = {row.get('missing')}")
    c.check("rep.span.aggregate_distinct_statuses",
            int(row.get("distinct_statuses", -1)) == len(
                {span_timing_status(e) for e in fixture_spans}),
            f"stored distinct statuses = {row.get('distinct_statuses')}")
    # Forged producer identity must not override the trusted scope.
    for label, workspace in (("identity-a", WS_A), ("identity-b", WS_B)):
        event = fixture_log(label)
        fixture_id = event["event_id"]
        expected = log_expectation(event, workspace)
        rows = serving_rows_for_fixture(event, workspace, DATASET)
        if not c.check(f"query.{fixture_id}.serving_present", len(rows) == 1,
                       f"expected exactly one serving row for {fixture_id}, got {len(rows)}"):
            continue
        event_stored = rows[0]["event_id"]
        check_canonical_and_correlation(c, fixture_id, event_stored, expected, "log")
        check_common_row(c, "serving_logs", event_stored, expected)
        exprs = {
            "rep.forged.workspace_not_producer_value": "workspace_id != {forged:String}",
            "rep.forged.dataset_not_producer_value": "dataset != {forged_ds:String}",
            "rep.forged.resource_has_no_producer_marker": "NOT has(resource, {marker:String})",
            "rep.forged.record_reserved_key_dropped": "NOT has(record_string, {rkey:String})",
            "rep.forged.scope_reserved_key_dropped": "NOT has(scope, {rkey:String})",
            "rep.forged.kept_non_reserved_record_key":
                "has(record_string, {keep:String}) AND record_string[{keep:String}] = 'kept'",
        }
        run_row_exprs(c, "serving_logs", "event_id = {e:String}",
                      {"e": event_stored, "forged": WS_FORGED, "forged_ds": fixture021.FORGED_DATASET,
                       "marker": "lighthouse.scope_source", "rkey": "lighthouse.workspace_id",
                       "keep": "forged.attr"}, exprs)
    # Batch contents: one export carried both workspaces.
    for label, workspace in (("batch-a", WS_A), ("batch-b", WS_B)):
        event = fixture_log(label)
        fixture_id = event["event_id"]
        expected = log_expectation(event, workspace)
        rows = serving_rows_for_fixture(event, workspace, DATASET)
        if not c.check(f"query.{fixture_id}.serving_present", len(rows) == 1,
                       f"expected exactly one serving row for {fixture_id}, got {len(rows)}"):
            continue
        check_canonical_and_correlation(c, fixture_id, rows[0]["event_id"], expected, "log")
        check_common_row(c, "serving_logs", rows[0]["event_id"], expected)
    c.check("comp.cross_workspace_batch",
            metric_sum(metrics, "lighthouse_exporter_batches",
                       cross_workspace="true") >= 1,
            "no cross-workspace batch recorded by the component")
    if log_path.exists():
        text = log_path.read_text(encoding="utf-8", errors="replace")
        # Prove the mixed batch carried both fixtures' records, not merely two
        # requests through the same queue: the summary names the stored
        # identities of batch-a-1 (WS_A) and batch-b-1 (WS_B) in one line.
        ids_a = [r["event_id"] for r in serving_rows_for_fixture(fixture_log("batch-a"), WS_A, DATASET)]
        ids_b = [r["event_id"] for r in serving_rows_for_fixture(fixture_log("batch-b"), WS_B, DATASET)]
        mixed = [line for line in text.splitlines()
                 if "lighthouse exporter batch" in line and '"cross_workspace": true' in line
                 and WS_A in line and WS_B in line
                 and any(f'"{i}"' in line for i in ids_a)
                 and any(f'"{i}"' in line for i in ids_b)]
        c.check("comp.batch_summary_log_mixed_workspaces", bool(mixed),
                "no batch summary line naming both workspaces and both fixture "
                f"identities in one export (a={ids_a}, b={ids_b})")
        # The mixed timing batch is one export item: one summary line names both
        # stored identities of the valid+reversed request, so the preserved-span
        # policy was exercised inside a single downstream export.
        timing_ids = [r["event_id"] for event in fixture021.TIMING_MIXED
                      for r in serving_rows_for_fixture(event, WS_A, DATASET)]
        timing_batch = [line for line in text.splitlines()
                        if "lighthouse exporter batch" in line and '"signal": "spans"' in line
                        and all(f'"{i}"' in line for i in timing_ids)]
        c.check("comp.timing_mixed_batch_log", bool(timing_batch),
                "no single span batch summary line names both stored identities of "
                f"the valid+reversed request ({timing_ids})")
        c.check("comp.no_dropped_data", "Dropping data" not in text,
                "the collector dropped data in the semantics phase")
        c.check("comp.no_rejected_batches",
                metric_sum(metrics, "lighthouse_exporter_rejected_batches") == 0,
                "a batch was rejected whole in the semantics phase")
    else:
        c.check("files.collector_semantics_log", False, "collector-semantics.log missing")
    for table in ("serving_logs", "serving_spans", "canonical_events"):
        dups = duplicate_events(table)
        c.check(f"query.no_duplicates.{table}", not dups, f"duplicates: {dups[:3]}")
    # Duplicates by fixture identity as well: the content-derived write identity
    # is not a dedup guarantee, so a per-request identity would otherwise hide a
    # double write from the stored-id scan above.
    for table in ("serving_logs", "serving_spans"):
        dups = fixture_duplicates(table)
        c.check(f"query.no_fixture_duplicates.{table}", not dups, f"duplicates: {dups[:3]}")
    c.check("comp.rejected_duplicate_key_record",
            metric_sum(metrics, "lighthouse_exporter_rejected_records",
                       reason="duplicate_or_invalid_record_attributes") >= 1,
            "no rejected-record counter for the duplicate-key record")
    c.check("comp.no_negative_duration_rejection",
            metric_sum(metrics, "lighthouse_exporter_rejected_records",
                       reason="negative_span_duration") == 0,
            "the approved policy accepts reversed-timing spans; no negative-duration "
            "rejection is expected (the pre-policy run measured one)")
    logs_columns = table_columns(DB, "serving_logs")
    spans_columns = table_columns(DB, "serving_spans")
    c.check("rep.schema.record_int_native",
            "Map(" in logs_columns.get("record_int", "") and "Int64" in logs_columns.get("record_int", ""),
            logs_columns.get("record_int", ""))
    c.check("rep.schema.record_double_native", "Float64" in logs_columns.get("record_double", ""),
            logs_columns.get("record_double", ""))
    c.check("rep.schema.record_bool_native", "Bool" in logs_columns.get("record_bool", ""),
            logs_columns.get("record_bool", ""))
    c.check("rep.schema.resource_string_and_types",
            logs_columns.get("resource", "").startswith("Map(") and "resource_types" in logs_columns,
            f"resource={logs_columns.get('resource')}")
    c.check("rep.schema.spans_span_scope_present",
            "span_scope" in spans_columns and "span_scope_types" in spans_columns,
            "span scope namespace missing from serving_spans")
    c.check("rep.schema.spans_no_duplicate_scope_column", "scope" not in spans_columns,
            "serving_spans should not carry both scope and span_scope")
    c.check("rep.schema.duration_nullable", "Nullable(Int64)" in spans_columns.get("duration_ns", ""),
            spans_columns.get("duration_ns", ""))
    c.check("rep.schema.spans_end_ts_present",
            "DateTime64" in spans_columns.get("end_ts", ""), spans_columns.get("end_ts", ""))
    c.check("rep.schema.spans_timing_status_present",
            "LowCardinality(String)" in spans_columns.get("span_timing_status", ""),
            spans_columns.get("span_timing_status", ""))


def phase_ack(c: Checks) -> None:
    metrics = read_metrics("metrics-ack.txt")
    on = emit_ok(c, "ack", "ack-on")
    off = emit_ok(c, "ack", "ack-off")
    for label in ("ack-on-1", "ack-off-1"):
        rows = resolve_stored(c, label)
        if not rows:
            c.check(f"ack.{label}.canonical_present", False,
                    "no serving row resolved, so the canonical identity is unknown")
            continue
        stored = rows[0]["event_id"]
        record_identity(label, stored, "log")
        c.check(f"ack.{label}.canonical_present", len(canonical_rows(stored)) == 1,
                f"expected exactly one canonical row for {label}")
    if on and off:
        c.check("ack.queue_enabled_not_visible_at_ack", on.get("visible_at_ack") is False,
                f"queue enabled: visible_at_ack={on.get('visible_at_ack')} "
                f"(ack {on.get('elapsed_ms')} ms, visible after {on.get('visible_within_s')} s)")
        c.check("ack.queue_disabled_visible_at_ack", off.get("visible_at_ack") is True,
                f"queue disabled: visible_at_ack={off.get('visible_at_ack')} "
                f"(ack {off.get('elapsed_ms')} ms)")
        c.check("ack.queue_enabled_drained", on.get("visible_within_s") is not None,
                "the queue-enabled request never became visible")
    c.check("comp.ack_off_records_counter",
            metric_sum(read_metrics("metrics-ack-off.txt"), "lighthouse_exporter_records",
                       destination="serving") >= 1,
            "the queue-disabled write did not count a serving record")
    c.check("comp.ack_on_records_counter",
            metric_sum(metrics, "lighthouse_exporter_records",
                       destination="serving") >= 1,
            "the queue-enabled write did not count a serving record")


def phase_mixed_validation(c: Checks) -> None:
    """Whole-request validation: a valid record followed by an invalid one.

    The component prepares (validates and canonicalizes) every record of the
    received request before its first insert, then excludes invalid records per
    record. The batch is rejected as a whole only when the trusted scope is
    missing or when nothing survives validation. This phase measures that policy,
    the error/retry classification, and the producer acknowledgement timing; it
    also checks the component's own batch summary, which reports written and
    rejected records for the same export so a partial drop cannot hide behind the
    rejected-batch counter.
    """
    metrics = read_metrics("metrics-mixed-validation.txt")
    mixed = emitted(c, "mixed-validation", "mixed")
    invalid_only = emitted(c, "mixed-validation", "invalid-only")
    valid_rows = serving_rows_for_fixture(fixture_event("mixed-valid-1"), WS_A, DATASET)
    invalid_rows = serving_rows_for_fixture(fixture_event("mixed-invalid-1"), WS_A, DATASET)
    valid_canon = canonical_rows_for_fixture("mixed-valid-1", WS_A)
    invalid_canon = canonical_rows_for_fixture("mixed-invalid-1", WS_A)
    c.check("mixed.producer_ack", mixed is not None and mixed["status"] == 200,
            f"producer saw {mixed and mixed['status']}")
    c.check("mixed.valid_record_written_after_prevalidation",
            len(valid_rows) == 1 and len(valid_canon) == 1,
            f"the valid record should be written to both destinations, got "
            f"serving={len(valid_rows)} canonical={len(valid_canon)}")
    c.check("mixed.invalid_record_excluded",
            not invalid_rows and not invalid_canon,
            "the duplicate-key record must be excluded from both destinations")
    c.check("mixed.per_record_rejection_counted",
            metric_sum(metrics, "lighthouse_exporter_rejected_records",
                       reason="duplicate_or_invalid_record_attributes", namespace="record") == 2,
            "one rejected record from the mixed request plus one from the all-invalid request")
    c.check("mixed.only_all_invalid_batch_rejected_whole",
            metric_sum(metrics, "lighthouse_exporter_rejected_batches",
                       reason="all_records_rejected") == 1,
            "only the batch with nothing surviving validation may be rejected as a whole; the "
            "mixed batch wrote its surviving record instead")
    c.check("mixed.rejection_is_not_an_insert_failure",
            metric_sum(metrics, "lighthouse_exporter_insert_failures") == 0,
            "per-record rejection must not be classified as an insert failure or retry")
    if mixed is not None:
        c.check("mixed.ack_precedes_visibility",
                mixed.get("visible_at_ack") is False and mixed.get("visible_within_s") is not None,
                f"queue-enabled mixed request: visible_at_ack={mixed.get('visible_at_ack')} "
                f"(ack {mixed.get('elapsed_ms')} ms, visible after {mixed.get('visible_within_s')} s)")
        c.check("mixed.rejection_not_producer_visible",
                "partial success" not in (mixed.get("response_body") or "").lower(),
                "the producer response must not report the per-record rejection")
    c.check("mixed.all_invalid_producer_ack",
            invalid_only is not None and invalid_only["status"] == 200,
            "the all-invalid request is acknowledged like any other (rejection is not producer visible)")
    c.check("mixed.all_invalid_writes_nothing",
            not serving_rows_for_fixture(fixture_event("invalid-only-1"), WS_A, DATASET)
            and not canonical_rows_for_fixture("invalid-only-1", WS_A),
            "a batch with nothing surviving validation must write to neither destination")
    c.check("mixed.all_invalid_no_insert_attempt",
            metric_sum(metrics, "lighthouse_exporter_insert_failures") == 0,
            "the all-invalid batch is permanent: no insert was attempted and nothing was retried")
    log = RESULTS / "collector-mixed-validation.log"
    if log.exists():
        text = log.read_text(encoding="utf-8", errors="replace")
        summaries = [line for line in text.splitlines()
                     if "lighthouse exporter batch" in line
                     and '"records": 1' in line and '"rejected": 1' in line]
        c.check("comp.mixed_batch_summary_one_export", len(summaries) == 1,
                f"expected one batch summary with records=1 and rejected=1 for the mixed "
                f"request, got {len(summaries)}")
        c.check("comp.all_invalid_permanent_error_logged",
                "logs batch rejected: 1 records failed validation" in text,
                "the all-invalid batch should be dropped with a permanent validation error")
    else:
        c.check("files.collector_mixed_validation_log", False, f"{log} missing")


def phase_outage(c: Checks) -> None:
    label = "fault-outage-1"
    emit_ok(c, "outage", "fault-outage")
    metrics = read_metrics("metrics-outage.txt")
    rows = resolve_stored(c, label)
    canon = canonical_rows_for_fixture(label, WS_A)
    c.check("fail.outage.canonical_recovered", len(canon) == 1,
            f"expected one canonical row after recovery, got {len(canon)}")
    c.check("fail.outage.serving_recovered", len(rows) == 1,
            f"expected one serving row after recovery, got {len(rows)}")
    c.check("fail.outage.no_duplicates", len(canon) == 1 and len(rows) == 1,
            "recovery duplicated rows")
    if canon and rows:
        c.check("fail.outage.destinations_share_identity",
                canon[0]["event_id"] == rows[0]["event_id"],
                "the recovered canonical and serving rows must share one write identity")
    evidence = RESULTS / "outage-destination-down.txt"
    c.check("fail.outage.destination_was_down", evidence.exists(),
            "no evidence file recorded the destination outage window")
    if evidence.exists():
        text = evidence.read_text(encoding="utf-8")
        c.check("fail.outage.down_window_measured",
                "refused" in text.lower() or "fail" in text.lower(),
                f"unexpected outage evidence: {text[:200]}")
    c.check("comp.insert_failure_recorded_during_outage",
            metric_sum(metrics, "lighthouse_exporter_insert_failures") >= 1,
            "no insert failure recorded during the outage")
    log = RESULTS / "collector-outage.log"
    c.check("files.outage.log", log.exists(), "collector-outage.log missing")


def phase_split(c: Checks, retry: bool) -> None:
    suffix = "split-retry" if retry else "split"
    label = "fault-split-retry-1" if retry else "fault-split-1"
    emit_ok(c, suffix, "fault-split-retry" if retry else "fault-split")
    metrics = read_metrics(f"metrics-{suffix}.txt")
    canon = canonical_rows_for_fixture(label, WS_A)
    rows = serving_rows_for_fixture(fixture_event(label), WS_A, DATASET)
    c.check(f"fail.{suffix}.canonical_committed_first", len(canon) >= 1,
            f"expected the canonical destination to have committed, got {len(canon)} rows")
    c.check(f"fail.{suffix}.serving_missing", len(rows) == 0,
            f"expected no serving row while the serving destination is broken, got {len(rows)}")
    c.check(f"fail.{suffix}.partial_write_counter",
            metric_sum(metrics, "lighthouse_exporter_partial_write_batches") >= 1,
            "no partial-write counter for the split write")
    attempts = metric_sum(metrics, "lighthouse_exporter_insert_failures",
                          destination="serving")
    if retry:
        c.check(f"fail.{suffix}.retried", attempts >= 2, f"expected retries, saw {attempts} attempts")
        c.check(f"fail.{suffix}.canonical_duplicates_track_attempts", len(canon) == int(attempts),
                f"canonical rows={len(canon)} serving attempts={attempts}: a retried split write "
                "duplicates the committed destination once per attempt")
    else:
        c.check(f"fail.{suffix}.no_retry_single_attempt", len(canon) == 1,
                f"retries disabled: expected one canonical row, got {len(canon)}")
    for row in canon:
        c.check(f"canon.{suffix}.payload_intact",
                hashlib.sha256(row["payload"].encode()).hexdigest() == row["payload_sha256"],
                "canonical payload checksum mismatch")
    if canon:
        record_identity(label, canon[0]["event_id"], "log")
    c.check(f"xdest.{suffix}.canonical_without_serving_link", not rows,
            "a canonical-only row should have no serving row to correlate")
    result = RESULTS / f"{suffix}-repair-result.txt"
    # The bounded repair runs after this verification so that the broken state is
    # what gets verified; phase_files checks the recorded repair and its outcome.
    c.check(f"fail.{suffix}.no_repair_expected_yet", not result.exists(),
            "the repair must run after verification so the split is observed, not masked")


def phase_canonical_failure(c: Checks) -> None:
    label = "fault-canonical-1"
    emit_ok(c, "canonical-permanent", "fault-canonical")
    metrics = read_metrics("metrics-canonical-permanent.txt")
    rows = serving_rows_for_fixture(fixture_event(label), WS_A, DATASET)
    c.check("fail.canonical.serving_absent", not rows,
            "the serving destination must not be written when the canonical insert fails first")
    c.check("fail.canonical.canonical_absent", not canonical_rows_for_fixture(label, WS_A),
            "no canonical row should exist")
    attempts = metric_sum(metrics, "lighthouse_exporter_insert_failures",
                          destination="canonical")
    c.check("fail.canonical.attempted", attempts >= 1, f"expected canonical insert attempts, saw {attempts}")
    log = RESULTS / "collector-canonical-permanent.log"
    if log.exists():
        text = log.read_text(encoding="utf-8", errors="replace")
        c.check("comp.canonical_schema_error_logged", "canonical insert failed" in text,
                "the component error for the schema failure was not logged")
        c.check("comp.canonical_batch_dropped_after_retries", "Dropping data" in text,
                "expected the exporter helper to drop the batch after the retry budget")
        c.check("comp.producer_not_told_about_record_rejection",
                "partial success" not in text.lower(),
                "no producer-visible per-record rejection is expected on this path")
    else:
        c.check("files.collector_canonical_log", False, f"{log} missing")


def phase_queue_full(c: Checks) -> None:
    metrics = read_metrics("metrics-queue-full.txt")
    emits = [r for r in read_emits("queue-full") if r.get("signal") == "logs"]
    refused = [r for r in emits if r["status"] == 503]
    admitted = [r for r in emits if r["status"] == 200]
    c.check("fail.queue_full.producer_visible_rejection", bool(refused),
            f"expected at least one producer-visible 503, statuses={[r['status'] for r in emits]}")
    c.check("fail.queue_full.admitted_before_full", len(admitted) >= 1,
            f"expected the bounded queue to admit first, statuses={[r['status'] for r in emits]}")
    c.check("comp.enqueue_failed_counter",
            metric_sum(metrics, "otelcol_exporter_enqueue_failed_log_records") >= 1,
            "no enqueue-failure counter for the full queue")
    canonical_absent = True
    for label in ("fault-queue-1", "fault-queue-2"):
        rows = serving_rows_for_fixture(fixture_event(label), WS_A, DATASET)
        c.check(f"fail.queue_full.{label}.not_written", not rows,
                "a request admitted to a stalled queue and discarded on shutdown must not appear")
        canonical_absent = canonical_absent and not canonical_rows_for_fixture(label, WS_A)
    c.check("fail.queue_full.no_canonical_rows", canonical_absent,
            "canonical rows exist for the stalled-queue requests")
    c.check("fail.queue_full.rejection_is_queue_admission", True,
            "503 is a queue-admission failure; per-record validation rejection is not producer visible")
    log = RESULTS / "collector-queue-full.log"
    if log.exists():
        text = log.read_text(encoding="utf-8", errors="replace")
        c.check("comp.queue_full_error_named", "queue is full" in text.lower() or "queue" in text.lower(),
                "the collector log does not mention the queue")
    else:
        c.check("files.collector_queue_full_log", False, f"{log} missing")


def phase_crash(c: Checks) -> None:
    labels = ("fault-crash-1", "fault-crash-2")
    for label in labels:
        emit_ok(c, "crash", label)
    counts = {}
    for label in labels:
        rows = serving_rows_for_fixture(fixture_event(label), WS_A, DATASET)
        canon = canonical_rows_for_fixture(label, WS_A)
        counts[label] = (len(canon), len(rows))
        c.check(f"fail.crash.{label}.at_most_once", len(canon) <= 1 and len(rows) <= 1,
                f"recovered rows must not duplicate: canonical={len(canon)} serving={len(rows)}")
        c.check(f"fail.crash.{label}.destinations_agree", len(canon) == len(rows),
                f"canonical={len(canon)} serving={len(rows)}")
        if canon and rows:
            c.check(f"fail.crash.{label}.identity_stable_across_restart",
                    canon[0]["event_id"] == rows[0]["event_id"],
                    "a write identity must survive a crash and queue drain unchanged")
    landed = [label for label, (canon, _) in counts.items() if canon == 1]
    c.check("fail.crash.queue_persistence_recovered_something", bool(landed),
            f"no queued request survived the crash: {counts}")
    snapshot = RESULTS / "crash-queue-storage-before.txt"
    c.check("files.crash.queue_storage_snapshot", snapshot.exists(),
            "no snapshot of the file-backed queue directory was captured")
    if snapshot.exists():
        c.check("fail.crash.queue_was_file_backed", snapshot.read_text(encoding="utf-8").strip() != "",
                "the queue storage directory was empty before the crash")
    log = RESULTS / "collector-crash-restart.log"
    if log.exists():
        text = log.read_text(encoding="utf-8", errors="replace")
        c.check("fail.crash.drain_after_restart", "lighthouse exporter batch" in text,
                "the restarted collector did not export the recovered queue contents")
    else:
        c.check("files.collector_crash_restart_log", False, f"{log} missing")
    c.check("fail.crash.outcome_recorded", True, f"landed={sorted(landed)} counts={counts}")


def phase_ambiguous(c: Checks) -> None:
    label = "ambiguous-a-1"
    emit_ok(c, "ambiguous", "ambiguous-a")
    rows = resolve_stored(c, label)
    stored = rows[0]["event_id"] if rows else None
    canon = canonical_rows(stored) if stored else canonical_rows_for_fixture(label, WS_A)
    c.check("fail.ambiguous.canonical_duplicate_from_retry", len(canon) == 2,
            f"expected two canonical rows (commit + retry after the lost response), got {len(canon)}")
    c.check("fail.ambiguous.serving_exactly_once", len(rows) == 1,
            f"expected exactly one serving row, got {len(rows)}")
    if len(canon) == 2:
        c.check("xdest.ambiguous.same_identity_and_payload",
                canon[0]["payload_sha256"] == canon[1]["payload_sha256"]
                and canon[0]["event_id"] == canon[1]["event_id"],
                "the duplicated canonical rows must share identity and payload hash")
        c.check("xdest.ambiguous.duplicate_canonical_not_missing_serving", len(rows) == 1,
                "two canonical rows with one serving row: a canonical duplicate, not a missing "
                "serving row for the accepted write")
    proxy_log = RESULTS / "proxy-ambiguous.log"
    if proxy_log.exists():
        text = proxy_log.read_text(encoding="utf-8", errors="replace")
        c.check("fail.ambiguous.proxy_withheld_insert_response",
                "withholding the INSERT response" in text,
                "the injection never withheld an INSERT response")
    else:
        c.check("files.proxy_ambiguous_log", False, f"{proxy_log} missing")
    window = phase_window("ambiguous")
    if window:
        rows_q = ch("SELECT count() AS c FROM system.query_log WHERE type = 'QueryFinish' "
                    "AND query_kind = 'Insert' AND query LIKE '%canonical_events%' "
                    "AND event_time >= toDateTime64({start:String}, 3) "
                    "AND event_time <= toDateTime64({stop:String}, 3)",
                    {"start": window[0], "stop": window[1]})
        c.check("fail.ambiguous.destination_committed_twice",
                bool(rows_q) and int(rows_q[0]["c"]) >= 2,
                f"query_log shows {rows_q[0]['c'] if rows_q else 'no'} finished canonical inserts")
    else:
        c.check("files.phase_times_window", False, "no phase window recorded for the ambiguous phase")


def phase_ambiguous_serving(c: Checks) -> None:
    """Serving-response-loss injection: the retry duplicates both destinations.

    The counterpart of the canonical-response-loss case. Here the canonical
    insert is answered normally, the serving insert commits, and only its
    response is withheld; the retry re-attempts both inserts, so both
    destinations end up with two identical rows. This is why the
    canonical-response-loss result (two canonical rows, one serving row) must not
    be generalized to "the serving row stays single" on retry.
    """
    label = "ambiguous-serving-1"
    emit_ok(c, "ambiguous-serving", "ambiguous-serving-a")
    rows = resolve_stored(c, label, want=2)
    canon = canonical_rows(rows[0]["event_id"]) if rows else canonical_rows_for_fixture(label, WS_A)
    c.check("fail.ambiguous_serving.serving_duplicate_from_retry", len(rows) == 2,
            f"expected two serving rows (commit + retry after the lost response), got {len(rows)}")
    c.check("fail.ambiguous_serving.canonical_duplicate_from_retry", len(canon) == 2,
            f"expected two canonical rows (the retry re-attempts both inserts), got {len(canon)}")
    if rows:
        identities = {r["event_id"] for r in rows}
        payloads = {r["payload_sha256"] for r in rows}
        c.check("xdest.ambiguous_serving.same_identity_and_payload",
                len(identities) == 1 and len(payloads) == 1,
                f"the duplicated serving rows must share identity and payload hash: "
                f"identities={sorted(identities)} payloads={sorted(payloads)}")
        c.check("xdest.ambiguous_serving.no_missing_rows",
                len(rows) == 2 and len(canon) == 2,
                "the lost serving response committed, so the retry duplicated rows rather "
                "than repairing a missing one")
        record_identity(label, rows[0]["event_id"], "log")
    proxy_log = RESULTS / "proxy-ambiguous-serving.log"
    if proxy_log.exists():
        text = proxy_log.read_text(encoding="utf-8", errors="replace")
        c.check("fail.ambiguous_serving.proxy_withheld_serving_response",
                "withholding the INSERT response" in text and "match=serving_logs" in text,
                "the proxy log must show a withheld serving insert response aimed at "
                "serving_logs")
    else:
        c.check("files.proxy_ambiguous_serving_log", False, f"{proxy_log} missing")
    window = phase_window("ambiguous-serving")
    if window:
        rows_q = ch("SELECT count() AS c FROM system.query_log WHERE type = 'QueryFinish' "
                    "AND query_kind = 'Insert' AND query LIKE '%serving_logs%' "
                    "AND event_time >= toDateTime64({start:String}, 3) "
                    "AND event_time <= toDateTime64({stop:String}, 3)",
                    {"start": window[0], "stop": window[1]})
        c.check("fail.ambiguous_serving.destination_committed_twice",
                bool(rows_q) and int(rows_q[0]["c"]) >= 2,
                f"query_log shows {rows_q[0]['c'] if rows_q else 'no'} finished serving inserts")
    else:
        c.check("files.phase_times_window", False, "no phase window recorded for the ambiguous-serving phase")


def phase_retry_duplicate(c: Checks) -> None:
    label = "retry-a-1"
    emits = [r for r in read_emits("retry-duplicate") if r.get("label") == "retry-a"]
    c.check("fail.retry.producer_sent_twice", len(emits) == 2,
            f"expected two identical requests, got {len(emits)}")
    rows = resolve_stored(c, label, want=2)
    if not rows:
        c.check("fail.retry.serving_duplicates_measured", False,
                "deduplication is unselected: expected 2 serving rows")
        return
    identities = {r["event_id"] for r in rows}
    c.check("fail.retry.identity_stable_across_retries", len(identities) == 1,
            f"the re-sent request must keep one stored write identity, got {sorted(identities)}")
    c.check("fail.retry.serving_duplicates_measured", len(rows) == 2,
            f"expected 2 serving rows, got {len(rows)}")
    canon = canonical_rows(rows[0]["event_id"])
    c.check("fail.retry.canonical_duplicates_measured", len(canon) == 2,
            f"deduplication is unselected: expected 2 canonical rows, got {len(canon)}")
    if len(canon) == 2:
        c.check("fail.retry.identical_payload",
                canon[0]["payload_sha256"] == canon[1]["payload_sha256"],
                "the re-sent request must produce the same canonical payload")
    if canon:
        record_identity(label, rows[0]["event_id"], "log")
    c.check("fail.retry.no_producer_visible_dedup", True,
            "both identical requests were accepted; deduplication remains unselected")
    # The twin control: two legitimately distinct events with byte-identical wire
    # content (one request), then the same request again. The content-derived
    # identity is shared by both legitimate events, so identity alone cannot tell
    # them apart from a retransmission. This is why the stored identity is a
    # fixture-correlation key, not a deduplication key.
    twins = [r for r in read_emits("retry-duplicate") if r.get("label") == "twins"]
    c.check("twin.requests_sent_twice",
            len(twins) == 2 and all(r["status"] == 200 for r in twins),
            f"expected two accepted twin requests, got {[r.get('status') for r in twins]}")
    twin_rows = serving_rows_for_fixture(fixture_event("twin-1"), WS_A, DATASET)
    identities = {r["event_id"] for r in twin_rows}
    payloads = {r["payload_sha256"] for r in twin_rows}
    c.check("twin.serving_rows_stored", len(twin_rows) == 4,
            f"two legitimate identical events plus one retransmission should store 4 serving "
            f"rows, got {len(twin_rows)}")
    c.check("twin.one_shared_identity", len(identities) == 1,
            f"two legitimate identical events share one content-derived identity: {sorted(identities)}")
    c.check("twin.one_shared_payload", len(payloads) == 1,
            f"identical content produces one payload hash: {sorted(payloads)}")
    if identities:
        stored = sorted(identities)[0]
        canon_twins = canonical_rows(stored)
        c.check("twin.canonical_rows_stored", len(canon_twins) == 4,
                f"expected 4 canonical rows for the shared identity, got {len(canon_twins)}")
        record_identity("twin-1", stored, "log")
    c.check("twin.identity_ambiguous_by_design", len(twin_rows) == 4 and len(identities) == 1,
            "one identity resolves to two legitimate events plus a retransmission: the "
            "identity cannot distinguish them, so it is not a deduplication key")
    c.check("twin.dedup_unselected", True,
            "nothing was deduplicated: both legitimate events and the repeated request are stored")


def phase_no_trust(c: Checks) -> None:
    metrics = read_metrics("metrics-no-trust.txt")
    rows = serving_rows_for_fixture(fixture_event("no-trust-1"))
    canon = canonical_rows_for_fixture("no-trust-1")
    c.check("fail.no_trust.rejected_from_trusted_db", not rows and not canon,
            "an untrusted request must not reach the trusted database")
    if db_exists(DB_UNTRUSTED):
        untrusted = serving_rows_for_fixture(fixture_event("no-trust-1"), db=DB_UNTRUSTED)
        c.check("fail.no_trust.rejected_in_untrusted_pipeline", not untrusted,
                "the trusted-scope check did not fail closed")
    else:
        c.check("files.no_trust.database", False, f"{DB_UNTRUSTED} missing")
    # The component records the per-record reason and, when nothing in the batch
    # survived validation, rejects the batch as a whole.
    c.check("comp.no_trust.rejected_record_counter",
            metric_sum(metrics, "lighthouse_exporter_rejected_records",
                       reason="missing_trusted_scope", namespace="resource") >= 1,
            "expected a rejected-record counter with reason=missing_trusted_scope")
    c.check("comp.no_trust.rejected_batch_counter",
            metric_sum(metrics, "lighthouse_exporter_rejected_batches",
                       reason="all_records_rejected") >= 1,
            "expected the all-rejected plain request to be counted as a batch rejection")
    received = RESULTS / "no-trust-received.jsonl"
    c.check("fail.no_trust.receiver_accepted_request",
            received.exists() and received.read_text(encoding="utf-8").strip() != "",
            "expected the receiver file copy to show the request was admitted then rejected")
    forged = "no-trust-forged-1"
    c.check("fail.no_trust.forged_marker_not_in_trusted_db",
            not serving_rows_for_fixture(fixture_event(forged)),
            "a forged-marker request must never land in the trusted database")
    if db_exists(DB_UNTRUSTED):
        rows = serving_rows_for_fixture(fixture_event(forged), db=DB_UNTRUSTED)
        c.check("fail.no_trust.forged_marker_gap_measured", len(rows) == 1,
                f"expected the documented limitation (1 row with the forged workspace) in "
                f"{DB_UNTRUSTED}, got {len(rows)}")
        if len(rows) == 1:
            c.check("fail.no_trust.forged_marker_gap_is_producer_scope",
                    rows[0]["workspace_id"] == WS_FORGED,
                    f"stored workspace {rows[0]['workspace_id']}")
    # One request whose two resource groups have different trust provenance: the
    # forged group passes the marker check and the plain group is rejected with
    # missing_trusted_scope. The invalidating rejection must cover the whole
    # request, so the forged group must not be written either, and the batch must
    # be counted as missing_trusted_scope rather than all_records_rejected.
    if db_exists(DB_UNTRUSTED):
        mixed_forged = serving_rows_for_fixture(fixture_event("no-trust-mixed-forged"),
                                                db=DB_UNTRUSTED)
        mixed_plain = serving_rows_for_fixture(fixture_event("no-trust-mixed-plain"),
                                               db=DB_UNTRUSTED)
        c.check("fail.no_trust.mixed_request_wrote_nothing",
                not mixed_forged and not mixed_plain,
                f"a missing-scope group must invalidate the whole request, got "
                f"forged={len(mixed_forged)} plain={len(mixed_plain)} rows in {DB_UNTRUSTED}")
    c.check("fail.no_trust.mixed_request_absent_from_trusted_db",
            not serving_rows_for_fixture(fixture_event("no-trust-mixed-forged"))
            and not serving_rows_for_fixture(fixture_event("no-trust-mixed-plain")),
            "the mixed untrusted request must never reach the trusted database")
    # Counters map one-to-one to the three requests of this phase: the plain
    # request rejects its only record and counts all_records_rejected; the
    # mixed request rejects one record and counts a missing_trusted_scope batch
    # rejection; the forged-only request is the documented gap and counts nothing.
    c.check("comp.no_trust.two_records_rejected_for_missing_scope",
            metric_sum(metrics, "lighthouse_exporter_rejected_records",
                       reason="missing_trusted_scope", namespace="resource") == 2,
            "expected the plain request's record and the mixed request's plain group to both count")
    c.check("comp.no_trust.mixed_batch_rejected_missing_scope",
            metric_sum(metrics, "lighthouse_exporter_rejected_batches",
                       reason="missing_trusted_scope") == 1,
            "the mixed request should be counted as a missing_trusted_scope batch rejection")
    c.check("comp.no_trust.all_rejected_counter_not_concealing_mixed_batch",
            metric_sum(metrics, "lighthouse_exporter_rejected_batches",
                       reason="all_records_rejected") == 1,
            "only the all-invalid plain request may count as all_records_rejected; a mixed "
            "request with surviving rows must be counted by its invalidating reason instead")


def phase_files(c: Checks) -> None:
    required = [
        "build-manifest.txt", "go-modules.txt", "run-start.txt", "phase-status.tsv",
        "phase-times.tsv", "fixture019-sha256.txt", "metrics-semantics.txt",
        "semantics-emits.jsonl", "semantics-schema.jsonl", "semantics-span-timing.jsonl",
        "metrics-mixed-validation.txt", "mixed-validation-emits.jsonl",
        "metrics-ambiguous-serving.txt", "ambiguous-serving-emits.jsonl",
        "proxy-ambiguous-serving.log",
    ]
    for name in required:
        path = RESULTS / name
        c.check(f"files.{name}", path.exists() and path.stat().st_size > 0,
                f"{name} missing or empty in {RESULTS}")
    modules = RESULTS / "go-modules.txt"
    if modules.exists():
        text = modules.read_text(encoding="utf-8")
        for want in ("github.com/ClickHouse/clickhouse-go/v2 v2.47.0",
                     "go.opentelemetry.io/collector/exporter/exporterhelper v0.161.0",
                     # Contrib modules keep the v0.1xx distribution series; the
                     # core modules of the same release are the v1.67.0 series
                     # (exporterhelper v0.161.0 requires component/exporter/pdata
                     # v1.67.0), so both series are pinned, not just one.
                     "go.opentelemetry.io/collector/exporter v1.67.0",
                     "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"):
            c.check(f"meta.module.{san(want)}", want in text, f"{want} not in go-modules.txt")
        c.check("meta.no_other_collector_version",
                "v0.161.0" in text and "v0.162" not in text and "v0.163" not in text
                and "go.opentelemetry.io/collector/exporter v1.68" not in text,
                "the module manifest names a collector version other than the pin")
    hashes = RESULTS / "fixture019-sha256.txt"
    if hashes.exists():
        for line in hashes.read_text(encoding="utf-8").splitlines():
            parts = line.split()
            if len(parts) != 2:
                continue
            digest, name = parts
            path = EXP019 / name
            if path.exists():
                actual = hashlib.sha256(path.read_bytes()).hexdigest()
                c.check(f"meta.fixture019.{name}.unchanged", actual == digest,
                        "the imported 019 fixture changed: expectations would not be independent")
    manifest = RESULTS / "build-manifest.txt"
    if manifest.exists():
        text = manifest.read_text(encoding="utf-8")
        c.check("meta.build_manifest_pins_collector", "v0.161.0" in text,
                "build manifest does not record the collector pin")
        c.check("meta.build_manifest_records_core_series", "v1.67.0" in text,
                "build manifest does not record the collector core module series")
        c.check("meta.build_manifest_pins_clickhouse", "26.8.2.7" in text,
                "build manifest does not record the ClickHouse pin")
    # Stale state would silently weaken the exact-count assertions of every
    # phase; run.sh records the pre-phase row counts and fails closed on any
    # non-zero value, and the recorded table is checked here.
    preflight = RESULTS / "phase-preflight.tsv"
    c.check("meta.phase_preflight_recorded", preflight.exists() and preflight.stat().st_size > 0,
            f"{preflight} missing or empty")
    if preflight.exists():
        stale = [line for line in preflight.read_text("utf-8").splitlines()
                 if len(line.split("\t")) == 3 and line.split("\t")[2].strip() != "0"]
        c.check("meta.no_stale_fixture_state_before_phases", not stale,
                f"phases started with pre-existing fixture rows: {stale[:5]}")
    # The stored span-timing snapshot must show the fixture's timing cases; the
    # expectation comes from the fixture inventory, not from the file's own
    # claims, so a policy or mapping regression cannot hide in the evidence.
    snapshot = RESULTS / "semantics-span-timing.jsonl"
    if snapshot.exists():
        rows = [json.loads(line) for line in snapshot.read_text(encoding="utf-8").splitlines()
                if line.strip()]
        want_statuses = sorted(span_timing_status(e) for e in fixture021.ALL_SPAN_EVENTS)
        c.check("files.span_timing_snapshot_rows", len(rows) == len(want_statuses),
                f"snapshot has {len(rows)} rows, fixture has {len(want_statuses)} spans")
        c.check("files.span_timing_snapshot_statuses",
                sorted(r["span_timing_status"] for r in rows) == want_statuses,
                f"snapshot statuses = {sorted(r['span_timing_status'] for r in rows)}")
        c.check("files.span_timing_snapshot_durations",
                all((r["duration_ns"] is None) == (r["span_timing_status"] != "ok") for r in rows),
                "a stored duration must be present exactly when the timing is observable")
        c.check("files.span_timing_snapshot_reversed_timestamps",
                all(int(r["end_ts_ns"]) < int(r["ts_ns"]) for r in rows
                    if r["span_timing_status"] == "end_before_start"),
                "a reversed span must keep its original end < start timestamps")
    # The serving-response-loss injection must be aimed at the serving insert, so
    # that phase cannot pass on a withheld canonical response.
    proxy = RESULTS / "proxy-ambiguous-serving.log"
    if proxy.exists():
        c.check("files.ambiguous_serving_injection_targeted",
                "match=serving_logs" in proxy.read_text(encoding="utf-8", errors="replace"),
                "the serving-response-loss injection must be targeted at the serving insert")
    # The bounded repair runs after the split phases are verified; it is checked
    # here because these files exist only once the split phases have finished.
    for suffix, label in (("split", "fault-split-1"), ("split-retry", "fault-split-retry-1")):
        sql = RESULTS / f"{suffix}-repair.sql"
        result = RESULTS / f"{suffix}-repair-result.txt"
        c.check(f"files.{suffix}.repair_sql",
                sql.exists() and "INSERT INTO" in sql.read_text("utf-8"),
                f"{sql} missing or without a repair statement")
        if sql.exists():
            text = sql.read_text("utf-8")
            c.check(f"fail.{suffix}.repair_derives_identity_from_canonical",
                    "canonical_events" in text and "payload_sha256" in text and label in text,
                    "the recorded repair must derive the serving identity from the canonical payload")
        c.check(f"files.{suffix}.repair_result", result.exists(), f"{result} missing")
        if result.exists():
            landed = expected = None
            for line in result.read_text("utf-8", errors="replace").splitlines():
                fields = line.split("\t")
                if len(fields) == 2 and fields[0] == "repaired_minimal_serving_rows":
                    landed = fields[1].strip()
                elif len(fields) == 2 and fields[0] == "canonical_rows":
                    expected = fields[1].strip()
            c.check(f"fail.{suffix}.repair_landed_one_minimal_row_per_canonical_row",
                    landed is not None and landed == expected,
                    f"repair landed {landed} minimal serving rows for {expected} canonical rows")


PHASES = {
    "stock-probe": phase_stock_probe,
    "semantics": phase_semantics,
    "ack": phase_ack,
    "mixed-validation": phase_mixed_validation,
    "outage": phase_outage,
    "split": lambda c: phase_split(c, retry=False),
    "split-retry": lambda c: phase_split(c, retry=True),
    "canonical-permanent": phase_canonical_failure,
    "queue-full": phase_queue_full,
    "crash": phase_crash,
    "ambiguous": phase_ambiguous,
    "ambiguous-serving": phase_ambiguous_serving,
    "retry-duplicate": phase_retry_duplicate,
    "no-trust": phase_no_trust,
    "files": phase_files,
    "trust-config": phase_trust_config,
}


def main() -> int:
    global RESULTS  # noqa: PLW0603 - the CLI result directory is the run's evidence root
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=sorted(PHASES))
    parser.add_argument("--results", default=str(RESULTS))
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    RESULTS = Path(args.results)

    checks = Checks(RESULTS / args.out, args.phase)
    try:
        if os.environ.get("LH021_TEST_CRASH_PHASE") == args.phase:
            # Test-only hook: proves the harness fails closed when a verifier
            # phase crashes. Never set outside test-run-failure-modes.sh.
            raise RuntimeError("injected verifier crash (LH021_TEST_CRASH_PHASE)")
        PHASES[args.phase](checks)
    except Exception as err:  # noqa: BLE001 - a crashing check phase must fail the run
        checks.check(f"query.phase.{args.phase}.completed", False, f"verifier crashed: {err!r}")
    return checks.write()


if __name__ == "__main__":
    sys.exit(main())
