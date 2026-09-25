#!/usr/bin/env python3
"""Typed value model for Experiment 019.

One place that converts the canonical typed inventory (`fixture.EVENTS`) into:

* the versioned post-policy canonical payload (explicit type tags, lossless),
* the stock serving strings (pcommon.Value.AsString semantics, so the serving
  Map values match what the pinned stock exporter writes),
* companion type Maps and tagged-value Maps for the two RFD-required encodings.

Expected query results are computed from the typed inventory here, not from
ClickHouse output.
"""

from __future__ import annotations

import base64
import json
import math

CANONICAL_SCHEMA = "lighthouse.canonical.event/1"
TENANT = "11111111-1111-4111-8111-111111111111"
DATASET = "fixture"
PAYLOAD_KEY = "lighthouse.payload"
TYPES_KEY = "lighthouse.types"

TAG_NAMES = {
    "string": "s",
    "int": "i",
    "double": "d",
    "bool": "b",
    "empty": "e",
    "bytes": "y",
    "array": "a",
    "map": "m",
}
TAG_TYPES = {v: k for k, v in TAG_NAMES.items()}


# ---------------------------------------------------------------------------
# Value conversions
# ---------------------------------------------------------------------------

def float_es6(f: float) -> str:
    """The pinned pcommon float64AsString conversion (ES6 number to string)."""
    if math.isnan(f):
        return "NaN"
    if math.isinf(f):
        return "Infinity" if f > 0 else "-Infinity"
    if f == 0:
        return "-0" if math.copysign(1.0, f) < 0 else "0"
    abs_f = abs(f)
    if abs_f < 1e-6 or abs_f >= 1e21:
        text = repr(f)
        mantissa, _, exponent = text.partition("e")
        sign = "-" if exponent.startswith("-") else ""
        digits = exponent.lstrip("+-").lstrip("0") or "0"
        return f"{mantissa}e{sign}{digits}"
    text = repr(f)
    if text.endswith(".0"):
        text = text[:-2]
    return text


def nested_json(type_name, value):
    """Go json.Marshal-compatible structures for arrays and maps."""
    if type_name == "array":
        return [nested_json("string" if isinstance(v, str) else
                            "bool" if isinstance(v, bool) else
                            "int" if isinstance(v, int) else
                            "double" if isinstance(v, float) else
                            "empty" if v is None else "map", v)
                for v in value]
    if type_name == "map":
        return {k: nested_json("string" if isinstance(v, str) else
                               "bool" if isinstance(v, bool) else
                               "int" if isinstance(v, int) else
                               "double" if isinstance(v, float) else
                               "empty" if v is None else "map", v)
                for k, v in value.items()}
    if type_name == "bool":
        return bool(value)
    if type_name in ("int", "double"):
        return value
    if type_name == "empty":
        return None
    return value


def json_compact(value) -> str:
    """Compact JSON with sorted keys, matching Go's json.Marshal ordering."""
    return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def stock_string(type_name, value) -> str:
    """pcommon.Value.AsString(): what the stock exporter stores in a Map."""
    if type_name == "empty":
        return ""
    if type_name == "string":
        return value
    if type_name == "bool":
        return "true" if value else "false"
    if type_name == "int":
        return str(value)
    if type_name == "double":
        return float_es6(value)
    if type_name == "bytes":
        return base64.b64encode(value).decode()
    if type_name in ("array", "map"):
        return json_compact(nested_json(type_name, value))
    raise ValueError(f"unknown type {type_name}")


def canonical_node(type_name, value) -> dict:
    """Explicitly typed canonical value; lossless within the fixture domain."""
    if type_name == "empty":
        return {"t": "empty"}
    if type_name == "string":
        return {"t": "string", "v": value}
    if type_name == "bool":
        return {"t": "bool", "v": bool(value)}
    if type_name == "int":
        return {"t": "int", "v": str(value)}
    if type_name == "double":
        if math.isnan(value):
            text = "nan"
        elif math.isinf(value):
            text = "inf" if value > 0 else "-inf"
        elif value == 0 and math.copysign(1.0, value) < 0:
            text = "-0.0"
        else:
            text = repr(value)
        return {"t": "double", "v": text}
    if type_name == "bytes":
        return {"t": "bytes", "v": base64.b64encode(value).decode()}
    if type_name == "array":
        return {"t": "array", "v": [canonical_node(_python_type(v), v) for v in value]}
    if type_name == "map":
        return {"t": "map", "v": {k: canonical_node(_python_type(v), v) for k, v in value.items()}}
    raise ValueError(f"unknown type {type_name}")


def _python_type(value) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, bytes):
        return "bytes"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (list, tuple)):
        return "array"
    if isinstance(value, dict):
        return "map"
    if value is None:
        return "empty"
    raise TypeError(f"unsupported value {value!r}")


def canonical_payload(event) -> str:
    """Versioned post-policy canonical payload for one fixture event."""
    def ns(pairs):
        return {key: canonical_node(type_name, value) for key, type_name, value in pairs}

    doc = {
        "schema": CANONICAL_SCHEMA,
        "kind": event["kind"],
        "identity": {"tenant_id": TENANT, "dataset": DATASET, "event_id": event["event_id"]},
        "time": {
            "timestamp_ns": str(event["time_ns"]),
            "observed_ns": str(event["observed_ns"] or (event["time_ns"] + 1_000_000)),
        },
        "body": canonical_node(event["body"][0], event["body"][1]),
        "resource": ns(event["resource"]),
        "scope": ns(event["scope"]),
        "record": ns(event["record"]),
    }
    if event["duplicate"]:
        doc["record_duplicates"] = [key for key, _, _ in event["duplicate"]]
    if event["trace_id"]:
        doc["trace"] = {
            "trace_id": event["trace_id"].hex(),
            "span_id": event["span_id"].hex() if event["span_id"] else "",
        }
    if event["kind"] == "span":
        doc["span"] = {
            "name": event["span_name"],
            "start_ns": str(event["start_ns"]),
            "end_ns": str(event["end_ns"]),
        }
    return json_compact(doc)


# ---------------------------------------------------------------------------
# Serving representations
# ---------------------------------------------------------------------------

def namespace_maps(event):
    """(values, types) per namespace for the companion-type representation."""
    out = {}
    for name in ("resource", "scope", "record"):
        pairs = event[name] + (event["duplicate"] or [] if name == "record" else [])
        values = {}
        types = {}
        for key, type_name, value in pairs:
            values[key] = stock_string(type_name, value)
            types[key] = type_name
        out[name] = (values, types)
    return out


def tagged_namespace(event, name):
    pairs = event[name] + (event["duplicate"] or [] if name == "record" else [])
    tagged = {}
    for key, type_name, value in pairs:
        tag = TAG_NAMES[type_name]
        if type_name == "empty":
            tagged[key] = f"{tag}:"
        else:
            tagged[key] = f"{tag}:{stock_string(type_name, value)}"
    return tagged


def decode_tagged(text: str) -> tuple[str, str]:
    """Decode a tagged value; the query prototype uses the same rule in SQL."""
    tag, _, rest = text.partition(":")
    return TAG_TYPES.get(tag, "?"), rest


# ---------------------------------------------------------------------------
# Independent expected results (computed from the typed inventory)
# ---------------------------------------------------------------------------

def _metric_rows(events):
    rows = []
    for event in events:
        for key, type_name, value in event["record"]:
            if key == "metric.value":
                rows.append((event["event_id"], type_name, value))
    return rows


def expected_type_counts(events) -> dict:
    """{type: count} for the metric.value key across events."""
    counts = {}
    for _, type_name, _ in _metric_rows(events):
        counts[type_name] = counts.get(type_name, 0) + 1
    return counts


def expected_int_order(events) -> list:
    """Numeric ordering of the int-valued metric.value rows (by value)."""
    rows = [(value, event_id) for event_id, type_name, value in _metric_rows(events)
            if type_name == "int"]
    return [event_id for _, event_id in sorted(rows)]


def expected_int_values(events) -> list:
    return [value for _, type_name, value in _metric_rows(events) if type_name == "int"]


def expected_lexical_order(events) -> list:
    """Lexical ordering of the same int values, to contrast with numeric."""
    rows = [(stock_string("int", value), event_id) for event_id, type_name, value in _metric_rows(events)
            if type_name == "int"]
    return [event_id for _, event_id in sorted(rows)]


def expected_sum(event_ids, events) -> int:
    total = 0
    for event in events:
        if event["event_id"] in event_ids:
            for key, type_name, value in event["record"]:
                if key == "metric.value" and type_name == "int":
                    total += value
    return total
