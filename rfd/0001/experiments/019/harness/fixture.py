#!/usr/bin/env python3
"""Deterministic typed fixture for Experiment 019 (typed canonical storage).

The fixture is the same value inventory Experiment 018 used for the typed
Collector mapping (integers above 2^53, int/double/string `1`, non-finite
doubles, bytes, arrays, maps, explicit empty, empty string, missing keys,
duplicate keys, case variants, namespace collisions) plus the extra rows this
experiment needs to compare storage representations: one row per type for the
same attribute key, numeric-ordering rows, and aggregate/overflow rows.

Everything is fixed: fixed timestamps, fixed values, and a canonical typed
inventory (`EVENTS`) from which both the OTLP request bytes and the expected
query results are derived independently of any storage implementation.

Commands:
  emit SIGNAL LABEL     build a named OTLP request and POST it to --url
  write SIGNAL LABEL    write the OTLP request bytes to --out
  inventory             print EVENTS as JSON (typed canonical model)
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.logs.v1 import logs_pb2
from opentelemetry.proto.resource.v1 import resource_pb2
from opentelemetry.proto.trace.v1 import trace_pb2

# Synthetic, non-secret credentials for the local fixture only.
KEY_A = "lh019-ws-a-key"
WS_A = "11111111-1111-4111-8111-111111111111"
WS_FORGED = "99999999-9999-4999-8999-999999999999"

# Fixed event time so every run compares against the same stored values.
# 2026-09-01T07:00:00.123456789Z
_EPOCH = datetime(2026, 9, 1, 7, 0, 0, tzinfo=timezone.utc)
TS = int(_EPOCH.timestamp()) * 1_000_000_000 + 123_456_789
TS2 = TS + 86_400_000_000_000  # +1 day, for the observed-timestamp fallback case

TRACE_ID = bytes.fromhex("4bf92f3577b34da6a3ce929d0e0e4736")
SPAN_ID = bytes.fromhex("00f067aa0ba902b7")
PARENT_SPAN_ID = bytes.fromhex("00f067aa0ba90200")

# 2^53 + 1 is exactly representable as int64 but not as float64, so any lossy
# path is visible in the stored value.
INT_ABOVE_2_53 = 9_007_199_254_740_993
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
BYTES_VALUE = b"\x00\x01\xfe\xff"
UNICODE = "héllo\u2014\U0001f30d"
LONG_STRING = "x" * 1024
NAN = float("nan")
POSINF = float("inf")
NEGINF = float("-inf")

# OTLP explicit empty: an AnyValue with no oneof member set.
EMPTY = "EMPTY"


class _Empty:
    def __repr__(self):  # pragma: no cover - debug aid only
        return "EMPTY"


_EMPTY = _Empty()


def _value(type_name, value):
    """Canonical typed value: (type, python value). `empty` has value None."""
    return {"type": type_name, "value": value}


def any_value(value):
    """Build an AnyValue that preserves the Python type, or an explicit empty."""
    v = common_pb2.AnyValue()
    if value is _EMPTY:
        return v
    if isinstance(value, bool):
        v.bool_value = value
    elif isinstance(value, int):
        v.int_value = value
    elif isinstance(value, float):
        v.double_value = value
    elif isinstance(value, bytes):
        v.bytes_value = value
    elif isinstance(value, str):
        v.string_value = value
    elif isinstance(value, (list, tuple)):
        for item in value:
            v.array_value.values.append(any_value(item))
    elif isinstance(value, dict):
        for key, item in value.items():
            kv = v.kvlist_value.values.add()
            kv.key = key
            kv.value.CopyFrom(any_value(item))
    else:
        raise TypeError(f"unsupported fixture value {value!r}")
    return v


def attrs_pairs(pairs):
    """Build KeyValue pairs from (key, type, value) triples, preserving order."""
    out = []
    for key, type_name, value in pairs:
        kv = common_pb2.KeyValue()
        kv.key = key
        kv.value.CopyFrom(any_value(_EMPTY if type_name == "empty" else value))
        out.append(kv)
    return out


# ---------------------------------------------------------------------------
# Canonical typed inventory
# ---------------------------------------------------------------------------

FULL_RECORD = [
    ("int.big", "int", INT_ABOVE_2_53),
    ("int.neg", "int", -7),
    ("int.min", "int", INT64_MIN),
    ("int.one", "int", 1),
    ("double.one", "double", 1.0),
    ("double.half", "double", 1.5),
    ("double.nan", "double", NAN),
    ("double.posinf", "double", POSINF),
    ("double.neginf", "double", NEGINF),
    ("zero.float", "double", -0.0),
    ("bool.true", "bool", True),
    ("bool.false", "bool", False),
    ("str.one", "string", "1"),
    ("str.empty", "string", ""),
    ("str.json_like", "string", '{"nested.bool":false,"nested.int":2}'),
    ("zero.int", "int", 0),
    ("bytes.raw", "bytes", BYTES_VALUE),
    ("array.mixed", "array", ["a", 1, True]),
    ("map.nested", "map", {"nested.int": 2, "nested.bool": False}),
    ("empty.value", "empty", None),
    ("collide", "string", "record-value"),
    ("Foo", "string", "record-upper"),
    ("unicode", "string", UNICODE),
    ("long.string", "string", LONG_STRING),
]

FULL_RESOURCE = [
    ("service.name", "string", "fixture-log-service"),
    ("service.version", "string", "1.0.0"),
    ("res.int64", "int", INT_ABOVE_2_53),
    ("res.bool", "bool", True),
    ("res.float", "double", 1.0),
    ("res.str_one", "string", "1"),
    ("res.bytes", "bytes", BYTES_VALUE),
    ("res.array", "array", ["a", 1, True]),
    ("res.map", "map", {"nested.int": 2, "nested.bool": False}),
    ("res.empty", "empty", None),
    ("res.empty_string", "string", ""),
    ("res.collide", "string", "resource-value"),
    ("Foo", "string", "resource-upper"),
    ("foo", "string", "resource-lower"),
]

FULL_SCOPE = [
    ("scope.attr", "string", "scope-value"),
    ("scope.int", "int", 7),
    ("scope.empty", "empty", None),
]

# Span scope attributes on a second instrumentation scope group. The stock
# traces table has no ScopeAttributes column in either mode, so this is the
# namespace-collision case the stock path cannot store.
SPAN_SCOPE_ATTRS = [("scope.span.attr", "string", "s"), ("scope.span.int", "int", 11)]


def _event(event_id, record, *, body=None, resource=None, scope=None,
           kind="log", trace_id=None, span_id=None, time_ns=TS,
           observed_ns=None, severity=("INFO", 9), duplicate=None, event_name=None,
           span_name=None, start_ns=None, end_ns=None):
    return {
        "event_id": event_id,
        "kind": kind,
        "time_ns": time_ns,
        "observed_ns": observed_ns,
        "body": body if body is not None else ("string", event_id),
        "resource": resource if resource is not None else FULL_RESOURCE,
        "scope": scope if scope is not None else FULL_SCOPE,
        "record": record,
        "severity": severity,
        "trace_id": trace_id,
        "span_id": span_id,
        "duplicate": duplicate,
        "event_name": event_name,
        "span_name": span_name,
        "start_ns": start_ns,
        "end_ns": end_ns,
    }


def _value_row(event_id, type_name, value):
    return _event(event_id, [("metric.value", type_name, value)])


EVENTS = [
    # The full typed inventory in one record: namespace separation, collisions,
    # bytes/arrays/maps, explicit empty, empty string, case, unicode.
    _event("typed-1", FULL_RECORD, body=("string", "typed-log-1"),
           trace_id=TRACE_ID, span_id=SPAN_ID),
    # Duplicate keys within one incoming attribute collection.
    _event("dup-1", [("dup.base", "string", "base")],
           duplicate=[("dup", "string", "first"), ("dup", "string", "second")]),
    # One row per type for the same key: type-sensitive filter/group cases.
    _value_row("group-int-1", "int", 1),
    _value_row("group-double-1", "double", 1.0),
    _value_row("group-string-1", "string", "1"),
    _value_row("group-bool-false", "bool", False),
    _value_row("group-int-zero", "int", 0),
    _value_row("group-empty", "empty", None),
    _value_row("group-empty-string", "string", ""),
    _event("group-missing", []),
    # Numeric ordering versus lexical ordering.
    _value_row("order-9", "int", 9),
    _value_row("order-10", "int", 10),
    _value_row("order-100", "int", 100),
    # Large signed integers.
    _value_row("big-int", "int", INT_ABOVE_2_53),
    _value_row("min-int", "int", INT64_MIN),
    # Non-finite doubles and negative zero.
    _value_row("nan", "double", NAN),
    _value_row("posinf", "double", POSINF),
    _value_row("neginf", "double", NEGINF),
    _value_row("negzero", "double", -0.0),
    # Aggregate inputs: exact sum, overflow, and an incompatible non-empty value.
    _value_row("sum-a", "int", INT64_MAX),
    _value_row("sum-b", "int", INT64_MAX),
    _value_row("sum-c", "int", 1),
    _value_row("sum-small-a", "int", 1),
    _value_row("sum-small-b", "int", 2),
    _value_row("sum-string", "string", "3"),
    # Unsupported query operations: bytes and nested collections.
    _value_row("bytes-row", "bytes", BYTES_VALUE),
    _value_row("nested-row", "map", {"nested.int": 1}),
    _event("no-service-name", [("no.service", "string", "yes")],
           resource=[("res.group", "string", "no-service")], scope=FULL_SCOPE),
]

# Spans: one span with typed attributes, one with span scope attributes on a
# separate scope group, one with a malformed (negative) duration.
SPAN_EVENTS = [
    _event("span-1", FULL_RECORD, kind="span", span_name="span/typed",
           start_ns=TS, end_ns=TS + 1500, trace_id=TRACE_ID, span_id=SPAN_ID),
    _event("span-scope", [("span.scope.attr", "string", "present")], kind="span",
           span_name="span/scope-attrs", start_ns=TS, end_ns=TS + 10,
           scope=SPAN_SCOPE_ATTRS, trace_id=TRACE_ID, span_id=SPAN_ID),
    _event("span-negative", [("span.malformed", "string", "yes")], kind="span",
           span_name="span/negative-duration", start_ns=TS, end_ns=TS - 1000,
           trace_id=TRACE_ID, span_id=SPAN_ID),
]


def inventory():
    return {"events": EVENTS, "spans": SPAN_EVENTS}


# ---------------------------------------------------------------------------
# OTLP request builders (used only for the stock Collector probes)
# ---------------------------------------------------------------------------

def _scope(name="lighthouse.fixture", version="0.1.0", extra=None):
    s = common_pb2.InstrumentationScope()
    s.name = name
    s.version = version
    s.attributes.extend(attrs_pairs(extra or FULL_SCOPE))
    return s


def _resource_logs(res_attrs, scope_obj, records, schema_url="https://opentelemetry.io/schemas/1.39.0"):
    rl = logs_pb2.ResourceLogs()
    rl.resource.CopyFrom(resource_pb2.Resource(attributes=attrs_pairs(res_attrs)))
    rl.schema_url = schema_url
    sl = rl.scope_logs.add()
    sl.scope.CopyFrom(scope_obj)
    sl.schema_url = schema_url
    sl.log_records.extend(records)
    return rl


def _log_record(event):
    r = logs_pb2.LogRecord()
    r.time_unix_nano = event["time_ns"]
    r.observed_time_unix_nano = event["observed_ns"] or (event["time_ns"] + 1_000_000)
    severity_text, severity_number = event["severity"]
    r.severity_text = severity_text
    r.severity_number = severity_number
    body_type, body_value = event["body"]
    if body_type != "string" or body_value is not None:
        r.body.CopyFrom(any_value(_EMPTY if body_type == "empty" else body_value))
    r.attributes.extend(attrs_pairs(event["record"]))
    if event["duplicate"]:
        r.attributes.extend(attrs_pairs(event["duplicate"]))
    if event["trace_id"]:
        r.trace_id = event["trace_id"]
    if event["span_id"]:
        r.span_id = event["span_id"]
    if event["event_name"]:
        r.event_name = event["event_name"]
    return r


NONFINITE_KEYS = {"double.nan", "double.posinf", "double.neginf"}
NONFINITE_EVENTS = {"nan", "posinf", "neginf"}


def _safe_events(events):
    """Drop non-finite doubles: the stock JSON exporter cannot marshal them."""
    safe = []
    for event in events:
        if event["event_id"] in NONFINITE_EVENTS:
            continue
        copy = dict(event)
        copy["record"] = [r for r in event["record"] if r[0] not in NONFINITE_KEYS]
        copy["resource"] = [r for r in event["resource"] if r[0] not in NONFINITE_KEYS]
        safe.append(copy)
    return safe


def build_logs(label="typed"):
    if label == "identity-a":
        # One record whose resource attributes already carry a producer-supplied
        # workspace ID; the credential-selected pipeline must overwrite it.
        event = _event("identity-ws-a", [("producer.attr", "string", "a")],
                       resource=FULL_RESOURCE + [("lighthouse.workspace_id", "string", WS_FORGED)])
        r1 = _resource_logs(event["resource"], _scope(), [_log_record(event)])
        req = logs_service_pb2.ExportLogsServiceRequest()
        req.resource_logs.extend([r1])
        return req.SerializeToString(), "identity record with a forged producer workspace attribute"
    if label not in ("typed", "typed-safe"):
        raise SystemExit(f"unknown logs fixture label: {label}")
    events = EVENTS if label == "typed" else _safe_events(EVENTS)
    # Two resource groups: the full typed record set, and one group without a
    # service name (its single record lives only in the second group).
    r1 = _resource_logs(FULL_RESOURCE, _scope(),
                        [_log_record(e) for e in events if e["event_id"] != "no-service-name"])
    r2 = _resource_logs([("res.group", "string", "no-service")],
                        _scope("lighthouse.fixture.secondary"),
                        [_log_record(e) for e in events if e["event_id"] == "no-service-name"],
                        schema_url="")
    req = logs_service_pb2.ExportLogsServiceRequest()
    req.resource_logs.extend([r1, r2])
    return req.SerializeToString(), f"{label} logs: {len(events)} records"


def build_spans(label="typed"):
    if label not in ("typed", "typed-safe"):
        raise SystemExit(f"unknown traces fixture label: {label}")
    span_events = SPAN_EVENTS if label == "typed" else _safe_events(SPAN_EVENTS)

    def span(event):
        s = trace_pb2.Span()
        s.trace_id = event["trace_id"]
        s.span_id = event["span_id"]
        s.parent_span_id = PARENT_SPAN_ID
        s.trace_state = "vendor=value"
        s.name = event["span_name"]
        s.kind = trace_pb2.Span.SpanKind.SPAN_KIND_SERVER
        s.start_time_unix_nano = event["start_ns"]
        s.end_time_unix_nano = event["end_ns"]
        s.attributes.extend(attrs_pairs(event["record"]))
        s.status.code = 1
        return s

    rl = trace_pb2.ResourceSpans()
    rl.resource.CopyFrom(resource_pb2.Resource(attributes=attrs_pairs(FULL_RESOURCE)))
    sl = rl.scope_spans.add()
    sl.scope.CopyFrom(_scope("lighthouse.fixture.spans"))
    sl.spans.extend([span(e) for e in span_events if e["event_id"] != "span-scope"])
    # Second scope group carries the span scope attributes.
    sl2 = rl.scope_spans.add()
    sl2.scope.CopyFrom(_scope("lighthouse.fixture.spans", extra=SPAN_SCOPE_ATTRS))
    sl2.spans.append(span(next(e for e in span_events if e["event_id"] == "span-scope")))
    req = trace_service_pb2.ExportTraceServiceRequest()
    req.resource_spans.extend([rl])
    return req.SerializeToString(), f"{label} spans: {len(span_events)} spans"


BUILDERS = {"logs": build_logs, "traces": build_spans}
SIGNAL_PATHS = {"logs": "/v1/logs", "traces": "/v1/traces"}


def post_bytes(url, data, headers=None, timeout=30):
    started = time.monotonic()
    resp = requests.post(url, data=data, headers=headers or {}, timeout=timeout)
    return resp, (time.monotonic() - started) * 1000


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    emit = sub.add_parser("emit", help="build and POST a named fixture")
    emit.add_argument("signal", choices=BUILDERS)
    emit.add_argument("label", nargs="?", default="typed")
    emit.add_argument("--url", required=True, help="base URL, e.g. http://127.0.0.1:4328")
    emit.add_argument("--header", action="append", default=[], help="Header: value")
    emit.add_argument("--label-out", help="write a JSON result record here")

    write = sub.add_parser("write", help="write request bytes without sending")
    write.add_argument("signal", choices=BUILDERS)
    write.add_argument("label", nargs="?", default="typed")
    write.add_argument("--out", required=True)

    sub.add_parser("inventory", help="print the canonical typed inventory as JSON")

    args = parser.parse_args(argv)

    if args.command == "inventory":
        print(json.dumps(inventory(), indent=2, default=_json_default))
        return 0

    if args.command in ("emit", "write"):
        data, description = BUILDERS[args.signal](args.label)
        if args.command == "write":
            Path(args.out).write_bytes(data)
            print(f"wrote {len(data)} bytes to {args.out}")
            return 0
        headers = {"Content-Type": "application/x-protobuf"}
        for header in args.header:
            key, _, value = header.partition(":")
            headers[key.strip()] = value.strip()
        url = args.url.rstrip("/") + SIGNAL_PATHS[args.signal]
        resp, elapsed_ms = post_bytes(url, data, headers)
        record = {
            "signal": args.signal,
            "label": args.label,
            "description": description,
            "url": url,
            "status": resp.status_code,
            "elapsed_ms": round(elapsed_ms, 2),
            "response_body": resp.text[:500],
            "request_bytes": len(data),
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        print(json.dumps(record))
        if args.label_out:
            Path(args.label_out).write_text(json.dumps(record) + "\n")
        return 0 if resp.ok else 1

    return 1


def _json_default(value):
    if isinstance(value, bytes):
        return {"__bytes__": base64.b64encode(value).decode()}
    if isinstance(value, float):
        if math.isnan(value):
            return {"__float__": "nan"}
        if math.isinf(value):
            return {"__float__": "inf" if value > 0 else "-inf"}
    raise TypeError(f"not JSON serializable: {value!r}")


if __name__ == "__main__":
    sys.exit(main())
