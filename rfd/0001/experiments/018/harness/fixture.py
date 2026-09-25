#!/usr/bin/env python3
"""Deterministic OTLP fixture for Experiment 018 (Collector compatibility).

Builds small, fixed OTLP protobuf requests by hand so the corpus does not depend
on an SDK's filtering, coercion, or batching behavior. The typed cases mirror the
RFD 0001 "PoC typed-value contract" and the failure cases mirror the acceptance
and tenant plans (duplicate keys, namespace collisions, empty versus missing,
malformed span timing, non-finite floats, integers above 2^53).

Commands:
  emit SIGNAL LABEL     build a named request and POST it to --url
  write SIGNAL LABEL    write the request bytes to --out without sending
  replay --file PATH    POST previously stored request bytes (overflow replay)
  dump                  print the fixture inventory as JSON
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
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.logs.v1 import logs_pb2
from opentelemetry.proto.metrics.v1 import metrics_pb2
from opentelemetry.proto.resource.v1 import resource_pb2
from opentelemetry.proto.trace.v1 import trace_pb2

# Synthetic, non-secret credentials for the local fixture only.
KEY_A = "lh018-ws-a-key"
KEY_B = "lh018-ws-b-key"
KEY_REPLAY = "lh018-catchup-key"
WS_A = "11111111-1111-4111-8111-111111111111"
WS_B = "22222222-2222-4222-8222-222222222222"
WS_FORGED = "99999999-9999-4999-8999-999999999999"

# Fixed event time so every run compares against the same stored values.
# 2026-09-01T07:00:00.123456789Z
_EPOCH = datetime(2026, 9, 1, 7, 0, 0, tzinfo=timezone.utc)
TS = int(_EPOCH.timestamp()) * 1_000_000_000 + 123_456_789
TS2 = TS + 86_400_000_000_000  # +1 day, for the observed-timestamp fallback case
TRACE_ID = bytes.fromhex("4bf92f3577b34da6a3ce929d0e0e4736")
SPAN_ID = bytes.fromhex("00f067aa0ba902b7")
PARENT_SPAN_ID = bytes.fromhex("00f067aa0ba90200")
LINK_SPAN_ID = bytes.fromhex("00f067aa0ba90201")

# The 2^53 boundary: 2^53 + 1 is exactly representable as int64 but not as
# float64, so any lossy path is visible in the stored value.
INT_ABOVE_2_53 = 9_007_199_254_740_993
INT_ABOVE_2_53_AS_FLOAT64 = 9_007_199_254_740_992

UNICODE = "héllo\u2014\U0001f30d"
LONG_STRING = "x" * 1024
BYTES_VALUE = b"\x00\x01\xfe\xff"


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


class _Empty:
    def __repr__(self):  # pragma: no cover - debug aid only
        return "EMPTY"


# OTLP explicit empty: an AnyValue with no oneof member set.
_EMPTY = _Empty()


def attrs(mapping, duplicate_keys=None):
    """Build KeyValue pairs; duplicate_keys adds a second entry for one key."""
    out = []
    for key, value in mapping.items():
        kv = common_pb2.KeyValue()
        kv.key = key
        kv.value.CopyFrom(any_value(value))
        out.append(kv)
    for key, value in (duplicate_keys or []):
        kv = common_pb2.KeyValue()
        kv.key = key
        kv.value.CopyFrom(any_value(value))
        out.append(kv)
    return out


def resource_attrs(extra=None, service_name="fixture-log-service", include_service=True):
    base = {
        "service.version": "1.0.0",
        "res.int64": INT_ABOVE_2_53,
        "res.bool": True,
        "res.float": 1.0,
        "res.str_one": "1",
        "res.bytes": BYTES_VALUE,
        "res.array": ["a", 1, True],
        "res.map": {"nested.int": 2, "nested.bool": False},
        "res.empty": _EMPTY,
        "res.empty_string": "",
        "res.collide": "resource-value",
        "Foo": "resource-upper",
        "foo": "resource-lower",
    }
    if include_service:
        base["service.name"] = service_name
    if extra:
        base.update(extra)
    return attrs(base)


def scope(name="lighthouse.fixture", version="0.1.0", extra=None):
    s = common_pb2.InstrumentationScope()
    s.name = name
    s.version = version
    base = {"scope.attr": "scope-value", "scope.int": 7, "scope.empty": _EMPTY}
    if extra:
        base.update(extra)
    s.attributes.extend(attrs(base))
    return s


def log_record(body, attributes=None, severity_text="INFO", severity_number=9,
               time_ns=TS, observed_ns=TS + 1_000_000, trace_id=None, span_id=None,
               flags=0, event_name=None, duplicate_keys=None, body_value=None):
    r = logs_pb2.LogRecord()
    r.time_unix_nano = time_ns
    r.observed_time_unix_nano = observed_ns
    r.severity_text = severity_text
    r.severity_number = severity_number
    if body_value is not None:
        r.body.CopyFrom(any_value(body_value))
    else:
        r.body.string_value = body
    r.attributes.extend(attrs(attributes or {}, duplicate_keys))
    r.flags = flags
    if trace_id:
        r.trace_id = trace_id
    if span_id:
        r.span_id = span_id
    if event_name:
        r.event_name = event_name
    return r


def logs_request(resource_logs):
    req = logs_service_pb2.ExportLogsServiceRequest()
    req.resource_logs.extend(resource_logs)
    return req.SerializeToString()


def resource_logs(res_attrs, scope_obj, records, res_schema_url="https://opentelemetry.io/schemas/1.39.0",
                  scope_schema_url="https://opentelemetry.io/schemas/1.39.0"):
    rl = logs_pb2.ResourceLogs()
    rl.resource.CopyFrom(resource_pb2.Resource(attributes=res_attrs))
    rl.schema_url = res_schema_url
    sl = rl.scope_logs.add()
    sl.scope.CopyFrom(scope_obj)
    sl.schema_url = scope_schema_url
    sl.log_records.extend(records)
    return rl


# ---------------------------------------------------------------------------
# Fixture inventory
# ---------------------------------------------------------------------------

TYPED_LOG_1_ATTRS = {
    "int.big": INT_ABOVE_2_53,
    "int.neg": -7,
    "double.one": 1.0,
    "double.half": 1.5,
    "double.nan": math.nan,
    "double.posinf": math.inf,
    "double.neginf": -math.inf,
    "bool.true": True,
    "bool.false": False,
    "str.one": "1",
    "int.one": 1,
    "str.empty": "",
    "zero.int": 0,
    "zero.float": -0.0,
    "bytes.raw": BYTES_VALUE,
    "array.mixed": ["a", 1, True],
    "map.nested": {"nested.int": 2, "nested.bool": False},
    "str.json_like": '{"nested.bool":false,"nested.int":2}',
    "empty.value": _EMPTY,
    "collide": "record-value",
    "Foo": "record-upper",
    "unicode": UNICODE,
    "long.string": LONG_STRING,
}


def build_logs(label):
    """Return (request_bytes, description) for a named logs fixture label."""
    if label == "typed":
        r1 = resource_logs(resource_attrs(), scope(), [
            log_record("typed-log-1", TYPED_LOG_1_ATTRS),
            log_record("bytes-body-log", {"body.bytes.test": "yes"},
                       severity_text="WARN", severity_number=13, body_value=BYTES_VALUE),
            log_record("int-body-log", {"body.int.test": "yes"}, body_value=42),
            log_record("map-body-log", {"body.map.test": "yes"},
                       body_value={"k": "v", "n": 1}),
            log_record("duplicate-key-log", {"dup.base": "base"},
                       duplicate_keys=[("dup", "first"), ("dup", "second")]),
            log_record("timestamp-fallback-log", {"time.fallback": "yes"},
                       time_ns=0, observed_ns=TS2),
            log_record("trace-correlated-log", {"trace.corr": "yes"},
                       trace_id=TRACE_ID, span_id=SPAN_ID, flags=1),
            log_record("event-name-log", {"event.attr": "yes"}, event_name="fixture.event"),
        ])
        r2 = resource_logs(resource_attrs({"res.group": "no-service"}, include_service=False),
                           scope("lighthouse.fixture.secondary"),
                           [log_record("no-service-name-log", {"no.service": "yes"})],
                           res_schema_url="")
        return logs_request([r1, r2]), "typed logs: resource/scope/record separation and value types"

    if label == "identity-a":
        forged = {"lighthouse.workspace_id": WS_FORGED}
        r_a = resource_logs(resource_attrs(forged, service_name="identity-fixture"),
                            scope(), [log_record("identity-ws-a-log", {"producer.attr": "a"})])
        return logs_request([r_a]), "workspace A logs: one forged producer workspace attribute"

    if label == "identity-b":
        r_b = resource_logs(resource_attrs(service_name="identity-fixture"),
                            scope(), [log_record("identity-ws-b-log", {"producer.attr": "b"})])
        return logs_request([r_b]), "workspace B logs"

    if label == "forge":
        r = resource_logs(resource_attrs({"producer.forge": "yes"}, service_name="identity-fixture"),
                          scope(), [log_record("identity-forge-log", {"producer.forge": "yes"})])
        return logs_request([r]), "identity negative control: producer-supplied workspace header"

    if label == "batch":
        records = [log_record(f"batch-log-{i}", {"batch.index": i}) for i in range(6)]
        r = resource_logs(resource_attrs({"batch.group": "yes"}, service_name="identity-fixture"),
                          scope(), records)
        return logs_request([r]), "six records in one request for rebatching observation"

    if label.startswith("fault"):
        n = int(label.split("-")[1])
        records = [log_record(f"fault-log-{i}", {"fault.index": i}) for i in range(n)]
        r = resource_logs(resource_attrs({"fault.group": "yes"}, service_name="identity-fixture"),
                          scope(), records)
        return logs_request([r]), f"{n} records for fault injection"

    if label == "ack":
        r = resource_logs(resource_attrs({"ack.group": "yes"}, service_name="identity-fixture"),
                          scope(), [log_record("ack-log-1", {"ack.index": 1})])
        return logs_request([r]), "one record for acknowledgement-boundary timing"

    raise SystemExit(f"unknown logs fixture label: {label}")


def build_spans(label="typed"):
    if label != "typed":
        raise SystemExit(f"unknown traces fixture label: {label}")

    def span(name, start, end, attributes=None, events=(), links=(), status_code=1,
             status_message=""):
        s = trace_pb2.Span()
        s.trace_id = TRACE_ID
        s.span_id = SPAN_ID
        s.parent_span_id = PARENT_SPAN_ID
        s.trace_state = "vendor=value"
        s.name = name
        s.kind = trace_pb2.Span.SpanKind.SPAN_KIND_SERVER
        s.start_time_unix_nano = start
        s.end_time_unix_nano = end
        s.attributes.extend(attrs(attributes or {}))
        for ev_name, ev_ts, ev_attrs in events:
            ev = s.events.add()
            ev.name = ev_name
            ev.time_unix_nano = ev_ts
            ev.attributes.extend(attrs(ev_attrs))
        for link_span, link_attrs in links:
            link = s.links.add()
            link.trace_id = TRACE_ID
            link.span_id = link_span
            link.trace_state = "l=1"
            link.attributes.extend(attrs(link_attrs))
        s.status.code = status_code
        s.status.message = status_message
        return s

    typed_attrs = {
        "span.int": INT_ABOVE_2_53,
        "span.bool": True,
        "span.str": "1",
        "span.bytes": BYTES_VALUE,
        "span.map": {"nested": "v"},
    }
    s1 = span("span/ok", TS, TS + 1500, typed_attrs,
              events=[("span/event", TS + 500, {"ev.attr": "e"})],
              links=[(LINK_SPAN_ID, {"link.attr": 1})])
    s2 = span("span/zero-duration", TS, TS)
    s3 = span("span/negative-duration", TS, TS - 1000, {"span.malformed": "yes"})
    s4 = span("span/scope-attrs", TS, TS + 10, {"span.scope.attr": "present"})
    s5 = span("span/no-timestamps", 0, 0, {"span.no.ts": "yes"})

    rl = trace_pb2.ResourceSpans()
    rl.resource.CopyFrom(resource_pb2.Resource(attributes=resource_attrs()))
    sl = rl.scope_spans.add()
    sl.scope.CopyFrom(scope("lighthouse.fixture.spans"))
    sl.spans.extend([s1, s2, s3, s5])
    # Second scope group carries scope attributes; the traces table has no
    # ScopeAttributes column, so this case records what the stock exporter drops.
    sl2 = rl.scope_spans.add()
    sl2.scope.CopyFrom(scope("lighthouse.fixture.spans", extra={"scope.span.attr": "s"}))
    sl2.spans.append(s4)
    req = trace_service_pb2.ExportTraceServiceRequest()
    req.resource_spans.extend([rl])
    return req.SerializeToString(), "typed spans: events, links, zero and negative durations"


def build_metrics(label="typed"):
    if label != "typed":
        raise SystemExit(f"unknown metrics fixture label: {label}")

    rm = metrics_pb2.ResourceMetrics()
    rm.resource.CopyFrom(resource_pb2.Resource(attributes=resource_attrs()))
    sm = rm.scope_metrics.add()
    sm.scope.CopyFrom(scope("lighthouse.fixture.metrics"))

    def number_point(value, attributes, as_int=False):
        dp = metrics_pb2.NumberDataPoint()
        if as_int:
            dp.as_int = value
        else:
            dp.as_double = value
        dp.start_time_unix_nano = TS
        dp.time_unix_nano = TS + 1_000_000_000
        dp.attributes.extend(attrs(attributes))
        return dp

    gauge = sm.metrics.add()
    gauge.name = "fixture.gauge"
    gauge.description = "typed gauge fixture"
    gauge.unit = "1"
    gauge.gauge.data_points.extend([
        number_point(1.5, {"metric.attr": "g"}),
        number_point(INT_ABOVE_2_53, {"metric.attr": "g-int"}, as_int=True),
    ])

    counter = sm.metrics.add()
    counter.name = "fixture.counter"
    counter.unit = "1"
    counter.sum.aggregation_temporality = metrics_pb2.AGGREGATION_TEMPORALITY_CUMULATIVE
    counter.sum.is_monotonic = True
    counter.sum.data_points.append(number_point(42, {"metric.attr": "s"}, as_int=True))

    hist = sm.metrics.add()
    hist.name = "fixture.histogram"
    hist.unit = "ms"
    hist.histogram.aggregation_temporality = metrics_pb2.AGGREGATION_TEMPORALITY_CUMULATIVE
    hdp = metrics_pb2.HistogramDataPoint()
    hdp.attributes.extend(attrs({"metric.attr": "h"}))
    hdp.start_time_unix_nano = TS
    hdp.time_unix_nano = TS + 1_000_000_000
    hdp.count = 3
    hdp.sum = 6.0
    hdp.bucket_counts.extend([1, 1, 1])
    hdp.explicit_bounds.extend([1.0, 2.0])
    hdp.min = 0.5
    hdp.max = 2.5
    hist.histogram.data_points.append(hdp)

    req = metrics_service_pb2.ExportMetricsServiceRequest()
    req.resource_metrics.extend([rm])
    return req.SerializeToString(), "typed metrics: gauge, sum, histogram"


BUILDERS = {
    "logs": build_logs,
    "traces": build_spans,
    "metrics": build_metrics,
}

LABELS = {
    "logs": ["typed", "identity-a", "identity-b", "forge", "batch", "ack",
             "fault-1", "fault-2", "fault-3", "fault-4"],
    "traces": ["typed"],
    "metrics": ["typed"],
}

SIGNAL_PATHS = {
    "logs": "/v1/logs",
    "traces": "/v1/traces",
    "metrics": "/v1/metrics",
}


def post_bytes(url, data, headers=None, timeout=30):
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    resp = requests.post(url, data=data, headers=headers or {}, timeout=timeout)
    elapsed_ms = (time.monotonic() - started) * 1000
    return resp, elapsed_ms, started_at


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    emit = sub.add_parser("emit", help="build and POST a named fixture")
    emit.add_argument("signal", choices=BUILDERS)
    emit.add_argument("label")
    emit.add_argument("--url", required=True, help="base URL, e.g. http://127.0.0.1:4318")
    emit.add_argument("--header", action="append", default=[], help="Header: value")
    emit.add_argument("--label-out", help="write a JSON result record here")
    emit.add_argument("--save-request", help="also store the request bytes here")

    write = sub.add_parser("write", help="write request bytes without sending")
    write.add_argument("signal", choices=BUILDERS)
    write.add_argument("label")
    write.add_argument("--out", required=True)

    replay = sub.add_parser("replay", help="POST stored request bytes")
    replay.add_argument("--file", required=True)
    replay.add_argument("--url", required=True, help="base URL, e.g. http://127.0.0.1:4319")
    replay.add_argument("--signal", choices=BUILDERS, default="logs")
    replay.add_argument("--header", action="append", default=[])
    replay.add_argument("--label-out")

    sub.add_parser("dump", help="print the fixture inventory")

    args = parser.parse_args(argv)

    if args.command == "dump":
        print(json.dumps(LABELS, indent=2))
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
        resp, elapsed_ms, started_at = post_bytes(url, data, headers)
        record = {
            "signal": args.signal,
            "label": args.label,
            "description": description,
            "url": url,
            "status": resp.status_code,
            "elapsed_ms": round(elapsed_ms, 2),
            "response_body": resp.text[:500],
            "request_bytes": len(data),
            "started_at": started_at,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }
        print(json.dumps(record))
        if args.save_request:
            Path(args.save_request).write_bytes(data)
        if args.label_out:
            Path(args.label_out).write_text(json.dumps(record) + "\n")
        return 0 if resp.ok else 1

    if args.command == "replay":
        data = Path(args.file).read_bytes()
        headers = {"Content-Type": "application/x-protobuf"}
        for header in args.header:
            key, _, value = header.partition(":")
            headers[key.strip()] = value.strip()
        url = args.url.rstrip("/") + SIGNAL_PATHS[args.signal]
        resp, elapsed_ms, started_at = post_bytes(url, data, headers)
        record = {
            "command": "replay",
            "file": args.file,
            "url": url,
            "status": resp.status_code,
            "elapsed_ms": round(elapsed_ms, 2),
            "response_body": resp.text[:500],
            "started_at": started_at,
            "replayed_at": datetime.now(timezone.utc).isoformat(),
        }
        print(json.dumps(record))
        if args.label_out:
            Path(args.label_out).write_text(json.dumps(record) + "\n")
        return 0 if resp.ok else 1

    return 1


if __name__ == "__main__":
    sys.exit(main())
