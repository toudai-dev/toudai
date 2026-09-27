#!/usr/bin/env python3
"""Deterministic fixture for Experiment 021 (hybrid layout Collector integration).

The typed inventory is Experiment 019's independent fixture, imported rather
than copied so both experiments send the same values on the same rows: int 1 /
double 1.0 / string "1", integers above 2^53 and the Int64 limits, non-finite
doubles and negative zero, bytes/arrays/maps, explicit empty versus empty string
versus missing keys, duplicate keys, case variants, resource/scope/record
namespace collisions, and span scope attributes on a separate scope.

This module adds the records the integration slice needs:

* two workspaces with the previously tested synthetic credential-to-workspace
  binding, each sending a forged ``lighthouse.workspace_id``/``lighthouse.dataset``
  in the resource namespace plus reserved keys in the scope and record namespaces;
* one-record requests for the two credentials that share the exporter's queue, so
  a single downstream batch can be shown to carry both workspaces;
* small fault requests for the split-write, outage, and duplicate phases;
* a request for a pipeline that has no trusted-scope processor (fail-closed
  probe);
* a span with missing timing (0/0) next to Experiment 019's zero, positive, and
  negative-duration spans.

Everything is deterministic: fixed timestamps and fixed values.

Commands:
  emit SIGNAL LABEL [--url URL] [--header 'Authorization: Bearer ...']
  write SIGNAL LABEL --out PATH
  inventory
  hashes
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXP019_HARNESS = HERE.parent.parent / "019" / "harness"
sys.path.insert(0, str(EXP019_HARNESS))

import fixture as fixture019  # noqa: E402  (path set above)

from opentelemetry.proto.collector.logs.v1 import logs_service_pb2  # noqa: E402
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2  # noqa: E402
from opentelemetry.proto.trace.v1 import trace_pb2  # noqa: E402

# Synthetic, non-secret credentials for the local fixture only.
KEY_A = "lh021-ws-a-key"
KEY_B = "lh021-ws-b-key"
WS_A = "11111111-1111-4111-8111-111111111111"
WS_B = "22222222-2222-4222-8222-222222222222"
WS_FORGED = "99999999-9999-4999-8999-999999999999"
DATASET = "fixture"
FORGED_DATASET = "forged-dataset"

IMPORTED_SOURCES = ("fixture.py", "model.py")

TS = fixture019.TS


def source_hashes() -> dict:
    out = {}
    for name in IMPORTED_SOURCES:
        path = EXP019_HARNESS / name
        out[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


# ---------------------------------------------------------------------------
# Local fixture additions
# ---------------------------------------------------------------------------

# Experiment 019's span set plus the missing-timing span: OTLP encodes missing
# timing as 0/0, which the contract requires to stay distinct from a
# zero-duration span.
SPAN_MISSING_TIME = fixture019._event(
    "span-missing-time", [("span.missing", "string", "yes")], kind="span",
    span_name="span/missing-timing", start_ns=0, end_ns=0,
    trace_id=fixture019.TRACE_ID, span_id=fixture019.SPAN_ID)

SPAN_EVENTS = list(fixture019.SPAN_EVENTS) + [SPAN_MISSING_TIME]

FORGED_RESOURCE = list(fixture019.FULL_RESOURCE) + [
    ("lighthouse.workspace_id", "string", WS_FORGED),
    ("lighthouse.dataset", "string", FORGED_DATASET),
]
FORGED_SCOPE = list(fixture019.FULL_SCOPE) + [
    ("lighthouse.workspace_id", "string", WS_FORGED),
    ("lighthouse.dataset", "string", FORGED_DATASET),
]
FORGED_RECORD = [
    ("forged.attr", "string", "kept"),
    ("lighthouse.workspace_id", "string", WS_FORGED),
    ("lighthouse.scope_source", "string", "producer"),
]


def _identity_event(event_id):
    return fixture019._event(
        event_id, FORGED_RECORD, resource=FORGED_RESOURCE, scope=FORGED_SCOPE,
        body=("string", event_id))


IDENTITY_A = _identity_event("identity-ws-a")
IDENTITY_B = _identity_event("identity-ws-b")

BATCH_A = fixture019._event("batch-a-1", [("batch.attr", "string", "a")],
                            body=("string", "batch-a-1"))
BATCH_B = fixture019._event("batch-b-1", [("batch.attr", "string", "b")],
                            body=("string", "batch-b-1"))

FAULT_A_1 = [fixture019._event("fault-a-1", [("fault.attr", "string", "one")],
                               body=("string", "fault-a-1"))]
FAULT_A_2 = [
    fixture019._event("fault-a-2", [("fault.attr", "string", "two")],
                      body=("string", "fault-a-2")),
    fixture019._event("fault-a-3", [("fault.attr", "string", "three")],
                      body=("string", "fault-a-3")),
]

RETRY_A = [fixture019._event("retry-a-1", [("retry.attr", "string", "same")],
                             body=("string", "retry-a-1"))]


def _phase_event(event_id):
    """One dedicated event per phase.

    Each phase needs its own event identity: the serving row for an injected
    fault is either absent or duplicated, so reusing a semantics-phase event
    would make that event's expected count ambiguous.
    """
    return [fixture019._event(event_id, [("phase.attr", "string", event_id)],
                              body=("string", event_id))]


FAULT_SPLIT = _phase_event("fault-split-1")
FAULT_SPLIT_RETRY = _phase_event("fault-split-retry-1")
FAULT_CANONICAL = _phase_event("fault-canonical-1")
FAULT_OUTAGE = _phase_event("fault-outage-1")
FAULT_QUEUE_1 = _phase_event("fault-queue-1")
FAULT_QUEUE_2 = _phase_event("fault-queue-2")
FAULT_CRASH_1 = _phase_event("fault-crash-1")
FAULT_CRASH_2 = _phase_event("fault-crash-2")
AMBIGUOUS_A = _phase_event("ambiguous-a-1")
ACK_ON = _phase_event("ack-on-1")
ACK_OFF = _phase_event("ack-off-1")

NO_TRUST = [fixture019._event("no-trust-1", [("no.trust", "string", "x")],
                              body=("string", "no-trust-1"))]

# A producer that forges the internal scope-source marker as well as the trusted
# keys. This exists to measure the limitation, not to propose a design: the
# component's marker check cannot distinguish a forged marker from an injected
# one, so the probe asserts the producer is rejected only when the marker is
# absent, and the report records the forged-marker case explicitly.
NO_TRUST_FORGED = [fixture019._event(
    "no-trust-forged-1", FORGED_RECORD,
    resource=[("lighthouse.workspace_id", "string", WS_FORGED),
              ("lighthouse.dataset", "string", FORGED_DATASET),
              ("lighthouse.scope_source", "string", "gateway"),
              ("service.name", "string", "probe")],
    scope=FORGED_SCOPE, body=("string", "no-trust-forged-1"))]

# The typed corpus: Experiment 019's 28 events (one of which carries duplicate
# attribute keys and is rejected by the mapping component).
TYPED_LOGS = list(fixture019.EVENTS)

LOG_LABELS = {
    "typed": TYPED_LOGS,
    "identity-a": [IDENTITY_A],
    "identity-b": [IDENTITY_B],
    "batch-a": [BATCH_A],
    "batch-b": [BATCH_B],
    "fault-a-1": FAULT_A_1,
    "fault-a-2": FAULT_A_2,
    "retry-a": RETRY_A,
    "no-trust": NO_TRUST,
    "no-trust-forged": NO_TRUST_FORGED,
    # One dedicated event per failure injection (see _fault_event).
    "fault-split": FAULT_SPLIT,
    "fault-split-retry": FAULT_SPLIT_RETRY,
    "fault-canonical": FAULT_CANONICAL,
    "fault-outage": FAULT_OUTAGE,
    "fault-queue-1": FAULT_QUEUE_1,
    "fault-queue-2": FAULT_QUEUE_2,
    "fault-crash-1": FAULT_CRASH_1,
    "fault-crash-2": FAULT_CRASH_2,
    "ambiguous-a": AMBIGUOUS_A,
    "ack-on": ACK_ON,
    "ack-off": ACK_OFF,
}


def _logs_request(events):
    """Two resource groups: the full typed set, and one without a service name."""
    primary = [e for e in events if e["event_id"] != "no-service-name"]
    secondary = [e for e in events if e["event_id"] == "no-service-name"]
    groups = []
    if primary:
        groups.append(fixture019._resource_logs(
            primary[0]["resource"], fixture019._scope(),
            [fixture019._log_record(e) for e in primary]))
    if secondary:
        groups.append(fixture019._resource_logs(
            [("res.group", "string", "no-service")],
            fixture019._scope("lighthouse.fixture.secondary"),
            [fixture019._log_record(e) for e in secondary], schema_url=""))
    req = logs_service_pb2.ExportLogsServiceRequest()
    req.resource_logs.extend(groups)
    return req.SerializeToString()


def build_logs(label):
    if label not in LOG_LABELS:
        raise SystemExit(f"unknown logs fixture label: {label}")
    events = LOG_LABELS[label]
    return _logs_request(events), f"{label} logs: {len(events)} records"


def build_spans(label):
    if label not in ("typed",):
        raise SystemExit(f"unknown traces fixture label: {label}")

    def span(event):
        s = trace_pb2.Span()
        s.trace_id = event["trace_id"] or fixture019.TRACE_ID
        s.span_id = event["span_id"] or fixture019.SPAN_ID
        s.parent_span_id = fixture019.PARENT_SPAN_ID
        s.trace_state = "vendor=value"
        s.name = event["span_name"]
        s.kind = trace_pb2.Span.SpanKind.SPAN_KIND_SERVER
        s.start_time_unix_nano = event["start_ns"]
        s.end_time_unix_nano = event["end_ns"]
        s.attributes.extend(fixture019.attrs_pairs(event["record"]))
        s.status.code = 1
        return s

    rl = trace_pb2.ResourceSpans()
    rl.resource.CopyFrom(fixture019.resource_pb2.Resource(
        attributes=fixture019.attrs_pairs(fixture019.FULL_RESOURCE)))
    first = [e for e in SPAN_EVENTS if e["event_id"] not in ("span-scope", "span-missing-time")]
    sl = rl.scope_spans.add()
    sl.scope.CopyFrom(fixture019._scope("lighthouse.fixture.spans"))
    sl.spans.extend([span(e) for e in first])
    # Span scope attributes on their own scope group.
    sl2 = rl.scope_spans.add()
    sl2.scope.CopyFrom(fixture019._scope("lighthouse.fixture.spans",
                                         extra=fixture019.SPAN_SCOPE_ATTRS))
    sl2.spans.append(span(next(e for e in SPAN_EVENTS if e["event_id"] == "span-scope")))
    # The missing-timing span shares the first scope group's scope attributes.
    sl3 = rl.scope_spans.add()
    sl3.scope.CopyFrom(fixture019._scope("lighthouse.fixture.spans"))
    sl3.spans.append(span(next(e for e in SPAN_EVENTS if e["event_id"] == "span-missing-time")))

    req = trace_service_pb2.ExportTraceServiceRequest()
    req.resource_spans.extend([rl])
    return req.SerializeToString(), f"typed spans: {len(SPAN_EVENTS)} spans"


BUILDERS = {"logs": build_logs, "traces": build_spans}
SIGNAL_PATHS = {"logs": "/v1/logs", "traces": "/v1/traces"}


def inventory():
    return {"logs": TYPED_LOGS, "spans": SPAN_EVENTS}


def _json_default(value):
    if isinstance(value, bytes):
        return {"__bytes__": base64.b64encode(value).decode()}
    return fixture019._json_default(value)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    emit = sub.add_parser("emit")
    emit.add_argument("signal", choices=BUILDERS)
    emit.add_argument("label")
    emit.add_argument("--url", required=True)
    emit.add_argument("--header", action="append", default=[])
    emit.add_argument("--label-out")

    write = sub.add_parser("write")
    write.add_argument("signal", choices=BUILDERS)
    write.add_argument("label")
    write.add_argument("--out", required=True)

    sub.add_parser("inventory")
    sub.add_parser("hashes")

    args = parser.parse_args(argv)

    if args.command == "inventory":
        print(json.dumps(inventory(), indent=2, default=_json_default))
        return 0
    if args.command == "hashes":
        for name, digest in source_hashes().items():
            print(f"{digest}\t{name}")
        return 0

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
    resp, elapsed_ms = fixture019.post_bytes(url, data, headers)
    record = {
        "signal": args.signal,
        "label": args.label,
        "description": description,
        "url": url,
        "status": resp.status_code,
        "elapsed_ms": round(elapsed_ms, 2),
        "response_body": resp.text[:500],
        "request_bytes": len(data),
        "sent_at": fixture019.datetime.now(fixture019.timezone.utc).isoformat(),
    }
    print(json.dumps(record))
    if args.label_out:
        Path(args.label_out).write_text(json.dumps(record) + "\n")
    return 0 if resp.ok else 1


if __name__ == "__main__":
    sys.exit(main())
