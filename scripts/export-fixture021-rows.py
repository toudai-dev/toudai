#!/usr/bin/env python3
"""Export the Experiment 021 fixture as serving rows for the query service tests.

The rows are the stored form the Experiment 021 verifier expects for the
fixture, produced by that verifier's own independent expectation functions
(`log_expectation`, `span_expectation`) rather than by any query-service code:

* logs: Experiment 019's typed inventory minus the duplicate-key record the
  owned exporter rejects, for workspace A and, as a scope-isolation control,
  an identical copy for workspace B;
* spans: the nine fixture spans (valid, zero, missing-timing, and reversed
  durations) for both workspaces.

Event identities are the fixture's own (`typed-1`, `sum-a`, ...); the owned
exporter stores content-derived identities instead, which this layer never
reads. The experiment harness needs its pinned Python dependencies; see
`rfd/0001/experiments/021-hybrid-layout-collector-integration/harness/setup.sh`.

Usage: export-fixture021-rows.py [--check] [OUT_DIR]   (default tests/fixtures/021)
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HARNESS = ROOT / "rfd/0001/experiments/021-hybrid-layout-collector-integration/harness"
sys.path.insert(0, str(HARNESS))

import fixture021  # noqa: E402

# fixture021 puts Experiment 019's harness first on sys.path, whose own
# verify.py would shadow this one, so load the 021 verifier by path.
_spec = importlib.util.spec_from_file_location("verify021", HARNESS / "verify.py")
verify = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(verify)

fixture019 = fixture021.fixture019


def dump(obj) -> str:
    # allow_nan keeps NaN/Infinity, which ClickHouse's JSONEachRow reader accepts.
    return json.dumps(obj, ensure_ascii=False, allow_nan=True, separators=(",", ":"))


def datetime64(ns: int) -> str:
    # A JSON number is read as seconds, and "0.000000000" does not parse, so
    # nanosecond precision (and the unset epoch-0 timestamp) travels as a
    # datetime string.
    seconds = datetime.fromtimestamp(ns // 1_000_000_000, tz=timezone.utc)
    return f"{seconds:%Y-%m-%d %H:%M:%S}.{ns % 1_000_000_000:09d}"


def hex_or_empty(value) -> str:
    return value.hex() if value else ""


def maps(prefix: str, record: dict) -> dict:
    return {
        f"{prefix}_int": record["int"],
        f"{prefix}_double": record["double"],
        f"{prefix}_string": record["string"],
        f"{prefix}_bool": record["bool"],
        f"{prefix}_other": record["other"],
    }


def log_row(event: dict, workspace: str) -> dict:
    e = verify.log_expectation(event, workspace)
    return {
        "workspace_id": workspace,
        "dataset": e["dataset"],
        "event_id": event["event_id"],
        "ts": datetime64(e["ts_ns"]),
        "observed_ts": datetime64(e["observed_ns"]),
        "kind": "log",
        "service_name": e["service_name"],
        "severity_text": e["severity_text"],
        "severity_number": e["severity_number"],
        "body": e["body"],
        "trace_id": hex_or_empty(event["trace_id"]),
        "span_id": hex_or_empty(event["span_id"]),
        "event_name": event["event_name"] or "",
        "resource": e["resource"],
        "scope": e["scope"],
        "resource_types": e["resource_types"],
        "scope_types": e["scope_types"],
        **maps("record", e["record"]),
        "payload_sha256": "",
    }


def span_row(event: dict, workspace: str) -> dict:
    e = verify.span_expectation(event)
    return {
        "workspace_id": workspace,
        "dataset": e["dataset"],
        "event_id": event["event_id"],
        "ts": datetime64(e["ts_ns"]),
        "end_ts": datetime64(e["end_ns"]),
        "kind": "span",
        "service_name": e["service_name"],
        "trace_id": e["trace_id"],
        "span_id": e["span_id"],
        "parent_span_id": e["parent_span_id"],
        "span_name": e["span_name"],
        "span_kind": e["span_kind"],
        "status_code": e["status_code"],
        "duration_ns": e["duration_ns"],
        "span_timing_status": e["timing_status"],
        "resource": e["resource"],
        "resource_types": e["resource_types"],
        "span_scope": e["span_scope"],
        "span_scope_types": e["span_scope_types"],
        **maps("span", e["record"]),
        "payload_sha256": "",
    }


def build() -> dict[str, str]:
    workspaces = (fixture021.WS_A, fixture021.WS_B)
    logs = [log_row(e, ws) for ws in workspaces for e in fixture019.EVENTS if not e["duplicate"]]
    spans = [span_row(e, ws) for ws in workspaces for e in fixture021.ALL_SPAN_EVENTS]
    return {
        "serving_logs.jsonl": "".join(dump(r) + "\n" for r in logs),
        "serving_spans.jsonl": "".join(dump(r) + "\n" for r in spans),
    }


def main(argv: list[str]) -> int:
    check = "--check" in argv
    args = [a for a in argv if a != "--check"]
    out = Path(args[0]) if args else ROOT / "tests/fixtures/021"
    files = build()
    if check:
        stale = [n for n, text in files.items() if not (out / n).exists() or (out / n).read_text() != text]
        if stale:
            print(f"stale fixture rows: {', '.join(stale)}", file=sys.stderr)
            return 1
        print("fixture rows are up to date")
        return 0
    out.mkdir(parents=True, exist_ok=True)
    for name, text in files.items():
        (out / name).write_text(text)
        print(f"wrote {out / name} ({len(text.splitlines())} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
