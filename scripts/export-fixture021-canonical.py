#!/usr/bin/env python3
"""Write the canonical-payload retrieval fixture for the query service tests.

`tests/fixtures/021/canonical_events.jsonl` holds `canonical_events` rows
(`clickhouse/schema/002-canonical-events.sql`) for exact-event retrieval. The
payloads are hand-authored in the type-preserving canonical form (every value
carries its received type; 64-bit integers are decimal strings), not Collector
output: the exporter owns the real encoding, and retrieval treats a payload as
opaque JSON text. `payload_sha256` is computed here, independently of the
service, so a test can check what the API returns against it.

The rows are chosen to separate the lookup key's parts:

* `typed-1` exists for both workspaces with different payloads, and in a second
  dataset of workspace A, so a lookup that ignored tenant or dataset returns the
  wrong payload;
* `only-b` exists for workspace B alone;
* `dup-1` is stored twice with the same payload (an at-least-once replay) and
  `conflict-1` twice with different payloads;
* `odd id/1 é+%` has characters that need URL escaping.

Usage: export-fixture021-canonical.py [--check] [OUT_DIR]   (default tests/fixtures/021)
"""

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WS_A = "11111111-1111-4111-8111-111111111111"
WS_B = "22222222-2222-4222-8222-222222222222"
# 2026-09-01 07:00:00.123456789, the serving fixture's event time.
TS = "2026-09-01 07:00:00.123456789"


def payload(tenant, dataset, signal, event_id, attrs):
    # Written with explicit separators so the stored text is stable; integers
    # beyond 2^53 are decimal strings.
    return json.dumps(
        {
            "schema_version": "1",
            "workspace_id": tenant,
            "dataset": dataset,
            "signal": signal,
            "event_id": event_id,
            "attributes": attrs,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def row(tenant, dataset, signal, event_id, attrs):
    text = payload(tenant, dataset, signal, event_id, attrs)
    return {
        "tenant_id": tenant,
        "dataset": dataset,
        "signal": signal,
        "event_id": event_id,
        "timestamp": TS,
        "schema_version": "1",
        "payload": text,
        "payload_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def typed(kind, value):
    return {"type": kind, "value": value}


def rows():
    big = typed("int", "9007199254740993")
    return [
        row(WS_A, "fixture", "logs", "typed-1", {
            "int.big": big, "double.one": typed("double", 1.0),
            "str.one": typed("string", "1"), "unicode": typed("string", "héllo—\U0001f30d"),
            "bytes.raw": typed("bytes", "AAH+/w=="), "empty.value": {"type": "empty"},
        }),
        row(WS_B, "fixture", "logs", "typed-1", {"int.big": typed("int", "7"), "marker": typed("string", "workspace-b")}),
        row(WS_A, "other", "logs", "typed-1", {"marker": typed("string", "dataset-other")}),
        row(WS_A, "fixture", "spans", "span-1", {"duration": typed("int", "1500")}),
        row(WS_B, "fixture", "logs", "only-b", {"marker": typed("string", "only-b")}),
        row(WS_A, "fixture", "logs", "dup-1", {"n": typed("int", "1")}),
        row(WS_A, "fixture", "logs", "dup-1", {"n": typed("int", "1")}),
        row(WS_A, "fixture", "logs", "conflict-1", {"n": typed("int", "1")}),
        row(WS_A, "fixture", "logs", "conflict-1", {"n": typed("int", "2")}),
        row(WS_A, "fixture", "logs", "odd id/1 é+%", {"marker": typed("string", "odd")}),
    ]


def main(argv):
    check = "--check" in argv
    args = [a for a in argv[1:] if a != "--check"]
    out_dir = Path(args[0]) if args else ROOT / "tests/fixtures/021"
    text = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows())
    path = out_dir / "canonical_events.jsonl"
    if check:
        if not path.exists() or path.read_text(encoding="utf-8") != text:
            print(f"{path} is stale; rerun without --check", file=sys.stderr)
            return 1
        return 0
    path.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
