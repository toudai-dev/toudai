#!/usr/bin/env python3
"""Compare the rows the Go path and toudai-ingest stored for one transport.

Each directory holds `serving_logs.jsonl`, `serving_spans.jsonl`, and
`canonical_events.jsonl`, dumped from one database with JSONEachRow and the
settings in run.sh, so the same value is written the same way on both sides.
Serving rows are joined on dataset, signal, and the record's `fixture.key`;
each serving row is joined to its own database's canonical row by `event_id`.

A difference is *expected* only when it is one `toudai-ingest` makes on purpose
(`ingest/src/mapping.rs` lists them), and then only in the exact form named
below. Everything else is unexpected, and the script exits 1 if there is any.
The identity each path assigns (`event_id`), and the payload hash that covers
it, always differ and are not compared. The Go path stores no span events, so
the content of ingest's `span.events` is checked against the fixture by
`ingest/tests/fixture_e2e.rs`; here only its shape is checked.

Harness-only code.
"""

from __future__ import annotations

import argparse
import copy
import json
import pathlib
import sys

SIGNALS = {"serving_logs": ("logs", "record_string"), "serving_spans": ("spans", "span_string")}
NOT_COMPARED = {"event_id", "payload_sha256"}
SCHEMAS = ("toudai.canonical.event/1", "toudai.canonical.event/2")  # (Go, ingest)


def load(path: pathlib.Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def by_key(rows: list[dict], key_map: str, problems: list[str], side: str, table: str) -> dict:
    out = {}
    for row in rows:
        key = (row["dataset"], row[key_map].get("fixture.key", ""))
        if key in out:
            problems.append(f"{side} {table}: {key} stored twice")
        out[key] = row
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", required=True, type=pathlib.Path)
    parser.add_argument("--ingest", required=True, type=pathlib.Path)
    parser.add_argument("--out", required=True, type=pathlib.Path)
    args = parser.parse_args()

    unexpected: list[str] = []
    expected: dict[str, int] = {}

    def allow(name: str) -> None:
        expected[name] = expected.get(name, 0) + 1

    canonical = {}
    for side, root in (("go", args.go), ("ingest", args.ingest)):
        rows = load(root / "canonical_events.jsonl")
        canonical[side] = {row["event_id"]: row for row in rows}
        if len(canonical[side]) != len(rows):
            unexpected.append(f"{side}: an event_id appears twice in canonical_events")

    counts = {}
    for table, (signal, key_map) in SIGNALS.items():
        go = by_key(load(args.go / f"{table}.jsonl"), key_map, unexpected, "go", table)
        ingest = by_key(load(args.ingest / f"{table}.jsonl"), key_map, unexpected, "ingest", table)
        counts[signal] = {"go": len(go), "ingest": len(ingest)}
        for key in sorted(go.keys() ^ ingest.keys()):
            unexpected.append(f"{table} {key}: stored by {'go' if key in go else 'ingest'} only")
        for key in sorted(go.keys() & ingest.keys()):
            g, i = go[key], ingest[key]
            for column in sorted((g.keys() | i.keys()) - NOT_COMPARED):
                if g.get(column) == i.get(column):
                    continue
                if (
                    column == "kind"
                    and g[column] == "log"
                    and i[column] == "event"
                    and i.get("event_name")
                ):
                    allow("serving kind: Go log, ingest event (application event)")
                    continue
                unexpected.append(f"{table} {key} {column}: go {g.get(column)!r}, ingest {i.get(column)!r}")

            gc, ic = canonical["go"].get(g["event_id"]), canonical["ingest"].get(i["event_id"])
            if gc is None or ic is None:
                unexpected.append(f"{table} {key}: no canonical row for its event_id")
                continue
            for c, row in (("go", gc), ("ingest", ic)):
                serving = g if c == "go" else i
                if row["payload_sha256"] != serving["payload_sha256"]:
                    unexpected.append(f"{c} {table} {key}: serving and canonical hashes differ")
                if json.loads(row["payload"])["identity"]["event_id"] != row["event_id"]:
                    unexpected.append(f"{c} {table} {key}: payload identity is not the row's event_id")
            for column in ("dataset", "signal", "timestamp"):
                if gc[column] != ic[column]:
                    unexpected.append(f"canonical {key} {column}: go {gc[column]!r}, ingest {ic[column]!r}")
            if (gc["schema_version"], ic["schema_version"]) == SCHEMAS:
                allow("schema_version: Go /1, ingest /2")
            else:
                unexpected.append(f"canonical {key} schema_version: go {gc['schema_version']!r}, ingest {ic['schema_version']!r}")

            gp, ip = json.loads(gc["payload"]), json.loads(ic["payload"])
            if (gp.get("schema"), ip.get("schema")) == SCHEMAS:
                allow("payload schema: Go /1, ingest /2")
            else:
                unexpected.append(f"canonical {key} payload schema: go {gp.get('schema')!r}, ingest {ip.get('schema')!r}")
            gp = copy.deepcopy(gp)
            for p in (gp, ip):
                p["schema"] = None
                p["identity"]["event_id"] = None
            if gp.get("kind") == "log" and ip.get("kind") == "event" and ip.get("log", {}).get("event_name"):
                allow("payload kind: Go log, ingest event (application event)")
                ip["kind"] = "log"
            events = ip.get("span", {}).get("events")
            if events is not None and "events" not in gp.get("span", {}):
                if not (isinstance(events, list) and all(isinstance(e, dict) and "name" in e for e in events)):
                    unexpected.append(f"canonical {key} span.events: not a list of events: {json.dumps(events)}")
                allow("payload span.events: kept by ingest only" + (" (non-empty)" if ip["span"]["events"] else " (empty list)"))
                del ip["span"]["events"]
            if gp != ip:
                unexpected.append(f"canonical {key} payload: go {json.dumps(gp, sort_keys=True)}, ingest {json.dumps(ip, sort_keys=True)}")

    for side in ("go", "ingest"):
        serving = sum(c[side] for c in counts.values())
        if len(canonical[side]) != serving:
            unexpected.append(f"{side}: {len(canonical[side])} canonical rows for {serving} serving rows")

    report = {"counts": counts, "expected_differences": expected, "unexpected": unexpected}
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"counts": counts, "expected": expected, "unexpected": len(unexpected)}, sort_keys=True))
    return 1 if unexpected else 0


if __name__ == "__main__":
    sys.exit(main())
