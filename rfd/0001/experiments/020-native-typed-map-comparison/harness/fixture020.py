#!/usr/bin/env python3
"""Deterministic fixture and corpora for Experiment 020 (native typed Maps).

The semantic fixture is Experiment 019's independent typed inventory, imported
rather than copied so both experiments compare the same values on the same
rows: int 1 / double 1.0 / string "1", integers above 2^53 and the Int64 limits,
non-finite doubles and negative zero, bytes/arrays/maps, explicit empty versus
empty string versus missing, duplicate keys, case variants, resource/scope/
record namespace collisions, and span scope attributes on a separate scope.

This module adds the Experiment 020 cost corpora:

* `base` reuses Experiment 019's 50,000-row typed corpus (`represent.cost_event`)
  so the prior micro-corpus is measured unchanged.
* `diverse` is a bounded realistic attribute-diversity variation: sparse keys, a
  bounded key population, a realistic string-dominant type mix, and one key that
  is observed with different types across rows. It exists so that repeated type
  tags alone do not decide the comparison.

Everything is deterministic: no randomness, no wall-clock input. Expected query
results are computed from this inventory in `verify.py`, never from the mapping
code under test.

Commands:
  inventory   print the typed inventory as JSON
  hashes      print sha256 for the imported 019 fixture/encoding sources
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The 019 harness owns the typed inventory; importing it keeps one source of
# truth for the values both experiments compare. `verify.py` pins these hashes
# so a change to the 019 fixture fails this experiment instead of silently
# changing its meaning.
EXP019_HARNESS = HERE.parent.parent / "019" / "harness"
sys.path.insert(0, str(EXP019_HARNESS))

import fixture as fixture019  # noqa: E402  (path set above)
import represent as represent019  # noqa: E402

EVENTS = fixture019.EVENTS
SPAN_EVENTS = fixture019.SPAN_EVENTS

IMPORTED_SOURCES = ("fixture.py", "model.py", "represent.py")


def source_hashes() -> dict:
    out = {}
    for name in IMPORTED_SOURCES:
        path = EXP019_HARNESS / name
        out[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


# ---------------------------------------------------------------------------
# Cost corpora
# ---------------------------------------------------------------------------

def diverse_event(index: int) -> dict:
    """A bounded realistic event: sparse keys, mixed types, bounded cardinality.

    Key presence is a deterministic function of the row index, so the corpus is
    reproducible. `attr.mixed` is deliberately observed with different types
    across rows (integer in some rows, string in others), which is the
    type-change-across-events case the contract requires the query layer to
    keep distinct.
    """
    record = []
    if index % 2 == 0:
        record.append(("http.method", "string", ("GET", "POST", "PUT")[index % 3]))
    if index % 3 == 0:
        record.append(("http.route", "string",
                       ("/api/items", "/api/items/{id}", "/healthz")[index % 3]))
    if index % 10 < 7:
        record.append(("http.status_code", "int", (200, 404, 500)[index % 3]))
    if index % 4 == 0:
        record.append(("duration_ms", "double", (index % 1000) / 8.0))
    if index % 5 == 0:
        record.append(("error", "bool", index % 15 == 0))
    if index % 7 == 0:
        record.append(("k8s.pod.name", "string", f"pod-{index % 20:02d}"))
    if index % 6 == 0:
        record.append(("service.version", "string", ("1.0.0", "1.1.0", "2.0.0")[index % 3]))
    if index % 11 == 0:
        record.append(("db.statement", "string", "SELECT " + "x" * (index % 64)))
    if index % 13 == 0:
        record.append(("retry.count", "int", index % 4))
    if index % 17 == 0:
        record.append(("sampled", "bool", True))
    slot = index % 10
    if slot < 3:
        record.append(("attr.mixed", "int", index % 100))
    elif slot < 5:
        record.append(("attr.mixed", "string", f"v{index % 100}"))
    if index % 23 == 0:
        record.append(("empty.value", "empty", None))
    resource = [
        ("service.name", "string", ("api", "worker", "web")[index % 3]),
        ("service.version", "string", "1.0.0"),
        ("host.name", "string", f"host-{index % 8}"),
    ]
    scope = [("scope.attr", "string", "scope-value")]
    return {
        "event_id": f"diverse-{index}",
        "kind": "log",
        "time_ns": fixture019.TS + index * 1_000_000_000,
        "observed_ns": fixture019.TS + index * 1_000_000_000 + 1_000_000,
        "body": ("string", f"diverse-body-{index}"),
        "resource": resource,
        "scope": scope,
        "record": record,
        "severity": ("INFO", 9),
        "trace_id": None,
        "span_id": None,
        "duplicate": None,
        "event_name": None,
        "span_name": None,
        "start_ns": None,
        "end_ns": None,
    }


def corpus_events(corpus: str, count: int) -> list:
    if corpus == "base":
        # Experiment 019's cost corpus, reused unchanged.
        return [represent019.cost_event(index) for index in range(count)]
    if corpus == "diverse":
        return [diverse_event(index) for index in range(count)]
    raise SystemExit(f"unknown corpus: {corpus}")


CORPORA = ("base", "diverse")


def inventory():
    return {"events": EVENTS, "spans": SPAN_EVENTS}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("inventory", help="print the canonical typed inventory as JSON")
    sub.add_parser("hashes", help="print sha256 of the imported 019 sources")
    args = parser.parse_args(argv)

    if args.command == "inventory":
        print(json.dumps(inventory(), indent=2, default=fixture019._json_default))
        return 0
    if args.command == "hashes":
        for name, digest in source_hashes().items():
            print(f"{digest}\t{name}")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
