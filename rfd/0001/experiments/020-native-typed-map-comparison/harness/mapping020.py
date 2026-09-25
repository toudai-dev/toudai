#!/usr/bin/env python3
"""Mapping prototype for Experiment 020 (harness code, not a Collector component).

Builds, from the Experiment 019 typed inventory and the Experiment 020 cost
corpora, the rows for each candidate representation:

* `rep_companion`     Experiment 019's companion-type encoding: one string value
                      Map per namespace plus a per-key type Map. `model.py` from
                      Experiment 019 is reused as the encoding, not as an
                      oracle: it was validated against the pinned exporter in
                      Experiment 019, and `verify.py` never derives expectations
                      from it.
* `rep_native_typed`  per-type native value Maps plus a full per-key type
                      sidecar (presence and type for every key).
* `rep_native_minimal` per-type native value Maps plus a key-only `other` Map
                      for the canonical-only types (bytes, array, map) and
                      explicit empty; scalar presence/type is derived from the
                      typed value Maps.
* `rep_wrong_*`       deliberately wrong layouts for the assertion-sensitivity
                      checks. They are inserted without validation on purpose.

Contract rules enforced here, not by ClickHouse: a duplicate key inside one
incoming attribute collection is rejected, every stored key must appear in
exactly one typed value Map with a bookkeeping entry that matches, and
canonical-only keys never carry a typed value.

Phases:
  semantics  insert the semantic fixture into every candidate table
  cost       insert one corpus into the cost candidate tables, timing mapping
             and insert work
  wrong      insert the deliberately wrong layouts
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

import fixture020

# Experiment 019's inventory and encoding, imported rather than copied so both
# experiments use the same values. `model019` is mapping code under test here.
sys.path.insert(0, str(fixture020.EXP019_HARNESS))
import fixture as fixture019  # noqa: E402
import model as model019  # noqa: E402

CH = "http://127.0.0.1:18125/"
SCALARS = ("int", "double", "string", "bool")
CANONICAL_ONLY = ("bytes", "array", "map", "empty")
VARIANTS = ("companion", "native_typed", "native_minimal")
SPAN_TABLES = {
    "companion": "rep_span_companion",
    "native_typed": "rep_span_native_typed",
    "native_minimal": "rep_span_native_minimal",
}
COST_TABLES = {
    "companion": "rep_companion",
    "native_typed": "rep_native_typed",
    "native_minimal": "rep_native_minimal",
}


class MappingError(Exception):
    """Harness mapping-boundary rejection, not a ClickHouse error."""


def ch(db, sql, timeout=300):
    resp = requests.post(CH, params={"database": db}, data=sql.encode(), timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"clickhouse error {resp.status_code}: {resp.text[:500]}\nSQL: {sql[:300]}")
    return resp.text.strip()


def insert_rows(db, table, rows, batch=4000):
    for start in range(0, len(rows), batch):
        chunk = rows[start:start + batch]
        body = f"INSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(
            json.dumps(row, ensure_ascii=False) for row in chunk)
        ch(db, body)


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------

def namespace_pairs(event, namespace):
    pairs = list(event[namespace])
    if namespace == "record" and event["duplicate"]:
        pairs += list(event["duplicate"])
    return pairs


def native_namespace(pairs, bookkeeping):
    """Per-type value Maps plus bookkeeping for one namespace.

    bookkeeping `full` records every present key's type in the sidecar;
    `minimal` records only the canonical-only types and explicit empty.
    """
    values = {type_name: {} for type_name in SCALARS}
    book = {}
    seen = set()
    for key, type_name, value in pairs:
        if key in seen:
            raise MappingError(f"duplicate attribute key {key!r} in one attribute collection")
        seen.add(key)
        if type_name in values:
            values[type_name][key] = value
            if bookkeeping == "full":
                book[key] = type_name
        elif type_name in CANONICAL_ONLY:
            book[key] = type_name
        else:
            raise MappingError(f"unknown value type {type_name!r}")
    validate_native(values, book, bookkeeping)
    return values, book


def validate_native(values, book, bookkeeping):
    """One typed value Map per key; bookkeeping must agree with it."""
    owner = {}
    for type_name in SCALARS:
        for key in values[type_name]:
            if key in owner:
                raise MappingError(f"key {key!r} present in more than one typed value map")
            owner[key] = type_name
    if bookkeeping == "full":
        for key, type_name in book.items():
            if type_name in SCALARS:
                if owner.get(key) != type_name:
                    raise MappingError(
                        f"sidecar type {type_name!r} for key {key!r} does not match its typed value map")
            elif type_name not in CANONICAL_ONLY:
                raise MappingError(f"unknown sidecar type {type_name!r} for key {key!r}")
            elif key in owner:
                raise MappingError(f"canonical-only key {key!r} also has a typed value")
        for key in owner:
            if key not in book:
                raise MappingError(f"key {key!r} has a typed value but no sidecar type")
    else:
        for key, type_name in book.items():
            if type_name not in CANONICAL_ONLY:
                raise MappingError(
                    f"minimal bookkeeping only carries canonical-only types, got {type_name!r} for key {key!r}")
            if key in owner:
                raise MappingError(f"key {key!r} is both a typed value and canonical-only bookkeeping")


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------

def _ts_text(time_ns):
    delta = time_ns - fixture019.TS
    seconds, nanos = divmod(delta, 1_000_000_000)
    base = datetime(2026, 9, 1, 7, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds)
    return base.strftime("%Y-%m-%d %H:%M:%S") + f".{nanos:09d}"


def _envelope(event):
    return {
        "event_id": event["event_id"],
        "kind": event["kind"],
        "ts": _ts_text(event["time_ns"]),
        "tenant_id": model019.TENANT,
        "dataset": model019.DATASET,
        "body": event["body"][1],
    }


def companion_row(event):
    row = _envelope(event)
    maps = model019.namespace_maps(event)
    for namespace in ("resource", "scope", "record"):
        row[namespace] = maps[namespace][0]
        row[f"{namespace}_types"] = maps[namespace][1]
    row["payload"] = model019.canonical_payload(event)
    return row


def native_row(event, bookkeeping):
    row = _envelope(event)
    for namespace in ("resource", "scope", "record"):
        values, book = native_namespace(namespace_pairs(event, namespace), bookkeeping)
        for type_name in SCALARS:
            row[f"{namespace}_{type_name}"] = values[type_name]
        row[f"{namespace}_{'types' if bookkeeping == 'full' else 'other'}"] = book
    row["payload"] = model019.canonical_payload(event)
    return row


def companion_span_row(event):
    maps = model019.namespace_maps(event)
    return {
        "event_id": event["event_id"],
        "tenant_id": model019.TENANT,
        "dataset": model019.DATASET,
        "ts": _ts_text(event["time_ns"]),
        "span_name": event["span_name"],
        "span": maps["record"][0],
        "span_types": maps["record"][1],
        "span_scope": maps["scope"][0],
        "span_scope_types": maps["scope"][1],
    }


def native_span_row(event, bookkeeping):
    row = {
        "event_id": event["event_id"],
        "tenant_id": model019.TENANT,
        "dataset": model019.DATASET,
        "ts": _ts_text(event["time_ns"]),
        "span_name": event["span_name"],
    }
    for namespace, prefix in (("record", "span"), ("scope", "span_scope")):
        values, book = native_namespace(namespace_pairs(event, namespace), bookkeeping)
        for type_name in SCALARS:
            row[f"{prefix}_{type_name}"] = values[type_name]
        row[f"{prefix}_{'types' if bookkeeping == 'full' else 'other'}"] = book
    return row


def variant_row(variant, event):
    if variant == "companion":
        return companion_row(event)
    if variant == "native_typed":
        return native_row(event, "full")
    if variant == "native_minimal":
        return native_row(event, "minimal")
    raise SystemExit(f"unknown variant {variant}")


# ---------------------------------------------------------------------------
# Deliberately wrong layouts (assertion-sensitivity evidence only)
# ---------------------------------------------------------------------------

def wrong_float_only_rows(events):
    """Plausible mistake: integer values collapsed into the Float64 map.

    The type sidecar still says `int`, so a typed int filter finds nothing, and
    the float map now holds both int 1 and double 1.0. Inserted without the
    validation the real mapping applies.
    """
    rows = []
    for event in events:
        if event["duplicate"]:
            continue
        row = native_row(event, "full")
        for namespace in ("resource", "scope", "record"):
            ints = row.pop(f"{namespace}_int")
            doubles = dict(row[f"{namespace}_double"])
            for key, value in ints.items():
                doubles[key] = float(value)
            row[f"{namespace}_double"] = doubles
            row[f"{namespace}_int"] = {}
        rows.append(row)
    return rows


def wrong_double_presence_rows(events):
    """Plausible mistake: one key written into two typed value maps.

    The first integer key of each row is also written into the string map,
    while the sidecar still records a single type. A union presence query then
    counts the key twice; ClickHouse cannot detect it.
    """
    rows = []
    for event in events:
        if event["duplicate"]:
            continue
        row = native_row(event, "full")
        ints = row["record_int"]
        if ints:
            key = "int.one" if "int.one" in ints else next(iter(ints))
            row["record_string"][key] = str(ints[key])
        rows.append(row)
    return rows


def crafted_rejections():
    """Mapping-boundary rejections the harness must produce (fail closed)."""
    cases = []

    def attempt(case, fn):
        try:
            fn()
        except MappingError as exc:
            cases.append({"case": case, "reason": str(exc)})
        else:
            cases.append({"case": case, "reason": "NOT-REJECTED"})

    attempt("duplicate-same-type",
            lambda: native_namespace([("k", "int", 1), ("k", "int", 2)], "full"))
    attempt("duplicate-different-type",
            lambda: native_namespace([("k", "int", 1), ("k", "string", "1")], "full"))
    attempt("cross-map-conflict",
            lambda: validate_native({"int": {"k": 1}, "double": {}, "string": {"k": "1"}, "bool": {}},
                                    {"k": "int"}, "full"))
    attempt("sidecar-type-mismatch",
            lambda: validate_native({"int": {"k": 1}, "double": {}, "string": {}, "bool": {}},
                                    {"k": "string"}, "full"))
    attempt("canonical-only-also-typed",
            lambda: validate_native({"int": {"k": 1}, "double": {}, "string": {}, "bool": {}},
                                    {"k": "bytes"}, "full"))
    attempt("missing-sidecar-entry",
            lambda: validate_native({"int": {"k": 1}, "double": {}, "string": {}, "bool": {}},
                                    {}, "full"))
    attempt("minimal-bookkeeping-scalar",
            lambda: validate_native({"int": {}, "double": {}, "string": {}, "bool": {}},
                                    {"k": "int"}, "minimal"))
    return cases


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def build_semantics_rows():
    rejections = []
    payloads = {}
    rows = {f"rep_{variant}": [] for variant in VARIANTS}
    span_rows = {SPAN_TABLES[variant]: [] for variant in VARIANTS}
    for event in fixture020.EVENTS:
        if event["duplicate"]:
            for key, _, _ in event["duplicate"]:
                rejections.append({
                    "event_id": event["event_id"],
                    "reason": f"duplicate attribute key {key!r} in record namespace",
                })
            continue
        payloads[event["event_id"]] = model019.canonical_payload(event)
        for variant in VARIANTS:
            rows[f"rep_{variant}"].append(variant_row(variant, event))
    for event in fixture020.SPAN_EVENTS:
        span_rows["rep_span_companion"].append(companion_span_row(event))
        span_rows["rep_span_native_typed"].append(native_span_row(event, "full"))
        span_rows["rep_span_native_minimal"].append(native_span_row(event, "minimal"))
    return rows, span_rows, rejections, payloads


def phase_semantics(db, results):
    rows, span_rows, rejections, payloads = build_semantics_rows()
    for table, table_rows in rows.items():
        insert_rows(db, table, table_rows)
        print(f"inserted {len(table_rows)} rows into {table}")
    for table, table_rows in span_rows.items():
        insert_rows(db, table, table_rows)
        print(f"inserted {len(table_rows)} rows into {table}")
    crafted = crafted_rejections()
    (results / "mapping-rejections.jsonl").write_text(
        "".join(json.dumps(item) + "\n" for item in rejections + crafted))
    (results / "canonical-payloads.jsonl").write_text(
        "".join(json.dumps({"event_id": key, "payload": value}) + "\n"
                for key, value in sorted(payloads.items())))
    print(f"mapping prototype rejected {len(rejections)} record(s) and "
          f"{len(crafted)} crafted mapping case(s); wrote {len(payloads)} canonical payloads")


def phase_cost(db, corpus, count, results):
    events = fixture020.corpus_events(corpus, count)
    entries = []
    for variant in VARIANTS:
        started = time.monotonic()
        rows = [variant_row(variant, event) for event in events]
        mapped = time.monotonic()
        insert_rows(db, COST_TABLES[variant], rows)
        inserted = time.monotonic()
        entry = {
            "corpus": corpus,
            "variant": variant,
            "rows": len(rows),
            "mapping_seconds": round(mapped - started, 3),
            "insert_seconds": round(inserted - mapped, 3),
            "rows_per_second": round(len(rows) / (inserted - started), 1) if inserted > started else None,
        }
        entries.append(entry)
        print(f"cost {corpus}/{variant}: {entry['rows']} rows, "
              f"mapping {entry['mapping_seconds']}s, insert {entry['insert_seconds']}s")
    with (results / "cost-insert.jsonl").open("a") as handle:
        for entry in entries:
            handle.write(json.dumps(entry) + "\n")


def phase_wrong(db):
    insert_rows(db, "rep_wrong_float_only", wrong_float_only_rows(fixture020.EVENTS))
    insert_rows(db, "rep_wrong_double_presence", wrong_double_presence_rows(fixture020.EVENTS))
    print("inserted the deliberately wrong layouts")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=["semantics", "cost", "wrong"])
    parser.add_argument("--db", default="lh020")
    parser.add_argument("--results", default="results")
    parser.add_argument("--corpus", choices=fixture020.CORPORA, default="base")
    parser.add_argument("--count", type=int, default=50_000)
    args = parser.parse_args(argv)

    results = Path(args.results)
    results.mkdir(parents=True, exist_ok=True)

    if args.phase == "semantics":
        phase_semantics(args.db, results)
        return 0
    if args.phase == "cost":
        phase_cost(args.db, args.corpus, args.count, results)
        return 0
    if args.phase == "wrong":
        phase_wrong(args.db)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
