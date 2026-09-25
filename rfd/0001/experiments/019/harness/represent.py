#!/usr/bin/env python3
"""Load the Experiment 019 candidate representations into ClickHouse.

Two phases:

* `semantics` reads the *stock exporter's* Map rows back out of `stock_map`
  (so the baseline representation is genuinely stock-produced) and writes the
  same logical events into every candidate representation. The mapping code
  here is a harness prototype of a Lighthouse mapping step: it is what a custom
  processor or exporter would have to do, not a Collector component.
* `cost` loads a larger deterministic corpus into the `lh019_cost` database for
  the storage and execution-cost measurements.

This loader is also the B-style mapping/schema spike: it inserts a canonical
payload column and serving data in one statement. It does not exercise
exporterhelper, queues, retries, the failover connector, or acknowledgement
boundaries, so it cannot support compatibility claims about a custom exporter.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import requests

import fixture
import model

CH = "http://127.0.0.1:18124/"


def ch(db, sql, timeout=180):
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


def read_stock_logs(db="stock_map"):
    text = ch(db, "SELECT Body AS body, toString(Timestamp) AS ts, "
                  "ResourceAttributes AS resource, ScopeAttributes AS scope, "
                  "LogAttributes AS record FROM otel_logs FORMAT JSONEachRow")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def stock_event_maps(event):
    """(values, types) per namespace, from the fixture inventory."""
    maps = model.namespace_maps(event)
    return {name: (values, types) for name, (values, types) in maps.items()}


def build_semantics_rows(events, stock_rows):
    """Return (rows_by_table, rejections, payloads) for the semantic fixture."""
    by_body = {event["body"][1]: event for event in events}
    rejections = []
    payloads = {}
    rows = {name: [] for name in (
        "rep_stock_map", "rep_payload_only", "rep_type_map", "rep_tagged_map",
        "rep_map_carried")}
    for stock in stock_rows:
        event = by_body.get(stock["body"])
        if event is None:
            continue
        if event["duplicate"]:
            # The mapping step enforces the contract's duplicate-key rejection;
            # the stock exporter stored both copies (measured in the stock phase).
            for key, _, _ in event["duplicate"]:
                rejections.append({
                    "event_id": event["event_id"],
                    "reason": f"duplicate attribute key {key!r} in record namespace",
                })
            continue
        payload = model.canonical_payload(event)
        payloads[event["event_id"]] = payload
        maps = model.namespace_maps(event)
        envelope = {
            "event_id": event["event_id"],
            "kind": event["kind"],
            "ts": stock["ts"],
            "tenant_id": model.TENANT,
            "dataset": model.DATASET,
            "body": stock["body"],
        }
        rows["rep_stock_map"].append({
            **envelope,
            "resource": stock["resource"], "scope": stock["scope"], "record": stock["record"],
        })
        rows["rep_payload_only"].append({
            **envelope,
            "resource": stock["resource"], "scope": stock["scope"], "record": stock["record"],
            "payload": payload,
        })
        rows["rep_type_map"].append({
            **envelope,
            "resource": stock["resource"], "scope": stock["scope"], "record": stock["record"],
            "resource_types": maps["resource"][1],
            "scope_types": maps["scope"][1],
            "record_types": maps["record"][1],
            "payload": payload,
        })
        rows["rep_tagged_map"].append({
            **envelope,
            "resource": model.tagged_namespace(event, "resource"),
            "scope": model.tagged_namespace(event, "scope"),
            "record": model.tagged_namespace(event, "record"),
            "payload": payload,
        })
        carried_record = dict(stock["record"])
        carried_record[model.PAYLOAD_KEY] = payload
        carried_record[model.TYPES_KEY] = model.json_compact(maps["record"][1])
        carried_resource = dict(stock["resource"])
        carried_resource[model.TYPES_KEY] = model.json_compact(maps["resource"][1])
        carried_scope = dict(stock["scope"])
        carried_scope[model.TYPES_KEY] = model.json_compact(maps["scope"][1])
        rows["rep_map_carried"].append({
            **envelope,
            "resource": carried_resource, "scope": carried_scope, "record": carried_record,
        })
    return rows, rejections, payloads


def build_span_scope_rows(spans):
    rows = []
    for event in spans:
        if event["event_id"] != "span-scope":
            continue
        span_types = {key: type_name for key, type_name, _ in event["record"]}
        scope_types = {key: type_name for key, type_name, _ in event["scope"]}
        rows.append({
            "event_id": event["event_id"],
            "tenant_id": model.TENANT,
            "dataset": model.DATASET,
            "ts": f"2026-09-01 07:00:00.123456789",
            "span": {key: model.stock_string(t, v) for key, t, v in event["record"]},
            "span_types": span_types,
            "span_scope": {key: model.stock_string(t, v) for key, t, v in event["scope"]},
            "span_scope_types": scope_types,
        })
    return rows


# ---------------------------------------------------------------------------
# Cost corpus
# ---------------------------------------------------------------------------

def cost_event(index):
    """Deterministic typed event for the cost corpus."""
    attrs = [
        ("int.one", "int", index),
        ("double.one", "double", index / 4.0),
        ("str.one", "string", f"s{index}"),
        ("bool.false", "bool", index % 2 == 0),
        ("int.big", "int", fixture.INT_ABOVE_2_53 + index % 7),
        ("double.half", "double", 1.5),
        ("bytes.raw", "bytes", fixture.BYTES_VALUE),
        ("array.mixed", "array", ["a", index % 10, True]),
        ("map.nested", "map", {"nested.int": index % 100, "nested.bool": False}),
        ("empty.value", "empty", None),
        ("zero.int", "int", 0),
        ("str.short", "string", "v" * (index % 16)),
    ]
    resource = [
        ("service.name", "string", "cost-fixture"),
        ("service.version", "string", "1.0.0"),
        ("res.int64", "int", fixture.INT_ABOVE_2_53),
    ]
    scope = [("scope.attr", "string", "scope-value"), ("scope.int", "int", 7)]
    return {
        "event_id": f"cost-{index}",
        "kind": "log",
        "time_ns": fixture.TS + index * 1_000_000_000,
        "observed_ns": fixture.TS + index * 1_000_000_000 + 1_000_000,
        "body": ("string", f"cost-body-{index}"),
        "resource": resource,
        "scope": scope,
        "record": attrs,
        "severity": ("INFO", 9),
        "trace_id": None,
        "span_id": None,
        "duplicate": None,
        "event_name": None,
        "span_name": None,
        "start_ns": None,
        "end_ns": None,
    }


def build_cost_rows(count):
    rows = {name: [] for name in (
        "rep_stock_map", "rep_payload_only", "rep_type_map", "rep_tagged_map",
        "rep_map_carried")}
    for index in range(count):
        event = cost_event(index)
        payload = model.canonical_payload(event)
        maps = model.namespace_maps(event)
        envelope = {
            "event_id": event["event_id"],
            "kind": event["kind"],
            "ts": f"2026-09-01 07:{index // 60 % 60:02d}:{index % 60:02d}.000000000",
            "tenant_id": model.TENANT,
            "dataset": model.DATASET,
            "body": event["body"][1],
        }
        values = {name: maps[name][0] for name in ("resource", "scope", "record")}
        types = {name: maps[name][1] for name in ("resource", "scope", "record")}
        rows["rep_stock_map"].append({**envelope, **values})
        rows["rep_payload_only"].append({**envelope, **values, "payload": payload})
        rows["rep_type_map"].append({
            **envelope, **values,
            "resource_types": types["resource"],
            "scope_types": types["scope"],
            "record_types": types["record"],
            "payload": payload,
        })
        rows["rep_tagged_map"].append({
            **envelope,
            "resource": model.tagged_namespace(event, "resource"),
            "scope": model.tagged_namespace(event, "scope"),
            "record": model.tagged_namespace(event, "record"),
            "payload": payload,
        })
        carried = {name: dict(values[name]) for name in values}
        carried["record"][model.PAYLOAD_KEY] = payload
        carried["record"][model.TYPES_KEY] = model.json_compact(types["record"])
        carried["resource"][model.TYPES_KEY] = model.json_compact(types["resource"])
        carried["scope"][model.TYPES_KEY] = model.json_compact(types["scope"])
        rows["rep_map_carried"].append({**envelope, **carried})
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=["semantics", "cost"])
    parser.add_argument("--db", default="lh019")
    parser.add_argument("--results", default="results")
    parser.add_argument("--cost-rows", type=int, default=50_000)
    args = parser.parse_args(argv)

    results = Path(args.results)
    results.mkdir(parents=True, exist_ok=True)

    if args.phase == "semantics":
        stock_rows = read_stock_logs()
        rows, rejections, payloads = build_semantics_rows(fixture.EVENTS, stock_rows)
        span_rows = build_span_scope_rows(fixture.SPAN_EVENTS)
        for table, table_rows in rows.items():
            if table_rows:
                insert_rows(args.db, table, table_rows)
                print(f"inserted {len(table_rows)} rows into {table}")
        insert_rows(args.db, "rep_span_scope", span_rows)
        print(f"inserted {len(span_rows)} rows into rep_span_scope")
        (results / "mapping-rejections.jsonl").write_text(
            "".join(json.dumps(r) + "\n" for r in rejections))
        (results / "canonical-payloads.jsonl").write_text(
            "".join(json.dumps({"event_id": k, "payload": v}) + "\n" for k, v in sorted(payloads.items())))
        print(f"mapping prototype rejected {len(rejections)} record(s); "
              f"wrote {len(payloads)} canonical payloads")
        return 0

    rows = build_cost_rows(args.cost_rows)
    for table, table_rows in rows.items():
        insert_rows(args.db, table, table_rows)
        print(f"inserted {len(table_rows)} rows into {table}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
