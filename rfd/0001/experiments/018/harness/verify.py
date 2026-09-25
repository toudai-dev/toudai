#!/usr/bin/env python3
"""Assertion harness for Experiment 018.

Every check is a (name, SQL or file probe, expected) triple evaluated against
the disposable ClickHouse instance and the files written by run.sh. Expectations
are the values recorded by the exploratory run and re-asserted on a second run
so drift in the stock components is visible; --capture dumps observations
without asserting.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import requests
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2

CH_URL = "http://127.0.0.1:18123/"
DB = "lh018"

# Fixture constants mirrored from fixture.py.
WS_A = "11111111-1111-4111-8111-111111111111"
WS_B = "22222222-2222-4222-8222-222222222222"
WS_FORGED = "99999999-9999-4999-8999-999999999999"
TS_1 = "2026-09-01 07:00:00.123456789"
TS_2 = "2026-09-02 07:00:00.123456789"
NEGATIVE_DURATION = "18446744073709550616"  # 2^64 - 1000
INT_ABOVE_2_53 = "9007199254740993"
FLOAT64_OF_INT_ABOVE_2_53 = "9007199254740992"
MAP_BODY_JSON = '{"k":"v","n":1}'


def ch(sql, timeout=60):
    resp = requests.post(CH_URL, params={"database": DB, "default_format": "TSV"},
                         data=sql.encode(), timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"clickhouse query failed ({resp.status_code}): {resp.text[:400]}\nSQL: {sql}")
    return resp.text.strip()


LOG = "lh018.otel_logs"
TRACE = "lh018.otel_traces"


def log_attr(key, body):
    return f"SELECT LogAttributes['{key}'] FROM {LOG} WHERE Body = '{body}'"


def res_attr(key, body):
    return f"SELECT ResourceAttributes['{key}'] FROM {LOG} WHERE Body = '{body}'"


TYPED_CHECKS = [
    # -- receiver/exporter boundary and resource separation -------------------
    ("typed.log.row_count", f"SELECT count() FROM {LOG}", "9"),
    ("typed.log.timestamp_ns_precision",
     f"SELECT toString(Timestamp) FROM {LOG} WHERE Body='typed-log-1'", TS_1),
    ("typed.log.service_name",
     f"SELECT ServiceName FROM {LOG} WHERE Body='typed-log-1'", "fixture-log-service"),
    ("typed.log.no_service_name_is_empty",
     f"SELECT ServiceName FROM {LOG} WHERE Body='no-service-name-log'", ""),
    ("typed.log.resource_schema_url",
     f"SELECT ResourceSchemaUrl FROM {LOG} WHERE Body='typed-log-1'",
     "https://opentelemetry.io/schemas/1.39.0"),
    ("typed.log.scope_schema_url",
     f"SELECT ScopeSchemaUrl FROM {LOG} WHERE Body='typed-log-1'",
     "https://opentelemetry.io/schemas/1.39.0"),
    ("typed.log.scope_name", f"SELECT ScopeName FROM {LOG} WHERE Body='typed-log-1'", "lighthouse.fixture"),
    ("typed.log.scope_version", f"SELECT ScopeVersion FROM {LOG} WHERE Body='typed-log-1'", "0.1.0"),
    ("typed.log.scope_attr_string", f"SELECT ScopeAttributes['scope.attr'] FROM {LOG} WHERE Body='typed-log-1'", "scope-value"),
    ("typed.log.scope_attr_int_stringified", f"SELECT ScopeAttributes['scope.int'] FROM {LOG} WHERE Body='typed-log-1'", "7"),
    ("typed.log.scope_empty_present",
     f"SELECT mapContains(ScopeAttributes,'scope.empty') FROM {LOG} WHERE Body='typed-log-1'", "1"),
    # -- resource attribute types --------------------------------------------
    ("typed.res.int_above_2_53", res_attr("res.int64", "typed-log-1"), INT_ABOVE_2_53),
    ("typed.res.bool", res_attr("res.bool", "typed-log-1"), "true"),
    ("typed.res.float_int_like", res_attr("res.float", "typed-log-1"), "1"),
    ("typed.res.string_one", res_attr("res.str_one", "typed-log-1"), "1"),
    ("typed.res.bytes", res_attr("res.bytes", "typed-log-1"), "AAH+/w=="),
    ("typed.res.array", res_attr("res.array", "typed-log-1"), '["a",1,true]'),
    ("typed.res.map", res_attr("res.map", "typed-log-1"), '{"nested.bool":false,"nested.int":2}'),
    ("typed.res.empty_present", f"SELECT mapContains(ResourceAttributes,'res.empty') FROM {LOG} WHERE Body='typed-log-1'", "1"),
    ("typed.res.empty_is_empty_string", res_attr("res.empty", "typed-log-1"), ""),
    ("typed.res.empty_string_is_empty_string", res_attr("res.empty_string", "typed-log-1"), ""),
    ("typed.res.case_sensitive_upper", res_attr("Foo", "typed-log-1"), "resource-upper"),
    ("typed.res.case_sensitive_lower", res_attr("foo", "typed-log-1"), "resource-lower"),
    # -- namespace separation -------------------------------------------------
    ("typed.namespace.resource_collide", res_attr("res.collide", "typed-log-1"), "resource-value"),
    ("typed.namespace.record_collide", log_attr("collide", "typed-log-1"), "record-value"),
    # -- record attribute types ----------------------------------------------
    ("typed.attr.int_big", log_attr("int.big", "typed-log-1"), INT_ABOVE_2_53),
    ("typed.attr.int_negative", log_attr("int.neg", "typed-log-1"), "-7"),
    ("typed.attr.double_int_like", log_attr("double.one", "typed-log-1"), "1"),
    ("typed.attr.double_half", log_attr("double.half", "typed-log-1"), "1.5"),
    ("typed.attr.double_nan", log_attr("double.nan", "typed-log-1"), "NaN"),
    ("typed.attr.double_posinf", log_attr("double.posinf", "typed-log-1"), "Infinity"),
    ("typed.attr.double_neginf", log_attr("double.neginf", "typed-log-1"), "-Infinity"),
    ("typed.attr.bool_true", log_attr("bool.true", "typed-log-1"), "true"),
    ("typed.attr.bool_false", log_attr("bool.false", "typed-log-1"), "false"),
    ("typed.attr.string_one", log_attr("str.one", "typed-log-1"), "1"),
    # int 1, double 1.0, and string "1" all land as the same Map string.
    ("typed.attr.int_double_string_indistinguishable",
     f"SELECT LogAttributes['int.one'] = LogAttributes['double.one'] AND LogAttributes['str.one'] = LogAttributes['double.one'] FROM {LOG} WHERE Body='typed-log-1'", "1"),
    ("typed.attr.empty_string_present", f"SELECT mapContains(LogAttributes,'str.empty') FROM {LOG} WHERE Body='typed-log-1'", "1"),
    ("typed.attr.empty_string_value", log_attr("str.empty", "typed-log-1"), ""),
    ("typed.attr.zero_int", log_attr("zero.int", "typed-log-1"), "0"),
    ("typed.attr.neg_zero_float", log_attr("zero.float", "typed-log-1"), "-0"),
    ("typed.attr.bytes", log_attr("bytes.raw", "typed-log-1"), "AAH+/w=="),
    ("typed.attr.array", log_attr("array.mixed", "typed-log-1"), '["a",1,true]'),
    ("typed.attr.map", log_attr("map.nested", "typed-log-1"), '{"nested.bool":false,"nested.int":2}'),
    # A producer string that already looks like the JSON text of a map is stored
    # identically to the real map value.
    ("typed.attr.json_like_string_collides_with_map",
     f"SELECT LogAttributes['str.json_like'] = LogAttributes['map.nested'] FROM {LOG} WHERE Body='typed-log-1'", "1"),
    ("typed.attr.explicit_empty_present", f"SELECT mapContains(LogAttributes,'empty.value') FROM {LOG} WHERE Body='typed-log-1'", "1"),
    ("typed.attr.explicit_empty_value", log_attr("empty.value", "typed-log-1"), ""),
    ("typed.attr.missing_absent", f"SELECT mapContains(LogAttributes,'missing.key') FROM {LOG} WHERE Body='typed-log-1'", "0"),
    ("typed.attr.unicode", log_attr("unicode", "typed-log-1"), "héllo—\U0001f30d"),
    ("typed.attr.long_string_length", f"SELECT length(LogAttributes['long.string']) FROM {LOG} WHERE Body='typed-log-1'", "1024"),
    # -- duplicate keys: both copies are stored, the lookup sees the first ----
    ("typed.attr.duplicate_keys_lookup_returns_first",
     f"SELECT LogAttributes['dup'] FROM {LOG} WHERE Body='duplicate-key-log'", "first"),
    ("typed.attr.duplicate_keys_both_stored",
     f"SELECT countEqual(mapKeys(LogAttributes),'dup') FROM {LOG} WHERE Body='duplicate-key-log'", "2"),
    ("typed.attr.duplicate_keys_map_size",
     f"SELECT length(LogAttributes) FROM {LOG} WHERE Body='duplicate-key-log'", "3"),
    # -- bodies ---------------------------------------------------------------
    ("typed.body.bytes_base64", f"SELECT Body FROM {LOG} WHERE Body='AAH+/w=='", "AAH+/w=="),
    ("typed.body.int", f"SELECT Body FROM {LOG} WHERE Body='42'", "42"),
    ("typed.body.map_json", f"SELECT Body FROM {LOG} WHERE Body='{MAP_BODY_JSON}'", MAP_BODY_JSON),
    # -- timestamps and trace correlation ------------------------------------
    ("typed.log.observed_timestamp_fallback",
     f"SELECT toString(Timestamp) FROM {LOG} WHERE Body='timestamp-fallback-log'", TS_2),
    ("typed.log.trace_id",
     f"SELECT TraceId FROM {LOG} WHERE Body='trace-correlated-log'",
     "4bf92f3577b34da6a3ce929d0e0e4736"),
    ("typed.log.span_id",
     f"SELECT SpanId FROM {LOG} WHERE Body='trace-correlated-log'", "00f067aa0ba902b7"),
    ("typed.log.trace_flags",
     f"SELECT TraceFlags FROM {LOG} WHERE Body='trace-correlated-log'", "1"),
    ("typed.log.event_name",
     f"SELECT EventName FROM {LOG} WHERE Body='event-name-log'", "fixture.event"),
    ("typed.log.event_name_absent_is_empty",
     f"SELECT EventName FROM {LOG} WHERE Body='typed-log-1'", ""),
    ("typed.log.severity",
     f"SELECT SeverityText, SeverityNumber FROM {LOG} WHERE LogAttributes['body.bytes.test']='yes'", "WARN\t13"),
    # -- spans ----------------------------------------------------------------
    ("typed.trace.row_count", f"SELECT count() FROM {TRACE}", "5"),
    ("typed.trace.duration_ok", f"SELECT Duration FROM {TRACE} WHERE SpanName='span/ok'", "1500"),
    ("typed.trace.duration_zero", f"SELECT Duration FROM {TRACE} WHERE SpanName='span/zero-duration'", "0"),
    ("typed.trace.duration_negative_underflow",
     f"SELECT Duration FROM {TRACE} WHERE SpanName='span/negative-duration'", NEGATIVE_DURATION),
    ("typed.trace.duration_missing_timestamps",
     f"SELECT Duration FROM {TRACE} WHERE SpanName='span/no-timestamps'", "0"),
    ("typed.trace.span_int", f"SELECT SpanAttributes['span.int'] FROM {TRACE} WHERE SpanName='span/ok'", INT_ABOVE_2_53),
    ("typed.trace.span_bool", f"SELECT SpanAttributes['span.bool'] FROM {TRACE} WHERE SpanName='span/ok'", "true"),
    ("typed.trace.span_str", f"SELECT SpanAttributes['span.str'] FROM {TRACE} WHERE SpanName='span/ok'", "1"),
    ("typed.trace.span_bytes", f"SELECT SpanAttributes['span.bytes'] FROM {TRACE} WHERE SpanName='span/ok'", "AAH+/w=="),
    ("typed.trace.span_map", f"SELECT SpanAttributes['span.map'] FROM {TRACE} WHERE SpanName='span/ok'", '{"nested":"v"}'),
    ("typed.trace.event", f"SELECT Events.Name, Events.Attributes FROM {TRACE} WHERE SpanName='span/ok'",
     "['span/event']\t[{'ev.attr':'e'}]"),
    ("typed.trace.link", f"SELECT Links.SpanId, Links.TraceState, Links.Attributes FROM {TRACE} WHERE SpanName='span/ok'",
     "['00f067aa0ba90201']\t['l=1']\t[{'link.attr':'1'}]"),
    ("typed.trace.trace_state", f"SELECT TraceState FROM {TRACE} WHERE SpanName='span/ok'", "vendor=value"),
    ("typed.trace.kind", f"SELECT SpanKind FROM {TRACE} WHERE SpanName='span/ok'", "Server"),
    ("typed.trace.status", f"SELECT StatusCode FROM {TRACE} WHERE SpanName='span/ok'", "Ok"),
    ("typed.trace.parent_span_id", f"SELECT ParentSpanId FROM {TRACE} WHERE SpanName='span/ok'", "00f067aa0ba90200"),
    # Scope attributes have no traces column; this asserts the schema gap.
    ("typed.trace.no_scope_attributes_column",
     "SELECT count() FROM system.columns WHERE database='lh018' AND table='otel_traces' AND name='ScopeAttributes'", "0"),
    ("typed.trace.scope_name_present", f"SELECT ScopeName FROM {TRACE} WHERE SpanName='span/scope-attrs'", "lighthouse.fixture.spans"),
    # -- metrics: separate tables --------------------------------------------
    ("typed.metrics.gauge_tables",
     "SELECT count() FROM system.tables WHERE database='lh018' AND name LIKE 'otel_metrics%'", "5"),
    ("typed.metrics.gauge_value",
     "SELECT Value FROM lh018.otel_metrics_gauge WHERE MetricName='fixture.gauge' AND Attributes['metric.attr']='g'", "1.5"),
    ("typed.metrics.gauge_int_above_2_53_becomes_float64",
     "SELECT Value FROM lh018.otel_metrics_gauge WHERE MetricName='fixture.gauge' AND Attributes['metric.attr']='g-int'",
     FLOAT64_OF_INT_ABOVE_2_53),
    ("typed.metrics.gauge_attr",
     "SELECT Attributes['metric.attr'] FROM lh018.otel_metrics_gauge WHERE MetricName='fixture.gauge' AND Attributes['metric.attr']='g'", "g"),
    ("typed.metrics.gauge_scope_attr_present",
     "SELECT ScopeAttributes['scope.int'] FROM lh018.otel_metrics_gauge WHERE MetricName='fixture.gauge' AND Attributes['metric.attr']='g'", "7"),
    ("typed.metrics.sum",
     "SELECT Value, IsMonotonic, AggregationTemporality FROM lh018.otel_metrics_sum WHERE MetricName='fixture.counter'",
     "42\ttrue\t2"),
    ("typed.metrics.histogram",
     "SELECT Count, Sum, BucketCounts, ExplicitBounds, Min, Max FROM lh018.otel_metrics_histogram WHERE MetricName='fixture.histogram'",
     "3\t6\t[1,1,1]\t[1,2]\t0.5\t2.5"),
]

IDENTITY_CHECKS = [
    ("identity.ws_a_trusted_value",
     f"SELECT ResourceAttributes['lighthouse.workspace_id'] FROM {LOG} WHERE Body='identity-ws-a-log'", WS_A),
    ("identity.ws_b_trusted_value",
     f"SELECT ResourceAttributes['lighthouse.workspace_id'] FROM {LOG} WHERE Body='identity-ws-b-log'", WS_B),
    ("identity.forged_producer_value_absent",
     f"SELECT count() FROM {LOG} WHERE ResourceAttributes['lighthouse.workspace_id'] = '{WS_FORGED}'", "0"),
    ("identity.producer_attr_preserved",
     f"SELECT LogAttributes['producer.attr'] FROM {LOG} WHERE Body='identity-ws-a-log'", "a"),
    ("identity.raw_key_not_in_resource_attributes",
     f"SELECT count() FROM {LOG} WHERE arrayStringConcat(mapValues(ResourceAttributes),' ') LIKE '%lh018-ws%'", "0"),
    ("identity.raw_key_not_in_record_attributes",
     f"SELECT count() FROM {LOG} WHERE arrayStringConcat(mapValues(LogAttributes),' ') LIKE '%lh018-ws%'", "0"),
    ("identity.single_row_per_body",
     f"SELECT count() FROM {LOG} WHERE Body IN ('identity-ws-a-log','identity-ws-b-log')", "2"),
    ("identity.trace_workspace",
     f"SELECT ResourceAttributes['lighthouse.workspace_id'] FROM {TRACE} "
     "WHERE SpanName='span/ok' AND ResourceAttributes['lighthouse.workspace_id'] != ''", WS_A),
    ("identity.materialized_column",
     f"SELECT ws_materialized FROM {LOG} WHERE Body='identity-ws-a-log'", WS_A),
]

FAILOVER_CHECKS = [
    # fault-4 is the fresh request sent after ClickHouse recovered; fault-log-3
    # exists only in that fixture, so one row proves the primary path resumed
    # while the spilled fault-2/fault-3 backlog was still in object storage.
    ("failover.fresh_row_landed",
     f"SELECT count() FROM {LOG} WHERE Body='fault-log-3'", "1"),
    ("failover.total_rows_after_two_replays",
     f"SELECT count() FROM {LOG} WHERE Body LIKE 'fault-log-%'", "9"),
    ("failover.replay_duplicates",
     f"SELECT count() FROM {LOG} WHERE Body='fault-log-0'", "4"),
    ("failover.replayed_workspace_distinct",
     f"SELECT DISTINCT ResourceAttributes['lighthouse.workspace_id'] FROM {LOG} WHERE Body LIKE 'fault-log-%'", WS_A),
]

FORGE_CHECKS = [
    ("forge.producer_header_overrides_owner",
     f"SELECT ResourceAttributes['lighthouse.workspace_id'] FROM {LOG} WHERE Body='identity-forge-log'", WS_B),
    # bearertokenauth.Authenticate returns the context unchanged (extension
    # source), so `from_context: auth.token` has no auth data to read and the
    # attribute is not created at all.
    ("forge.auth_context_absent",
     f"SELECT mapContains(LogAttributes,'lighthouse.auth_identity') FROM {LOG} WHERE Body='identity-forge-log'", "0"),
]

FAULT_CHECKS = [
    ("fault.permanent_error_rows_dropped",
     "SELECT count() FROM lh018.otel_logs_broken", "0"),
]


def load_json(path):
    return json.loads(Path(path).read_text())


def load_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def overflow_object_checks(results):
    """Evidence from the spilled OTLP object itself."""
    path = Path(results) / "overflow-object-1.binpb"
    keys_path = Path(results) / "overflow-keys-after-outage.txt"
    checks = []
    if keys_path.exists():
        # `aws --output text` prints multiple keys tab-separated on one line.
        keys = [key for key in keys_path.read_text().split() if not key.startswith("path")]
        checks.append(("failover.overflow_keys_after_outage", str(len(keys)), "1"))
        if keys:
            checks.append(("failover.overflow_key_prefix", keys[0].split("/")[0], "overflow"))
    if not path.exists() or path.stat().st_size == 0:
        checks.append(("failover.overflow_object_present", "missing", "present"))
        return checks
    req = logs_service_pb2.ExportLogsServiceRequest()
    req.ParseFromString(path.read_bytes())
    bodies = []
    workspaces = set()
    for rl in req.resource_logs:
        for kv in rl.resource.attributes:
            if kv.key == "lighthouse.workspace_id":
                workspaces.add(kv.value.string_value)
        for sl in rl.scope_logs:
            for lr in sl.log_records:
                bodies.append(lr.body.string_value)
    checks.append(("failover.overflow_object_workspace", ",".join(sorted(workspaces)), WS_A))
    checks.append(("failover.overflow_object_bodies", ",".join(sorted(bodies)), "fault-log-0,fault-log-1"))
    return checks


def received_file_checks(results):
    """Receiver-side evidence: the file exporter copy keeps the OTLP types."""
    checks = []
    logs_path = Path(results) / "typed-received-logs.jsonl"
    traces_path = Path(results) / "typed-received-traces.jsonl"
    if not logs_path.exists() or not traces_path.exists():
        return [("received.files_present", "missing", "present")]

    records = {}
    count = 0
    for line in logs_path.read_text().splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        for rl in obj.get("resourceLogs", []):
            res = {kv["key"]: kv["value"] for kv in rl["resource"]["attributes"]}
            for sl in rl.get("scopeLogs", []):
                scope = {kv["key"]: kv["value"] for kv in sl["scope"].get("attributes", [])}
                for lr in sl.get("logRecords", []):
                    body = lr.get("body", {}).get("stringValue", "")
                    records[body] = (res, scope, lr)
                    count += 1

    checks.append(("received.log_record_count", str(count), "9"))
    typed_res, typed_scope, typed_lr = records.get("typed-log-1", ({}, {}, {}))
    typed_attrs = {kv["key"]: kv["value"] for kv in typed_lr.get("attributes", [])}
    checks.append(("received.res.int64_kept_as_int", json.dumps(typed_res.get("res.int64")),
                   '{"intValue": "9007199254740993"}'))
    checks.append(("received.res.bytes_kept_as_bytes", json.dumps(typed_res.get("res.bytes")),
                   '{"bytesValue": "AAH+/w=="}'))
    checks.append(("received.res.empty_kept_as_empty", json.dumps(typed_res.get("res.empty")), "{}"))
    checks.append(("received.res.map_kept_as_kvlist", str("kvlistValue" in typed_res.get("res.map", {})), "True"))
    checks.append(("received.res.array_kept_as_array", str("arrayValue" in typed_res.get("res.array", {})), "True"))
    checks.append(("received.attr.nan_kept_as_double", json.dumps(typed_attrs.get("double.nan")),
                   '{"doubleValue": "NaN"}'))
    checks.append(("received.attr.map_kept_as_kvlist", str("kvlistValue" in typed_attrs.get("map.nested", {})), "True"))
    checks.append(("received.scope_attrs_separate", json.dumps(typed_scope.get("scope.int")), '{"intValue": "7"}'))
    checks.append(("received.resource_attr_present", str("res.collide" in typed_res), "True"))
    checks.append(("received.record_attr_present", str("collide" in typed_attrs), "True"))
    checks.append(("received.duplicate_keys_kept_as_sent",
                   json.dumps([kv["value"].get("stringValue") for kv in typed_lr.get("attributes", [])
                               if kv["key"] == "dup"]), "[]"))
    dup_lr = records.get("duplicate-key-log", ({}, {}, {}))[2]
    checks.append(("received.duplicate_keys_present_twice",
                   json.dumps([kv["value"].get("stringValue") for kv in dup_lr.get("attributes", [])
                               if kv["key"] == "dup"]), '["first", "second"]'))

    spans = {}
    for line in traces_path.read_text().splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        for rl in obj.get("resourceSpans", []):
            for sl in rl.get("scopeSpans", []):
                for span in sl.get("spans", []):
                    spans[span.get("name")] = span
    ok = spans.get("span/ok", {})
    checks.append(("received.span_timing_raw_not_duration",
                   json.dumps({"start": ok.get("startTimeUnixNano"), "end": ok.get("endTimeUnixNano"),
                               "has_duration": "duration" in ok}),
                   json.dumps({"start": "1788246000123456789", "end": "1788246000123458289",
                               "has_duration": False})))
    return checks


def file_checks(results):
    """Checks that read files run.sh wrote; the probe is already an observation."""
    checks = list(received_file_checks(results))

    sink_path = Path(results) / "sink-captured.jsonl"
    if sink_path.exists():
        records = [json.loads(line) for line in sink_path.read_text().splitlines() if line.strip()]
        groups = [g for r in records for g in r.get("resources", [])]
        total = sum(g["records"] for g in groups)
        workspaces = {g["workspace"] for g in groups}
        checks.append(("batch.sink_requests_after_rebatching", str(len(records)), "1"))
        checks.append(("batch.sink_resource_groups", str(len(groups)), "2"))
        checks.append(("batch.sink_records_total", str(total), "7"))
        checks.append(("batch.sink_every_record_has_trusted_workspace", ",".join(sorted(workspaces)), WS_A))
    else:
        checks.append(("batch.sink_file_present", "missing", "present"))

    for name, path in [
        ("identity.quarantine_file_empty", Path(results) / "quarantine-logs.jsonl"),
        ("forge.quarantine_file_empty", Path(results) / "quarantine-forge-logs.jsonl"),
    ]:
        checks.append((name, quarantine_observation(path), "no-records"))

    unauthorized = Path(results) / "unauthorized-response.jsonl"
    if unauthorized.exists():
        rec = load_json(unauthorized)
        checks.append(("identity.unauthorized_status", str(rec["status"]), "401"))

    typed_emit = Path(results) / "emit-typed-logs.jsonl"
    if typed_emit.exists():
        rec = json.loads(typed_emit.read_text())
        observed = "under-1s" if rec["elapsed_ms"] < 1000 else f"{rec['elapsed_ms']}ms"
        checks.append(("ack.typed_logs_response_fast", observed, "under-1s"))
    visibility = Path(results) / "typed-visibility-ms.txt"
    if visibility.exists():
        checks.append(("ack.typed_visibility_ms", visibility.read_text().strip(), None))
    return checks


def quarantine_observation(path):
    """Count records in a quarantine file: absent or empty means no records."""
    if not path.exists():
        return "no-records"
    count = 0
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        for rl in obj.get("resourceLogs", []):
            for sl in rl.get("scopeLogs", []):
                count += len(sl.get("logRecords", []))
        for rl in obj.get("resourceSpans", []):
            for sl in rl.get("scopeSpans", []):
                count += len(sl.get("spans", []))
    return "no-records" if count == 0 else f"{count}-records"


def failover_file_checks(results):
    checks = list(overflow_object_checks(results))
    checks.append(("failover.quarantine_file_empty",
                   quarantine_observation(Path(results) / "quarantine-failover-logs.jsonl"), "no-records"))
    outage = Path(results) / "emit-failover-outage.jsonl"
    sync_outage = Path(results) / "emit-failover-sync-outage.jsonl"
    if outage.exists():
        rec = load_json(outage)
        checks.append(("failover.outage_queued_ack_status", str(rec["status"]), "200"))
        checks.append(("failover.outage_queued_ack_ms", str(rec["elapsed_ms"]), None))
    if sync_outage.exists():
        rec = load_json(sync_outage)
        checks.append(("failover.outage_sync_ack_status", str(rec["status"]), "200"))
        checks.append(("failover.outage_sync_ack_ms", str(rec["elapsed_ms"]), None))
    if outage.exists() and sync_outage.exists():
        queued = load_json(outage)["elapsed_ms"]
        sync = load_json(sync_outage)["elapsed_ms"]
        checks.append(("failover.sync_ack_waits_for_destination", str(sync > queued * 10), "True"))
    sync_keys = Path(results) / "overflow-keys-after-sync-outage.txt"
    if sync_keys.exists():
        keys = [key for key in sync_keys.read_text().split() if not key.startswith("path")]
        checks.append(("failover.overflow_objects_after_sync_outage", str(len(keys)), "2"))
    return checks


def metric_value(path, metric):
    """Return the value of the first metric line whose name matches."""
    if not path.exists():
        return "missing"
    for line in path.read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name = line.split("{", 1)[0].split(" ", 1)[0]
        if name == metric:
            return line.rsplit(" ", 1)[-1]
    return "absent"


def obs_file_checks(results):
    """Which stock telemetry is visible during outages (checklist item 22)."""
    results = Path(results)
    checks = [
        ("obs.permanent.send_failed_log_records",
         metric_value(results / "metrics-permanent.txt", "otelcol_exporter_send_failed_log_records"), "2"),
        ("obs.permanent.sent_log_records",
         metric_value(results / "metrics-permanent.txt", "otelcol_exporter_sent_log_records"), "0"),
        ("obs.permanent.receiver_accepted_log_records",
         metric_value(results / "metrics-permanent.txt", "otelcol_receiver_accepted_log_records"), "2"),
        ("obs.queue_full.enqueue_failed_log_records",
         metric_value(results / "metrics-queue-full.txt", "otelcol_exporter_enqueue_failed_log_records"), "6"),
        ("obs.queue_full.receiver_refused_log_records",
         metric_value(results / "metrics-queue-full.txt", "otelcol_receiver_refused_log_records"), "6"),
        ("obs.queue_full.queue_size",
         metric_value(results / "metrics-queue-full.txt", "otelcol_exporter_queue_size"), "2"),
        ("obs.failover.overflow_exporter_queue_visible",
         metric_value(results / "metrics-failover.txt", "otelcol_exporter_queue_capacity"), "1000"),
    ]
    # Stock components expose no overflow object backlog or catch-up progress
    # metric; only the local exporter queues are visible.
    metrics = (results / "metrics-failover.txt").read_text() if (results / "metrics-failover.txt").exists() else ""
    backlog = [line for line in metrics.splitlines()
               if any(token in line.lower() for token in ("backlog", "catchup", "spill", "object_count", "objects_"))
               and not line.startswith("#")]
    checks.append(("obs.overflow_backlog_metric_absent", str(len(backlog)), "0"))
    settings = results / "clickhouse-async-settings.tsv"
    if settings.exists():
        rows = {line.split("\t")[0]: line.split("\t") for line in settings.read_text().splitlines() if line.strip()}
        checks.append(("obs.clickhouse.wait_for_async_insert_default",
                       rows.get("wait_for_async_insert", ["?", "?", "?"])[2], "1"))
        # ClickHouse 26.8.2.7 enables async inserts server-side by default; the
        # exporter's async_insert=1 is not overriding a disabled default.
        checks.append(("obs.clickhouse.async_insert_default",
                       rows.get("async_insert", ["?", "?", "?"])[2], "1"))
    memory_rows = results / "crash-memory-rows.txt"
    if memory_rows.exists():
        checks.append(("obs.crash_memory_no_rows_after_kill", memory_rows.read_text().strip(), "0"))
    # Startup against an unresponsive ClickHouse endpoint: the exporter's schema
    # detection query blocks on the driver connection timeout and then degrades
    # to "no EventName column" without failing the collector.
    log_path = Path.home() / "lh018" / "logs" / "collector-crash-persistent.log"
    if log_path.exists():
        lines = log_path.read_text().splitlines()
        checks.append(("obs.schema_detection_failure_logged",
                       "present" if any("schema detection failed" in line for line in lines) else "absent", "present"))
        started = next((line.split("\t")[0] for line in lines if "Starting otelcol-contrib" in line), None)
        serving = next((line.split("\t")[0] for line in lines if "Begin running and processing data" in line), None)
        if started and serving:
            from datetime import datetime
            def parse(ts):
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            gap = (parse(serving) - parse(started)).total_seconds()
            checks.append(("obs.startup_delay_seconds_unreachable_downstream", f"{gap:.1f}", None))
    return checks


def fault_file_checks(results):
    checks = []
    queue_full_rows = Path(results) / "queue-full-rows.txt"
    if queue_full_rows.exists():
        checks.append(("fault.queue_full_inserted_no_rows", queue_full_rows.read_text().strip(), "0"))
    permanent = Path(results) / "permanent-response.jsonl"
    if permanent.exists():
        rec = load_json(permanent)
        checks.append(("ack.permanent_error_producer_sees_success", str(rec["status"]), "200"))
    queue_full = Path(results) / "queue-full-responses.jsonl"
    if queue_full.exists():
        statuses = [r["status"] for r in load_jsonl(queue_full)]
        checks.append(("ack.queue_full_has_rejection",
                       "rejected" if any(s >= 300 for s in statuses) else "all-2xx", "rejected"))
        checks.append(("ack.queue_full_statuses", ",".join(str(s) for s in statuses), None))
    crash_memory = Path(results) / "crash-memory-responses.jsonl"
    if crash_memory.exists():
        statuses = [r["status"] for r in load_jsonl(crash_memory)]
        checks.append(("ack.crash_memory_producer_saw_success", str(statuses), "[200, 200, 200]"))
    drain = Path(results) / "crash-persistent-drain-ms.txt"
    if drain.exists():
        checks.append(("ack.crash_persistent_drain_ms", drain.read_text().strip(), None))
    return checks


def run_phase(phase, results, capture=False):
    if phase == "typed":
        checks = TYPED_CHECKS
    elif phase == "identity":
        checks = IDENTITY_CHECKS
    elif phase == "forge":
        checks = FORGE_CHECKS
    elif phase == "faults":
        checks = FAULT_CHECKS
    elif phase == "failover":
        checks = FAILOVER_CHECKS
    elif phase == "files":
        checks = file_checks(results)
    elif phase == "failover-files":
        checks = failover_file_checks(results)
    elif phase == "fault-files":
        checks = fault_file_checks(results)
    elif phase == "obs":
        checks = obs_file_checks(results)
    else:
        raise SystemExit(f"unknown phase {phase}")

    outcomes = []
    failures = 0
    file_phase = phase in ("files", "failover-files", "fault-files", "obs")
    for name, probe, expected in checks:
        try:
            observed = probe if file_phase else ch(probe)
        except Exception as exc:
            observed = f"ERROR: {exc}"
        ok = observed == expected
        if expected is None:
            ok = not str(observed).startswith("ERROR")
        if not ok and not capture:
            failures += 1
        outcomes.append({"name": name, "expected": expected, "observed": observed,
                         "ok": ok, "probe": probe})
    return outcomes, failures


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--results", default="results")
    parser.add_argument("--capture", action="store_true", help="record observations without failing")
    parser.add_argument("--out", help="write JSON outcomes here")
    args = parser.parse_args(argv)

    outcomes, failures = run_phase(args.phase, args.results, args.capture)
    for item in outcomes:
        mark = "PASS" if item["ok"] else ("CAPTURE" if args.capture else "FAIL")
        print(f"[{mark}] {item['name']}: expected={item['expected']!r} observed={item['observed']!r}")
    if args.out:
        Path(args.out).write_text("".join(json.dumps(item) + "\n" for item in outcomes))
    total = len(outcomes)
    print(f"\n{args.phase}: {total - failures}/{total} checks passed"
          + (" (capture mode)" if args.capture else ""))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
