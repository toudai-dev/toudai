//go:build toudaigolden

// Golden generator for toudai-ingest's mapping test. It is not part of this
// repository's build: `../regenerate.sh` copies it into a toudai-dev/collector
// checkout at the reference commit and runs it there, so the expected rows come
// from the Go exporter's own prepareLogs and prepareSpans.
//
// For each `<signal>.otlp.json` it writes:
//   - `<signal>.otlp.pb`: the producer request as OTLP protobuf, which the Rust
//     test decodes;
//   - `<signal>.expected.jsonl`: one line per serving row (with its canonical
//     row's payload) and one line per rejection, after stamping the request
//     exactly as the gateway processor does.
package toudaiexporter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const goldenDataset = "fixture"

// goldenEventID is the n-th (0-based) event_id, in request order. The Rust test
// uses the same formula.
func goldenEventID(n int) string {
	return fmt.Sprintf("0198a0c4-7b3e-7c11-8a52-%012x", n)
}

// stampResource is the gateway processor's stampResource.
func stampResource(attrs pcommon.Map) {
	attrs.RemoveIf(func(k string, _ pcommon.Value) bool { return k == datasetAttr })
	attrs.PutStr(datasetAttr, goldenDataset)
}

func doubles(m map[string]float64) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[k] = doubleText(v)
	}
	return out
}

func orEmpty[V any](m map[string]V) map[string]V {
	if m == nil {
		return map[string]V{}
	}
	return m
}

func writeLines(t *testing.T, path string, lines []map[string]any) {
	t.Helper()
	var out []byte
	for _, line := range lines {
		b, err := json.Marshal(line)
		if err != nil {
			t.Fatal(err)
		}
		out = append(append(out, b...), '\n')
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}
}

func rejectionLines(rejections []rejection) []map[string]any {
	var out []map[string]any
	for _, r := range rejections {
		out = append(out, map[string]any{"rejected": r.Reason, "namespace": r.Namespace})
	}
	return out
}

func TestToudaiGolden(t *testing.T) {
	dir := os.Getenv("TOUDAI_GOLDEN_DIR")
	if dir == "" {
		t.Fatal("TOUDAI_GOLDEN_DIR is required")
	}

	raw, err := os.ReadFile(filepath.Join(dir, "logs.otlp.json"))
	if err != nil {
		t.Fatal(err)
	}
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(raw)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "logs.otlp.pb"), pb, 0o644); err != nil {
		t.Fatal(err)
	}
	n := 0
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		stampResource(rl.Resource().Attributes())
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			recs := rl.ScopeLogs().At(j).LogRecords()
			for k := 0; k < recs.Len(); k++ {
				recs.At(k).Attributes().PutStr(eventIDAttr, goldenEventID(n))
				n++
			}
		}
	}
	logs := prepareLogs(ld)
	var lines []map[string]any
	for i, r := range logs.rows {
		lines = append(lines, map[string]any{
			"dataset": r.Dataset, "event_id": r.EventID, "routing_epoch": 1,
			"timestamp": r.TS.UnixNano(), "observed_ts": r.ObservedTS.UnixNano(), "kind": "log",
			"service_name": r.ServiceName, "severity_text": r.SeverityText, "severity_number": r.SeverityNumber,
			"body": r.Body, "trace_id": r.TraceID, "span_id": r.SpanID, "event_name": r.EventName,
			"resource": orEmpty(r.Resource), "scope": orEmpty(r.Scope),
			"resource_types": orEmpty(r.ResourceTypes), "scope_types": orEmpty(r.ScopeTypes),
			"record_int": orEmpty(r.RecordInt), "record_double": doubles(r.RecordDouble),
			"record_string": orEmpty(r.RecordString), "record_bool": orEmpty(r.RecordBool),
			"record_other": orEmpty(r.RecordOther), "payload_sha256": r.PayloadSHA256,
			"canonical": map[string]any{
				"dataset": logs.canonical[i].Dataset, "signal": logs.canonical[i].Signal,
				"event_id": logs.canonical[i].EventID, "timestamp": logs.canonical[i].TS.UnixNano(),
				"schema_version": canonicalSchema, "payload": logs.canonical[i].Payload,
				"payload_sha256": logs.canonical[i].PayloadSHA256,
			},
		})
	}
	writeLines(t, filepath.Join(dir, "logs.expected.jsonl"), append(lines, rejectionLines(logs.rejections)...))

	raw, err = os.ReadFile(filepath.Join(dir, "traces.otlp.json"))
	if err != nil {
		t.Fatal(err)
	}
	td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(raw)
	if err != nil {
		t.Fatal(err)
	}
	pb, err = (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "traces.otlp.pb"), pb, 0o644); err != nil {
		t.Fatal(err)
	}
	n = 0
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		stampResource(rs.Resource().Attributes())
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			spans := rs.ScopeSpans().At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				spans.At(k).Attributes().PutStr(eventIDAttr, goldenEventID(n))
				n++
			}
		}
	}
	spans := prepareSpans(td)
	lines = nil
	for i, r := range spans.rows {
		lines = append(lines, map[string]any{
			"dataset": r.Dataset, "event_id": r.EventID, "routing_epoch": 1,
			"timestamp": r.TS.UnixNano(), "end_ts": r.EndTS.UnixNano(), "kind": "span",
			"service_name": r.ServiceName, "trace_id": r.TraceID, "span_id": r.SpanID,
			"parent_span_id": r.ParentID, "span_name": r.SpanName, "span_kind": r.SpanKind,
			"status_code": r.StatusCode, "duration_ns": r.DurationNS, "span_timing_status": r.TimingStatus,
			"resource": orEmpty(r.Resource), "resource_types": orEmpty(r.ResourceTypes),
			"span_scope": orEmpty(r.SpanScope), "span_scope_types": orEmpty(r.SpanScopeTypes),
			"span_int": orEmpty(r.SpanInt), "span_double": doubles(r.SpanDouble),
			"span_string": orEmpty(r.SpanString), "span_bool": orEmpty(r.SpanBool),
			"span_other": orEmpty(r.SpanOther), "payload_sha256": r.PayloadSHA256,
			"canonical": map[string]any{
				"dataset": spans.canonical[i].Dataset, "signal": spans.canonical[i].Signal,
				"event_id": spans.canonical[i].EventID, "timestamp": spans.canonical[i].TS.UnixNano(),
				"schema_version": canonicalSchema, "payload": spans.canonical[i].Payload,
				"payload_sha256": spans.canonical[i].PayloadSHA256,
			},
		})
	}
	writeLines(t, filepath.Join(dir, "traces.expected.jsonl"), append(lines, rejectionLines(spans.rejections)...))
}
