package lighthouseexporter

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestDoubleText(t *testing.T) {
	tests := []struct {
		name string
		in   float64
		want string
	}{
		{"zero", 0, "0.0"},
		{"negative zero", math.Copysign(0, -1), "-0.0"},
		{"integral", 1.0, "1.0"},
		{"fraction", 1.5, "1.5"},
		{"small exponent", 1e-7, "1e-07"},
		{"large exponent", 1e21, "1e+21"},
		{"nan", math.NaN(), "nan"},
		{"positive infinity", math.Inf(1), "inf"},
		{"negative infinity", math.Inf(-1), "-inf"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := doubleText(tc.in); got != tc.want {
				t.Fatalf("doubleText(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCanonicalNodeTypes(t *testing.T) {
	tests := []struct {
		name string
		in   value
		want string
	}{
		{"int", value{kind: kindInt, i: 9007199254740993}, `{"t":"int","v":"9007199254740993"}`},
		{"int min", value{kind: kindInt, i: math.MinInt64}, `{"t":"int","v":"-9223372036854775808"}`},
		{"string one", value{kind: kindString, s: "1"}, `{"t":"string","v":"1"}`},
		{"empty string", value{kind: kindString, s: ""}, `{"t":"string","v":""}`},
		{"double one", value{kind: kindDouble, d: 1}, `{"t":"double","v":"1.0"}`},
		{"bool false", value{kind: kindBool, b: false}, `{"t":"bool","v":false}`},
		{"empty", value{kind: kindEmpty}, `{"t":"empty"}`},
		{"bytes", value{kind: kindBytes, y: []byte{0, 1, 0xfe, 0xff}}, `{"t":"bytes","v":"AAH+/w=="}`},
		{"array", value{kind: kindArray, arr: []value{
			{kind: kindString, s: "a"}, {kind: kindInt, i: 1}, {kind: kindBool, b: true},
		}}, `{"t":"array","v":[{"t":"string","v":"a"},{"t":"int","v":"1"},{"t":"bool","v":true}]}`},
		{"map", value{kind: kindMap, m: map[string]value{
			"nested.bool": {kind: kindBool, b: false},
			"nested.int":  {kind: kindInt, i: 2},
		}}, `{"t":"map","v":{"nested.bool":{"t":"bool","v":false},"nested.int":{"t":"int","v":"2"}}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(canonicalNode(tc.in))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("canonicalNode = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestRecordMapsRouteEachKindOnce(t *testing.T) {
	tests := []struct {
		name      string
		attr      value
		wantMap   string
		wantValue string
	}{
		{"string", value{kind: kindString, s: "1"}, "string", "1"},
		{"int", value{kind: kindInt, i: 1}, "int", "1"},
		{"double", value{kind: kindDouble, d: 1}, "double", "1"},
		{"bool", value{kind: kindBool, b: false}, "bool", "false"},
		{"bytes", value{kind: kindBytes, y: []byte{0}}, "other", "bytes"},
		{"array", value{kind: kindArray, arr: []value{{kind: kindInt, i: 1}}}, "other", "array"},
		{"map", value{kind: kindMap, m: map[string]value{}}, "other", "map"},
		{"empty", value{kind: kindEmpty}, "other", "empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			maps := newRecordMaps()
			if err := maps.place("k", tc.attr); err != nil {
				t.Fatalf("place: %v", err)
			}
			locations := 0
			if _, ok := maps.ints["k"]; ok {
				locations++
				if tc.wantMap != "int" {
					t.Fatalf("key landed in int map, want %s", tc.wantMap)
				} else if got := maps.ints["k"]; got != tc.attr.i {
					t.Fatalf("int value %d, want %d", got, tc.attr.i)
				}
			}
			if _, ok := maps.doubles["k"]; ok {
				locations++
			}
			if _, ok := maps.strs["k"]; ok {
				locations++
			}
			if _, ok := maps.bools["k"]; ok {
				locations++
			}
			if got, ok := maps.others["k"]; ok {
				locations++
				if tc.wantMap == "other" && got != tc.wantValue {
					t.Fatalf("other value %q, want %q", got, tc.wantValue)
				}
			}
			if locations != 1 {
				t.Fatalf("key present in %d maps, want exactly one", locations)
			}
		})
	}
}

// fixtureLogs builds one log record with the trusted gateway scope and the
// supplied record attributes.
func fixtureLogs(t *testing.T, record func(plog.LogRecord)) plog.Logs {
	t.Helper()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	res := rl.Resource().Attributes()
	res.PutStr(workspaceAttr, "11111111-1111-4111-8111-111111111111")
	res.PutStr(datasetAttr, "fixture")
	res.PutStr(scopeSourceAttr, scopeSourceValue)
	res.PutStr("service.name", "fixture-service")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("lighthouse.fixture")
	sl.Scope().Attributes().PutStr("scope.attr", "scope-value")
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(1788246000123456789))
	lr.SetObservedTimestamp(pcommon.Timestamp(1788246000123456789 + 1_000_000))
	lr.Body().SetStr("body")
	if record != nil {
		record(lr)
	}
	return ld
}

// Duplicate attribute keys cannot be built through pcommon.Map's Go API (PutStr
// upserts), so the duplicate-key rejection is exercised in the integration
// harness with real OTLP protobuf bytes carrying two entries for one key.
// decodeAttrs detects the duplicate while ranging the collection.

func TestPrepareLogsRejectsMissingTrustedScope(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(workspaceAttr, "producer-supplied")
	rl.Resource().Attributes().PutStr(datasetAttr, "fixture")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.Timestamp(1))
	prepared := prepareLogs(ld)
	if len(prepared.rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(prepared.rows))
	}
	if !batchInvalidated(prepared.rejections) {
		t.Fatalf("rejections = %+v, want the batch to be invalidated without the gateway marker", prepared.rejections)
	}
}

func TestPrepareLogsDropsProducerReservedKeys(t *testing.T) {
	ld := fixtureLogs(t, func(lr plog.LogRecord) {
		lr.Attributes().PutStr("lighthouse.workspace_id", "99999999-9999-4999-8999-999999999999")
		lr.Attributes().PutStr("lighthouse.scope_source", "producer")
		lr.Attributes().PutStr("normal", "value")
	})
	ld.ResourceLogs().At(0).ScopeLogs().At(0).Scope().Attributes().PutStr("lighthouse.dataset", "forged")
	prepared := prepareLogs(ld)
	if len(prepared.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(prepared.rows))
	}
	row := prepared.rows[0]
	if row.WorkspaceID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("workspace = %q, want the gateway value", row.WorkspaceID)
	}
	if _, ok := row.RecordString["lighthouse.workspace_id"]; ok {
		t.Fatal("producer record attribute lighthouse.workspace_id must be dropped")
	}
	if _, ok := row.Scope["lighthouse.dataset"]; ok {
		t.Fatal("producer scope attribute lighthouse.dataset must be dropped")
	}
	if row.Resource["lighthouse.workspace_id"] != "11111111-1111-4111-8111-111111111111" {
		t.Fatal("stored resource namespace must keep the trusted workspace")
	}
	if _, ok := row.Resource[scopeSourceAttr]; ok {
		t.Fatal("the internal scope-source marker must not be stored")
	}
	if !strings.Contains(row.Payload, `"lighthouse.workspace_id":{"t":"string","v":"11111111-1111-4111-8111-111111111111"}`) {
		t.Fatal("canonical resource namespace must carry the trusted workspace")
	}
	if strings.Contains(row.Payload, "9999999999") {
		t.Fatal("canonical payload must not contain the forged workspace value")
	}
}

func TestSpanDurationRejectionAndMissingTiming(t *testing.T) {
	build := func(start, end uint64) preparedSpans {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		res := rs.Resource().Attributes()
		res.PutStr(workspaceAttr, "ws")
		res.PutStr(datasetAttr, "fixture")
		res.PutStr(scopeSourceAttr, scopeSourceValue)
		ss := rs.ScopeSpans().AppendEmpty()
		sp := ss.Spans().AppendEmpty()
		sp.SetStartTimestamp(pcommon.Timestamp(start))
		sp.SetEndTimestamp(pcommon.Timestamp(end))
		sp.SetName("span")
		return prepareSpans(td)
	}
	tests := []struct {
		name       string
		start, end uint64
		wantRows   int
		wantReject string
		wantDur    *int64
	}{
		{"positive duration", 10, 20, 1, "", ptr(int64(10))},
		{"zero duration", 10, 10, 1, "", ptr(int64(0))},
		{"missing timing", 0, 0, 1, "", nil},
		{"negative duration", 20, 10, 0, "negative_span_duration", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := build(tc.start, tc.end)
			if len(got.rows) != tc.wantRows {
				t.Fatalf("rows = %d, want %d (rejections %+v)", len(got.rows), tc.wantRows, got.rejections)
			}
			if tc.wantReject != "" {
				if len(got.rejections) != 1 || got.rejections[0].Reason != tc.wantReject {
					t.Fatalf("rejections = %+v, want reason %s", got.rejections, tc.wantReject)
				}
				return
			}
			row := got.rows[0]
			switch {
			case tc.wantDur == nil && row.DurationNS != nil:
				t.Fatalf("duration = %d, want NULL", *row.DurationNS)
			case tc.wantDur != nil && row.DurationNS == nil:
				t.Fatalf("duration = NULL, want %d", *tc.wantDur)
			case tc.wantDur != nil && *row.DurationNS != *tc.wantDur:
				t.Fatalf("duration = %d, want %d", *row.DurationNS, *tc.wantDur)
			}
		})
	}
}

func TestEventIDIsStableAcrossRetries(t *testing.T) {
	build := func() preparedLogs {
		return prepareLogs(fixtureLogs(t, func(lr plog.LogRecord) {
			lr.Attributes().PutInt("metric.value", 1)
			lr.Attributes().PutDouble("other.value", 1.5)
		}))
	}
	first, second := build(), build()
	if len(first.rows) != 1 || len(second.rows) != 1 {
		t.Fatalf("expected one row per build, got %d and %d", len(first.rows), len(second.rows))
	}
	if first.rows[0].EventID != second.rows[0].EventID {
		t.Fatalf("event identity changed across identical exports: %q vs %q", first.rows[0].EventID, second.rows[0].EventID)
	}
	if first.rows[0].PayloadSHA256 != second.rows[0].PayloadSHA256 {
		t.Fatal("payload checksum changed across identical exports")
	}
	// Different content must produce a different identity.
	third := prepareLogs(fixtureLogs(t, func(lr plog.LogRecord) {
		lr.Attributes().PutInt("metric.value", 2)
	}))
	if third.rows[0].EventID == first.rows[0].EventID {
		t.Fatal("different content produced the same event identity")
	}
}

func TestCanonicalPayloadMatchesIndependentShape(t *testing.T) {
	prepared := prepareLogs(fixtureLogs(t, func(lr plog.LogRecord) {
		lr.Attributes().PutInt("metric.value", 1)
		lr.Attributes().PutDouble("zero.float", math.Copysign(0, -1))
		lr.Attributes().PutEmptyBytes("bytes.raw").FromRaw([]byte{0, 1})
	}))
	var doc map[string]any
	if err := json.Unmarshal([]byte(prepared.rows[0].Payload), &doc); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	identity := doc["identity"].(map[string]any)
	if identity["tenant_id"] != "11111111-1111-4111-8111-111111111111" || identity["dataset"] != "fixture" {
		t.Fatalf("identity = %v", identity)
	}
	if identity["event_id"] != prepared.rows[0].EventID {
		t.Fatal("payload identity.event_id does not match the row identity")
	}
	record := doc["record"].(map[string]any)
	if got := record["metric.value"].(map[string]any); got["t"] != "int" || got["v"] != "1" {
		t.Fatalf("metric.value node = %v", got)
	}
	if got := record["zero.float"].(map[string]any); got["t"] != "double" || got["v"] != "-0.0" {
		t.Fatalf("zero.float node = %v", got)
	}
	if got := record["bytes.raw"].(map[string]any); got["t"] != "bytes" || got["v"] != "AAE=" {
		t.Fatalf("bytes.raw node = %v", got)
	}
	if doc["schema"] != canonicalSchema || doc["kind"] != "log" {
		t.Fatalf("schema/kind = %v/%v", doc["schema"], doc["kind"])
	}
}

func ptr[T any](v T) *T { return &v }
