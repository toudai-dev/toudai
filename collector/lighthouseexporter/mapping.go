package lighthouseexporter

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Reserved attribute keys. The gateway's resource processor is the only place
// trusted scope is injected; every reserved key that arrives in a
// producer-controlled namespace (record, scope, span, span scope) is dropped and
// counted, and the internal scope-source marker is never stored.
const (
	workspaceAttr    = "lighthouse.workspace_id"
	datasetAttr      = "lighthouse.dataset"
	scopeSourceAttr  = "lighthouse.scope_source"
	scopeSourceValue = "gateway"
	reservedPrefix   = "lighthouse."
)

const canonicalSchema = "lighthouse.canonical.event/1"

// Span timing diagnostics stored in serving_spans.span_timing_status. OTLP has
// no presence for a span timestamp: start_time_unix_nano and end_time_unix_nano
// are proto3 fixed64 fields whose unset value is 0 (the pinned pdata v1.67.0
// generated proto also omits a zero timestamp from its JSON form), so 0 is the
// only available "unset" signal and an explicit 0 is indistinguishable from it.
// An unset start or end therefore means missing timing and takes precedence over
// the reversed-timing classification: a missing start must not fabricate a
// duration from epoch 0, and a missing end must not be read as end < start. The
// raw timestamps are always preserved; the derived duration is unavailable with
// an explicit diagnostic, never discarded, never substituted with zero, and
// never converted by unsigned underflow into a huge duration (Experiment 018
// measured 18446744073709550616 for a reversed span).
const (
	spanTimingOK             = "ok"               // duration is observable, including a valid zero
	spanTimingMissing        = "missing"          // start or end unset (0)
	spanTimingEndBeforeStart = "end_before_start" // data error: both set and end < start
)

// valueKind is the OTLP AnyValue kind the exporter preserves.
type valueKind uint8

const (
	kindEmpty valueKind = iota
	kindString
	kindInt
	kindDouble
	kindBool
	kindBytes
	kindArray
	kindMap
)

func (k valueKind) String() string {
	switch k {
	case kindEmpty:
		return "empty"
	case kindString:
		return "string"
	case kindInt:
		return "int"
	case kindDouble:
		return "double"
	case kindBool:
		return "bool"
	case kindBytes:
		return "bytes"
	case kindArray:
		return "array"
	case kindMap:
		return "map"
	}
	return "unknown"
}

// value is a decoded OTLP AnyValue. Doubles and integers stay distinct; bytes
// keep their raw form; arrays and maps keep nested values.
type value struct {
	kind valueKind
	s    string
	i    int64
	d    float64
	b    bool
	y    []byte
	arr  []value
	m    map[string]value
}

func decodeValue(v pcommon.Value) (value, error) {
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		return value{kind: kindEmpty}, nil
	case pcommon.ValueTypeStr:
		return value{kind: kindString, s: v.Str()}, nil
	case pcommon.ValueTypeInt:
		return value{kind: kindInt, i: v.Int()}, nil
	case pcommon.ValueTypeDouble:
		return value{kind: kindDouble, d: v.Double()}, nil
	case pcommon.ValueTypeBool:
		return value{kind: kindBool, b: v.Bool()}, nil
	case pcommon.ValueTypeBytes:
		return value{kind: kindBytes, y: append([]byte(nil), v.Bytes().AsRaw()...)}, nil
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		out := make([]value, 0, s.Len())
		for i := 0; i < s.Len(); i++ {
			item, err := decodeValue(s.At(i))
			if err != nil {
				return value{}, err
			}
			out = append(out, item)
		}
		return value{kind: kindArray, arr: out}, nil
	case pcommon.ValueTypeMap:
		m := v.Map()
		out := make(map[string]value, m.Len())
		var failure error
		m.Range(func(k string, mv pcommon.Value) bool {
			if _, seen := out[k]; seen {
				failure = fmt.Errorf("duplicate nested key %q", k)
				return false
			}
			item, err := decodeValue(mv)
			if err != nil {
				failure = fmt.Errorf("nested key %q: %w", k, err)
				return false
			}
			out[k] = item
			return true
		})
		if failure != nil {
			return value{}, failure
		}
		return value{kind: kindMap, m: out}, nil
	}
	return value{}, fmt.Errorf("unsupported AnyValue type %v", v.Type())
}

// attrs is one decoded attribute collection: typed values, the stock-compatible
// serving strings (pcommon.Value.AsString, the conversion the pinned stock
// exporter stores), type names, and the producer-supplied reserved keys that
// were dropped.
type attrs struct {
	values  map[string]value
	strings map[string]string
	types   map[string]string
	dropped []string
}

// decodeAttrs decodes one attribute collection. Reserved keys are dropped when
// dropReserved is set, which is the rule for every producer-controlled namespace
// (record, scope, span, span scope). The resource namespace is decoded whole
// because the gateway's trusted scope arrives there; resourceForStorage applies
// the reserved-key rule afterwards.
func decodeAttrs(m pcommon.Map, dropReserved bool) (attrs, error) {
	out := attrs{
		values:  map[string]value{},
		strings: map[string]string{},
		types:   map[string]string{},
	}
	var failure error
	m.Range(func(k string, v pcommon.Value) bool {
		if dropReserved && strings.HasPrefix(k, reservedPrefix) {
			out.dropped = append(out.dropped, k)
			return true
		}
		if _, seen := out.values[k]; seen {
			failure = fmt.Errorf("duplicate attribute key %q", k)
			return false
		}
		item, err := decodeValue(v)
		if err != nil {
			failure = fmt.Errorf("attribute %q: %w", k, err)
			return false
		}
		out.values[k] = item
		out.strings[k] = v.AsString()
		out.types[k] = item.kind.String()
		return true
	})
	if failure != nil {
		return attrs{}, failure
	}
	return out, nil
}

// canonicalNode renders the versioned typed canonical value (Experiment 019's
// node encoding: an explicit type tag per value, decimal strings for numbers,
// base64 for bytes, "nan"/"inf"/"-inf"/"-0.0" for the special doubles).
func canonicalNode(v value) any {
	switch v.kind {
	case kindEmpty:
		return map[string]any{"t": "empty"}
	case kindString:
		return map[string]any{"t": "string", "v": v.s}
	case kindBool:
		return map[string]any{"t": "bool", "v": v.b}
	case kindInt:
		return map[string]any{"t": "int", "v": strconv.FormatInt(v.i, 10)}
	case kindDouble:
		return map[string]any{"t": "double", "v": doubleText(v.d)}
	case kindBytes:
		return map[string]any{"t": "bytes", "v": base64.StdEncoding.EncodeToString(v.y)}
	case kindArray:
		items := make([]any, 0, len(v.arr))
		for _, item := range v.arr {
			items = append(items, canonicalNode(item))
		}
		return map[string]any{"t": "array", "v": items}
	case kindMap:
		items := make(map[string]any, len(v.m))
		for k, item := range v.m {
			items[k] = canonicalNode(item)
		}
		return map[string]any{"t": "map", "v": items}
	}
	return map[string]any{"t": "unknown"}
}

func canonicalNodes(values map[string]value) map[string]any {
	out := make(map[string]any, len(values))
	for k, v := range values {
		out[k] = canonicalNode(v)
	}
	return out
}

// doubleText matches the canonical double text used by Experiment 019's typed
// model: shortest round-trip decimal, "nan"/"inf"/"-inf" for non-finite values,
// and an explicit "-0.0" that preserves the sign of zero.
func doubleText(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case f == 0:
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	text := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(text, ".eE") {
		text += ".0"
	}
	return text
}

func marshalCanonical(doc map[string]any) (string, error) {
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	// The canonical payload is UTF-8 text; HTML escaping would change bytes
	// without changing meaning, so it is disabled to keep the encoding stable.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return "", err
	}
	return strings.TrimRight(sb.String(), "\n"), nil
}

// eventID is a stable, content-derived fixture-scale identity: retries and
// re-batching of the same record produce the same ID, which is what lets the
// harness correlate canonical and serving rows and tell canonical duplicates
// apart from missing serving rows. It is not a deduplication mechanism and does
// not yet implement Decision 0001's gateway-assigned identity.
func eventID(kind, workspace, dataset string, ts, observed uint64, traceID, spanID, payloadWithoutID string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%d\x00%d\x00%s\x00%s\x00", kind, workspace, dataset, ts, observed, traceID, spanID)
	h.Write([]byte(payloadWithoutID))
	return "lh1-" + hex.EncodeToString(h.Sum(nil))[:32]
}

func hexTraceID(id pcommon.TraceID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

func hexSpanID(id pcommon.SpanID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

func nsTime(ns uint64) time.Time {
	return time.Unix(0, int64(ns)).UTC()
}

// recordMaps routes each record attribute into exactly one serving Map. The
// canonical-only kinds (bytes, array, map) and explicit empty keep their type in
// the key-only `other` Map; a key that would land in two Maps is a mapping bug,
// so it is reported instead of silently overwritten.
type recordMaps struct {
	ints    map[string]int64
	doubles map[string]float64
	strs    map[string]string
	bools   map[string]bool
	others  map[string]string
}

func newRecordMaps() *recordMaps {
	return &recordMaps{
		ints:    map[string]int64{},
		doubles: map[string]float64{},
		strs:    map[string]string{},
		bools:   map[string]bool{},
		others:  map[string]string{},
	}
}

func (r *recordMaps) place(key string, v value) error {
	switch v.kind {
	case kindString:
		r.strs[key] = v.s
	case kindInt:
		r.ints[key] = v.i
	case kindDouble:
		r.doubles[key] = v.d
	case kindBool:
		r.bools[key] = v.b
	case kindBytes, kindArray, kindMap, kindEmpty:
		r.others[key] = v.kind.String()
	default:
		return fmt.Errorf("attribute %q has unsupported kind %d", key, v.kind)
	}
	return nil
}

func nilIfEmpty[K comparable, V any](m map[K]V) map[K]V {
	if len(m) == 0 {
		return nil
	}
	return m
}

// logRow is one serving log row; spanRow is one serving span row. Both carry the
// payload checksum so a serving row can be correlated with its canonical row.
type logRow struct {
	WorkspaceID    string
	Dataset        string
	EventID        string
	TS             time.Time
	ObservedTS     time.Time
	ServiceName    string
	SeverityText   string
	SeverityNumber int32
	Body           string
	TraceID        string
	SpanID         string
	EventName      string

	Resource      map[string]string
	ResourceTypes map[string]string
	Scope         map[string]string
	ScopeTypes    map[string]string

	RecordInt    map[string]int64
	RecordDouble map[string]float64
	RecordString map[string]string
	RecordBool   map[string]bool
	RecordOther  map[string]string

	Payload       string
	PayloadSHA256 string
}

type spanRow struct {
	WorkspaceID string
	Dataset     string
	EventID     string
	TS          time.Time
	// EndTS is the supplied end timestamp as received (zero when OTLP carries
	// no timing); it is kept so a reversed span stays searchable by both
	// timestamps instead of only by its derived duration.
	EndTS       time.Time
	ServiceName string
	TraceID     string
	SpanID      string
	ParentID    string
	SpanName    string
	SpanKind    string
	StatusCode  int32
	DurationNS  *int64
	// TimingStatus is the machine-readable diagnostic (spanTimingOK,
	// spanTimingMissing, or spanTimingEndBeforeStart).
	TimingStatus string

	Resource       map[string]string
	ResourceTypes  map[string]string
	SpanScope      map[string]string
	SpanScopeTypes map[string]string

	SpanInt    map[string]int64
	SpanDouble map[string]float64
	SpanString map[string]string
	SpanBool   map[string]bool
	SpanOther  map[string]string

	Payload       string
	PayloadSHA256 string
}

type canonicalRow struct {
	WorkspaceID   string
	Dataset       string
	Signal        string
	EventID       string
	TS            time.Time
	Payload       string
	PayloadSHA256 string
}

// rejection records why one received record was not exported. Reasons are
// counted per reason; the batch continues with the remaining records, and
// nothing about a rejection is producer-visible (OTLP success is queue
// admission, Experiment 018).
type rejection struct {
	Reason    string
	Detail    string
	Namespace string
}

type preparedLogs struct {
	rows       []logRow
	canonical  []canonicalRow
	rejections []rejection
}

type preparedSpans struct {
	rows       []spanRow
	canonical  []canonicalRow
	rejections []rejection
}

// trustedScope reads the gateway-injected workspace and dataset from a decoded
// resource namespace and refuses to guess when either is absent, is not a
// string, or when the gateway's scope-source marker is missing. The marker is a
// misconfiguration check, not producer authentication: it proves the tested
// gateway pipeline (credential routing plus the trusted resource processor) ran
// before the exporter.
func trustedScope(res attrs) (workspace, dataset string, err error) {
	wv, ok := res.values[workspaceAttr]
	if !ok || wv.kind != kindString || wv.s == "" {
		return "", "", errMissingTrustedScope
	}
	dv, ok := res.values[datasetAttr]
	if !ok || dv.kind != kindString || dv.s == "" {
		return "", "", errMissingTrustedScope
	}
	marker, ok := res.values[scopeSourceAttr]
	if !ok || marker.kind != kindString || marker.s != scopeSourceValue {
		return "", "", errMissingTrustedScope
	}
	return wv.s, dv.s, nil
}

// resourceForStorage applies the reserved-key rule to the resource namespace:
// the trusted workspace and dataset stay addressable as both columns and
// attributes, every other reserved key (including the internal scope-source
// marker) is dropped, and the returned typed values are what the canonical
// payload stores.
func resourceForStorage(res attrs) (values map[string]value, strings_, types map[string]string, dropped []string) {
	values = map[string]value{}
	strings_ = map[string]string{}
	types = map[string]string{}
	for k, v := range res.values {
		if strings.HasPrefix(k, reservedPrefix) && k != workspaceAttr && k != datasetAttr {
			dropped = append(dropped, k)
			continue
		}
		values[k] = v
		strings_[k] = res.strings[k]
		types[k] = res.types[k]
	}
	return values, strings_, types, dropped
}

func prepareLogs(ld plog.Logs) preparedLogs {
	var out preparedLogs
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		res, err := decodeAttrs(rl.Resource().Attributes(), false)
		if err != nil {
			out.rejections = append(out.rejections, rejection{Reason: "invalid_resource_attributes", Detail: err.Error(), Namespace: "resource"})
			continue
		}
		workspace, dataset, err := trustedScope(res)
		if err != nil {
			out.rejections = append(out.rejections, rejection{Reason: "missing_trusted_scope", Detail: err.Error(), Namespace: "resource"})
			continue
		}
		resTyped, resValues, resTypes, _ := resourceForStorage(res)
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			scope, err := decodeAttrs(sl.Scope().Attributes(), true)
			if err != nil {
				out.rejections = append(out.rejections, rejection{Reason: "invalid_scope_attributes", Detail: err.Error(), Namespace: "scope"})
				continue
			}
			logs := sl.LogRecords()
			for k := 0; k < logs.Len(); k++ {
				lr := logs.At(k)
				rec, err := decodeAttrs(lr.Attributes(), true)
				if err != nil {
					out.rejections = append(out.rejections, rejection{Reason: "duplicate_or_invalid_record_attributes", Detail: err.Error(), Namespace: "record"})
					continue
				}
				body, err := decodeValue(lr.Body())
				if err != nil {
					out.rejections = append(out.rejections, rejection{Reason: "invalid_body", Detail: err.Error(), Namespace: "body"})
					continue
				}
				tsRaw := uint64(lr.Timestamp())
				observedRaw := uint64(lr.ObservedTimestamp())
				// A record with no source timestamp is served at its observed
				// time (the pinned stock exporter uses the same fallback); the
				// canonical payload keeps the raw values.
				servedTS := tsRaw
				if servedTS == 0 {
					servedTS = observedRaw
				}
				row := logRow{
					WorkspaceID:    workspace,
					Dataset:        dataset,
					TS:             nsTime(servedTS),
					ObservedTS:     nsTime(observedRaw),
					ServiceName:    resValues["service.name"],
					SeverityText:   lr.SeverityText(),
					SeverityNumber: int32(lr.SeverityNumber()),
					Body:           lr.Body().AsString(),
					TraceID:        hexTraceID(lr.TraceID()),
					SpanID:         hexSpanID(lr.SpanID()),
					EventName:      lr.EventName(),
					Resource:       nilIfEmpty(resValues),
					ResourceTypes:  nilIfEmpty(resTypes),
					Scope:          nilIfEmpty(scope.strings),
					ScopeTypes:     nilIfEmpty(scope.types),
				}
				canonical, err := buildCanonical(canonicalInput{
					kind:        "log",
					workspace:   workspace,
					dataset:     dataset,
					tsRaw:       tsRaw,
					observedRaw: observedRaw,
					traceID:     row.TraceID,
					spanID:      row.SpanID,
					body:        body,
					resource:    resTyped,
					scope:       scope.values,
					record:      rec.values,
					logEnvelope: map[string]any{
						"severity_text":   row.SeverityText,
						"severity_number": int64(row.SeverityNumber),
						"event_name":      row.EventName,
					},
				})
				if err != nil {
					out.rejections = append(out.rejections, rejection{Reason: "canonical_encoding_failed", Detail: err.Error()})
					continue
				}
				maps := newRecordMaps()
				mappingFailed := false
				for key, v := range rec.values {
					if err := maps.place(key, v); err != nil {
						out.rejections = append(out.rejections, rejection{Reason: "record_mapping_failed", Detail: err.Error(), Namespace: "record"})
						mappingFailed = true
						break
					}
				}
				if mappingFailed {
					continue
				}
				row.EventID = canonical.eventID
				row.Payload = canonical.payload
				row.PayloadSHA256 = canonical.sha256
				row.RecordInt = nilIfEmpty(maps.ints)
				row.RecordDouble = nilIfEmpty(maps.doubles)
				row.RecordString = nilIfEmpty(maps.strs)
				row.RecordBool = nilIfEmpty(maps.bools)
				row.RecordOther = nilIfEmpty(maps.others)
				out.rows = append(out.rows, row)
				out.canonical = append(out.canonical, canonicalRow{
					WorkspaceID: workspace, Dataset: dataset, Signal: "logs", EventID: canonical.eventID,
					TS: row.TS, Payload: canonical.payload, PayloadSHA256: canonical.sha256,
				})
			}
		}
	}
	return out
}

func prepareSpans(td ptrace.Traces) preparedSpans {
	var out preparedSpans
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		res, err := decodeAttrs(rs.Resource().Attributes(), false)
		if err != nil {
			out.rejections = append(out.rejections, rejection{Reason: "invalid_resource_attributes", Detail: err.Error(), Namespace: "resource"})
			continue
		}
		workspace, dataset, err := trustedScope(res)
		if err != nil {
			out.rejections = append(out.rejections, rejection{Reason: "missing_trusted_scope", Detail: err.Error(), Namespace: "resource"})
			continue
		}
		resTyped, resValues, resTypes, _ := resourceForStorage(res)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			scope, err := decodeAttrs(ss.Scope().Attributes(), true)
			if err != nil {
				out.rejections = append(out.rejections, rejection{Reason: "invalid_scope_attributes", Detail: err.Error(), Namespace: "scope"})
				continue
			}
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)
				rec, err := decodeAttrs(sp.Attributes(), true)
				if err != nil {
					out.rejections = append(out.rejections, rejection{Reason: "duplicate_or_invalid_span_attributes", Detail: err.Error(), Namespace: "span"})
					continue
				}
				start, end := uint64(sp.StartTimestamp()), uint64(sp.EndTimestamp())
				var duration *int64
				timing := spanTimingOK
				switch {
				case start == 0 || end == 0:
					// An unset OTLP timestamp is 0 and cannot be distinguished
					// from an explicit 0, so either side being unset means the
					// duration is not observable. Missing timing takes precedence
					// over the reversed classification: a missing start must not
					// fabricate a duration from epoch 0, and a missing end must
					// not be read as end < start.
					timing = spanTimingMissing
				case end < start:
					// Reversed timing is accepted and preserved: the supplied
					// timestamps are stored as received and the derived duration
					// is unavailable with an explicit diagnostic, instead of the
					// unsigned underflow Experiment 018 measured.
					timing = spanTimingEndBeforeStart
				default:
					d := int64(end - start)
					duration = &d
				}
				row := spanRow{
					WorkspaceID:    workspace,
					Dataset:        dataset,
					TS:             nsTime(start),
					EndTS:          nsTime(end),
					ServiceName:    resValues["service.name"],
					TraceID:        hexTraceID(sp.TraceID()),
					SpanID:         hexSpanID(sp.SpanID()),
					ParentID:       hexSpanID(sp.ParentSpanID()),
					SpanName:       sp.Name(),
					SpanKind:       sp.Kind().String(),
					StatusCode:     int32(sp.Status().Code()),
					DurationNS:     duration,
					TimingStatus:   timing,
					Resource:       nilIfEmpty(resValues),
					ResourceTypes:  nilIfEmpty(resTypes),
					SpanScope:      nilIfEmpty(scope.strings),
					SpanScopeTypes: nilIfEmpty(scope.types),
				}
				canonical, err := buildCanonical(canonicalInput{
					kind:        "span",
					workspace:   workspace,
					dataset:     dataset,
					tsRaw:       start,
					observedRaw: start,
					traceID:     row.TraceID,
					spanID:      row.SpanID,
					body:        value{kind: kindEmpty},
					resource:    resTyped,
					scope:       scope.values,
					record:      rec.values,
					spanEnvelope: &spanEnvelope{
						name:         row.SpanName,
						parentSpanID: row.ParentID,
						kind:         row.SpanKind,
						statusCode:   row.StatusCode,
						startNS:      start,
						endNS:        end,
						hasDuration:  duration != nil,
						durationNS:   durationValue(duration),
					},
				})
				if err != nil {
					out.rejections = append(out.rejections, rejection{Reason: "canonical_encoding_failed", Detail: err.Error()})
					continue
				}
				maps := newRecordMaps()
				mappingFailed := false
				for key, v := range rec.values {
					if err := maps.place(key, v); err != nil {
						out.rejections = append(out.rejections, rejection{Reason: "record_mapping_failed", Detail: err.Error(), Namespace: "span"})
						mappingFailed = true
						break
					}
				}
				if mappingFailed {
					continue
				}
				row.EventID = canonical.eventID
				row.Payload = canonical.payload
				row.PayloadSHA256 = canonical.sha256
				row.SpanInt = nilIfEmpty(maps.ints)
				row.SpanDouble = nilIfEmpty(maps.doubles)
				row.SpanString = nilIfEmpty(maps.strs)
				row.SpanBool = nilIfEmpty(maps.bools)
				row.SpanOther = nilIfEmpty(maps.others)
				out.rows = append(out.rows, row)
				out.canonical = append(out.canonical, canonicalRow{
					WorkspaceID: workspace, Dataset: dataset, Signal: "spans", EventID: canonical.eventID,
					TS: row.TS, Payload: canonical.payload, PayloadSHA256: canonical.sha256,
				})
			}
		}
	}
	return out
}

func durationValue(d *int64) int64 {
	if d == nil {
		return 0
	}
	return *d
}

type spanEnvelope struct {
	name         string
	parentSpanID string
	kind         string
	statusCode   int32
	startNS      uint64
	endNS        uint64
	hasDuration  bool
	durationNS   int64
}

type canonicalInput struct {
	kind         string
	workspace    string
	dataset      string
	tsRaw        uint64
	observedRaw  uint64
	traceID      string
	spanID       string
	body         value
	resource     map[string]value
	scope        map[string]value
	record       map[string]value
	logEnvelope  map[string]any
	spanEnvelope *spanEnvelope
}

type canonicalDoc struct {
	eventID string
	payload string
	sha256  string
}

// buildCanonical produces the versioned post-policy canonical payload. It
// extends Experiment 019's namespace/value encoding with the envelope fields the
// harness needs to reconstruct the serving row; Experiment 019 explicitly did
// not propose its payload encoding as the final wire format.
func buildCanonical(in canonicalInput) (canonicalDoc, error) {
	doc := map[string]any{
		"schema": canonicalSchema,
		"kind":   in.kind,
		"identity": map[string]any{
			"tenant_id": in.workspace,
			"dataset":   in.dataset,
			"event_id":  "",
		},
		"time": map[string]any{
			"timestamp_ns": strconv.FormatUint(in.tsRaw, 10),
			"observed_ns":  strconv.FormatUint(in.observedRaw, 10),
		},
		"body":     canonicalNode(in.body),
		"resource": canonicalNodes(in.resource),
		"scope":    canonicalNodes(in.scope),
		"record":   canonicalNodes(in.record),
	}
	if in.traceID != "" {
		doc["trace"] = map[string]any{"trace_id": in.traceID, "span_id": in.spanID}
	}
	if in.logEnvelope != nil {
		doc["log"] = in.logEnvelope
	}
	if in.spanEnvelope != nil {
		span := map[string]any{
			"name":           in.spanEnvelope.name,
			"parent_span_id": in.spanEnvelope.parentSpanID,
			"kind":           in.spanEnvelope.kind,
			"status_code":    strconv.FormatInt(int64(in.spanEnvelope.statusCode), 10),
			"start_ns":       strconv.FormatUint(in.spanEnvelope.startNS, 10),
			"end_ns":         strconv.FormatUint(in.spanEnvelope.endNS, 10),
		}
		if in.spanEnvelope.hasDuration {
			span["duration_ns"] = strconv.FormatInt(in.spanEnvelope.durationNS, 10)
		}
		doc["span"] = span
	}

	// Identity is content-derived: encode once with an empty event id, hash it,
	// then re-encode with the derived id.
	pre, err := marshalCanonical(doc)
	if err != nil {
		return canonicalDoc{}, err
	}
	id := eventID(in.kind, in.workspace, in.dataset, in.tsRaw, in.observedRaw, in.traceID, in.spanID, pre)
	doc["identity"].(map[string]any)["event_id"] = id
	payload, err := marshalCanonical(doc)
	if err != nil {
		return canonicalDoc{}, err
	}
	sum := sha256.Sum256([]byte(payload))
	return canonicalDoc{eventID: id, payload: payload, sha256: hex.EncodeToString(sum[:])}, nil
}
