package lighthouseexporter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type lighthouseExporter struct {
	cfg     *Config
	set     exporter.Settings
	conn    clickhouse.Conn
	metrics *exporterMetrics
}

func newExporter(cfg *Config, set exporter.Settings) (*lighthouseExporter, error) {
	m, err := newExporterMetrics(set.MeterProvider)
	if err != nil {
		return nil, err
	}
	return &lighthouseExporter{cfg: cfg, set: set, metrics: m}, nil
}

func (e *lighthouseExporter) start(ctx context.Context, _ component.Host) error {
	conn, err := clickhouse.Open(e.cfg.newClickHouseOptions())
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}
	e.conn = conn
	// A failed ping is not fatal: the destination may be down when the gateway
	// restarts, and the queue plus retry path is what surfaces that. This
	// mirrors the pinned stock exporter continuing after schema detection fails
	// (Experiment 018 measured that behavior).
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		e.set.Logger.Warn("lighthouse exporter could not reach ClickHouse at start; the queue will retry", zap.Error(err))
	}
	return nil
}

func (e *lighthouseExporter) shutdown(_ context.Context) error {
	if e.conn == nil {
		return nil
	}
	return e.conn.Close()
}

// pushLogs validates and canonicalizes the complete request before the first
// write, so a later invalid record cannot leave the canonical destination
// written and the serving destination missing. Validation failures are counted
// per record and excluded from both destinations; the remaining records are
// exported. A batch that is missing its gateway-injected scope is rejected as a
// whole with a permanent error (fail closed), because it means the pipeline is
// misconfigured rather than that one producer record is bad.
func (e *lighthouseExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	prepared := prepareLogs(ld)
	e.recordRejections(ctx, "logs", prepared.rejections)
	if len(prepared.rows) == 0 {
		if len(prepared.rejections) > 0 {
			e.metrics.rejectedBatches.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", "logs"), attribute.String("reason", "all_records_rejected")))
			return consumererror.NewPermanent(fmt.Errorf("logs batch rejected: %d records failed validation", len(prepared.rejections)))
		}
		return nil
	}
	if batchInvalidated(prepared.rejections) {
		e.metrics.rejectedBatches.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", "logs"), attribute.String("reason", "missing_trusted_scope")))
		return consumererror.NewPermanent(errMissingTrustedScope)
	}
	e.logBatchSummary(ctx, "logs", len(prepared.rows), prepared.rejections, logWorkspaces(prepared.rows), logEventIDs(prepared.rows))
	if err := e.insertCanonical(ctx, prepared.canonical); err != nil {
		e.metrics.insertFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", "logs"), attribute.String("destination", "canonical")))
		return fmt.Errorf("canonical insert failed: %w", err)
	}
	e.metrics.records.Add(ctx, int64(len(prepared.canonical)), metric.WithAttributes(attribute.String("signal", "logs"), attribute.String("destination", "canonical")))
	if err := e.insertServingLogs(ctx, prepared.rows); err != nil {
		e.metrics.insertFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", "logs"), attribute.String("destination", "serving")))
		// The canonical insert already committed. There is no cross-destination
		// transaction; a retry re-attempts both writes and can duplicate the
		// committed destination. The experiment measures that explicitly.
		e.metrics.partialWriteBatches.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", "logs")))
		return fmt.Errorf("serving insert failed after the canonical insert committed: %w", err)
	}
	e.metrics.records.Add(ctx, int64(len(prepared.rows)), metric.WithAttributes(attribute.String("signal", "logs"), attribute.String("destination", "serving")))
	return nil
}

func (e *lighthouseExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	prepared := prepareSpans(td)
	e.recordRejections(ctx, "spans", prepared.rejections)
	if len(prepared.rows) == 0 {
		if len(prepared.rejections) > 0 {
			e.metrics.rejectedBatches.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", "spans"), attribute.String("reason", "all_records_rejected")))
			return consumererror.NewPermanent(fmt.Errorf("spans batch rejected: %d records failed validation", len(prepared.rejections)))
		}
		return nil
	}
	if batchInvalidated(prepared.rejections) {
		e.metrics.rejectedBatches.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", "spans"), attribute.String("reason", "missing_trusted_scope")))
		return consumererror.NewPermanent(errMissingTrustedScope)
	}
	e.logBatchSummary(ctx, "spans", len(prepared.rows), prepared.rejections, spanWorkspaces(prepared.rows), spanEventIDs(prepared.rows))
	if err := e.insertCanonical(ctx, prepared.canonical); err != nil {
		e.metrics.insertFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", "spans"), attribute.String("destination", "canonical")))
		return fmt.Errorf("canonical insert failed: %w", err)
	}
	e.metrics.records.Add(ctx, int64(len(prepared.canonical)), metric.WithAttributes(attribute.String("signal", "spans"), attribute.String("destination", "canonical")))
	if err := e.insertServingSpans(ctx, prepared.rows); err != nil {
		e.metrics.insertFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", "spans"), attribute.String("destination", "serving")))
		e.metrics.partialWriteBatches.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", "spans")))
		return fmt.Errorf("serving insert failed after the canonical insert committed: %w", err)
	}
	e.metrics.records.Add(ctx, int64(len(prepared.rows)), metric.WithAttributes(attribute.String("signal", "spans"), attribute.String("destination", "serving")))
	return nil
}

func batchInvalidated(rejections []rejection) bool {
	for _, r := range rejections {
		if r.Reason == "missing_trusted_scope" {
			return true
		}
	}
	return false
}

func (e *lighthouseExporter) recordRejections(ctx context.Context, signal string, rejections []rejection) {
	for _, r := range rejections {
		attrs := []attribute.KeyValue{
			attribute.String("signal", signal),
			attribute.String("reason", r.Reason),
		}
		if r.Namespace != "" {
			attrs = append(attrs, attribute.String("namespace", r.Namespace))
		}
		e.metrics.rejectedRecords.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

func (e *lighthouseExporter) logBatchSummary(ctx context.Context, signal string, records int, rejections []rejection, workspaces, eventIDs []string) {
	mixed := false
	if len(workspaces) > 1 {
		mixed = true
	}
	e.metrics.batches.Add(ctx, 1, metric.WithAttributes(attribute.String("signal", signal), attribute.Bool("cross_workspace", mixed)))
	if !e.cfg.LogBatchSummaries {
		return
	}
	e.set.Logger.Info("lighthouse exporter batch",
		zap.String("signal", signal),
		zap.Int("records", records),
		zap.Int("rejected", len(rejections)),
		zap.Bool("cross_workspace", mixed),
		zap.Strings("workspaces", workspaces),
		zap.Strings("event_ids", eventIDs),
	)
}

func logWorkspaces(rows []logRow) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range rows {
		if !seen[r.WorkspaceID] {
			seen[r.WorkspaceID] = true
			out = append(out, r.WorkspaceID)
		}
	}
	return out
}

func logEventIDs(rows []logRow) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.EventID)
	}
	return out
}

func spanWorkspaces(rows []spanRow) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range rows {
		if !seen[r.WorkspaceID] {
			seen[r.WorkspaceID] = true
			out = append(out, r.WorkspaceID)
		}
	}
	return out
}

func spanEventIDs(rows []spanRow) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.EventID)
	}
	return out
}

// Column lists of the Decision 0001 DDL in clickhouse/schema, in the order the
// batch appends below supply values. schema_test.go fails if a list drifts from
// the DDL. The Go row structs keep their experiment-era field names (WorkspaceID,
// TS); the stored columns are tenant_id and timestamp.
var (
	canonicalColumns = []string{"tenant_id", "dataset", "signal", "event_id", "timestamp", "schema_version", "payload", "payload_sha256"}

	servingLogsColumns = []string{"tenant_id", "dataset", "event_id", "routing_epoch", "timestamp", "observed_ts", "kind", "service_name", "severity_text", "severity_number", "body", "trace_id", "span_id", "event_name", "resource", "scope", "resource_types", "scope_types", "record_int", "record_double", "record_string", "record_bool", "record_other", "payload_sha256"}

	servingSpansColumns = []string{"tenant_id", "dataset", "event_id", "routing_epoch", "timestamp", "end_ts", "kind", "service_name", "trace_id", "span_id", "parent_span_id", "span_name", "span_kind", "status_code", "duration_ns", "span_timing_status", "resource", "resource_types", "span_scope", "span_scope_types", "span_int", "span_double", "span_string", "span_bool", "span_other", "payload_sha256"}
)

func insertStatement(table string, columns []string) string {
	return "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ")"
}

func (e *lighthouseExporter) insertCanonical(ctx context.Context, rows []canonicalRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := e.conn.PrepareBatch(ctx, insertStatement(e.cfg.databaseTable(e.cfg.CanonicalTable), canonicalColumns))
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := batch.Append(r.WorkspaceID, r.Dataset, r.Signal, r.EventID, r.TS, canonicalSchema, r.Payload, r.PayloadSHA256); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (e *lighthouseExporter) insertServingLogs(ctx context.Context, rows []logRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := e.conn.PrepareBatch(ctx, insertStatement(e.cfg.databaseTable(e.cfg.ServingLogsTable), servingLogsColumns))
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := batch.Append(r.WorkspaceID, r.Dataset, r.EventID, e.cfg.RoutingEpoch, r.TS, r.ObservedTS, "log", r.ServiceName,
			r.SeverityText, r.SeverityNumber, r.Body, r.TraceID, r.SpanID, r.EventName,
			r.Resource, r.Scope, r.ResourceTypes, r.ScopeTypes,
			r.RecordInt, r.RecordDouble, r.RecordString, r.RecordBool, r.RecordOther, r.PayloadSHA256); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (e *lighthouseExporter) insertServingSpans(ctx context.Context, rows []spanRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := e.conn.PrepareBatch(ctx, insertStatement(e.cfg.databaseTable(e.cfg.ServingSpansTable), servingSpansColumns))
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := batch.Append(r.WorkspaceID, r.Dataset, r.EventID, e.cfg.RoutingEpoch, r.TS, r.EndTS, "span", r.ServiceName,
			r.TraceID, r.SpanID, r.ParentID, r.SpanName, r.SpanKind, r.StatusCode, r.DurationNS, r.TimingStatus,
			r.Resource, r.ResourceTypes, r.SpanScope, r.SpanScopeTypes,
			r.SpanInt, r.SpanDouble, r.SpanString, r.SpanBool, r.SpanOther, r.PayloadSHA256); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *Config) databaseTable(table string) string {
	if c.Database == "" {
		return quoteIdentifier(table)
	}
	return quoteIdentifier(c.Database) + "." + quoteIdentifier(table)
}

// quoteIdentifier quotes a table identifier; the configuration is operator
// input, not producer input, and quoted identifiers keep the insert statements
// explicit.
func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
