package lighthouseexporter

import (
	"go.opentelemetry.io/otel/metric"
)

// exporterMetrics instruments outcomes for each destination separately, because
// the two writes are not a transaction: a batch can commit canonically and fail
// on serving.
type exporterMetrics struct {
	records             metric.Int64Counter
	insertFailures      metric.Int64Counter
	rejectedRecords     metric.Int64Counter
	rejectedBatches     metric.Int64Counter
	partialWriteBatches metric.Int64Counter
	batches             metric.Int64Counter
}

func newExporterMetrics(mp metric.MeterProvider) (*exporterMetrics, error) {
	meter := mp.Meter("lighthouseexporter")
	records, err := meter.Int64Counter("lighthouse_exporter_records",
		metric.WithDescription("Records successfully inserted, by signal and destination."))
	if err != nil {
		return nil, err
	}
	insertFailures, err := meter.Int64Counter("lighthouse_exporter_insert_failures",
		metric.WithDescription("Failed inserts, by signal and destination."))
	if err != nil {
		return nil, err
	}
	rejectedRecords, err := meter.Int64Counter("lighthouse_exporter_rejected_records",
		metric.WithDescription("Records rejected by pre-write validation, by signal, reason, and namespace."))
	if err != nil {
		return nil, err
	}
	rejectedBatches, err := meter.Int64Counter("lighthouse_exporter_rejected_batches",
		metric.WithDescription("Batches rejected as a whole, by signal and reason."))
	if err != nil {
		return nil, err
	}
	partialWriteBatches, err := meter.Int64Counter("lighthouse_exporter_partial_write_batches",
		metric.WithDescription("Batches where the canonical insert committed and the serving insert failed."))
	if err != nil {
		return nil, err
	}
	batches, err := meter.Int64Counter("lighthouse_exporter_batches",
		metric.WithDescription("Exported batches, by signal and whether the batch carried more than one workspace."))
	if err != nil {
		return nil, err
	}
	return &exporterMetrics{
		records:             records,
		insertFailures:      insertFailures,
		rejectedRecords:     rejectedRecords,
		rejectedBatches:     rejectedBatches,
		partialWriteBatches: partialWriteBatches,
		batches:             batches,
	}, nil
}
