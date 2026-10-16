package lighthouseexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// componentType is the exporter's configuration type. The component is
// development-stability: Decision 0001 fixes the layout for the PoC only.
var componentType = component.MustNewType("lighthouse")

// NewFactory returns the exporter factory. Queue, retry, and timeout behavior
// come from exporterhelper; only the mapping and the two insert statements are
// owned code.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		componentType,
		func() component.Config { return defaultConfig() },
		exporter.WithLogs(createLogs, component.StabilityLevelDevelopment),
		exporter.WithTraces(createTraces, component.StabilityLevelDevelopment),
	)
}

func createLogs(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	exp, err := newExporter(c, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(ctx, set, cfg, exp.pushLogs,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	c := cfg.(*Config)
	exp, err := newExporter(c, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(ctx, set, cfg, exp.pushTraces,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}
