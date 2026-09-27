// Program lighthouse-collector-021 is Experiment 021's bounded Collector
// distribution: the stock gateway components the experiment needs plus the owned
// lighthouseexporter. Building a small distribution is how the owned exporter
// becomes a real Collector component instead of a test harness, without
// building a parallel runtime: the entry point is the same otelcol.NewCommand
// the OpenTelemetry Collector Builder generates, and every component except the
// exporter is a stock component at the pinned version.
package main

import (
	"os"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/routingconnector"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/bearertokenauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"

	"lighthouse.dev/rfd0001/exp021/lighthouseexporter"
)

// buildVersion is recorded here and in the build manifest because the
// distribution is built from the repository, not from a pinned release artifact.
const buildVersion = "0.1.0-exp021"

func components() (otelcol.Factories, error) {
	var err error
	factories := otelcol.Factories{}
	if factories.Receivers, err = otelcol.MakeFactoryMap(
		otlpreceiver.NewFactory(),
	); err != nil {
		return otelcol.Factories{}, err
	}
	if factories.Processors, err = otelcol.MakeFactoryMap(
		resourceprocessor.NewFactory(),
		// The stock transform processor is included so the stock write-path probe
		// can test stock capability in the same binary instead of asserting it.
		transformprocessor.NewFactory(),
	); err != nil {
		return otelcol.Factories{}, err
	}
	if factories.Connectors, err = otelcol.MakeFactoryMap(
		routingconnector.NewFactory(),
	); err != nil {
		return otelcol.Factories{}, err
	}
	if factories.Exporters, err = otelcol.MakeFactoryMap(
		lighthouseexporter.NewFactory(),
		fileexporter.NewFactory(),
		prometheusexporter.NewFactory(),
		// The stock ClickHouse exporter is included so the probe measures its
		// actual stored columns against the same fixture in the same binary.
		clickhouseexporter.NewFactory(),
	); err != nil {
		return otelcol.Factories{}, err
	}
	if factories.Extensions, err = otelcol.MakeFactoryMap(
		bearertokenauthextension.NewFactory(),
		filestorage.NewFactory(),
	); err != nil {
		return otelcol.Factories{}, err
	}
	// The telemetry factory is required for the config to unmarshal at all;
	// without it every config load fails with
	// "otelcol.Factories.Telemetry must not be nil" (measured while validating
	// the first version of this harness). It also parses service.telemetry,
	// which is where the Prometheus scrape endpoint is configured.
	factories.Telemetry = otelconftelemetry.NewFactory()
	return factories, nil
}

func main() {
	set := otelcol.CollectorSettings{
		BuildInfo: component.BuildInfo{
			Command:     "lighthouse-collector-021",
			Description: "Experiment 021 bounded Collector distribution with the owned lighthouse exporter",
			Version:     buildVersion,
		},
		Factories: components,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				ProviderFactories: []confmap.ProviderFactory{
					envprovider.NewFactory(),
					fileprovider.NewFactory(),
					yamlprovider.NewFactory(),
				},
			},
		},
	}
	if err := run(set); err != nil {
		os.Exit(1)
	}
}

func run(params otelcol.CollectorSettings) error {
	cmd := otelcol.NewCommand(params)
	return cmd.Execute()
}
