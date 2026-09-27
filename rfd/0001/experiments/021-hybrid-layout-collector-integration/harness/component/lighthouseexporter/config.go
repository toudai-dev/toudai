// Package lighthouseexporter is Experiment 021's bounded owned exporter: it maps
// OTLP logs and spans into the provisional hybrid serving layout (native typed
// record Maps, string + companion type Maps for resource/scope, a separate
// canonical payload table) and writes them to ClickHouse.
//
// It is deliberately small: mapping, validation-before-write, two ordered
// inserts, and counters. Queue, retry, timeout, and exporter telemetry come from
// exporterhelper, the same machinery the stock ClickHouse exporter reuses.
// Schema creation is not part of this component; the harness manages the DDL.
package lighthouseexporter

import (
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config is the exporter configuration. There is no option that names or
// computes additional table columns: the exporter owns the insert statements for
// exactly the tables below, which is the capability the pinned stock exporter
// lacks (Experiment 019 measured its fixed column lists).
type Config struct {
	exporterhelper.TimeoutConfig `mapstructure:",squash"`
	QueueConfig                  configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	BackOffConfig                configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`

	// Endpoint is the ClickHouse native-protocol address (host:port).
	Endpoint string `mapstructure:"endpoint"`
	// Database holds the serving and canonical tables.
	Database string `mapstructure:"database"`
	Username string `mapstructure:"username"`
	// Password is opaque so a configuration dump cannot print it.
	Password configopaque.String `mapstructure:"password"`

	CanonicalTable    string `mapstructure:"canonical_table"`
	ServingLogsTable  string `mapstructure:"serving_logs_table"`
	ServingSpansTable string `mapstructure:"serving_spans_table"`

	// LogBatchSummaries logs one line per export with the batch's workspace and
	// event identities. It exists so a test can assert what a single export
	// actually carried (including mixed-workspace batches) instead of inferring
	// it from two requests that merely shared a queue.
	LogBatchSummaries bool `mapstructure:"log_batch_summaries"`

	// MaxOpenConns bounds the driver's connection pool.
	MaxOpenConns int `mapstructure:"max_open_conns"`
}

var (
	errMissingTrustedScope = errors.New("trusted scope is missing: the gateway must inject the lighthouse workspace/dataset attributes before the exporter")
	errNoRecords           = errors.New("no records to export")
)

func defaultConfig() *Config {
	return &Config{
		TimeoutConfig:     exporterhelper.TimeoutConfig{Timeout: 10 * time.Second},
		QueueConfig:       configoptional.Default(exporterhelper.NewDefaultQueueConfig()),
		BackOffConfig:     configretry.NewDefaultBackOffConfig(),
		Endpoint:          "127.0.0.1:19101",
		Database:          "lh021",
		CanonicalTable:    "canonical_events",
		ServingLogsTable:  "serving_logs",
		ServingSpansTable: "serving_spans",
		MaxOpenConns:      4,
	}
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	if c.Database == "" {
		return errors.New("database is required")
	}
	if c.CanonicalTable == "" || c.ServingLogsTable == "" || c.ServingSpansTable == "" {
		return errors.New("canonical_table, serving_logs_table, and serving_spans_table are required")
	}
	return nil
}

func (c *Config) newClickHouseOptions() *clickhouse.Options {
	opts := &clickhouse.Options{
		Addr: []string{c.Endpoint},
		Auth: clickhouse.Auth{
			Database: c.Database,
			Username: c.Username,
			Password: string(c.Password),
		},
		DialTimeout:  5 * time.Second,
		MaxOpenConns: c.MaxOpenConns,
	}
	// The driver's own read timeout defaults to five minutes, so without this a
	// destination that accepts a write and never answers parks the export until
	// the process exits: Experiment 021 measured a withheld insert response
	// leaving the export blocked for 31s with no retry log and no failure, i.e.
	// the exporterhelper timeout alone did not unblock the socket read. Binding
	// the read deadline to the configured export timeout keeps a lost response a
	// bounded, retryable failure.
	if c.Timeout > 0 {
		opts.ReadTimeout = c.Timeout
	}
	return opts
}
