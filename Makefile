VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "devel")
REVISION  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BRANCH    ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
BUILD_USER ?= $(shell whoami)@$(shell hostname)
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS   := -X github.com/chaba2/lighthouse/pkg/version.Version=$(VERSION) \
             -X github.com/chaba2/lighthouse/pkg/version.Revision=$(REVISION) \
             -X github.com/chaba2/lighthouse/pkg/version.Branch=$(BRANCH) \
             -X github.com/chaba2/lighthouse/pkg/version.BuildUser=$(BUILD_USER) \
             -X github.com/chaba2/lighthouse/pkg/version.BuildDate=$(BUILD_DATE)
BUILD_TAGS ?=
GO_BUILD_TAGS := $(if $(strip $(BUILD_TAGS)),-tags "$(BUILD_TAGS)",)

PROM_VERSION ?= v3.11.3
LOKI_VERSION ?= 3.7.1
PROM_GOLDEN ?= latest
LOKI_GOLDEN ?= latest

.PHONY: build
build:
	go build $(GO_BUILD_TAGS) -ldflags "$(LDFLAGS)" -o lighthouse ./cmd/lighthouse

.PHONY: build-release
build-release:
	$(MAKE) build BUILD_TAGS=release

.PHONY: verify-release-build
verify-release-build:
	scripts/verify-release-build.sh

SCHEMA_PROFILE ?=
SCHEMA_OUT ?= pkg/db/schema.sql
SCHEMA_MODE ?= live
SCHEMA_IMAGE ?= clickhouse/clickhouse-server:25.8-alpine

.PHONY: schema-dump
schema-dump:
	go run ./tools/schema-dump -mode "$(SCHEMA_MODE)" -profile "$(SCHEMA_PROFILE)" -out "$(SCHEMA_OUT)" -image "$(SCHEMA_IMAGE)"

.PHONY: dev
dev:
	go run -ldflags "$(LDFLAGS)" ./cmd/lighthouse dev --config ./dev/config.yml

SAMPLE_LOG_FILES ?= ./samples/logs/*.sample.log

.PHONY: dev-load-sample-logs
dev-load-sample-logs:
	go run -ldflags "$(LDFLAGS)" ./cmd/lighthouse load-sample-logs $(SAMPLE_LOG_FILES)

.PHONY: test
test:
	go test -v -count=1 ./...

.PHONY: conformance
conformance:
	CONFORMANCE_PROM_GOLDEN=$(PROM_GOLDEN) \
	CONFORMANCE_LOKI_GOLDEN=$(LOKI_GOLDEN) \
	go test -v -count=1 -tags=conformance -timeout=10m ./pkg/conformance/...

.PHONY: conformance-promql
conformance-promql:
	CONFORMANCE_PROM_GOLDEN=$(PROM_GOLDEN) \
	go test -v -count=1 -tags=conformance -timeout=10m ./pkg/conformance/promql/...

.PHONY: conformance-logql
conformance-logql:
	CONFORMANCE_LOKI_GOLDEN=$(LOKI_GOLDEN) \
	go test -v -count=1 -tags=conformance -timeout=10m ./pkg/conformance/logql/...

.PHONY: conformance-record
conformance-record:
	CONFORMANCE_MODE=record \
	CONFORMANCE_PROM_IMAGE=prom/prometheus:$(PROM_VERSION) \
	CONFORMANCE_LOKI_IMAGE=grafana/loki:$(LOKI_VERSION) \
	go test -v -count=1 -tags=conformance -timeout=15m ./pkg/conformance/...

.PHONY: conformance-compat
conformance-compat:
	@for dir in pkg/conformance/promql/golden/prometheus-*; do \
		version=$$(basename $$dir | sed 's/prometheus-//'); \
		echo "--- Checking against Prometheus $$version ---"; \
		CONFORMANCE_PROM_GOLDEN=$$version \
		go test -v -count=1 -tags=conformance -timeout=10m ./pkg/conformance/promql/... || true; \
	done
	@for dir in pkg/conformance/logql/golden/loki-*; do \
		version=$$(basename $$dir | sed 's/loki-//'); \
		echo "--- Checking against Loki $$version ---"; \
		CONFORMANCE_LOKI_GOLDEN=$$version \
		go test -v -count=1 -tags=conformance -timeout=10m ./pkg/conformance/logql/... || true; \
	done

.PHONY: conformance-report
conformance-report:
	CONFORMANCE_REPORT_PATH=conformance-report.json \
	CONFORMANCE_PROM_GOLDEN=$(PROM_GOLDEN) \
	CONFORMANCE_LOKI_GOLDEN=$(LOKI_GOLDEN) \
	go test -v -count=1 -tags=conformance -timeout=10m ./pkg/conformance/...

.PHONY: conformance-buildcheck
conformance-buildcheck:
	go test -v -count=1 ./...

.PHONY: smoke-metrics
smoke-metrics:
	go test -v -count=1 -timeout=10m ./pkg/e2e -run 'TestPipeline_.*Metrics'

.PHONY: lint
lint:
	golangci-lint run --verbose

.PHONY: up
up:
	docker-compose up -d --remove-orphans

.PHONY: build-up
build-up:
	docker-compose up -d --build --remove-orphans

.PHONY: down
down:
	docker-compose stop

.PHONY: gen
gen:
	buf generate

.PHONY: gen-check
gen-check:
	buf generate
	git diff --exit-code -- proto/gen

.PHONY: chcl
chcl:
	clickhouse-client --port 9000 --user admin --password admin -d lighthouse_dev
