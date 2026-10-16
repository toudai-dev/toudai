.PHONY: build
build:
	cargo build

.PHONY: build-release
build-release:
	cargo build --release

.PHONY: dev
dev:
	cargo run

.PHONY: test
test:
	cargo test --all-targets

.PHONY: lint
lint:
	cargo clippy --all-targets --all-features -- -D warnings

.PHONY: fmt
fmt:
	cargo fmt --check

.PHONY: check-rfds
check-rfds:
	scripts/check-rfd-status.sh
	scripts/check-rfd-status-test.sh
	@for file in $$(find rfd -name '*.adoc' -type f | sort); do \
		asciidoctor -w --failure-level WARN -o /dev/null "$$file"; \
	done

.PHONY: up
up:
	docker-compose up -d --remove-orphans

.PHONY: build-up
build-up:
	docker-compose up -d --build --remove-orphans

.PHONY: down
down:
	docker-compose stop

.PHONY: chcl
chcl:
	clickhouse-client --port 9000 --user admin --password admin -d lighthouse_dev

.PHONY: collector-build
collector-build:
	cd collector && go build -o ../target/collector/lighthouse-collector .

.PHONY: collector-test
collector-test:
	cd collector && go vet ./... && go test ./...
