.PHONY: build
build:
	cargo build

.PHONY: build-release
build-release:
	cargo build --release

.PHONY: dev
dev:
	cargo run

# The fixture emitter is a standalone crate (not a workspace member), so each
# quality target covers it explicitly.
FIXTURE_EMITTER := fixture-emitter/Cargo.toml

.PHONY: build-fixture-emitter
build-fixture-emitter:
	cargo build --release --manifest-path $(FIXTURE_EMITTER)

.PHONY: test
test:
	cargo test --all-targets
	cargo test --all-targets --manifest-path $(FIXTURE_EMITTER)
	fixture-emitter/check-reproducible.sh

.PHONY: lint
lint:
	cargo clippy --all-targets --all-features -- -D warnings
	cargo clippy --all-targets --all-features --manifest-path $(FIXTURE_EMITTER) -- -D warnings

.PHONY: fmt
fmt:
	cargo fmt --check
	cargo fmt --check --manifest-path $(FIXTURE_EMITTER)

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
	clickhouse-client --port 9000 --user admin --password admin -d toudai_dev
