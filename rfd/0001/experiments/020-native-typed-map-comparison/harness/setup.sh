#!/usr/bin/env bash
# One-time setup for Experiment 020: the pinned Python fixture environment and
# resolution of the Nix-provided ClickHouse binary.
#
# Everything lands in LH020_ROOT (default /home/user/lh020), outside the
# repository. Re-running is safe: the venv is only created once and the pinned
# ClickHouse binary is resolved from the repository flake. No Collector build is
# needed: this experiment compares storage layouts and queries, not the stock
# Collector path (Experiment 019 owns that).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH020_ROOT:-/home/user/lh020}"
mkdir -p "$ROOT/logs" "$ROOT/run"

log() { printf '[setup-020] %s\n' "$*"; }

# Same pinned fixture runtime as Experiment 019: fixture020.py imports the 019
# typed inventory, which needs opentelemetry-proto to build its OTLP values.
if [[ ! -x "$ROOT/venv/bin/python" ]]; then
  log "creating the Python fixture environment"
  python3 -m venv "$ROOT/venv"
  "$ROOT/venv/bin/pip" -q install --disable-pip-version-check \
    'opentelemetry-proto==1.39.0' 'protobuf==6.33.6' 'requests==2.34.2'
fi
"$ROOT/venv/bin/pip" freeze

log "resolving ClickHouse from the pinned Nix input"
(cd "$HERE" && nix develop --command bash -c 'clickhouse-server --version')

log "setup complete; run harness/run.sh all"
