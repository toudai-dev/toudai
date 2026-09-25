#!/usr/bin/env bash
# One-time setup for Experiment 019: pinned Collector build, Python fixture
# environment, and resolution of the Nix-provided ClickHouse binary.
#
# Everything lands in LH019_ROOT (default /home/user/lh019), outside the
# repository. Re-running is safe: downloads are checksum-verified and skipped
# when the artifact already exists. MinIO is not needed: this experiment does
# not exercise overflow or replay (Experiment 018 covers those).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH019_ROOT:-/home/user/lh019}"
mkdir -p "$ROOT/bin" "$ROOT/logs" "$ROOT/run"

OTELCOL_VERSION="0.161.0"
OTELCOL_SHA256="778c689efa681ff6e4722ce9f66b9b7f57c3ba009ab2e2b43dc2e0315862c731"
OTELCOL_URL="https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v${OTELCOL_VERSION}/otelcol-contrib_${OTELCOL_VERSION}_linux_amd64.tar.gz"

log() { printf '[setup-019] %s\n' "$*"; }

if [[ ! -x "$ROOT/bin/otelcol-contrib" ]]; then
  log "downloading otelcol-contrib v${OTELCOL_VERSION}"
  curl -sSfL -o "$ROOT/bin/otelcol-contrib.tar.gz" "$OTELCOL_URL"
  echo "${OTELCOL_SHA256}  $ROOT/bin/otelcol-contrib.tar.gz" | sha256sum --check -
  tar -xzf "$ROOT/bin/otelcol-contrib.tar.gz" -C "$ROOT/bin"
fi
"$ROOT/bin/otelcol-contrib" --version

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
