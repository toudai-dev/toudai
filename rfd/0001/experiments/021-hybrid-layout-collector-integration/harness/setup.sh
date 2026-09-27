#!/usr/bin/env bash
# Experiment 021 environment setup: pinned downloads, the component build, and
# the build manifest. Idempotent: existing pinned artifacts are reused.
#
# Everything is disposable and lives under LH021_ROOT (default /home/user/lh021);
# no repository file is modified except the tracked harness itself.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH021_ROOT:-/home/user/lh021}"
BIN="$ROOT/bin"
RESULTS="${LH021_RESULTS:-$HERE/results}"
GO="$BIN/go/bin/go"
COLLECTOR_RELEASE="v0.161.0"
COLLECTOR_TARBALL="otelcol-contrib_0.161.0_linux_amd64.tar.gz"
COLLECTOR_URL="https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/${COLLECTOR_RELEASE}/${COLLECTOR_TARBALL}"
CHECKSUM_URL="${COLLECTOR_URL}.sha256"
GO_VERSION="go1.26.8"
GO_TARBALL="${GO_VERSION}.linux-amd64.tar.gz"
GO_URL="https://go.dev/dl/${GO_TARBALL}"

log() { printf '[setup021] %s\n' "$*"; }

mkdir -p "$BIN" "$ROOT/logs" "$ROOT/run" "$RESULTS" "$ROOT/gomodcache" "$ROOT/gopath"

# Go toolchain -----------------------------------------------------------------
if [[ ! -x "$GO" ]]; then
  log "downloading $GO_VERSION"
  curl -sSL -o "$BIN/$GO_TARBALL" "$GO_URL"
  tar -C "$BIN" -xzf "$BIN/$GO_TARBALL"
fi
"$GO" version

# Pinned stock collector baseline ----------------------------------------------
# The released artifact is verified against the release's own .sha256 file, which
# is what makes the "pinned Collector baseline 0.161.0" claim checkable.
TARBALL_LOCAL=""
for candidate in "$BIN/$COLLECTOR_TARBALL" "$BIN/otelcol-contrib.tar.gz"; do
  if [[ -f "$candidate" ]]; then TARBALL_LOCAL="$candidate"; break; fi
done
if [[ -z "$TARBALL_LOCAL" ]]; then
  log "downloading otelcol-contrib $COLLECTOR_RELEASE"
  curl -sSL -o "$BIN/$COLLECTOR_TARBALL" "$COLLECTOR_URL"
  TARBALL_LOCAL="$BIN/$COLLECTOR_TARBALL"
fi
if [[ ! -x "$BIN/otelcol-contrib" ]]; then
  tar -C "$BIN" -xzf "$TARBALL_LOCAL"
fi
actual_sha="$(sha256sum "$TARBALL_LOCAL" | cut -d' ' -f1)"
expected_sha="$(curl -sSL "$CHECKSUM_URL" | tr -d '[:space:]')"
echo "$actual_sha  $TARBALL_LOCAL" >"$BIN/otelcol-contrib.tar.gz.sha256"
if [[ "$actual_sha" != "$expected_sha" ]]; then
  log "PIN MISMATCH: local $actual_sha != release $expected_sha"
  exit 1
fi
log "collector artifact verified: $actual_sha ($TARBALL_LOCAL)"

# Python environment for the fixture and the verifier --------------------------
PY="$ROOT/venv/bin/python"
if [[ ! -x "$PY" ]]; then
  log "creating the python venv"
  (cd "$ROOT" && uv venv --python 3.12 venv)
  (cd "$ROOT" && uv pip install --python "$PY" \
    opentelemetry-proto==1.39.0 protobuf==6.33.6 requests==2.34.2)
fi
"$PY" -c 'import google.protobuf, opentelemetry.proto, requests; print("python deps ok")'

# The owned exporter distribution ---------------------------------------------
log "building the experiment 021 collector distribution"
(cd "$HERE/component" && GOFLAGS=-mod=mod GOMODCACHE="$ROOT/gomodcache" GOPATH="$ROOT/gopath" \
  GOTOOLCHAIN=local "$GO" build -o "$BIN/lighthouse-collector-021" .)
(cd "$HERE/component" && GOFLAGS=-mod=mod GOMODCACHE="$ROOT/gomodcache" GOPATH="$ROOT/gopath" \
  GOTOOLCHAIN=local "$GO" vet ./...)
"$BIN/lighthouse-collector-021" --version | head -3

log "done. Start the supervised ClickHouse with:"
log "  amp orb service start clickhouse-021 --command $HERE/start-clickhouse.sh"
