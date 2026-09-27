#!/usr/bin/env bash
# Disposable supervised ClickHouse 26.8.2.7-lts for Experiment 021.
#
# Started through the orb service supervisor so it survives Amp CLI restarts and
# orb pause/resume:
#
#   amp orb service start clickhouse-021 --command <this script>
#
# The pinned server comes from the repository's merged Nix setup
# (nix develop; flake.nix pins ClickHouse 26.8.2.7-lts). All state lives under
# LH021_ROOT, the ports are loopback-only, and nothing here is a production
# service.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH021_ROOT:-/home/user/lh021}"

export LH021_CH_PATH="$ROOT/run/clickhouse/data/"
export LH021_CH_TMP="$ROOT/run/clickhouse/tmp/"
export LH021_CH_USER_FILES="$ROOT/run/clickhouse/user_files/"
export LH021_CH_FORMAT_SCHEMAS="$ROOT/run/clickhouse/format_schemas/"
export LH021_CH_LOG="$ROOT/logs/clickhouse-server.log"
export LH021_CH_ERRORLOG="$ROOT/logs/clickhouse-server.err.log"
export LH021_CH_USERS="$HERE/clickhouse-users.xml"

mkdir -p "$LH021_CH_PATH" "$LH021_CH_TMP" "$LH021_CH_USER_FILES" "$LH021_CH_FORMAT_SCHEMAS" "$ROOT/logs"

server_bin="$(cd "$HERE" && nix develop --command bash -c 'command -v clickhouse-server')"
echo "[exp021] starting $("$server_bin" --version | head -1) with $HERE/clickhouse-config.xml"
exec "$server_bin" --config-file="$HERE/clickhouse-config.xml"
