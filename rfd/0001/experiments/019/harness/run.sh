#!/usr/bin/env bash
# Experiment 019 orchestration: disposable local ClickHouse 26.8.2.7 and the
# pinned otelcol-contrib v0.161.0 build, plus the deterministic typed fixture.
# Every service is started and stopped by this script; nothing here is a
# production service, and no repository or benchmark data is touched.
#
# Usage: run.sh [stock-map|stock-json|managed|transform|identity|semantics|cost|files|all]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH019_ROOT:-/home/user/lh019}"
RESULTS="${LH019_RESULTS:-$HERE/results}"
OTELCOL="$ROOT/bin/otelcol-contrib"
PY="$ROOT/venv/bin/python"
RUN="$ROOT/run"
LOGS="$ROOT/logs"
mkdir -p "$RESULTS" "$RUN" "$LOGS"

export LH019_ROOT="$ROOT" LH019_RESULTS="$RESULTS"
# The harness imports sibling modules; keep bytecode caches out of the tree.
export PYTHONDONTWRITEBYTECODE=1
export LH019_CH_PATH="$RUN/clickhouse/data/"
export LH019_CH_TMP="$RUN/clickhouse/tmp/"
export LH019_CH_USER_FILES="$RUN/clickhouse/user_files/"
export LH019_CH_FORMAT_SCHEMAS="$RUN/clickhouse/format_schemas/"
export LH019_CH_LOG="$LOGS/clickhouse-server.log"
export LH019_CH_ERRORLOG="$LOGS/clickhouse-server.err.log"
export LH019_CH_USERS="$HERE/clickhouse-users.xml"

CLICKHOUSE_BIN=""
OWNS_CLICKHOUSE=0
CH_STARTED=0
# Failure accounting: each phase records its process exit status, and
# summarize() fails the run when an expected phase is missing, empty, stale, or
# nonzero. The run's exit status is therefore part of the evidence.
HARNESS_FAILED=0
PHASE_STATUS="$RESULTS/phase-status.tsv"
RUN_MARKER="$RESULTS/run-start.txt"
ALL_PHASES="stock-map stock-json managed transform identity semantics cost files"

log() { printf '[019] %s\n' "$*"; }

clickhouse_bin() {
  if [[ -z "$CLICKHOUSE_BIN" ]]; then
    CLICKHOUSE_BIN="$(cd "$HERE" && nix develop --command bash -c 'command -v clickhouse-server')"
  fi
  printf '%s' "$CLICKHOUSE_BIN"
}

clickhouse_client() {
  "$(dirname "$(clickhouse_bin)")/clickhouse-client" --host 127.0.0.1 --port 19001 "$@"
}

start_bg() { # start_bg NAME CMD...
  local name="$1"; shift
  "$@" >"$LOGS/$name.log" 2>&1 &
  echo $! >"$RUN/$name.pid"
}

stop_bg() { # stop_bg NAME [signal]
  local name="$1" signal="${2:-TERM}"
  [[ -f "$RUN/$name.pid" ]] || return 0
  local pid
  pid="$(cat "$RUN/$name.pid")"
  kill "-$signal" "$pid" 2>/dev/null || true
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    kill -0 "$pid" 2>/dev/null || break
    local state
    state="$(ps -o stat= -p "$pid" 2>/dev/null || true)"
    [[ "$state" == Z* || -z "$state" ]] && break
    sleep 0.25
  done
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  rm -f "$RUN/$name.pid"
}

is_running() {
  [[ -f "$RUN/$1.pid" ]] && kill -0 "$(cat "$RUN/$1.pid")" 2>/dev/null
}

port_open() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&- 2>/dev/null || true; return 0; }
  return 1
}

wait_port() { # wait_port PORT [seconds]
  local port="$1" seconds="${2:-60}" start=$SECONDS
  while (( SECONDS - start < seconds )); do
    if port_open "$port"; then return 0; fi
    sleep 0.5
  done
  return 1
}

start_clickhouse() {
  # Idempotent within a run: a later phase must not re-detect the server this
  # run already started as an external one.
  if [[ "$CH_STARTED" == 1 ]]; then return 0; fi
  if port_open 18124; then
    log "using the ClickHouse already listening on 18124 (external server)"
    OWNS_CLICKHOUSE=0
    CH_STARTED=1
    return 0
  fi
  log "starting ClickHouse $("$(clickhouse_bin)" --version 2>/dev/null | head -1)"
  mkdir -p "$LH019_CH_PATH" "$LH019_CH_TMP" "$LH019_CH_USER_FILES" "$LH019_CH_FORMAT_SCHEMAS"
  # setsid gives ClickHouse its own session/process group: its server watchdog
  # manages that group, and the harness can stop the whole group on cleanup
  # without signalling its own process group.
  setsid "$(clickhouse_bin)" --config-file="$HERE/clickhouse-config.xml" >"$LOGS/clickhouse.log" 2>&1 &
  echo $! >"$RUN/clickhouse.pid"
  OWNS_CLICKHOUSE=1
  CH_STARTED=1
  wait_port 18124 90 || { log "ClickHouse did not start"; tail -20 "$LOGS/clickhouse.log" || true; exit 1; }
}

stop_clickhouse() {
  if [[ "$OWNS_CLICKHOUSE" == 1 ]] && { is_running clickhouse || port_open 18124; }; then
    log "stopping ClickHouse"
    local pid
    pid="$(cat "$RUN/clickhouse.pid" 2>/dev/null || true)"
    if [[ -n "$pid" ]]; then
      kill -TERM "-$pid" 2>/dev/null || true
      kill -TERM "$pid" 2>/dev/null || true
    fi
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )) && port_open 18124; do sleep 0.5; done
    if [[ -n "$pid" ]]; then kill -KILL "-$pid" 2>/dev/null || true; fi
    rm -f "$RUN/clickhouse.pid"
  fi
}

ch() { clickhouse_client --multiquery --query "$1"; }
ch_file() { clickhouse_client --multiquery < "$1"; }

reset_state() {
  # `all` starts from an empty state. An externally managed server is reset by
  # SQL only; a harness-owned one is started fresh after removing its data.
  if port_open 18124; then
    OWNS_CLICKHOUSE=0
  else
    rm -rf "$RUN/clickhouse"
    mkdir -p "$LH019_CH_PATH" "$LH019_CH_TMP" "$LH019_CH_USER_FILES" "$LH019_CH_FORMAT_SCHEMAS"
    start_clickhouse
  fi
  ch "DROP DATABASE IF EXISTS lh019;
      DROP DATABASE IF EXISTS lh019_cost;
      DROP DATABASE IF EXISTS stock_map;
      DROP DATABASE IF EXISTS stock_json;
      CREATE DATABASE IF NOT EXISTS lh019;"
  rm -f "$RESULTS"/*
  : >"$PHASE_STATUS"
  date -u +"%Y-%m-%dT%H:%M:%SZ" >"$RUN_MARKER"
}

start_collector() { # start_collector NAME CONFIG
  local name="$1" config="$2"
  start_bg "$name" "$OTELCOL" --config "$HERE/$config"
  wait_port 4328 60 || { log "$name did not open 4328"; tail -30 "$LOGS/$name.log"; exit 1; }
  sleep 0.5
}

stop_collector() { stop_bg "$1" "${2:-TERM}"; }

emit() { # emit SIGNAL LABEL OUTFILE HEADER...
  local signal="$1" label="$2" out="$3"; shift 3
  local args=()
  for header in "$@"; do args+=(--header "$header"); done
  "$PY" "$HERE/fixture.py" emit "$signal" "$label" --url "http://127.0.0.1:4328" "${args[@]}" \
    --label-out "$RESULTS/$out"
}

wait_rows() { # wait_rows DB SQL MIN_ROWS TIMEOUT_S
  local db="$1" sql="$2" want="$3" timeout="${4:-30}" start
  start=$(date +%s%3N)
  while :; do
    local got
    got="$(clickhouse_client --database "$db" --query "$sql" 2>/dev/null || echo 0)"
    [[ "$got" =~ ^[0-9]+$ ]] || got=0
    if (( got >= want )); then return 0; fi
    if (( $(date +%s%3N) - start > timeout * 1000 )); then return 1; fi
    sleep 0.1
  done
}

scrape_metrics() {
  curl -sS "http://127.0.0.1:8889/metrics" >"$1" 2>/dev/null || echo "metrics endpoint unavailable" >"$1"
}

record_phase() { # record_phase NAME EXIT_STATUS
  touch "$PHASE_STATUS"
  awk -v n="$1" '$1 != n' "$PHASE_STATUS" >"$PHASE_STATUS.tmp" 2>/dev/null || true
  mv "$PHASE_STATUS.tmp" "$PHASE_STATUS"
  printf '%s\t%s\n' "$1" "$2" >>"$PHASE_STATUS"
}

verify() { # verify PHASE OUT [EXPECTED_EXIT]
  # The phase's process exit status is part of the evidence. A crashed or
  # missing phase must fail the run even if a checks file happens to exist, and
  # the target file is removed first so a crash cannot leave a stale one.
  local phase="$1" out="$2" expected="${3:-0}" rc
  rm -f "$RESULTS/checks-$out.jsonl"
  set +e
  "$PY" "$HERE/verify.py" --phase "$phase" --results "$RESULTS" --out "checks-$out.jsonl"
  rc=$?
  set -e
  record_phase "$out" "$rc"
  if (( rc != expected )); then
    log "verify $out exited $rc (expected $expected)"
    HARNESS_FAILED=1
  fi
}

summarize() { # summarize "phase ..." -> nonzero when coverage or checks fail
  local expected="$1" phase file status n f total=0 failed=0 problems=""
  local py="$PY"
  [[ -x "$py" ]] || py="$(command -v python3)"
  for phase in $expected; do
    status="$(awk -v n="$phase" '$1 == n { print $2 }' "$PHASE_STATUS" 2>/dev/null | tail -1)"
    file="$RESULTS/checks-$phase.jsonl"
    if [[ -z "$status" ]]; then
      problems="$problems $phase(no-status)"
    elif (( status != 0 )); then
      problems="$problems $phase(exit=$status)"
    elif [[ ! -s "$file" ]]; then
      problems="$problems $phase(no-output)"
    elif [[ -f "$RUN_MARKER" && "$file" -ot "$RUN_MARKER" ]]; then
      problems="$problems $phase(stale)"
    else
      n="$(wc -l <"$file")"
      f="$("$py" - "$file" <<'PYEOF'
import json
import sys
print(sum(1 for line in open(sys.argv[1]) if line.strip() and not json.loads(line)["ok"]))
PYEOF
)"
      total=$((total + n))
      failed=$((failed + f))
    fi
  done
  log "phase coverage: ${expected:-none}"
  log "checks: $failed failed of $total recorded"
  if [[ -n "$problems" ]]; then
    log "coverage problems:$problems"
  fi
  if (( HARNESS_FAILED != 0 )); then
    log "harness failure flag set"
  fi
  if [[ -n "$problems" || "$failed" != 0 || "$HARNESS_FAILED" != 0 ]]; then
    log "HARNESS FAILED"
    return 1
  fi
  log "HARNESS PASSED"
  return 0
}

phase_stock_map() {
  log "phase stock-map: stock Map exporter"
  start_clickhouse
  start_collector collector-map collector-map.yaml
  emit logs typed emit-stock-map-logs.jsonl >"$RESULTS/emit-stock-map-logs.out"
  emit traces typed emit-stock-map-traces.jsonl >"$RESULTS/emit-stock-map-traces.out"
  wait_rows stock_map "SELECT count() FROM otel_logs" 28 30 || log "warning: logs did not reach 28 rows"
  wait_rows stock_map "SELECT count() FROM otel_traces" 3 20 || log "warning: traces did not reach 3 rows"
  sleep 2
  scrape_metrics "$RESULTS/metrics-stock-map.txt"
  stop_collector collector-map
  clickhouse_client --database stock_map --query "SHOW CREATE TABLE otel_logs FORMAT TSVRaw" >"$RESULTS/stock-map-ddl-logs.sql"
  clickhouse_client --database stock_map --query "SHOW CREATE TABLE otel_traces FORMAT TSVRaw" >"$RESULTS/stock-map-ddl-traces.sql"
  clickhouse_client --query "SELECT table, name, type FROM system.columns WHERE database='stock_map' ORDER BY table, position FORMAT TSV" \
    >"$RESULTS/stock-map-columns.tsv"
  verify stock-map stock-map
}

phase_stock_json() {
  log "phase stock-json: stock json:true exporter"
  start_clickhouse
  start_collector collector-json collector-json.yaml
  # The full typed fixture contains NaN/Inf; the JSON exporter cannot marshal
  # them and drops the whole request. This emit is the negative evidence.
  emit logs typed emit-stock-json-logs-nan.jsonl >"$RESULTS/emit-stock-json-logs-nan.out"
  sleep 8
  scrape_metrics "$RESULTS/metrics-stock-json-nan.txt"
  # Reduced fixture without non-finite doubles, so the remaining typed cases
  # can be characterized.
  emit logs typed-safe emit-stock-json-logs-safe.jsonl >"$RESULTS/emit-stock-json-logs-safe.out"
  wait_rows stock_json "SELECT count() FROM otel_logs" 25 30 || log "warning: safe logs did not reach 25 rows"
  emit traces typed-safe emit-stock-json-traces-safe.jsonl >"$RESULTS/emit-stock-json-traces-safe.out"
  wait_rows stock_json "SELECT count() FROM otel_traces" 3 20 || log "warning: safe traces did not reach 3 rows"
  sleep 2
  scrape_metrics "$RESULTS/metrics-stock-json.txt"
  stop_collector collector-json
  clickhouse_client --database stock_json --query "SHOW CREATE TABLE otel_logs FORMAT TSVRaw" >"$RESULTS/stock-json-ddl-logs.sql"
  clickhouse_client --database stock_json --query "SHOW CREATE TABLE otel_traces FORMAT TSVRaw" >"$RESULTS/stock-json-ddl-traces.sql"
  clickhouse_client --query "SELECT table, name, type FROM system.columns WHERE database='stock_json' ORDER BY table, position FORMAT TSV" \
    >"$RESULTS/stock-json-columns.tsv"
  grep -E 'failed to marshal|Exporting failed|Dropping data' "$LOGS/collector-json.log" >"$RESULTS/stock-json.log" || true
  verify stock-json stock-json
}

phase_managed() {
  log "phase managed: create_schema:false probes"
  start_clickhouse
  ch_file "$HERE/managed-schema.sql"
  start_collector collector-managed collector-managed.yaml
  emit logs typed emit-managed-logs.jsonl >"$RESULTS/emit-managed-logs.out"
  emit traces typed emit-managed-traces.jsonl >"$RESULTS/emit-managed-traces.out"
  wait_rows lh019 "SELECT count() FROM managed_logs" 28 30 || log "warning: managed logs did not reach 28 rows"
  wait_rows lh019 "SELECT count() FROM managed_traces" 3 20 || log "warning: managed traces did not reach 3 rows"
  sleep 2
  stop_collector collector-managed
  # The required/plain payload column probe: the stock INSERT never names it.
  start_collector collector-managed-required collector-managed-required.yaml
  emit logs typed-safe emit-managed-required.jsonl >"$RESULTS/emit-managed-required.out"
  wait_rows lh019 "SELECT count() FROM managed_logs_payload" 25 30 || log "warning: payload-column rows missing"
  sleep 2
  stop_collector collector-managed-required
  verify managed managed
}

phase_transform() {
  log "phase transform: stock-processor gap probes"
  # Expected-negative component failure: the per-key type expression must be
  # rejected at config load. Assert the nonzero exit instead of ignoring it; an
  # unexpected success is a harness failure, and verify.py separately asserts
  # the exact error text.
  local rc=0
  set +e
  timeout 20 "$OTELCOL" --config "$HERE/collector-transform-types.yaml" >"$RESULTS/transform-types.log" 2>&1
  rc=$?
  set -e
  record_phase "transform-config-load" "$rc"
  if (( rc == 0 )); then
    log "expected the transform/types config to be rejected, but it loaded"
    HARNESS_FAILED=1
  fi
  start_collector collector-transform-stringify collector-transform-stringify.yaml
  emit logs typed-safe emit-transform-stringify.jsonl >"$RESULTS/emit-transform-stringify.out"
  sleep 2
  stop_collector collector-transform-stringify
  verify transform transform
}

phase_identity() {
  log "phase identity: trusted workspace ordering"
  start_clickhouse
  start_collector collector-identity collector-identity.yaml
  "$PY" "$HERE/fixture.py" emit logs identity-a --url http://127.0.0.1:4328 \
    --header "Authorization: Bearer lh019-ws-a-key" --label-out "$RESULTS/emit-identity-a.jsonl" >/dev/null
  wait_rows lh019 "SELECT count() FROM managed_logs WHERE Body='identity-ws-a'" 1 30 || log "warning: identity row missing"
  sleep 2
  stop_collector collector-identity
  verify identity identity
}

phase_semantics() {
  log "phase semantics: candidate representations"
  start_clickhouse
  "$PY" "$HERE/represent.py" --phase semantics --db lh019 --results "$RESULTS"
  verify semantics semantics
}

phase_cost() {
  log "phase cost: storage and execution cost corpus"
  start_clickhouse
  ch "CREATE DATABASE IF NOT EXISTS lh019_cost" >/dev/null
  sed 's/{{DB}}/lh019_cost/g' "$HERE/representations.sql" >"$RUN/representations-cost.sql"
  ch_file "$RUN/representations-cost.sql"
  "$PY" "$HERE/represent.py" --phase cost --db lh019_cost --results "$RESULTS" --cost-rows 50000
  verify cost cost
}

cleanup() {
  stop_bg collector-map
  stop_bg collector-json
  stop_bg collector-managed
  stop_bg collector-managed-required
  stop_bg collector-transform-stringify
  stop_bg collector-identity
  stop_clickhouse
}
trap cleanup EXIT

apply_representations() {
  sed 's/{{DB}}/lh019/g' "$HERE/representations.sql" >"$RUN/representations.sql"
  ch_file "$RUN/representations.sql"
}

case "${1:-all}" in
  stock-map) phase_stock_map; summarize "stock-map" ;;
  stock-json) phase_stock_json; summarize "stock-json" ;;
  managed) phase_managed; summarize "managed" ;;
  transform) phase_transform; summarize "transform" ;;
  identity) phase_identity; summarize "identity" ;;
  semantics)
    start_clickhouse
    apply_representations
    phase_semantics
    summarize "semantics"
    ;;
  cost) phase_cost; summarize "cost" ;;
  files) verify files files; summarize "files" ;;
  check-results) shift || true; summarize "$*" ;;
  all)
    reset_state
    apply_representations
    phase_stock_map
    phase_stock_json
    phase_managed
    phase_transform
    phase_identity
    phase_semantics
    phase_cost
    verify files files
    summarize "$ALL_PHASES"
    ;;
  *) echo "usage: $0 [stock-map|stock-json|managed|transform|identity|semantics|cost|files|check-results|all]" >&2; exit 2 ;;
esac

log "done"
