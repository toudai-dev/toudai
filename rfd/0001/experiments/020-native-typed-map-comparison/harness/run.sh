#!/usr/bin/env bash
# Experiment 020 orchestration: disposable local ClickHouse 26.8.2.7-lts plus the
# deterministic typed fixture and the two cost corpora. No Collector and no
# repository or benchmark data is touched; every service is started and stopped
# by this script.
#
# Usage: run.sh [capabilities|semantics|negative|cost|files|all|check-results]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH020_ROOT:-/home/user/lh020}"
RESULTS="${LH020_RESULTS:-$HERE/results}"
PY="$ROOT/venv/bin/python"
RUN="$ROOT/run"
LOGS="$ROOT/logs"
mkdir -p "$RESULTS" "$RUN" "$LOGS"

export LH020_ROOT="$ROOT" LH020_RESULTS="$RESULTS"
# The harness imports sibling modules; keep bytecode caches out of the tree.
export PYTHONDONTWRITEBYTECODE=1
export LH020_CH_PATH="$RUN/clickhouse/data/"
export LH020_CH_TMP="$RUN/clickhouse/tmp/"
export LH020_CH_USER_FILES="$RUN/clickhouse/user_files/"
export LH020_CH_FORMAT_SCHEMAS="$RUN/clickhouse/format_schemas/"
export LH020_CH_LOG="$LOGS/clickhouse-server.log"
export LH020_CH_ERRORLOG="$LOGS/clickhouse-server.err.log"
export LH020_CH_USERS="$HERE/clickhouse-users.xml"

CLICKHOUSE_BIN=""
OWNS_CLICKHOUSE=0
CH_STARTED=0
# Failure accounting: each phase records its process exit status, and
# summarize() fails the run when an expected phase is missing, empty, stale, or
# nonzero. The run's exit status is therefore part of the evidence.
HARNESS_FAILED=0
PHASE_STATUS="$RESULTS/phase-status.tsv"
RUN_MARKER="$RESULTS/run-start.txt"
ALL_PHASES="capabilities semantics negative cost files"
COST_DBS="lh020_cost_base lh020_cost_diverse"

log() { printf '[020] %s\n' "$*"; }

clickhouse_bin() {
  if [[ -z "$CLICKHOUSE_BIN" ]]; then
    CLICKHOUSE_BIN="$(cd "$HERE" && nix develop --command bash -c 'command -v clickhouse-server')"
  fi
  printf '%s' "$CLICKHOUSE_BIN"
}

clickhouse_client() {
  "$(dirname "$(clickhouse_bin)")/clickhouse-client" --host 127.0.0.1 --port 19002 "$@"
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
  if port_open 18125; then
    log "using the ClickHouse already listening on 18125 (external server)"
    OWNS_CLICKHOUSE=0
    CH_STARTED=1
    return 0
  fi
  log "starting ClickHouse $("$(clickhouse_bin)" --version 2>/dev/null | head -1)"
  mkdir -p "$LH020_CH_PATH" "$LH020_CH_TMP" "$LH020_CH_USER_FILES" "$LH020_CH_FORMAT_SCHEMAS"
  # setsid gives ClickHouse its own session/process group: its server watchdog
  # manages that group, and the harness can stop the whole group on cleanup
  # without signalling its own process group.
  setsid "$(clickhouse_bin)" --config-file="$HERE/clickhouse-config.xml" >"$LOGS/clickhouse.log" 2>&1 &
  echo $! >"$RUN/clickhouse.pid"
  OWNS_CLICKHOUSE=1
  CH_STARTED=1
  wait_port 18125 90 || { log "ClickHouse did not start"; tail -20 "$LOGS/clickhouse.log" || true; exit 1; }
}

stop_clickhouse() {
  if [[ "$OWNS_CLICKHOUSE" == 1 ]] && { is_running clickhouse || port_open 18125; }; then
    log "stopping ClickHouse"
    local pid
    pid="$(cat "$RUN/clickhouse.pid" 2>/dev/null || true)"
    if [[ -n "$pid" ]]; then
      kill -TERM "-$pid" 2>/dev/null || true
      kill -TERM "$pid" 2>/dev/null || true
    fi
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )) && port_open 18125; do sleep 0.5; done
    if [[ -n "$pid" ]]; then kill -KILL "-$pid" 2>/dev/null || true; fi
    rm -f "$RUN/clickhouse.pid"
  fi
}

ch() { clickhouse_client --multiquery --query "$1"; }
ch_file() { clickhouse_client --multiquery < "$1"; }

apply_schema() { # apply_schema DB
  local db="$1"
  ch "CREATE DATABASE IF NOT EXISTS $db"
  sed "s/{{DB}}/$db/g" "$HERE/native-maps.sql" >"$RUN/native-maps-$db.sql"
  ch_file "$RUN/native-maps-$db.sql"
}

reset_state() {
  # `all` starts from an empty state. An externally managed server is reset by
  # SQL only; a harness-owned one is started fresh after removing its data.
  if port_open 18125; then
    OWNS_CLICKHOUSE=0
  else
    rm -rf "$RUN/clickhouse"
    mkdir -p "$LH020_CH_PATH" "$LH020_CH_TMP" "$LH020_CH_USER_FILES" "$LH020_CH_FORMAT_SCHEMAS"
    start_clickhouse
  fi
  ch "DROP DATABASE IF EXISTS lh020;
      DROP DATABASE IF EXISTS lh020_caps;
      DROP DATABASE IF EXISTS lh020_cost_base;
      DROP DATABASE IF EXISTS lh020_cost_diverse;
      CREATE DATABASE IF NOT EXISTS lh020;"
  rm -f "$RESULTS"/*
  : >"$PHASE_STATUS"
  date -u +"%Y-%m-%dT%H:%M:%SZ" >"$RUN_MARKER"
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

phase_capabilities() {
  log "phase capabilities: pinned-version type and behavior probes"
  start_clickhouse
  verify capabilities capabilities
}

phase_semantics() {
  log "phase semantics: candidate representations"
  start_clickhouse
  apply_schema lh020
  "$PY" "$HERE/mapping020.py" --phase semantics --db lh020 --results "$RESULTS"
  "$PY" "$HERE/mapping020.py" --phase wrong --db lh020
  verify semantics semantics
}

phase_negative() {
  log "phase negative: deliberately wrong layouts"
  start_clickhouse
  verify negative negative
}

phase_cost() {
  log "phase cost: storage and execution cost corpora"
  start_clickhouse
  local corpus db
  for corpus in base diverse; do
    db="lh020_cost_$corpus"
    ch "CREATE DATABASE IF NOT EXISTS $db"
    apply_schema "$db"
    "$PY" "$HERE/mapping020.py" --phase cost --db "$db" --corpus "$corpus" \
      --results "$RESULTS" --count 50000
  done
  verify cost cost
}

cleanup() {
  stop_clickhouse
}

apply_representations() {
  apply_schema lh020
}

trap cleanup EXIT

case "${1:-all}" in
  capabilities) phase_capabilities; summarize "capabilities" ;;
  semantics)
    start_clickhouse
    apply_representations
    phase_semantics
    summarize "semantics"
    ;;
  negative) phase_negative; summarize "negative" ;;
  cost) phase_cost; summarize "cost" ;;
  files) verify files files; summarize "files" ;;
  check-results) shift || true; summarize "$*" ;;
  all)
    reset_state
    apply_representations
    phase_capabilities
    phase_semantics
    phase_negative
    phase_cost
    verify files files
    summarize "$ALL_PHASES"
    ;;
  *) echo "usage: $0 [capabilities|semantics|negative|cost|files|check-results|all]" >&2; exit 2 ;;
esac

log "done"
