#!/usr/bin/env bash
# Experiment 021 orchestration: a real OTLP -> Collector -> provisional hybrid
# serving layout + separate canonical payload integration on disposable local
# ClickHouse 26.8.2.7-lts.
#
# Every phase records its own exit status and its verifier's exit status, so a
# missing, crashed, stale, or failing phase fails the run. Nothing here is a
# production service, and no shared database, cloud resource, or repository
# benchmark data is touched.
#
# Usage: run.sh [PHASE|all]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH021_ROOT:-/home/user/lh021}"
RESULTS="${LH021_RESULTS:-$HERE/results}"
COLLECTOR="$ROOT/bin/lighthouse-collector-021"
PY="$ROOT/venv/bin/python"
RUN="$ROOT/run"
LOGS="$ROOT/logs"
CH_HTTP="${LH021_CH_HTTP:-http://127.0.0.1:18224}"
mkdir -p "$RESULTS" "$RUN" "$LOGS"

DB="${LH021_DATABASE:-lh021}"
DB_UNTRUSTED="${LH021_UNTRUSTED_DATABASE:-lh021_untrusted}"
DB_STOCK="${LH021_STOCK_DATABASE:-lh021_stock}"

export LH021_ROOT="$ROOT" LH021_RESULTS="$RESULTS"
export LH021_CH_HTTP="$CH_HTTP" LH021_DATABASE="$DB"
export LH021_UNTRUSTED_DATABASE="$DB_UNTRUSTED" LH021_STOCK_DATABASE="$DB_STOCK"
# The harness imports sibling modules; keep bytecode caches out of the tree.
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$HERE"

GW_PORT=4338           # gateway OTLP/HTTP (bearertokenauth + routing)
UNTRUSTED_PORT=4339    # pipeline with no trusted-scope processor
STOCK_PORT=4337        # stock write-path probe
STALL_PORT="${LH021_STALL_PORT:-19102}"   # stalled ClickHouse endpoint / crash drain relay
LOST_PORT="${LH021_LOST_PORT:-19103}"     # lost-response proxy for the ambiguous insert
CH_PORT=19101
# Trusted workspace used by the single-credential phase fixtures.
WS_A="11111111-1111-4111-8111-111111111111"

HARNESS_FAILED=0
PHASE_STATUS="$RESULTS/phase-status.tsv"
PHASE_TIMES="$RESULTS/phase-times.tsv"
RUN_MARKER="$RESULTS/run-start.txt"
ALL_PHASES="stock-probe semantics ack mixed-validation outage split split-retry canonical-permanent queue-full crash ambiguous ambiguous-serving retry-duplicate no-trust files"

log() { printf '[021] %s\n' "$*"; }

# ---------------------------------------------------------------------------
# process and port helpers
# ---------------------------------------------------------------------------

start_bg() { # start_bg NAME CMD...
  local name="$1"; shift
  # Never overwrite a live predecessor's pid file: doing so loses the only handle
  # the harness has on that process (observed with a leftover lost-response
  # proxy that kept 19103 bound and could no longer be reaped by name).
  stop_bg "$name" >/dev/null 2>&1 || true
  "$@" >"$LOGS/$name.log" 2>&1 &
  echo $! >"$RUN/$name.pid"
}

stop_bg() { # stop_bg NAME [signal]
  local name="$1" signal="${2:-TERM}"
  [[ -f "$RUN/$name.pid" ]] || return 0
  local pid
  pid="$(cat "$RUN/$name.pid")"
  kill "-$signal" "$pid" 2>/dev/null || true
  local deadline=$((SECONDS + 20))
  while (( SECONDS < deadline )); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.25
  done
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  rm -f "$RUN/$name.pid"
  log "stopped $name"
}

hard_kill_bg() { # hard_kill_bg NAME -> SIGKILL without graceful shutdown
  local name="$1" pid
  pid="$(cat "$RUN/$name.pid" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 0
  kill -9 "$pid" 2>/dev/null || true
  local deadline=$((SECONDS + 10))
  while (( SECONDS < deadline )); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.2
  done
  rm -f "$RUN/$name.pid"
  log "SIGKILLed $name (pid $pid)"
}

port_open() {
  # A subshell owns the probe socket so the caller's file descriptors (and
  # stderr) are never touched by a successful probe.
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

wait_port() { # wait_port PORT [seconds]
  local port="$1" seconds="${2:-60}" start=$SECONDS
  while (( SECONDS - start < seconds )); do
    if port_open "$port"; then return 0; fi
    sleep 0.5
  done
  return 1
}

wait_port_closed() { # wait_port_closed PORT [seconds]
  local port="$1" seconds="${2:-60}" start=$SECONDS
  while (( SECONDS - start < seconds )); do
    if ! port_open "$port"; then return 0; fi
    sleep 0.5
  done
  return 1
}

# ---------------------------------------------------------------------------
# ClickHouse access (HTTP interface of the disposable server)
# ---------------------------------------------------------------------------

ch_sql() { # ch_sql DB SQL (one or more ';' separated statements)
  local db="$1" sql="$2"
  "$PY" "$HERE/sql.py" --db "$db" --sql "$sql"
}

ch_file() { # ch_file DB PATH ({{DB}} substituted by sql.py)
  local db="$1" file="$2"
  "$PY" "$HERE/sql.py" --db "$db" --file "$file"
}

ch_scalar() { # ch_scalar DB SQL
  local db="$1" sql="$2"
  curl -sS --fail-with-body --max-time 60 "http://127.0.0.1:18224/?database=$db&default_format=TSV" \
    --data-binary "$sql" | head -1 | tr -d '\n'
}

start_clickhouse() {
  if port_open 18224; then return 0; fi
  log "starting the supervised ClickHouse service"
  amp orb service start clickhouse-021 --command "$HERE/start-clickhouse.sh" >/dev/null
  wait_port 18224 90 || { log "ClickHouse did not start"; exit 1; }
}

stop_clickhouse() {
  log "stopping the supervised ClickHouse service (destination outage)"
  amp orb service stop clickhouse-021 >/dev/null
  wait_port_closed 18224 30 || log "warning: 18224 still open"
}

restart_clickhouse() {
  amp orb service start clickhouse-021 --command "$HERE/start-clickhouse.sh" >/dev/null
  wait_port 18224 90 || { log "ClickHouse did not come back"; exit 1; }
}

wait_rows() { # wait_rows DB SQL MIN TIMEOUT_S
  local db="$1" sql="$2" want="$3" timeout="${4:-30}" start got
  start=$(date +%s%3N)
  while :; do
    got="$(ch_scalar "$db" "$sql" 2>/dev/null || echo 0)"
    [[ "$got" =~ ^[0-9]+$ ]] || got=0
    if (( got >= want )); then return 0; fi
    if (( $(date +%s%3N) - start > timeout * 1000 )); then return 1; fi
    sleep 0.2
  done
}

# ---------------------------------------------------------------------------
# collector helpers
# ---------------------------------------------------------------------------

start_collector() { # start_collector NAME CONFIG WAIT_PORT [ENV_ASSIGNMENTS...]
  local name="$1" config="$2" wait="${3:-$GW_PORT}"; shift 3
  local env_args=("$@")
  # If the port is still held, the previous collector has not finished draining
  # (or a foreign process owns it). Waiting briefly covers the normal sequential
  # case; a port that stays held fails the phase instead of letting it verify a
  # stale process.
  if port_open "$wait"; then
    wait_port_closed "$wait" 20 ||
      { log "FAIL: $name cannot start: $wait is already in use"; HARNESS_FAILED=1; exit 1; }
  fi
  local daemon_env=(LH021_RESULTS="$RESULTS")
  daemon_env+=("${env_args[@]}")
  start_bg "$name" env "${daemon_env[@]}" "$COLLECTOR" --config "$HERE/$config"
  wait_port "$wait" 60 || { log "$name did not open $wait"; tail -40 "$LOGS/$name.log" || true; exit 1; }
  sleep 0.3
}

start_injector() { # start_injector NAME PORT CMD...
  # Harness-only injectors (stalled listener, lost-response proxy) must be the
  # process that answers the phase's traffic. A stale injector from an earlier
  # run is worse than a missing one: it would serve the phase with a spent
  # injection budget, so a held port is reaped only when the holder is provably
  # this harness's own injector, and otherwise fails the phase.
  local name="$1" port="$2"; shift 2
  if port_open "$port"; then
    if ! stop_own_injector "$port"; then
      log "FAIL: $name cannot start: $port is already in use by another process"
      HARNESS_FAILED=1
      exit 1
    fi
  fi
  start_bg "$name" "$@"
  sleep 0.5
  local pid
  pid="$(cat "$RUN/$name.pid" 2>/dev/null || true)"
  if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
    log "FAIL: $name exited during startup; see $LOGS/$name.log"
    HARNESS_FAILED=1
    exit 1
  fi
  wait_port "$port" 15 || { log "FAIL: $name did not open $port"; HARNESS_FAILED=1; exit 1; }
}

stop_own_injector() { # stop_own_injector PORT -> 0 when a harness injector was reaped
  # Identified by the process command line, so a listener that merely happens to
  # hold the port is never killed by the harness.
  local port="$1" pid cmd
  command -v ss >/dev/null 2>&1 || return 1
  pid="$(ss -ltnp "sport = :$port" 2>/dev/null | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2 || true)"
  [[ -n "$pid" ]] || return 1
  cmd="$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)"
  case "$cmd" in
    *"$HERE/lost-response-proxy.py"*|*"$HERE/stalled-listener.py"*) ;;
    *) return 1 ;;
  esac
  log "reaping stale harness injector on port $port (pid $pid)"
  kill "$pid" 2>/dev/null || true
  wait_port_closed "$port" 10 || true
  return 0
}

# Graceful stop: the collector drains exporter queues during shutdown, so
# phases that assert on flushed data stop it with TERM (hard_kill_bg is only
# for the crash phase, which must not flush).
stop_collector() { # stop_collector NAME
  stop_bg "$1" TERM
}

copy_log() { # copy_log NAME DEST
  cp "$LOGS/$1.log" "$RESULTS/$2" 2>/dev/null || true
}

scrape_metrics() { # scrape_metrics OUT [PORT]
  local out="$1" port="${2:-8891}"
  if ! curl -sS --max-time 10 "http://127.0.0.1:$port/metrics" >"$RESULTS/$out" 2>/dev/null; then
    echo "metrics endpoint unavailable" >"$RESULTS/$out"
  fi
}

# ---------------------------------------------------------------------------
# fixture emission
# ---------------------------------------------------------------------------

emit_jq() { # emit_jq PHASE URL SIGNAL LABEL JQ_FILTER [HEADER...]
  local phase="$1" url="$2" signal="$3" label="$4" filter="$5"; shift 5
  local args=() header out record
  for header in "$@"; do args+=(--header "$header"); done
  out="$("$PY" "$HERE/fixture021.py" emit "$signal" "$label" --url "$url" "${args[@]}" 2>&1)" || true
  record="$(printf '%s\n' "$out" | grep -E '^\{' | tail -1)"
  if [[ -z "$record" ]]; then
    # The request never reached a receiver: record the attempt so the verifier
    # reports it instead of silently missing an emit record.
    record="$(jq -cn --arg signal "$signal" --arg label "$label" --arg url "$url" \
      --arg body "$(printf '%s' "$out" | tail -3 | tr '\n' ' ' | cut -c1-300)" \
      '{signal: $signal, label: $label, url: $url, status: 0, response_body: $body, elapsed_ms: null}')"
  fi
  printf '%s\n' "$record" | jq -c "$filter" >>"$RESULTS/$phase-emits.jsonl"
}

emit() { # emit PHASE URL SIGNAL LABEL [HEADER...]
  local phase="$1" url="$2" signal="$3" label="$4"; shift 4
  emit_jq "$phase" "$url" "$signal" "$label" '.' "$@"
}

emit_with_visibility() { # emit_with_visibility PHASE URL SIGNAL LABEL TIMEOUT_S [HEADER...]
  local phase="$1" url="$2" signal="$3" label="$4" timeout="${5:-10}"; shift 5
  local body row visible_at_ack visible_within_s record args=() header
  for header in "$@"; do args+=(--header "$header"); done
  body="$(fixture_body "$label")"
  record="$("$PY" "$HERE/fixture021.py" emit "$signal" "$label" --url "$url" "${args[@]}" 2>&1)" || true
  record="$(printf '%s\n' "$record" | grep -E '^\{' | tail -1)"
  if [[ -z "$record" ]]; then
    record="$(jq -cn --arg signal "$signal" --arg label "$label" --arg url "$url" \
      '{signal: $signal, label: $label, url: $url, status: 0, response_body: "emit did not return a record", elapsed_ms: null}')"
  fi
  # Visibility is probed by the fixture's own body text: the stored write
  # identity is content-derived and not known to the harness before the write.
  if [[ "$(ch_scalar "$DB" "SELECT count() FROM serving_logs WHERE body = '$body' AND workspace_id = '$WS_A'" 2>/dev/null || echo 0)" == "1" ]]; then
    visible_at_ack=true
  else
    visible_at_ack=false
  fi
  visible_within_s=null
  local start_ms now_ms
  start_ms=$(date +%s%3N)
  while :; do
    row="$(ch_scalar "$DB" "SELECT count() FROM serving_logs WHERE body = '$body' AND workspace_id = '$WS_A'" 2>/dev/null || echo 0)"
    if [[ "$row" == "1" ]]; then
      now_ms=$(date +%s%3N)
      visible_within_s="$(awk -v a="$start_ms" -v b="$now_ms" 'BEGIN {printf "%.2f", (b - a) / 1000}')"
      break
    fi
    if (( $(date +%s%3N) - start_ms > timeout * 1000 )); then break; fi
    sleep 0.05
  done
  printf '%s\n' "$record" |
    jq -c --argjson va "$visible_at_ack" --argjson vw "$visible_within_s" \
      '. + {visible_at_ack: $va, visible_within_s: $vw}' >>"$RESULTS/$phase-emits.jsonl"
}

fixture_body() { # fixture_body LABEL
  "$PY" - "$HERE" "$1" <<'PYEOF'
import sys
sys.path.insert(0, sys.argv[1])
import fixture021
print(fixture021.LOG_LABELS[sys.argv[2]][0]["body"][1])
PYEOF
}

# ---------------------------------------------------------------------------
# phase bookkeeping
# ---------------------------------------------------------------------------

record_phase() { # record_phase NAME EXIT_STATUS
  touch "$PHASE_STATUS"
  awk -v n="$1" '$1 != n' "$PHASE_STATUS" >"$PHASE_STATUS.tmp" 2>/dev/null || true
  mv "$PHASE_STATUS.tmp" "$PHASE_STATUS"
  printf '%s\t%s\n' "$1" "$2" >>"$PHASE_STATUS"
}

phase_start() { PHASE_CURRENT="$1"; PHASE_STARTED="$(date -u '+%Y-%m-%d %H:%M:%S.000')"; }
phase_end() {
  local name="${1:-$PHASE_CURRENT}" stop
  stop="$(date -u '+%Y-%m-%d %H:%M:%S.999')"
  touch "$PHASE_TIMES"
  awk -v n="$name" '$1 != n' "$PHASE_TIMES" >"$PHASE_TIMES.tmp" 2>/dev/null || true
  mv "$PHASE_TIMES.tmp" "$PHASE_TIMES"
  printf '%s\t%s\t%s\n' "$name" "$PHASE_STARTED" "$stop" >>"$PHASE_TIMES"
}
PHASE_CURRENT=""; PHASE_STARTED=""

verify() { # verify PHASE OUT [EXPECTED_EXIT]
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
      f="$("$PY" - "$file" <<'PYEOF'
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
  if [[ -n "$problems" ]]; then log "coverage problems:$problems"; fi
  if (( HARNESS_FAILED != 0 )); then log "harness failure flag set"; fi
  if [[ -n "$problems" || "$failed" != 0 || "$HARNESS_FAILED" != 0 ]]; then
    log "HARNESS FAILED"
    return 1
  fi
  log "HARNESS PASSED"
  return 0
}

# ---------------------------------------------------------------------------
# state setup
# ---------------------------------------------------------------------------

reset_state() {
  start_clickhouse
  reap_stale
  log "resetting disposable databases"
  ch_sql default "DROP DATABASE IF EXISTS $DB; DROP DATABASE IF EXISTS $DB_UNTRUSTED; DROP DATABASE IF EXISTS $DB_STOCK;"
  ch_sql default "CREATE DATABASE IF NOT EXISTS $DB; CREATE DATABASE IF NOT EXISTS $DB_UNTRUSTED; CREATE DATABASE IF NOT EXISTS $DB_STOCK;"
  ch_file "$DB" "$HERE/serving-tables.sql"
  ch_file "$DB_UNTRUSTED" "$HERE/serving-tables.sql"
  rm -rf "$RUN/queue-crash" "$RUN/queue-full"
  mkdir -p "$RUN/queue-crash"
  rm -f "$RESULTS"/*.jsonl "$RESULTS"/*.txt "$RESULTS"/*.tsv "$RESULTS"/*.sql "$RESULTS"/*.log
  : >"$PHASE_STATUS"
  : >"$PHASE_TIMES"
  date -u +"%Y-%m-%dT%H:%M:%SZ" >"$RUN_MARKER"
  "$PY" "$HERE/fixture021.py" hashes >"$RESULTS/fixture019-sha256.txt"
  write_build_manifest
}

write_build_manifest() {
  {
    echo "experiment: 021-hybrid-layout-collector-integration"
    echo "generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    # "--version" output ends in a newline and a trailing space; strip trailing
    # whitespace so the generated manifest stays clean under `git diff --check`.
    echo "collector: $("$COLLECTOR" --version 2>&1 | head -2 | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
    echo "collector_bin_sha256: $(sha256sum "$COLLECTOR" | cut -d' ' -f1)"
    echo "collector_source_pin: otelcol-contrib v0.161.0 (release tarball, see setup.sh)"
    echo "collector_build_inputs: $(cat "$ROOT/bin/otelcol-contrib.tar.gz.sha256" 2>/dev/null || echo "otelcol-contrib.tar.gz sha256 not recorded")"
    echo "go: $("$ROOT/bin/go/bin/go" version)"
    echo "clickhouse: $(ch_scalar default 'SELECT version()')"
    echo "clickhouse_config: $HERE/clickhouse-config.xml (26.8.2.7-lts from the repository Nix setup)"
    echo "component_module: lighthouse.dev/rfd0001/exp021 (harness/component, see go.mod and go-modules.txt)"
    echo "pins: clickhouse-go/v2 v2.47.0; otelcol-contrib components v0.161.0 (distro release); collector core modules v1.67.0"
    echo "core_module_series: exporterhelper v0.161.0 requires go.opentelemetry.io/collector/{component,exporter,pdata,confmap,consumer} v1.67.0, the core series of the v0.161.0 release train"
  } >"$RESULTS/build-manifest.txt"
  (cd "$HERE/component" && GOFLAGS=-mod=mod GOMODCACHE="$ROOT/gomodcache" GOPATH="$ROOT/gopath" \
    GOTOOLCHAIN=local "$ROOT/bin/go/bin/go" list -m all) >"$RESULTS/go-modules.txt" 2>"$RESULTS/go-modules.err" || true
}

DONE_PHASES=""
reap_stale() {
  # A process left behind by an interrupted run keeps its harness port bound, and
  # the next phase would then talk to that stale process instead of the one it
  # just started: an earlier `Address already in use` in the proxy log let the
  # ambiguous-serving phase silently run against a spent injector. Reaping before
  # every phase (and in `reset`) keeps that from happening; `start_injector`
  # additionally refuses to start on a port that is still held.
  for name in lighthouse-gateway lighthouse-ack lighthouse-mixed lighthouse-outage \
              lighthouse-split lighthouse-split-retry lighthouse-canonical \
              lighthouse-queue-full lighthouse-crash lighthouse-ambiguous \
              lighthouse-ambiguous-serving lighthouse-no-trust lighthouse-stock \
              lighthouse-transform-types stall-19102 proxy-19102 proxy-19103; do
    stop_bg "$name" >/dev/null 2>&1 || true
  done
}
cleanup() {
  log "cleaning up harness processes"
  reap_stale
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# phases
# ---------------------------------------------------------------------------

guard_fresh() { # guard_fresh PHASE MATCH...
  # Each phase asserts exact row counts for its own fixtures, so pre-existing
  # rows for the same labels make those assertions meaningless (or silently
  # weakens them). Detect stale state fail-closed instead of re-running a phase
  # on top of a previous run; `run.sh reset` clears the disposable databases.
  #
  # MATCH is the stored body text (or span name prefix), not the fixture's
  # shorthand label: the guard's `body LIKE` is what makes it effective, so a
  # label whose body differs from the label (`identity-a` stores
  # `identity-ws-a`) must be passed as the stored text.
  local phase="$1" label count db="${GUARD_DB:-$DB}"
  shift
  for label in "$@"; do
    count="$(ch_scalar "$db" "SELECT (SELECT count() FROM serving_logs WHERE body LIKE '$label%') \
      + (SELECT count() FROM serving_spans WHERE span_name LIKE '$label%') \
      + (SELECT count() FROM canonical_events WHERE position(payload, '$label') > 0)" 2>/dev/null || echo "?")"
    printf '%s\t%s\t%s\n' "$phase" "$label" "$count" >>"$RESULTS/phase-preflight.tsv"
    if [[ "$count" != "0" ]]; then
      log "stale state for $label before phase $phase: count=$count (run ./run.sh reset first)"
      HARNESS_FAILED=1
    fi
  done
}

phase_stock_probe() {
  log "phase stock-probe: stock write path and stock transform capability"
  phase_start stock-probe
  start_collector lighthouse-stock collector-stock-probe.yaml "$STOCK_PORT" \
    LH021_OTLP_PORT="$STOCK_PORT" LH021_METRICS_PORT=8893
  emit stock-probe "http://127.0.0.1:$STOCK_PORT" logs typed
  emit stock-probe "http://127.0.0.1:$STOCK_PORT" traces typed
  wait_rows "$DB_STOCK" "SELECT count() FROM otel_logs" 27 30 || log "stock logs row count short"
  sleep 1
  scrape_metrics metrics-stock-probe.txt 8893
  stop_collector lighthouse-stock
  cp "$LOGS/lighthouse-stock.log" "$RESULTS/collector-stock.log" 2>/dev/null || true
  # The type-companion expression is expected to fail config load: record the
  # nonzero exit instead of silently skipping the probe.
  local rc
  set +e
  timeout 30 "$COLLECTOR" --config "$HERE/collector-transform-types.yaml" \
    >"$RESULTS/transform-types.log" 2>&1
  rc=$?
  set -e
  record_phase "transform-config-load" "$rc"
  if (( rc == 0 )); then
    log "transform/types configuration loaded: the stock-probe expectation must be revisited"
    HARNESS_FAILED=1
  fi
  phase_end stock-probe
  verify stock-probe stock-probe
}

phase_semantics() {
  log "phase semantics: typed representation through the gateway pipeline"
  phase_start semantics
  guard_fresh semantics typed identity-ws-a identity-ws-b batch-a batch-b timing-mixed
  # The four single-record requests below (identity-a, identity-b, batch-a,
  # batch-b) are admitted roughly 230ms apart, so the queue's min_size of four
  # items is what makes them one downstream export. The flush window is the
  # safety net, not the trigger: a 700ms window expired ~40ms before the fourth
  # request was admitted in one run (observed: a three-record mixed export plus a
  # stranded batch-b), stranding the pair this check exists to observe. Three
  # seconds leaves margin for the whole admission sequence, and the window only
  # delays a batch that is already below min_size.
  start_collector lighthouse-gateway collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 \
    LH021_BATCH_FLUSH=3s
  local url="http://127.0.0.1:$GW_PORT"
  # Real credential validation: no token and a wrong token are rejected by the
  # receiver's authenticator before any pipeline runs.
  emit_jq semantics "$url" logs identity-a '.label = "no-token"' || true
  emit_jq semantics "$url" logs identity-a '.label = "bad-token"' "Authorization: Bearer not-a-key" || true
  # Typed corpus, then the forged-scope and shared-batch identities.
  emit semantics "$url" logs typed "Authorization: Bearer lh021-ws-a-key"
  emit semantics "$url" traces typed "Authorization: Bearer lh021-ws-a-key"
  # One trace request carrying a valid span and a reversed-timing span together,
  # so the preserved-span policy is exercised inside a mixed batch.
  emit semantics "$url" traces timing-mixed "Authorization: Bearer lh021-ws-a-key"
  emit semantics "$url" logs identity-a "Authorization: Bearer lh021-ws-a-key"
  emit semantics "$url" logs identity-b "Authorization: Bearer lh021-ws-b-key"
  # Back-to-back requests from both credentials so one downstream batch can merge
  # them (the shared exporter batches below its min_size and flushes on timeout).
  emit semantics "$url" logs batch-a "Authorization: Bearer lh021-ws-a-key"
  emit semantics "$url" logs batch-b "Authorization: Bearer lh021-ws-b-key"
  wait_rows "$DB" "SELECT count() FROM serving_logs WHERE dataset = 'fixture'" 31 40 ||
    log "serving logs row count short"
  wait_rows "$DB" "SELECT count() FROM serving_spans" 7 40 || log "serving spans row count short"
  sleep 1
  scrape_metrics metrics-semantics.txt 8891
  "$PY" - <<PYEOF >"$RESULTS/semantics-schema.jsonl"
import json
import verify
for table in ("canonical_events", "serving_logs", "serving_spans"):
    print(json.dumps({"table": table, "columns": verify.table_columns(verify.DB, table)}))
PYEOF
  # The stored span-timing rows as they were written, so the preserved-span
  # policy is reviewable from the evidence and not only from check names.
  "$PY" - <<PYEOF >"$RESULTS/semantics-span-timing.jsonl"
import json
import verify
rows = verify.ch(
    "SELECT span_name, event_id, workspace_id, dataset, "
    "toUnixTimestamp64Nano(ts) AS ts_ns, toUnixTimestamp64Nano(end_ts) AS end_ts_ns, "
    "duration_ns, span_timing_status FROM serving_spans "
    "WHERE dataset = {d:String} ORDER BY span_name", {"d": verify.DATASET})
for row in rows:
    print(json.dumps(row, default=str))
PYEOF
  stop_collector lighthouse-gateway
  copy_log lighthouse-gateway collector-semantics.log
  phase_end semantics
  verify semantics semantics
}

phase_ack() {
  log "phase ack: producer acknowledgement versus visibility"
  phase_start ack
  guard_fresh ack ack-off ack-on
  local url="http://127.0.0.1:$GW_PORT"
  # Queue disabled: the acknowledgement follows the destination write.
  start_collector lighthouse-ack collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 LH021_QUEUE_ENABLED=false
  emit_with_visibility ack "$url" logs ack-off 10 "Authorization: Bearer lh021-ws-a-key"
  sleep 0.5
  scrape_metrics metrics-ack-off.txt 8891
  stop_collector lighthouse-ack
  # Queue enabled: the acknowledgement follows queue admission.
  start_collector lighthouse-ack collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 LH021_QUEUE_ENABLED=true
  emit_with_visibility ack "$url" logs ack-on 10 "Authorization: Bearer lh021-ws-a-key"
  sleep 0.5
  scrape_metrics metrics-ack.txt 8891
  stop_collector lighthouse-ack
  copy_log lighthouse-ack collector-ack.log
  phase_end ack
  verify ack ack
}

phase_mixed_validation() {
  log "phase mixed-validation: whole-request validation with an invalid record"
  phase_start mixed-validation
  guard_fresh mixed-validation mixed-valid-1 invalid-only-1
  # A four-item batch window keeps the acknowledgement clearly ahead of
  # visibility for the mixed request; the invalid-only request is sent after the
  # mixed batch has been exported so the two requests stay separate batches.
  start_collector lighthouse-mixed collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 \
    LH021_BATCH_MIN=4 LH021_BATCH_FLUSH=3s
  local url="http://127.0.0.1:$GW_PORT"
  # One request with a valid record followed by a duplicate-key record: the
  # component validates the complete request before its first insert, then
  # excludes the invalid record and writes the valid one.
  emit_with_visibility mixed-validation "$url" logs mixed 10 "Authorization: Bearer lh021-ws-a-key"
  # The same invalid record alone: nothing survives validation, so the batch is
  # dropped as a whole with a permanent error instead of being retried.
  emit mixed-validation "$url" logs invalid-only "Authorization: Bearer lh021-ws-a-key"
  sleep 4
  scrape_metrics metrics-mixed-validation.txt 8891
  stop_collector lighthouse-mixed
  copy_log lighthouse-mixed collector-mixed-validation.log
  phase_end mixed-validation
  verify mixed-validation mixed-validation
}

phase_outage() {
  log "phase outage: transient destination outage and recovery"
  phase_start outage
  guard_fresh outage fault-outage
  start_collector lighthouse-outage collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 \
    LH021_RETRY_MAX_ELAPSED=90s LH021_RETRY_MAX_INTERVAL=2s
  local url="http://127.0.0.1:$GW_PORT"
  stop_clickhouse
  {
    echo "destination outage window: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    curl -sS --max-time 5 "http://127.0.0.1:18224/?query=SELECT%201" 2>&1 ||
      echo "clickhouse endpoint refused while stopped"
  } >"$RESULTS/outage-destination-down.txt" 2>&1 || true
  emit outage "$url" logs fault-outage "Authorization: Bearer lh021-ws-a-key"
  sleep 3
  scrape_metrics metrics-outage.txt 8891
  restart_clickhouse
  wait_rows "$DB" "SELECT count() FROM serving_logs WHERE body = 'fault-outage-1'" 1 90 ||
    log "outage recovery did not land the queued request"
  sleep 1
  scrape_metrics metrics-outage-recovered.txt 8891
  stop_collector lighthouse-outage
  copy_log lighthouse-outage collector-outage.log
  phase_end outage
  verify outage outage
}

break_serving() {
  ch_sql "$DB" "RENAME TABLE $DB.serving_logs TO $DB.serving_logs_broken;"
  ch_sql "$DB" "CREATE TABLE $DB.serving_logs (workspace_id String, event_id String) \
    ENGINE = MergeTree ORDER BY (workspace_id, event_id);"
}

restore_serving() {
  ch_sql "$DB" "DROP TABLE IF EXISTS $DB.serving_logs;"
  ch_sql "$DB" "RENAME TABLE $DB.serving_logs_broken TO $DB.serving_logs;"
}

phase_split() {
  log "phase split: canonical-first write with the serving destination broken (no retries)"
  phase_start split
  guard_fresh split fault-split
  break_serving
  start_collector lighthouse-split collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 LH021_RETRY_ENABLED=false
  emit split "http://127.0.0.1:$GW_PORT" logs fault-split "Authorization: Bearer lh021-ws-a-key"
  sleep 2
  scrape_metrics metrics-split.txt 8891
  stop_collector lighthouse-split
  copy_log lighthouse-split collector-split.log
  # Restore the real serving table before verifying: the verifier then proves the
  # event never reached the serving destination, not merely that the temporary
  # stub table lacks the row.
  restore_serving
  phase_end split
  verify split split
  repair_from_canonical split fault-split-1
}

phase_split_retry() {
  log "phase split-retry: retried split write duplicates the committed canonical row"
  phase_start split-retry
  guard_fresh split-retry fault-split-retry
  break_serving
  start_collector lighthouse-split-retry collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 \
    LH021_RETRY_MAX_ELAPSED=3s LH021_RETRY_INITIAL=200ms LH021_RETRY_MAX_INTERVAL=500ms
  emit split-retry "http://127.0.0.1:$GW_PORT" logs fault-split-retry "Authorization: Bearer lh021-ws-a-key"
  sleep 5
  scrape_metrics metrics-split-retry.txt 8891
  stop_collector lighthouse-split-retry
  copy_log lighthouse-split-retry collector-split-retry.log
  restore_serving
  phase_end split-retry
  verify split-retry split-retry
  repair_from_canonical split-retry fault-split-retry-1
}

break_canonical() {
  ch_sql "$DB" "RENAME TABLE $DB.canonical_events TO $DB.canonical_events_broken;"
  ch_sql "$DB" "CREATE TABLE $DB.canonical_events (workspace_id String, event_id String) \
    ENGINE = MergeTree ORDER BY (workspace_id, event_id);"
}

restore_canonical() {
  ch_sql "$DB" "DROP TABLE IF EXISTS $DB.canonical_events;"
  ch_sql "$DB" "RENAME TABLE $DB.canonical_events_broken TO $DB.canonical_events;"
}

phase_canonical_permanent() {
  log "phase canonical-permanent: permanent schema failure on the canonical destination"
  phase_start canonical-permanent
  guard_fresh canonical-permanent fault-canonical
  break_canonical
  start_collector lighthouse-canonical collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 \
    LH021_RETRY_MAX_ELAPSED=2s LH021_RETRY_INITIAL=200ms LH021_RETRY_MAX_INTERVAL=500ms
  emit canonical-permanent "http://127.0.0.1:$GW_PORT" logs fault-canonical \
    "Authorization: Bearer lh021-ws-a-key"
  sleep 5
  scrape_metrics metrics-canonical-permanent.txt 8891
  stop_collector lighthouse-canonical
  copy_log lighthouse-canonical collector-canonical-permanent.log
  restore_canonical
  phase_end canonical-permanent
  verify canonical-permanent canonical-permanent
}

repair_from_canonical() { # repair_from_canonical NAME FIXTURE_ID
  local name="$1" fixture="$2" out="$RESULTS/$1-repair.sql"
  # The stored write identity is content-derived, so the canonical row is
  # selected by the fixture's own identity string inside the payload.
  cat >"$out" <<SQL
-- Bounded repair demonstration (Experiment 021, $name).
-- The canonical payload committed first and is retrievable with its identity and
-- checksum, so a minimal serving row can be derived from it: routing identity,
-- timestamps, kind, and the payload checksum. Rebuilding the full typed serving
-- representation from the canonical payload is NOT implemented as a service
-- here; the semantics phase verifies those reconstruction rules against the
-- fixture instead.
INSERT INTO $DB.serving_logs (workspace_id, dataset, event_id, ts, kind, payload_sha256)
SELECT workspace_id, dataset, event_id, ts, 'log', payload_sha256
FROM $DB.canonical_events WHERE position(payload, '"$fixture"') > 0;
SQL
  ch_sql "$DB" "$(cat "$out")" >"$RESULTS/$1-repair-result.txt" 2>&1 || true
  local landed expected
  landed="$(ch_scalar "$DB" "SELECT count() FROM serving_logs WHERE event_id IN (SELECT event_id FROM canonical_events WHERE position(payload, '\"$fixture\"') > 0)" 2>/dev/null || echo 0)"
  expected="$(ch_scalar "$DB" "SELECT count() FROM canonical_events WHERE position(payload, '\"$fixture\"') > 0" 2>/dev/null || echo 0)"
  printf 'repaired_minimal_serving_rows\t%s\ncanonical_rows\t%s\n' "$landed" "$expected" \
    >>"$RESULTS/$1-repair-result.txt"
  if [[ "$landed" != "$expected" ]]; then
    log "repair $name landed $landed of $expected canonical rows"
    HARNESS_FAILED=1
  fi
}

phase_queue_full() {
  log "phase queue-full: bounded queue, stalled destination"
  phase_start queue-full
  guard_fresh queue-full fault-queue-1 fault-queue-2
  start_injector stall-19102 "$STALL_PORT" "$PY" "$HERE/stalled-listener.py" --listen "$STALL_PORT"
  # The queue is bounded to two items, the consumer blocks on the stalled
  # destination, and batching is immediate so each request holds one slot.
  start_collector lighthouse-queue-full collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 \
    LH021_ENDPOINT="127.0.0.1:$STALL_PORT" LH021_TIMEOUT=30s LH021_QUEUE_SIZE=2 \
    LH021_BATCH_MIN=0 LH021_BATCH_FLUSH=1ms LH021_RETRY_ENABLED=false
  local url="http://127.0.0.1:$GW_PORT" label
  for label in fault-queue-1 fault-queue-2 fault-queue-1 fault-queue-2 fault-queue-1 fault-queue-2; do
    emit queue-full "$url" logs "$label" "Authorization: Bearer lh021-ws-a-key"
  done
  scrape_metrics metrics-queue-full.txt 8891
  stop_collector lighthouse-queue-full
  stop_bg stall-19102
  copy_log lighthouse-queue-full collector-queue-full.log
  phase_end queue-full
  verify queue-full queue-full
}

phase_crash() {
  log "phase crash: SIGKILL with a file-backed queue, then drain after restart"
  phase_start crash
  guard_fresh crash fault-crash-1 fault-crash-2
  rm -rf "$RUN/queue-crash"
  mkdir -p "$RUN/queue-crash"
  start_injector stall-19102 "$STALL_PORT" "$PY" "$HERE/stalled-listener.py" --listen "$STALL_PORT"
  start_collector lighthouse-crash collector-crash.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8894 \
    LH021_ENDPOINT="127.0.0.1:$STALL_PORT" LH021_QUEUE_DIR="$RUN/queue-crash"
  local url="http://127.0.0.1:$GW_PORT"
  emit crash "$url" logs fault-crash-1 "Authorization: Bearer lh021-ws-a-key"
  sleep 1
  emit crash "$url" logs fault-crash-2 "Authorization: Bearer lh021-ws-a-key"
  sleep 1
  find "$RUN/queue-crash" -type f 2>/dev/null | sed "s#$RUN/queue-crash/##" \
    >"$RESULTS/crash-queue-storage-before.txt" || true
  hard_kill_bg lighthouse-crash
  stop_bg stall-19102
  # The queued batches were admitted against 19102; a relay now forwards that
  # port to the real destination so the restart can drain them.
  start_injector proxy-19102 "$STALL_PORT" "$PY" "$HERE/lost-response-proxy.py" --listen "$STALL_PORT" \
    --target "127.0.0.1:$CH_PORT"
  start_collector lighthouse-crash collector-crash.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8894 \
    LH021_ENDPOINT="127.0.0.1:$STALL_PORT" LH021_QUEUE_DIR="$RUN/queue-crash"
  local landed
  landed="$(ch_scalar "$DB" "SELECT count() FROM serving_logs WHERE body LIKE 'fault-crash-%'" 2>/dev/null || echo 0)"
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )) && [[ "$landed" == "0" ]]; do
    sleep 1
    landed="$(ch_scalar "$DB" "SELECT count() FROM serving_logs WHERE body LIKE 'fault-crash-%'" 2>/dev/null || echo 0)"
  done
  sleep 2
  scrape_metrics metrics-crash.txt 8894
  stop_collector lighthouse-crash
  copy_log lighthouse-crash collector-crash-restart.log
  stop_bg proxy-19102
  phase_end crash
  verify crash crash
}

phase_ambiguous() {
  log "phase ambiguous: insert committed, response lost, exporter retries"
  phase_start ambiguous
  guard_fresh ambiguous ambiguous-a
  start_injector proxy-19103 "$LOST_PORT" "$PY" "$HERE/lost-response-proxy.py" --listen "$LOST_PORT" \
    --target "127.0.0.1:$CH_PORT" --drop-insert-responses 1 --grace-ms 400
  start_collector lighthouse-ambiguous collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 \
    LH021_ENDPOINT="127.0.0.1:$LOST_PORT" LH021_TIMEOUT=5s \
    LH021_BATCH_MIN=0 LH021_BATCH_FLUSH=10ms
  emit ambiguous "http://127.0.0.1:$GW_PORT" logs ambiguous-a "Authorization: Bearer lh021-ws-a-key"
  # The first insert commits but its response never reaches the exporter, which
  # then retries the same event once its own timeout expires.
  wait_rows "$DB" "SELECT count() FROM canonical_events WHERE position(payload, '\"ambiguous-a-1\"') > 0" 2 30 ||
    log "the ambiguous insert did not produce a second canonical row"
  sleep 1
  scrape_metrics metrics-ambiguous.txt 8891
  stop_collector lighthouse-ambiguous
  copy_log lighthouse-ambiguous collector-ambiguous.log
  cp "$LOGS/proxy-19103.log" "$RESULTS/proxy-ambiguous.log" 2>/dev/null || true
  phase_end ambiguous
  verify ambiguous ambiguous
}

phase_ambiguous_serving() {
  log "phase ambiguous-serving: serving insert committed, response lost, exporter retries"
  phase_start ambiguous-serving
  guard_fresh ambiguous-serving ambiguous-serving-1
  # --match aims the withholding at the serving insert: the canonical insert is
  # answered normally, then the serving insert commits and its response never
  # reaches the exporter, which retries the whole export.
  start_injector proxy-19103 "$LOST_PORT" "$PY" "$HERE/lost-response-proxy.py" --listen "$LOST_PORT" \
    --target "127.0.0.1:$CH_PORT" --drop-insert-responses 1 --match serving_logs --grace-ms 400
  start_collector lighthouse-ambiguous-serving collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891 \
    LH021_ENDPOINT="127.0.0.1:$LOST_PORT" LH021_TIMEOUT=5s \
    LH021_BATCH_MIN=0 LH021_BATCH_FLUSH=10ms
  emit ambiguous-serving "http://127.0.0.1:$GW_PORT" logs ambiguous-serving-a \
    "Authorization: Bearer lh021-ws-a-key"
  wait_rows "$DB" "SELECT count() FROM serving_logs WHERE body = 'ambiguous-serving-1'" 2 30 ||
    log "the serving-response-loss retry did not produce a second serving row"
  sleep 1
  scrape_metrics metrics-ambiguous-serving.txt 8891
  stop_collector lighthouse-ambiguous-serving
  copy_log lighthouse-ambiguous-serving collector-ambiguous-serving.log
  cp "$LOGS/proxy-19103.log" "$RESULTS/proxy-ambiguous-serving.log" 2>/dev/null || true
  phase_end ambiguous-serving
  verify ambiguous-serving ambiguous-serving
}

phase_retry_duplicate() {
  log "phase retry-duplicate: producer retry, and the identical-events ambiguity"
  phase_start retry-duplicate
  guard_fresh retry-duplicate retry-a legitimate-twin
  start_collector lighthouse-gateway collector-gateway.yaml "$GW_PORT" \
    LH021_OTLP_PORT="$GW_PORT" LH021_METRICS_PORT=8891
  local url="http://127.0.0.1:$GW_PORT"
  emit retry-duplicate "$url" logs retry-a "Authorization: Bearer lh021-ws-a-key"
  sleep 0.5
  emit retry-duplicate "$url" logs retry-a "Authorization: Bearer lh021-ws-a-key"
  wait_rows "$DB" "SELECT count() FROM serving_logs WHERE body = 'retry-a-1'" 2 20 ||
    log "duplicate rows did not appear"
  # The twin control: two legitimately distinct events with identical wire
  # content, then the same request again. The content-derived identity is shared
  # by both legitimate events, so it cannot tell them apart from a retransmission;
  # deduplication remains unselected.
  emit retry-duplicate "$url" logs twins "Authorization: Bearer lh021-ws-a-key"
  sleep 0.5
  emit retry-duplicate "$url" logs twins "Authorization: Bearer lh021-ws-a-key"
  wait_rows "$DB" "SELECT count() FROM serving_logs WHERE body = 'legitimate-twin'" 4 20 ||
    log "twin rows did not appear"
  sleep 0.5
  scrape_metrics metrics-retry-duplicate.txt 8891
  stop_collector lighthouse-gateway
  copy_log lighthouse-gateway collector-retry-duplicate.log
  phase_end retry-duplicate
  verify retry-duplicate retry-duplicate
}

phase_no_trust() {
  log "phase no-trust: pipeline without the trusted-scope processor fails closed"
  phase_start no-trust
  GUARD_DB="$DB_UNTRUSTED" guard_fresh no-trust no-trust no-trust-forged no-trust-mixed
  start_collector lighthouse-no-trust collector-no-trust.yaml "$UNTRUSTED_PORT" \
    LH021_OTLP_PORT="$UNTRUSTED_PORT" LH021_METRICS_PORT=8892 LH021_DATABASE="$DB_UNTRUSTED"
  local url="http://127.0.0.1:$UNTRUSTED_PORT"
  emit no-trust "$url" logs no-trust
  emit no-trust "$url" logs no-trust-forged
  # One request with a forged group and a group without the marker: the missing
  # scope must invalidate the whole request, so even the group that passed the
  # marker check must not be written.
  emit no-trust "$url" logs no-trust-mixed
  sleep 4
  scrape_metrics metrics-no-trust.txt 8892
  stop_collector lighthouse-no-trust
  copy_log lighthouse-no-trust collector-no-trust.log
  phase_end no-trust
  verify no-trust no-trust
}

phase_files() {
  log "phase files: evidence files and module pins"
  phase_start files
  phase_end files
  verify files files
}

# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

run_phase() {
  reap_stale
  case "$1" in
    stock-probe) phase_stock_probe ;;
    semantics) phase_semantics ;;
    ack) phase_ack ;;
    mixed-validation) phase_mixed_validation ;;
    outage) phase_outage ;;
    split) phase_split ;;
    split-retry) phase_split_retry ;;
    canonical-permanent) phase_canonical_permanent ;;
    queue-full) phase_queue_full ;;
    crash) phase_crash ;;
    ambiguous) phase_ambiguous ;;
    ambiguous-serving) phase_ambiguous_serving ;;
    retry-duplicate) phase_retry_duplicate ;;
    no-trust) phase_no_trust ;;
    files) phase_files ;;
    *) echo "unknown phase: $1" >&2; return 2 ;;
  esac
}

case "${1:-all}" in
  all)
    reset_state
    for phase in $ALL_PHASES; do
      run_phase "$phase"
    done
    summarize "$ALL_PHASES"
    ;;
  reset)
    reset_state
    ;;
  summarize)
    summarize "$ALL_PHASES"
    ;;
  check-results)
    shift
    summarize "${*:-$ALL_PHASES}"
    ;;
  files|stock-probe|transform-probe|semantics|ack|mixed-validation|outage|split|split-retry|canonical-permanent|\
queue-full|crash|ambiguous|ambiguous-serving|retry-duplicate|no-trust)
    run_phase "$1"
    ;;
  *)
    echo "usage: $0 [all|reset|summarize|check-results|stock-probe|semantics|ack|mixed-validation|outage|split|split-retry|canonical-permanent|queue-full|crash|ambiguous|ambiguous-serving|retry-duplicate|no-trust|files]" >&2
    exit 2
    ;;
esac
