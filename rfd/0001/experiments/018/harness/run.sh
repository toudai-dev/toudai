#!/usr/bin/env bash
# Experiment 018 orchestration: a disposable local ClickHouse 26.8.2.7, the
# pinned otelcol-contrib build, MinIO as the S3-compatible overflow target, and
# the deterministic fixture. Every phase starts what it needs and stops it again;
# nothing here is a production service.
#
# Usage: run.sh [typed|identity|forge|failover|faults|all]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH018_ROOT:-/home/user/lh018}"
RESULTS="${LH018_RESULTS:-$HERE/results}"
OTELCOL="$ROOT/bin/otelcol-contrib"
PY="$ROOT/venv/bin/python"
RUN="$ROOT/run"
LOGS="$ROOT/logs"
mkdir -p "$RESULTS" "$RUN" "$LOGS"

export LH018_ROOT="$ROOT" LH018_RESULTS="$RESULTS"
export LH018_CH_PATH="$RUN/clickhouse/data/"
export LH018_CH_TMP="$RUN/clickhouse/tmp/"
export LH018_CH_USER_FILES="$RUN/clickhouse/user_files/"
export LH018_CH_FORMAT_SCHEMAS="$RUN/clickhouse/format_schemas/"
export LH018_CH_LOG="$LOGS/clickhouse-server.log"
export LH018_CH_ERRORLOG="$LOGS/clickhouse-server.err.log"
export LH018_CH_USERS="$HERE/clickhouse-users.xml"
export MINIO_ROOT_USER="lh018-minio"
export MINIO_ROOT_PASSWORD="lh018-minio-local"
export AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER"
export AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_PAGER=""

CLICKHOUSE_BIN=""
MINIO_BIN=""

log() { printf '[018] %s\n' "$*"; }

clickhouse_bin() {
  if [[ -z "$CLICKHOUSE_BIN" ]]; then
    CLICKHOUSE_BIN="$(cd "$HERE" && nix develop --command bash -c 'command -v clickhouse-server')"
  fi
  printf '%s' "$CLICKHOUSE_BIN"
}

minio_bin() {
  if [[ -z "$MINIO_BIN" ]]; then
    MINIO_BIN="$(ls -d /nix/store/*minio-2025-10-15*/bin/minio 2>/dev/null | head -1)"
    [[ -n "$MINIO_BIN" ]] || {
      log "MinIO is not realized; run: NIXPKGS_ALLOW_INSECURE=1 nix shell --impure github:nixos/nixpkgs/e5bdc4a41d4c072fe1e3787eaa0320a384741d44#minio"
      exit 1
    }
  fi
  printf '%s' "$MINIO_BIN"
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
    # A zombie still answers kill -0; stop polling and reap it below.
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

port_open() { # port_open PORT
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&- 2>/dev/null || true; return 0; }
  return 1
}

wait_port() { # wait_port PORT [seconds]
  local port="$1" seconds="${2:-60}" start=$SECONDS
  while (( SECONDS - start < seconds )); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      return 0
    fi
    sleep 0.5
  done
  return 1
}

start_clickhouse() {
  is_running clickhouse && return 0
  if port_open 18123; then
    log "port 18123 is already in use by a ClickHouse this run does not own; stop it first"
    exit 1
  fi
  log "starting ClickHouse $("$(clickhouse_bin)" --version 2>/dev/null | head -1)"
  start_bg clickhouse "$(clickhouse_bin)" --config-file="$HERE/clickhouse-config.xml"
  wait_port 18123 90 || { log "ClickHouse did not start"; tail -20 "$LOGS/clickhouse.log" || true; exit 1; }
}

stop_clickhouse() {
  if is_running clickhouse; then
    log "stopping ClickHouse"
    stop_bg clickhouse
  fi
}

ch_sql() { # ch_sql SQL
  curl -sS --fail-with-body "http://127.0.0.1:18123/?database=lh018" --data-binary "$1"
}

ch_admin() { # ch_admin SQL against the default database (DDL, resets)
  curl -sS --fail-with-body "http://127.0.0.1:18123/" --data-binary "$1"
}

start_collector() { # start_collector NAME CONFIG
  local name="$1" config="$2"
  start_bg "$name" "$OTELCOL" --config "$HERE/$config"
  wait_port 4318 60 || { log "$name did not open 4318"; tail -30 "$LOGS/$name.log"; exit 1; }
  sleep 0.5
}

stop_collector() { stop_bg "$1" "${2:-TERM}"; }

scrape_metrics() { # scrape_metrics OUT
  curl -sS "http://127.0.0.1:8888/metrics" >"$1" 2>/dev/null || echo "metrics endpoint unavailable" >"$1"
}

emit() { # emit SIGNAL LABEL HEADER...
  local signal="$1" label="$2"; shift 2
  local args=()
  for header in "$@"; do args+=(--header "$header"); done
  "$PY" "$HERE/fixture.py" emit "$signal" "$label" --url "http://127.0.0.1:4318" "${args[@]}" \
    --label-out "$RESULTS/emit-$label.jsonl"
}

wait_rows() { # wait_rows SQL MIN_ROWS TIMEOUT_S -> elapsed ms on stdout
  local sql="$1" want="$2" timeout="${3:-30}" start
  start=$(date +%s%3N)
  while :; do
    local got
    got="$(ch_sql "$sql" 2>/dev/null || echo 0)"
    [[ "$got" =~ ^[0-9]+$ ]] || got=0
    if (( got >= want )); then
      echo $(( $(date +%s%3N) - start ))
      return 0
    fi
    if (( $(date +%s%3N) - start > timeout * 1000 )); then
      echo "-1"
      return 1
    fi
    sleep 0.1
  done
}

ddl_evidence() {
  {
    echo "-- generated by stock clickhouseexporter v0.161.0 (create_schema: true)"
    ch_sql "SHOW CREATE TABLE lh018.otel_logs FORMAT TSVRaw"
    echo
    ch_sql "SHOW CREATE TABLE lh018.otel_traces FORMAT TSVRaw"
    echo
    ch_sql "SHOW CREATE TABLE lh018.otel_metrics_gauge FORMAT TSVRaw"
  } >"$RESULTS/ddl-stock-exporter.sql" 2>&1 || true
  ch_sql "SELECT table, name, type FROM system.columns WHERE database='lh018' ORDER BY table, position FORMAT TSV" \
    >"$RESULTS/columns.tsv" || true
  # The exporter inserts with async_insert=1; the server default for
  # wait_for_async_insert decides whether an insert ack means "flushed".
  ch_sql "SELECT name, value, default FROM system.settings WHERE name IN ('async_insert','wait_for_async_insert') ORDER BY name FORMAT TSV" \
    >"$RESULTS/clickhouse-async-settings.tsv" || true
}

# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

phase_typed() {
  log "phase typed: stock OTLP -> ClickHouse mapping"
  start_clickhouse
  # Deterministic expectations require a clean database; the exporter recreates
  # its schema (create_schema: true) below.
  ch_admin "DROP DATABASE IF EXISTS lh018" >/dev/null
  : >"$RESULTS/typed-received-logs.jsonl"
  : >"$RESULTS/typed-received-traces.jsonl"
  start_collector collector-typed collector-typed.yaml
  emit logs typed >"$RESULTS/emit-typed-logs.jsonl"
  emit traces typed >"$RESULTS/emit-typed-traces.jsonl"
  emit metrics typed >"$RESULTS/emit-typed-metrics.jsonl"
  local ms
  ms="$(wait_rows "SELECT count() FROM lh018.otel_logs" 9 30)" || log "warning: logs did not reach 9 rows"
  log "typed rows visible ${ms}ms after the logs export request"
  echo "$ms" >"$RESULTS/typed-visibility-ms.txt"
  scrape_metrics "$RESULTS/metrics-typed.txt"
  stop_collector collector-typed
  ddl_evidence
  # Additive schema path: a promoted column computed from the trusted resource
  # attribute. The stock exporter cannot insert a column it does not know, but a
  # MATERIALIZED expression over the Map columns works with create_schema: false.
  ch_sql "ALTER TABLE lh018.otel_logs ADD COLUMN IF NOT EXISTS ws_materialized LowCardinality(String) MATERIALIZED ResourceAttributes['lighthouse.workspace_id']" >/dev/null
  "$PY" "$HERE/verify.py" --phase typed --results "$RESULTS" --out "$RESULTS/checks-typed.jsonl"
}

phase_identity() {
  log "phase identity: credential routing, batching, replay credential"
  start_clickhouse
  # Repeatable phase: remove only this phase's fixture rows before re-inserting.
  ch_sql "ALTER TABLE lh018.otel_logs DELETE WHERE Body LIKE 'identity-%' OR Body LIKE 'batch-log-%' SETTINGS mutations_sync=2" >/dev/null
  ch_sql "ALTER TABLE lh018.otel_traces DELETE WHERE SpanName='span/ok' AND ResourceAttributes['lighthouse.workspace_id'] != '' SETTINGS mutations_sync=2" >/dev/null
  : >"$RESULTS/sink-captured.jsonl"
  : >"$RESULTS/quarantine-logs.jsonl"
  : >"$RESULTS/quarantine-traces.jsonl"
  start_bg sink-capture "$PY" "$HERE/sink.py" capture --port 4320 --out "$RESULTS/sink-captured.jsonl"
  wait_port 4320 20
  start_collector collector-identity collector-identity.yaml
  emit logs identity-a "Authorization: Bearer lh018-ws-a-key" >"$RESULTS/emit-identity-a.jsonl"
  emit logs identity-b "Authorization: Bearer lh018-ws-b-key" >"$RESULTS/emit-identity-b.jsonl"
  emit traces typed "Authorization: Bearer lh018-ws-a-key" >"$RESULTS/emit-identity-traces.jsonl"
  # Unauthorized request: valid shape, unknown credential.
  "$PY" "$HERE/fixture.py" emit logs identity-a --url "http://127.0.0.1:4318" \
    --header "Authorization: Bearer lh018-not-a-key" --label-out "$RESULTS/unauthorized-response.jsonl" \
    >"$RESULTS/emit-unauthorized.jsonl" || true
  # Rebatching observation: six records, queue batch min_size 4.
  emit logs batch "Authorization: Bearer lh018-ws-a-key" >"$RESULTS/emit-batch.jsonl"
  wait_rows "SELECT count() FROM lh018.otel_logs WHERE Body LIKE 'identity-%'" 2 30 >/dev/null || true
  wait_rows "SELECT count() FROM lh018.otel_logs WHERE Body LIKE 'batch-log-%'" 6 30 >/dev/null || true
  wait_rows "SELECT count() FROM lh018.otel_traces WHERE SpanName='span/ok'" 1 30 >/dev/null || true
  sleep 2  # let the sink flush its 2s batch timeout
  scrape_metrics "$RESULTS/metrics-identity.txt"
  stop_collector collector-identity
  stop_bg sink-capture
  "$PY" "$HERE/verify.py" --phase identity --results "$RESULTS" --out "$RESULTS/checks-identity.jsonl"
  "$PY" "$HERE/verify.py" --phase files --results "$RESULTS" --out "$RESULTS/checks-identity-files.jsonl"
}

phase_forge() {
  log "phase forge: negative control for producer-supplied workspace headers"
  start_clickhouse
  ch_sql "ALTER TABLE lh018.otel_logs DELETE WHERE Body='identity-forge-log' SETTINGS mutations_sync=2" >/dev/null
  : >"$RESULTS/quarantine-forge-logs.jsonl"
  start_collector collector-forge collector-forge.yaml
  "$PY" "$HERE/fixture.py" emit logs forge --url "http://127.0.0.1:4318" \
    --header "Authorization: Bearer lh018-ws-a-key" \
    --header "X-Lighthouse-Workspace: $WS_B" --label-out "$RESULTS/emit-forge.jsonl" || true
  wait_rows "SELECT count() FROM lh018.otel_logs WHERE Body='identity-forge-log'" 1 20 >/dev/null || true
  stop_collector collector-forge
  "$PY" "$HERE/verify.py" --phase forge --results "$RESULTS" --out "$RESULTS/checks-forge.jsonl"
  "$PY" "$HERE/verify.py" --phase files --results "$RESULTS" --out "$RESULTS/checks-forge-files.jsonl"
}

phase_failover() {
  log "phase failover: ClickHouse outage -> S3 overflow -> replay"
  start_clickhouse
  ch_sql "ALTER TABLE lh018.otel_logs DELETE WHERE Body LIKE 'fault-log-%' SETTINGS mutations_sync=2" >/dev/null
  if ! is_running minio; then
    start_bg minio "$(minio_bin)" server "$RUN/minio" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001
    wait_port 9000 60
  fi
  nix develop --command bash -c "aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://lh018-overflow 2>/dev/null || true" >/dev/null
  nix develop --command bash -c "aws --endpoint-url http://127.0.0.1:9000 s3 rm s3://lh018-overflow --recursive" >/dev/null 2>&1 || true
  : >"$RESULTS/quarantine-failover-logs.jsonl"
  start_collector collector-failover collector-failover.yaml
  emit logs fault-1 "Authorization: Bearer lh018-ws-a-key" >"$RESULTS/emit-failover-baseline.jsonl"
  wait_rows "SELECT count() FROM lh018.otel_logs WHERE Body='fault-log-0'" 1 20 >/dev/null || true

  # Outage with the failover connector's default sending queue enabled: OTLP
  # success is connector queue admission, not a destination write.
  stop_clickhouse
  sleep 1
  log "outage emit at $(date -u +%FT%T.%3NZ)"
  emit logs fault-2 "Authorization: Bearer lh018-ws-a-key" >"$RESULTS/emit-failover-outage.jsonl"
  log "outage emit returned at $(date -u +%FT%T.%3NZ)"
  sleep 3  # allow the failover connector to observe the failed primary and spill
  nix develop --command bash -c "aws --endpoint-url http://127.0.0.1:9000 s3api list-objects-v2 --bucket lh018-overflow" \
    >"$RESULTS/overflow-objects-after-outage.txt" 2>/dev/null || true
  nix develop --command bash -c "aws --endpoint-url http://127.0.0.1:9000 s3api list-objects-v2 --bucket lh018-overflow --query 'Contents[].Key' --output text" \
    >"$RESULTS/overflow-keys-after-outage.txt" 2>/dev/null || true
  local key
  key="$(head -1 "$RESULTS/overflow-keys-after-outage.txt" | tr -d '\r' | awk '{print $1}')"
  if [[ -n "$key" && "$key" != "None" ]]; then
    nix develop --command bash -c "aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://lh018-overflow/$key $RESULTS/overflow-object-1.binpb" >/dev/null
  fi

  # Same outage with the connector queue disabled: the producer waits for the
  # primary retry budget and the overflow write.
  stop_collector collector-failover
  start_collector collector-failover-sync collector-failover-sync.yaml
  log "sync outage emit at $(date -u +%FT%T.%3NZ)"
  emit logs fault-3 "Authorization: Bearer lh018-ws-a-key" >"$RESULTS/emit-failover-sync-outage.jsonl"
  log "sync outage emit returned at $(date -u +%FT%T.%3NZ)"
  sleep 2
  nix develop --command bash -c "aws --endpoint-url http://127.0.0.1:9000 s3api list-objects-v2 --bucket lh018-overflow --query 'Contents[].Key' --output text" \
    >"$RESULTS/overflow-keys-after-sync-outage.txt" 2>/dev/null || true
  stop_collector collector-failover-sync

  start_clickhouse
  start_collector collector-failover collector-failover.yaml
  log "waiting for the failover connector to re-establish the primary pipeline"
  sleep 12
  emit logs fault-4 "Authorization: Bearer lh018-ws-a-key" >"$RESULTS/emit-failover-fresh.jsonl"
  # Baseline fault-log-0 plus the four fresh records; the spilled backlog is
  # still in object storage at this point.
  wait_rows "SELECT count() FROM lh018.otel_logs WHERE Body LIKE 'fault-log-%'" 5 20 >/dev/null || true

  # Replay: catch-up credential authorizes the replay; the payload already
  # carries the trusted workspace attribute, which must not be re-derived.
  if [[ -s "$RESULTS/overflow-object-1.binpb" ]]; then
    "$PY" "$HERE/fixture.py" replay --file "$RESULTS/overflow-object-1.binpb" \
      --url "http://127.0.0.1:4319" --header "Authorization: Bearer lh018-catchup-key" \
      --label-out "$RESULTS/replay-1.jsonl" || true
    wait_rows "SELECT count() FROM lh018.otel_logs WHERE Body LIKE 'fault-log-%'" 7 20 >/dev/null || true
    sleep 1
    "$PY" "$HERE/fixture.py" replay --file "$RESULTS/overflow-object-1.binpb" \
      --url "http://127.0.0.1:4319" --header "Authorization: Bearer lh018-catchup-key" \
      --label-out "$RESULTS/replay-2.jsonl" || true
    sleep 2
  fi
  ch_sql "SELECT Body, count() AS rows, groupArray(ResourceAttributes['lighthouse.workspace_id']) AS ws FROM lh018.otel_logs WHERE Body LIKE 'fault-log-%' GROUP BY Body ORDER BY Body FORMAT TSV" \
    >"$RESULTS/failover-duplicates.tsv" || true
  scrape_metrics "$RESULTS/metrics-failover.txt"
  stop_collector collector-failover
  "$PY" "$HERE/verify.py" --phase failover --results "$RESULTS" --out "$RESULTS/checks-failover.jsonl"
  "$PY" "$HERE/verify.py" --phase failover-files --results "$RESULTS" --out "$RESULTS/checks-failover-files.jsonl"
}

phase_faults() {
  log "phase faults: permanent error, bounded queue, crash durability"
  start_clickhouse
  ch_admin "DROP DATABASE IF EXISTS lh018" >/dev/null
  ch_admin "CREATE DATABASE IF NOT EXISTS lh018" >/dev/null
  # Table with a missing ScopeAttributes column: every exporter INSERT fails.
  ch_sql "CREATE TABLE IF NOT EXISTS lh018.otel_logs_broken (
      Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8,
      SeverityText LowCardinality(String), SeverityNumber UInt8, ServiceName LowCardinality(String),
      Body String, ResourceSchemaUrl LowCardinality(String),
      ResourceAttributes Map(LowCardinality(String), String), ScopeSchemaUrl LowCardinality(String),
      ScopeName String, ScopeVersion LowCardinality(String),
      LogAttributes Map(LowCardinality(String), String))
      ENGINE = MergeTree PARTITION BY toDate(Timestamp) ORDER BY Timestamp" >/dev/null
  # Fault table for queue/crash phases: no EventName column, so insert SQL is
  # stable whether or not schema detection can reach the server at startup.
  ch_sql "CREATE TABLE IF NOT EXISTS lh018.otel_logs_faults (
      Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8,
      SeverityText LowCardinality(String), SeverityNumber UInt8, ServiceName LowCardinality(String),
      Body String, ResourceSchemaUrl LowCardinality(String),
      ResourceAttributes Map(LowCardinality(String), String), ScopeSchemaUrl LowCardinality(String),
      ScopeName String, ScopeVersion LowCardinality(String), ScopeAttributes Map(LowCardinality(String), String),
      LogAttributes Map(LowCardinality(String), String))
      ENGINE = MergeTree PARTITION BY toDate(Timestamp) ORDER BY Timestamp" >/dev/null

  # 4a: permanent insert error with the queue enabled.
  start_collector collector-permanent collector-permanent.yaml
  emit logs fault-2 >"$RESULTS/permanent-response.jsonl" || true
  sleep 6
  scrape_metrics "$RESULTS/metrics-permanent.txt"
  grep -E 'Exporting failed|dropping data|insert failed|retry' "$LOGS/collector-permanent.log" | tail -20 \
    >"$RESULTS/permanent-log-excerpt.txt" || true
  stop_collector collector-permanent

  # 4b: bounded queue against a stalled downstream.
  start_bg stall "$PY" "$HERE/sink.py" tcp-stall --port 19100
  wait_port 19100 20
  start_collector collector-queue-full collector-queue-full.yaml
  : >"$RESULTS/queue-full-responses.jsonl"
  for i in 1 2 3 4 5 6 7 8; do
    "$PY" "$HERE/fixture.py" emit logs fault-1 --url "http://127.0.0.1:4318" \
      --label-out "$RESULTS/queue-full-$i.jsonl" >>"$RESULTS/queue-full-raw.jsonl" 2>&1 || true
  done
  "$PY" - "$RESULTS" <<'PYEOF'
import json, sys, pathlib
results = pathlib.Path(sys.argv[1])
records = []
for i in range(1, 9):
    path = results / f"queue-full-{i}.jsonl"
    if path.exists():
        records.append(json.loads(path.read_text()))
(results / "queue-full-responses.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
PYEOF
  scrape_metrics "$RESULTS/metrics-queue-full.txt"
  ch_sql "SELECT count() FROM lh018.otel_logs_faults" >"$RESULTS/queue-full-rows.txt" 2>/dev/null || echo "unavailable" >"$RESULTS/queue-full-rows.txt"
  stop_collector collector-queue-full
  stop_bg stall

  # 4c: in-memory queue crash (acknowledged but lost).
  start_bg stall "$PY" "$HERE/sink.py" tcp-stall --port 19100
  wait_port 19100 20
  start_collector collector-crash-memory collector-crash-memory.yaml
  : >"$RESULTS/crash-memory-responses.jsonl"
  for i in 1 2 3; do
    "$PY" "$HERE/fixture.py" emit logs fault-1 --url "http://127.0.0.1:4318" \
      --label-out "$RESULTS/crash-memory-$i.jsonl" >>"$RESULTS/crash-memory-raw.jsonl" 2>&1 || true
  done
  sleep 1
  stop_collector collector-crash-memory KILL
  stop_bg stall
  ch_sql "SELECT count() FROM lh018.otel_logs_faults" >"$RESULTS/crash-memory-rows.txt" 2>/dev/null || echo "unavailable" >"$RESULTS/crash-memory-rows.txt"
  "$PY" - "$RESULTS" <<'PYEOF'
import json, sys, pathlib
results = pathlib.Path(sys.argv[1])
records = []
for i in range(1, 4):
    path = results / f"crash-memory-{i}.jsonl"
    if path.exists():
        records.append(json.loads(path.read_text()))
(results / "crash-memory-responses.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
PYEOF

  # 4d: file-backed queue survives a crash and drains when the downstream returns.
  rm -rf "$RUN/queue-storage"
  start_bg stall "$PY" "$HERE/sink.py" tcp-stall --port 19100
  wait_port 19100 20
  log "crash-persistent pre-crash run at $(date -u +%FT%T.%3NZ)"
  start_collector collector-crash-persistent collector-crash-persistent.yaml
  for i in 1 2 3; do
    "$PY" "$HERE/fixture.py" emit logs fault-2 --url "http://127.0.0.1:4318" \
      --label-out "$RESULTS/crash-persistent-$i.jsonl" >>"$RESULTS/crash-persistent-raw.jsonl" 2>&1 || true
  done
  sleep 1
  stop_collector collector-crash-persistent KILL
  log "crash-persistent killed at $(date -u +%FT%T.%3NZ)"
  stop_bg stall
  start_bg proxy "$PY" "$HERE/sink.py" tcp-proxy --port 19100 --target 127.0.0.1:19000 --mode up
  wait_port 19100 20
  start_collector collector-crash-persistent-drain collector-crash-persistent.yaml
  local ms
  ms="$(wait_rows "SELECT count() FROM lh018.otel_logs_faults WHERE Body LIKE 'fault-log-%'" 1 30)" || log "warning: persistent queue did not drain"
  echo "$ms" >"$RESULTS/crash-persistent-drain-ms.txt"
  # Producer retry after the crash: the same records are delivered again.
  emit logs fault-2 >"$RESULTS/crash-persistent-retry.jsonl" || true
  sleep 2
  ch_sql "SELECT Body, count() AS rows FROM lh018.otel_logs_faults WHERE Body LIKE 'fault-log-%' GROUP BY Body ORDER BY Body FORMAT TSV" \
    >"$RESULTS/crash-persistent-duplicates.tsv" || true
  grep -E 'Exporting failed|retry|queue' "$LOGS/collector-crash-persistent.log" | tail -20 \
    >"$RESULTS/crash-persistent-log-excerpt.txt" || true
  stop_collector collector-crash-persistent-drain
  stop_bg proxy
  "$PY" "$HERE/verify.py" --phase faults --results "$RESULTS" --out "$RESULTS/checks-faults.jsonl"
  "$PY" "$HERE/verify.py" --phase fault-files --results "$RESULTS" --out "$RESULTS/checks-faults-files.jsonl"
  "$PY" "$HERE/verify.py" --phase obs --results "$RESULTS" --out "$RESULTS/checks-obs.jsonl"
}

cleanup() {
  stop_bg proxy
  stop_bg stall
  stop_bg sink-capture
  stop_bg minio
  stop_bg collector-typed
  stop_bg collector-identity
  stop_bg collector-forge
  stop_bg collector-failover
  stop_bg collector-failover-sync
  stop_bg collector-permanent
  stop_bg collector-queue-full
  stop_bg collector-crash-memory
  stop_bg collector-crash-persistent
  stop_bg collector-crash-persistent-drain
  stop_bg clickhouse
}
trap cleanup EXIT

WS_B="22222222-2222-4222-8222-222222222222"

reset_state() {
  # Disposable state only: ClickHouse data, MinIO objects, and any queued
  # batches from a previous run. Never touches repository or benchmark data.
  if ! is_running clickhouse; then
    rm -rf "$RUN/clickhouse"
  fi
  rm -rf "$RUN/queue-storage"
  mkdir -p "$RUN/clickhouse/data" "$RUN/clickhouse/tmp" "$RUN/clickhouse/user_files" "$RUN/clickhouse/format_schemas"
  rm -rf "$RUN/minio"
  rm -f "$RESULTS"/checks-*.json
}

case "${1:-all}" in
  typed) phase_typed ;;
  identity) phase_identity ;;
  forge) phase_forge ;;
  failover) phase_failover ;;
  faults) phase_faults ;;
  all)
    reset_state
    phase_typed
    phase_identity
    phase_forge
    phase_failover
    phase_faults
    ;;
  *) echo "usage: $0 [typed|identity|forge|failover|faults|all]" >&2; exit 2 ;;
esac

log "done"
