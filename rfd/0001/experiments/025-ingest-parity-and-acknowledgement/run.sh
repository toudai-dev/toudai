#!/usr/bin/env bash
# Experiment 025: toudai-ingest parity with the Go path (collector #5), and what
# stored acknowledgement means under Experiment 021's failure cases.
#
# Run from the toudai repository inside its dev shell (cargo, clickhouse), with a
# clean collector checkout at f780574 whose Nix shell provides go:
#
#   COLLECTOR_REPO=<collector checkout> nix develop --command \
#     bash rfd/0001/experiments/025-ingest-parity-and-acknowledgement/run.sh
#
# Every check writes its expected and observed value to results/checks.tsv.
# Expected values come from the experiment's hypothesis and from what each case
# injects, never from the rows observed. Exit status is nonzero if any check
# fails.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../.." && pwd)"
COLLECTOR_REPO="${COLLECTOR_REPO:?set COLLECTOR_REPO to a collector checkout at f780574}"
ROOT="${EXP025_ROOT:-$(mktemp -d)}"
RESULTS="${EXP025_RESULTS:-$HERE/results}"
CH_TCP=19225 CH_HTTP=18325 RELAY=18326 STALL=18327
GO_GRPC=14317 GO_HTTP=14318 GO_TELEMETRY=18888
IN_GRPC=14417 IN_HTTP=14418
KEY=tk_exp025_writer_0123456789abcdef
FAIL_DB=exp025_fail
rm -rf "$RESULTS"
mkdir -p "$ROOT"/{ch/data,ch/tmp,ch/user_files,ch/format_schemas,bin,load} "$RESULTS"/{parity,phases/requests}
CONTROL="$ROOT/inject.json"
: >"$CONTROL"
printf 'name\texpected\tobserved\tresult\n' >"$RESULTS/checks.tsv"

declare -A PIDS=()
cleanup() {
  for name in "${!PIDS[@]}"; do kill "${PIDS[$name]}" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

log() { printf '[025] %s\n' "$*"; }
FAILED=0
check() { # check NAME EXPECTED OBSERVED
  local result=pass
  [ "$2" = "$3" ] || { result=FAIL; FAILED=$((FAILED + 1)); }
  printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$result" >>"$RESULTS/checks.tsv"
  log "$result $1: expected $2, observed $3"
}
holds() { if "$@"; then echo 0; else echo 1; fi; } # holds CMD... -> 0 or 1, never exits
check_true() { # check_true NAME DESCRIPTION "$(holds CMD...)" OBSERVED
  local result=pass
  [ "$3" = 0 ] || { result=FAIL; FAILED=$((FAILED + 1)); }
  printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$4" "$result" >>"$RESULTS/checks.tsv"
  log "$result $1: expected $2, observed $4"
}

ch() { clickhouse-client --port "$CH_TCP" --query "$1"; }
port_busy() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
for p in $CH_TCP $CH_HTTP $RELAY $STALL $GO_GRPC $GO_HTTP $GO_TELEMETRY $IN_GRPC $IN_HTTP; do
  if port_busy "$p"; then echo "port $p is in use; refusing to run against a service this script did not start" >&2; exit 2; fi
done

start_bg() { # start_bg NAME LOGFILE CMD... (environment from the caller)
  local name="$1" out="$2"; shift 2
  "$@" >"$out" 2>&1 &
  PIDS[$name]=$!
}
stop_bg() { # stop_bg NAME [SIGNAL]
  local pid="${PIDS[$1]:-}"
  [ -n "$pid" ] || return 0
  kill "-${2:-TERM}" "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  unset "PIDS[$1]"
}
wait_until() { # wait_until NAME SECONDS CMD... -> fails if CMD never succeeds or NAME exits
  local name="$1" seconds="$2"; shift 2
  for _ in $(seq 1 $((seconds * 5))); do
    kill -0 "${PIDS[$name]}" 2>/dev/null || { echo "$name exited" >&2; return 1; }
    "$@" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "$name did not become ready" >&2
  return 1
}

# --- build ------------------------------------------------------------------
log "building toudai-ingest, the fixture emitter, and the collector"
(cd "$REPO" && cargo build --locked -p toudai-ingest --bin toudai-ingest)
INGEST="$REPO/target/debug/toudai-ingest"
(cd "$REPO" && CARGO_TARGET_DIR="$REPO/fixture-emitter/target" cargo build --locked --manifest-path fixture-emitter/Cargo.toml)
EMITTER="$REPO/fixture-emitter/target/debug/fixture-emitter"
(cd "$COLLECTOR_REPO" && nix develop --command go build -o "$ROOT/bin/toudai-collector" .)
{
  echo "toudai $(git -C "$REPO" rev-parse HEAD) (uncommitted paths outside results/: $(git -C "$REPO" status --porcelain --untracked-files=all -- . ":(exclude)${RESULTS#"$REPO"/}" | wc -l))"
  echo "collector $(git -C "$COLLECTOR_REPO" rev-parse HEAD) (uncommitted paths: $(git -C "$COLLECTOR_REPO" status --porcelain | wc -l))"
} >"$RESULTS/revisions.txt"

# --- ClickHouse --------------------------------------------------------------
export EXP025_CH_PATH="$ROOT/ch/data/" EXP025_CH_TMP="$ROOT/ch/tmp/" \
  EXP025_CH_USER_FILES="$ROOT/ch/user_files/" EXP025_CH_FORMAT_SCHEMAS="$ROOT/ch/format_schemas/" \
  EXP025_CH_LOG="$ROOT/ch.log" EXP025_CH_ERRORLOG="$ROOT/ch.err.log" EXP025_CH_USERS="$HERE/clickhouse-users.xml"
start_clickhouse() {
  start_bg clickhouse "$ROOT/ch.stdout" clickhouse-server --config-file="$HERE/clickhouse-config.xml"
  wait_until clickhouse 60 ch 'SELECT 1'
}
start_clickhouse
ch 'SELECT version()' >>"$RESULTS/revisions.txt"

# The repo DDL targets the S3-backed payload policy; locally the TTL move and
# storage policy are dropped, which changes where parts live and nothing else.
apply_ddl() { # apply_ddl DATABASE
  for f in "$REPO"/clickhouse/schema/*.sql; do
    perl -0pe "s/\nTTL[^\n]*\nSETTINGS storage_policy = 'toudai_payload';/;/" "$f" |
      clickhouse-client --port "$CH_TCP" --param_database="$1" --multiquery
  done
}

# --- services ---------------------------------------------------------------
printf '%s fixture fixture-b exp\n' "$KEY" >"$ROOT/write-keys.txt"
printf 'keys:\n  %s:\n    datasets: [fixture, fixture-b, exp]\n    write: true\n' "$KEY" >"$ROOT/keys.yaml"
# The example gateway config has no telemetry section; its Prometheus endpoint
# moves off the default port so a stray local Collector cannot answer for it.
printf 'service:\n  telemetry:\n    metrics:\n      readers:\n        - pull:\n            exporter:\n              prometheus:\n                host: 127.0.0.1\n                port: %s\n' "$GO_TELEMETRY" >"$ROOT/telemetry.yaml"

start_collector() { # start_collector DATABASE
  TOUDAI_KEYS_FILE="$ROOT/keys.yaml" TOUDAI_CLICKHOUSE_ENDPOINT="127.0.0.1:$CH_TCP" \
    TOUDAI_CLICKHOUSE_DATABASE="$1" TOUDAI_OTLP_HTTP_ENDPOINT="127.0.0.1:$GO_HTTP" \
    TOUDAI_OTLP_GRPC_ENDPOINT="127.0.0.1:$GO_GRPC" \
    start_bg collector "$ROOT/collector-$1.log" "$ROOT/bin/toudai-collector" \
    --config "$COLLECTOR_REPO/config/gateway.example.yaml" --config "$ROOT/telemetry.yaml"
  wait_until collector 60 curl -s -o /dev/null "http://127.0.0.1:$GO_HTTP/v1/logs"
}
start_ingest() { # start_ingest NAME DATABASE CLICKHOUSE_URL [VAR=VALUE...]
  local name="$1" database="$2" url="$3"; shift 3
  env TOUDAI_WRITE_KEYS_FILE="$ROOT/write-keys.txt" TOUDAI_CLICKHOUSE_URL="$url" \
    TOUDAI_CLICKHOUSE_DATABASE="$database" TOUDAI_OTLP_GRPC_LISTEN="127.0.0.1:$IN_GRPC" \
    TOUDAI_OTLP_HTTP_LISTEN="127.0.0.1:$IN_HTTP" "$@" \
    "$INGEST" >"$ROOT/ingest-$name.log" 2>&1 &
  PIDS[ingest]=$!
  wait_until ingest 30 curl -sf "http://127.0.0.1:$IN_HTTP/healthz"
}
emit() { # emit PROTOCOL ENDPOINT LOG
  local unset=()
  while IFS= read -r name; do unset+=(-u "$name"); done < <(env | sed -n 's/^\(OTEL_[A-Z_]*\)=.*/\1/p')
  env "${unset[@]}" OTEL_EXPORTER_OTLP_HEADERS="x-toudai-key=$KEY" \
    "$EMITTER" emit --protocol "$1" --endpoint "$2" --signals logs,traces >"$3" 2>&1
}
dump() { # dump DATABASE DIR
  mkdir -p "$2"
  local settings="SETTINGS output_format_json_quote_64bit_integers=1, output_format_json_quote_denormals=1"
  for table in serving_logs serving_spans canonical_events; do
    ch "SELECT * FROM $1.$table ORDER BY event_id FORMAT JSONEachRow $settings" >"$2/$table.jsonl"
  done
}
stored_count() { # stored_count DATABASE -> "logs spans canonical"
  echo "$(ch "SELECT count() FROM $1.serving_logs") $(ch "SELECT count() FROM $1.serving_spans") $(ch "SELECT count() FROM $1.canonical_events")"
}

# --- phase: parity -------------------------------------------------------------
# The fixture serves 26 log records and 16 spans over its two datasets
# (Experiment 023's expected.jsonl) and rejects two repeated-key log records.
log "phase parity"
WANT_ROWS="26 16 42"
for protocol in grpc http; do
  for side in go ingest; do apply_ddl "exp025_${side}_$protocol"; done

  start_collector "exp025_go_$protocol"
  port=$GO_GRPC; [ "$protocol" = http ] && port=$GO_HTTP
  emit "$protocol" "http://127.0.0.1:$port" "$RESULTS/parity/emit-go-$protocol.log"
  # The Go path answers at queue admission, so its rows arrive after the flush.
  for _ in $(seq 1 150); do [ "$(stored_count "exp025_go_$protocol")" = "$WANT_ROWS" ] && break; sleep 0.2; done
  curl -s "http://127.0.0.1:$GO_TELEMETRY/metrics" >"$RESULTS/parity/metrics-go-$protocol.txt"
  stop_bg collector
  check "parity.$protocol.go_rows" "$WANT_ROWS" "$(stored_count "exp025_go_$protocol")"
  check "parity.$protocol.go_rejected_records" 2 \
    "$(awk '/^toudai_exporter_rejected_records/ { n += $NF } END { print n + 0 }' "$RESULTS/parity/metrics-go-$protocol.txt")"

  start_ingest "parity-$protocol" "exp025_ingest_$protocol" "http://127.0.0.1:$CH_HTTP"
  port=$IN_GRPC; [ "$protocol" = http ] && port=$IN_HTTP
  emit "$protocol" "http://127.0.0.1:$port" "$RESULTS/parity/emit-ingest-$protocol.log"
  # Counted while the service still runs, with no wait: a graceful stop finishes
  # writes in flight, so counting after it would not show rows stored at the 200.
  check "parity.$protocol.ingest_rows_at_ack" "$WANT_ROWS" "$(stored_count "exp025_ingest_$protocol")"
  curl -s "http://127.0.0.1:$IN_HTTP/metrics" >"$RESULTS/parity/metrics-ingest-$protocol.txt"
  stop_bg ingest
  check "parity.$protocol.ingest_rejected_records" 2 \
    "$(awk '/^toudai_ingest_rejected_records_total/ { n += $NF } END { print n + 0 }' "$RESULTS/parity/metrics-ingest-$protocol.txt")"
  check "parity.$protocol.ingest_dropped_reserved" 2 \
    "$(awk '/^toudai_ingest_reserved_attributes_dropped_total/ { n += $NF } END { print n + 0 }' "$RESULTS/parity/metrics-ingest-$protocol.txt")"

  # The dumps stay with the results so the comparison can be rerun from them.
  dump "exp025_go_$protocol" "$RESULTS/parity/dump/go-$protocol"
  dump "exp025_ingest_$protocol" "$RESULTS/parity/dump/ingest-$protocol"
  set +e
  python3 -I "$HERE/compare.py" --go "$RESULTS/parity/dump/go-$protocol" --ingest "$RESULTS/parity/dump/ingest-$protocol" \
    --out "$RESULTS/parity/compare-$protocol.json"
  rc=$?
  set -e
  check "parity.$protocol.unexpected_differences" 0 \
    "$(python3 -I -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["unexpected"]))' "$RESULTS/parity/compare-$protocol.json")"
  check "parity.$protocol.compare_exit" 0 "$rc"
done

# --- failure phases: helpers ----------------------------------------------------
apply_ddl "$FAIL_DB"
start_bg relay "$RESULTS/phases/relay.log" python3 -I "$HERE/relay.py" --listen "$RELAY" --upstream "$CH_HTTP" --control "$CONTROL"
wait_until relay 10 port_busy "$RELAY"
start_fail_ingest() { start_ingest "$1" "$FAIL_DB" "http://127.0.0.1:$RELAY" "${@:2}"; }

record_json() { # record_json LABEL [DUPLICATE_KEY] -> one OTLP/JSON log record
  local attrs="{\"key\":\"exp.key\",\"value\":{\"stringValue\":\"$1\"}}"
  [ -n "${2:-}" ] && attrs="$attrs,{\"key\":\"dup\",\"value\":{\"intValue\":\"1\"}},{\"key\":\"dup\",\"value\":{\"intValue\":\"2\"}}"
  printf '{"timeUnixNano":"1788246000000000000","body":{"stringValue":"%s"},"attributes":[%s]}' "$1" "$attrs"
}
body_json() { # body_json RECORD_JSON...
  local IFS=,
  printf '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"exp025"}}]},"scopeLogs":[{"logRecords":[%s]}]}]}' "$*"
}
send() { # send NAME BODY_FILE [DIR] -> prints "STATUS SECONDS"; keeps the response in DIR
  local dir="${3:-$RESULTS/phases/requests}"
  curl -s -o "$dir/$1.response" -D "$dir/$1.headers" \
    -w '%{http_code} %{time_total}' -H "X-Toudai-Key: $KEY" -H 'X-Toudai-Dataset: exp' \
    -H 'Content-Type: application/json' \
    --max-time 30 --data-binary "@$2" "http://127.0.0.1:$IN_HTTP/v1/logs" || true
}
make_body() { # make_body FILE RECORD_JSON...
  local file="$1"; shift
  body_json "$@" >"$file"
}
serving() { ch "SELECT count() FROM $FAIL_DB.serving_logs WHERE record_string['exp.key'] = '$1'"; }
canonical() { ch "SELECT count() FROM $FAIL_DB.canonical_events WHERE position(payload, '\"exp.key\":{\"t\":\"string\",\"v\":\"$1\"}') > 0"; }
canonical_ids() { ch "SELECT uniqExact(event_id) FROM $FAIL_DB.canonical_events WHERE position(payload, '\"exp.key\":{\"t\":\"string\",\"v\":\"$1\"}') > 0"; }
serving_ids() { ch "SELECT uniqExact(event_id) FROM $FAIL_DB.serving_logs WHERE record_string['exp.key'] = '$1'"; }
rows() { echo "$(canonical "$1")/$(serving "$1")"; }
metric() { # metric NAME LABEL_SUBSTRING -> summed value from /metrics
  curl -s "http://127.0.0.1:$IN_HTTP/metrics" | awk -v n="toudai_ingest_$1_total" -v l="$2" \
    'index($0, n) == 1 && index($0, l) { v += $NF } END { print v + 0 }'
}
inject() { printf '{"match": "%s", "mode": "%s", "count": 1}\n' "$1" "$2" >"$CONTROL"; }
injections_left() { python3 -I -c 'import json,sys; t=open(sys.argv[1]).read().strip(); print(json.loads(t)["count"] if t else 0)' "$CONTROL"; }
seconds_at_least() { awk -v t="$1" -v m="$2" 'BEGIN { exit !(t >= m) }'; }
seconds_below() { awk -v t="$1" -v m="$2" 'BEGIN { exit !(t < m) }'; }
status_of() { echo "${1%% *}"; }
time_of() { echo "${1##* }"; }

start_fail_ingest main

# --- phase: acknowledgement ---------------------------------------------------------
log "phase ack"
visible=0
for i in 1 2 3 4 5; do
  make_body "$ROOT/ack-$i.json" "$(record_json "ack-$i")"
  r="$(send "ack-$i" "$ROOT/ack-$i.json")"
  # Read straight after the response, with no wait: stored means readable now.
  [ "$(status_of "$r")" = 200 ] && [ "$(rows "ack-$i")" = 1/1 ] && visible=$((visible + 1))
  echo "ack-$i $r" >>"$RESULTS/phases/ack-times.txt"
done
check ack.visible_at_200 5 "$visible"

# --- phase: valid and invalid records in one request ---------------------------------
log "phase mixed-validation"
rejected_before="$(metric rejected_records 'signal="logs"')"
make_body "$ROOT/mixed.json" "$(record_json mixed-valid)" "$(record_json mixed-invalid dup)"
r="$(send mixed "$ROOT/mixed.json")"
check mixed.status 200 "$(status_of "$r")"
check mixed.partial_success_rejected 1 \
  "$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1])).get("partialSuccess", {}).get("rejectedLogRecords"))' "$RESULTS/phases/requests/mixed.response")"
check mixed.valid_rows 1/1 "$(rows mixed-valid)"
check mixed.invalid_rows 0/0 "$(rows mixed-invalid)"
check mixed.rejected_counter_delta 1 "$(( $(metric rejected_records 'signal="logs"') - rejected_before ))"

# --- phase: producer resend of identical bytes ---------------------------------------
log "phase retry-duplicate"
make_body "$ROOT/resend.json" "$(record_json resend)"
check resend.first "200" "$(status_of "$(send resend-1 "$ROOT/resend.json")")"
check resend.second "200" "$(status_of "$(send resend-2 "$ROOT/resend.json")")"
check resend.rows 2/2 "$(rows resend)"
check resend.distinct_event_ids 2 "$(canonical_ids resend)"

# --- phase: ClickHouse stopped -------------------------------------------------------
log "phase outage"
failures_before="$(metric insert_failures 'signal="logs"')"
stop_bg clickhouse
make_body "$ROOT/outage.json" "$(record_json outage)"
r="$(send outage-down "$ROOT/outage.json")"
check outage.status_while_down 503 "$(status_of "$r")"
check_true outage.answered_within_budget "under 6 s (5 s budget)" "$(holds seconds_below "$(time_of "$r")" 6)" "$(time_of "$r") s"
check outage.healthz_while_down 200 "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$IN_HTTP/healthz")"
curl -s "http://127.0.0.1:$IN_HTTP/metrics" >"$RESULTS/phases/metrics-outage.txt"
failures_after="$(metric insert_failures 'signal="logs"')"
check_true outage.failures_visible_while_down "insert_failures increased" "$(holds [ "$failures_after" -gt "$failures_before" ])" "$failures_before -> $failures_after"
start_clickhouse
check outage.rows_after_restart_before_resend 0/0 "$(rows outage)"
r="$(send outage-resend "$ROOT/outage.json")"
check outage.resend_status 200 "$(status_of "$r")"
check outage.rows_after_resend 1/1 "$(rows outage)"

# --- phase: canonical destination permanently broken ----------------------------------
log "phase canonical-permanent"
ch "ALTER TABLE $FAIL_DB.canonical_events RENAME COLUMN payload TO payload_moved"
make_body "$ROOT/permanent.json" "$(record_json permanent)"
r="$(send permanent "$ROOT/permanent.json")"
check permanent.status 500 "$(status_of "$r")"
check_true permanent.answered_without_retrying "under 2 s" "$(holds seconds_below "$(time_of "$r")" 2)" "$(time_of "$r") s"
check permanent.serving_rows 0 "$(serving permanent)"
check permanent.canonical_rows 0 "$(ch "SELECT count() FROM $FAIL_DB.canonical_events WHERE position(payload_moved, 'permanent') > 0")"
ch "ALTER TABLE $FAIL_DB.canonical_events RENAME COLUMN payload_moved TO payload"
check permanent.resend_after_repair "200" "$(status_of "$(send permanent-resend "$ROOT/permanent.json")")"
check permanent.rows_after_repair 1/1 "$(rows permanent)"

# --- phase: split write (serving destination missing) -----------------------------------
log "phase split"
attempts_before="$(metric insert_failures 'destination="serving"')"
ch "RENAME TABLE $FAIL_DB.serving_logs TO $FAIL_DB.serving_logs_away"
make_body "$ROOT/split.json" "$(record_json split)"
r="$(send split "$ROOT/split.json")"
check split.status 503 "$(status_of "$r")"
serving_attempts=$(( $(metric insert_failures 'destination="serving"') - attempts_before ))
check_true split.serving_retried "at least 2 failed serving attempts" "$(holds [ "$serving_attempts" -ge 2 ])" "$serving_attempts"
check split.canonical_rows 1 "$(canonical split)"
check split.serving_rows_in_moved_table 0 "$(ch "SELECT count() FROM $FAIL_DB.serving_logs_away WHERE record_string['exp.key'] = 'split'")"
ch "RENAME TABLE $FAIL_DB.serving_logs_away TO $FAIL_DB.serving_logs"
check split.resend_status 200 "$(status_of "$(send split-resend "$ROOT/split.json")")"
check split.rows_after_resend 2/1 "$(rows split)"
check split.orphan_canonical_rows 1 "$(ch "SELECT count() FROM $FAIL_DB.canonical_events WHERE position(payload, '\"v\":\"split\"}') > 0 AND event_id NOT IN (SELECT event_id FROM $FAIL_DB.serving_logs)")"

# --- phase: ambiguous inserts ----------------------------------------------------------
# The relay lets the INSERT commit, reads ClickHouse's answer, and then either
# resets the client's connection or withholds the answer (see relay.py).
ambiguous() { # ambiguous NAME TABLE MODE EXPECT_STATUS EXPECT_ROWS EXPECT_IDS
  local name="$1"
  log "phase $name"
  make_body "$ROOT/$name.json" "$(record_json "$name")"
  inject "$2" "$3"
  local r; r="$(send "$name" "$ROOT/$name.json")"
  check "$name.injected" 0 "$(injections_left)"
  : >"$CONTROL"
  check "$name.status" "$4" "$(status_of "$r")"
  echo "$name $r" >>"$RESULTS/phases/ambiguous-times.txt"
  check "$name.rows" "$5" "$(rows "$name")"
  check "$name.distinct_event_ids" "$6" "$(ch "SELECT uniqExact(event_id) FROM (SELECT event_id FROM $FAIL_DB.canonical_events WHERE position(payload, '\"v\":\"$name\"}') > 0 UNION ALL SELECT event_id FROM $FAIL_DB.serving_logs WHERE record_string['exp.key'] = '$name')")"
}
ambiguous ambiguous-canonical-reset canonical_events reset 200 2/1 1
ambiguous ambiguous-serving-reset serving_logs reset 200 1/2 1
ambiguous ambiguous-canonical-withhold canonical_events withhold 503 1/0 1
ambiguous ambiguous-serving-withhold serving_logs withhold 503 1/1 1
# The producer resends after the 503; the new attempt gets a new event_id.
check ambiguous-serving-withhold.resend_status 200 "$(status_of "$(send ambiguous-serving-withhold-resend "$ROOT/ambiguous-serving-withhold.json")")"
check ambiguous-serving-withhold.rows_after_resend 2/2 "$(rows ambiguous-serving-withhold)"
check ambiguous-serving-withhold.ids_after_resend 2 "$(serving_ids ambiguous-serving-withhold)"
stop_bg ingest

# --- phase: stalled destination, small admission budget ---------------------------------
log "phase stall"
start_bg stall "$RESULTS/phases/stall-listener.log" python3 -I "$REPO/rfd/0001/experiments/021-hybrid-layout-collector-integration/harness/stalled-listener.py" --listen "$STALL"
wait_until stall 10 port_busy "$STALL"
start_ingest stall "$FAIL_DB" "http://127.0.0.1:$STALL" TOUDAI_INGEST_MEMORY_BUDGET=20MiB
# 1200 records charge about 4.7 MiB each at 4 KiB per record plus the body, so
# the 20 MiB budget admits four such requests and refuses the rest.
python3 -I -c '
import json, sys
records = [{"timeUnixNano": "1788246000000000000", "attributes": [{"key": "exp.key", "value": {"stringValue": "stall"}}]}] * 1200
print(json.dumps({"resourceLogs": [{"scopeLogs": [{"logRecords": records}]}]}))' >"$ROOT/stall.json"
senders=()
for i in 1 2 3 4 5 6 7 8; do
  ( send "stall-$i" "$ROOT/stall.json" >"$ROOT/stall-$i.result" ) &
  senders+=($!)
done
wait "${senders[@]}"
admitted=0 refused=0 other=0 refused_fast=1 admitted_slow=1 retry_after=1
for i in 1 2 3 4 5 6 7 8; do
  r="$(cat "$ROOT/stall-$i.result")"; echo "stall-$i $r" >>"$RESULTS/phases/stall-results.txt"
  case "$(status_of "$r")" in
    429) refused=$((refused + 1)); seconds_below "$(time_of "$r")" 1 || refused_fast=0
         grep -qi '^retry-after: 1' "$RESULTS/phases/requests/stall-$i.headers" || retry_after=0 ;;
    503) admitted=$((admitted + 1)); seconds_at_least "$(time_of "$r")" 4.5 || admitted_slow=0 ;;
    *) other=$((other + 1)) ;;
  esac
done
check stall.other_statuses 0 "$other"
check_true stall.some_refused_some_admitted "both 429s and 503s" "$(holds [ "$refused" -ge 1 -a "$admitted" -ge 1 ])" "$refused x 429, $admitted x 503"
check stall.refusals_immediate 1 "$refused_fast"
check stall.refusals_carry_retry_after 1 "$retry_after"
check stall.admitted_answered_at_budget 1 "$admitted_slow"
# Once the admitted requests are answered, their share is back: a lone request
# is admitted again (and times out against the stalled destination).
check stall.budget_returned 503 "$(status_of "$(send stall-after "$ROOT/stall.json")")"
stop_bg ingest
stop_bg stall
check stall.rows 0/0 "$(rows stall)"

# --- phase: killed process under load -----------------------------------------------------
load() { # load PREFIX COUNT -> background senders, one request every 40 ms
  local i
  for i in $(seq 1 "$2"); do
    make_body "$ROOT/$1-$i.json" "$(record_json "$1-$i")"
    ( send "$1-$i" "$ROOT/$1-$i.json" "$ROOT/load" >"$ROOT/$1-$i.result" ) &
    sleep 0.04
  done
  wait
}
tally() { # tally PREFIX COUNT -> "acked acked_lost unacked unacked_stored unacked_partial error_statuses"
  local i acked=0 lost=0 unacked=0 stored=0 partial=0 errors=0 r s
  for i in $(seq 1 "$2"); do
    r="$(cat "$ROOT/$1-$i.result")"; s="$(status_of "$r")"; echo "$1-$i $r $(rows "$1-$i")" >>"$RESULTS/phases/$1-results.txt"
    if [ "$s" = 200 ]; then
      acked=$((acked + 1)); [ "$(rows "$1-$i")" = 1/1 ] || lost=$((lost + 1))
    else
      unacked=$((unacked + 1)); [ "$s" = 000 ] || errors=$((errors + 1))
      case "$(rows "$1-$i")" in 1/1) stored=$((stored + 1)) ;; 0/0) ;; *) partial=$((partial + 1)) ;; esac
    fi
  done
  echo "$acked $lost $unacked $stored $partial $errors"
}

log "phase crash"
start_fail_ingest crash
load crash 60 &
loader=$!
sleep 1.5
stop_bg ingest KILL
wait "$loader"
read -r acked lost unacked stored partial errors < <(tally crash 60)
check crash.acknowledged_but_lost 0 "$lost"
check_true crash.kill_landed_mid_load "some answered, some not" "$(holds [ "$acked" -ge 1 -a "$unacked" -ge 1 ])" "$acked answered 200, $unacked not"
echo "acked=$acked unacked=$unacked unacked_stored=$stored unacked_partial=$partial unacked_error_status=$errors" >"$RESULTS/phases/crash-summary.txt"

log "phase graceful-stop"
start_fail_ingest term
load term 20 &
loader=$!
sleep 0.4
stop_bg ingest TERM
wait "$loader"
read -r acked lost unacked stored partial errors < <(tally term 20)
check term.acknowledged_but_lost 0 "$lost"
check term.error_statuses 0 "$errors"
# An unanswered request cut off by the stop would also show as 000; only a
# refusal at connect answers in well under 10 ms.
check term.unanswered_cut_off 0 "$(awk '$2 == "000" && $3 >= 0.01 { n++ } END { print n + 0 }' "$RESULTS/phases/term-results.txt")"
check_true term.in_flight_answered "at least one 200" "$(holds [ "$acked" -ge 1 ])" "$acked answered 200, $unacked refused at connect"
echo "acked=$acked unacked=$unacked unacked_stored=$stored unacked_partial=$partial" >"$RESULTS/phases/term-summary.txt"

# --- credentials and summary ----------------------------------------------------------------
cp "$ROOT"/ingest-*.log "$ROOT"/collector-*.log "$RESULTS/phases/" 2>/dev/null || true
leaks="$({ grep -rl "$KEY" "$RESULTS/phases" "$RESULTS/parity" 2>/dev/null || true; } | wc -l)"
check credentials.key_in_logs_and_responses 0 "$leaks"
in_tables=0
for db in $(ch "SHOW DATABASES" | grep '^exp025_'); do
  for table in $(ch "SHOW TABLES FROM $db"); do
    in_tables=$((in_tables + $(ch "SELECT count() FROM $db.$table WHERE position(toString(tuple(*)), '$KEY') > 0")))
  done
done
check credentials.key_in_tables 0 "$in_tables"

total=$(($(wc -l <"$RESULTS/checks.tsv") - 1))
printf 'checks: %s, failed: %s\n' "$total" "$FAILED" | tee "$RESULTS/summary.txt"
[ "$FAILED" = 0 ]
