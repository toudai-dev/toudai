#!/usr/bin/env bash
# Experiment 024: key-derived trusted scope and gateway-assigned event IDs
# through the product gateway config of toudai-dev/collector, against a
# disposable local ClickHouse 26.8.2.7-lts.
#
# Run from a checkout of toudai-dev/collector, inside its Nix dev shell (which
# provides go and the pinned ClickHouse):
#
#   cd <collector> && nix develop --command bash <this dir>/run.sh
#
# The Collector runs config/gateway.example.yaml unmodified; only environment
# variables (ports, key file, ClickHouse endpoint) are set. Expected values below
# are written from the experiment's design, not read from the observed rows.
# Exit status is nonzero if any check fails.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COLLECTOR_REPO="${COLLECTOR_REPO:-$PWD}"
ROOT="${EXP024_ROOT:-$(mktemp -d)}"
RESULTS="${EXP024_RESULTS:-$HERE/results}"
CH_TCP=19222
HTTP_PORT=14318
GRPC_PORT=14317
BASE="http://127.0.0.1:$HTTP_PORT"
mkdir -p "$ROOT"/{ch/data,ch/tmp,ch/user_files,ch/format_schemas,resp} "$RESULTS"

CH_PID="" COL_PID=""
cleanup() {
  [ -n "$COL_PID" ] && kill "$COL_PID" 2>/dev/null || true
  [ -n "$CH_PID" ] && kill "$CH_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

ch() { clickhouse-client --port "$CH_TCP" --query "$1"; }

# Everything below runs against services this script starts. A successful
# connection alone cannot prove that, because an unrelated listener on a fixed port
# would answer too, so the ports must be free before launch and the children must
# still be alive once they answer.
port_busy() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
for p in "$CH_TCP" 18322 "$HTTP_PORT" "$GRPC_PORT"; do
  if port_busy "$p"; then echo "port $p is already in use; refusing to run against a service this script did not start" >&2; exit 2; fi
done

# --- build and start -------------------------------------------------------
REV="$(git -C "$COLLECTOR_REPO" rev-parse HEAD)"
DIRTY="$(git -C "$COLLECTOR_REPO" status --porcelain | wc -l)"
echo "collector revision: $REV (uncommitted paths: $DIRTY)"
(cd "$COLLECTOR_REPO" && go build -o "$ROOT/toudai-collector" .)

export EXP024_CH_PATH="$ROOT/ch/data/" EXP024_CH_TMP="$ROOT/ch/tmp/" \
  EXP024_CH_USER_FILES="$ROOT/ch/user_files/" EXP024_CH_FORMAT_SCHEMAS="$ROOT/ch/format_schemas/" \
  EXP024_CH_LOG="$ROOT/ch.log" EXP024_CH_ERRORLOG="$ROOT/ch.err.log" EXP024_CH_USERS="$HERE/clickhouse-users.xml"
clickhouse-server --config-file="$HERE/clickhouse-config.xml" >"$ROOT/ch.stdout" 2>&1 &
CH_PID=$!
ready=""
for _ in $(seq 1 150); do
  kill -0 "$CH_PID" 2>/dev/null || { echo "clickhouse exited during startup" >&2; cat "$ROOT/ch.stdout" >&2; exit 1; }
  ch 'SELECT 1' >/dev/null 2>&1 && { ready=1; break; }
  sleep 0.2
done
[ -n "$ready" ] && kill -0 "$CH_PID" 2>/dev/null || { echo "clickhouse did not become ready" >&2; exit 1; }
ch 'SELECT version()' >"$RESULTS/clickhouse-version.txt"

# The vendored DDL targets the S3-backed payload policy; locally the TTL move and
# storage policy are dropped, which changes where parts live and nothing else.
for f in "$COLLECTOR_REPO"/testdata/schema/*.sql; do
  perl -0pe "s/\nTTL[^\n]*\nSETTINGS storage_policy = 'toudai_payload';/;/" "$f" | clickhouse-client --port "$CH_TCP" --multiquery
done

export TOUDAI_KEYS_FILE="$HERE/keys.yaml" TOUDAI_CLICKHOUSE_ENDPOINT="127.0.0.1:$CH_TCP" \
  TOUDAI_OTLP_HTTP_ENDPOINT="127.0.0.1:$HTTP_PORT" TOUDAI_OTLP_GRPC_ENDPOINT="127.0.0.1:$GRPC_PORT"
(cd "$COLLECTOR_REPO" && exec "$ROOT/toudai-collector" --config config/gateway.example.yaml) >"$ROOT/collector.log" 2>&1 &
COL_PID=$!
ready=""
for _ in $(seq 1 150); do
  kill -0 "$COL_PID" 2>/dev/null || { echo "collector exited during startup" >&2; cat "$ROOT/collector.log" >&2; exit 1; }
  curl -s -o /dev/null "$BASE/v1/logs" 2>/dev/null && { ready=1; break; }
  sleep 0.2
done
[ -n "$ready" ] && kill -0 "$COL_PID" 2>/dev/null || { echo "collector did not become ready" >&2; exit 1; }

# --- fixture ---------------------------------------------------------------
WSA=11111111-1111-4111-8111-111111111111
WSB=22222222-2222-4222-8222-222222222222
VICTIM=99999999-9999-4999-8999-999999999999
KEY_A_PROD=tk_exp024_SECRETMARKER_wsa_prod_000000
KEY_A_MULTI=tk_exp024_SECRETMARKER_wsa_multi_00000
KEY_B_PROD=tk_exp024_SECRETMARKER_wsb_prod_000000
KEY_A_RO=tk_exp024_SECRETMARKER_wsa_readonly_00
FORGED_ID=00000000-0000-7000-8000-000000000000
NO_ATTRS='[]'

# logs_json EXTRA_RESOURCE_ATTRS RECORD_ATTRS BODY...: one record per body, identical
# timestamps, so any difference between stored rows is the assigned identity.
logs_json() {
  local extra="$1" rattrs="$2"; shift 2
  jq -nc --argjson extra "$extra" --argjson rattrs "$rattrs" '
    {resourceLogs:[{resource:{attributes:([{key:"service.name",value:{stringValue:"exp024"}}]+$extra)},
     scopeLogs:[{scope:{name:"exp024"},logRecords:[$ARGS.positional[] | {timeUnixNano:"1788246000123456789",
     observedTimeUnixNano:"1788246000123456790",severityText:"INFO",body:{stringValue:.},attributes:$rattrs}]}]}]}' --args "$@"
}
traces_json() {
  jq -nc '{resourceSpans:[{resource:{attributes:[{key:"service.name",value:{stringValue:"exp024"}}]},
    scopeSpans:[{scope:{name:"exp024"},spans:[range(2) | {traceId:"0102030405060708090a0b0c0d0e0f10",spanId:"0102030405060708",
    name:"dup-span",kind:2,startTimeUnixNano:"1788246000000000000",endTimeUnixNano:"1788246000500000000"}]}]}]}'
}

: >"$RESULTS/requests.tsv"
printf 'id\tendpoint\tdescription\texpected_http\tobserved_http\n' >"$RESULTS/requests.tsv"
FAILS=0
: >"$RESULTS/checks.tsv"
printf 'id\tdescription\texpected\tobserved\tresult\n' >"$RESULTS/checks.tsv"

# send ID ENDPOINT EXPECTED_STATUS DESCRIPTION BODY [curl header args...]
send() {
  local id="$1" ep="$2" want="$3" desc="$4" body="$5"; shift 5
  local got
  got="$(curl -s -o "$ROOT/resp/$id.txt" -w '%{http_code}' -X POST -H 'Content-Type: application/json' "$@" --data "$body" "$BASE$ep")"
  printf '%s\t%s\t%s\t%s\t%s\n' "$id" "$ep" "$desc" "$want" "$got" >>"$RESULTS/requests.tsv"
  check "http-$id" "$desc -> HTTP status" "$want" "$got"
}
flat() { local v="${1//$'\t'/ }"; printf '%s' "${v//$'\n'/ | }"; }
check() {
  local id="$1" desc="$2" want="$3" got="$4" res=PASS
  [ "$want" = "$got" ] || { res=FAIL; FAILS=$((FAILS + 1)); }
  # Query results contain tabs and newlines; flatten them so every check is
  # exactly one five-column row.
  printf '%s\t%s\t%s\t%s\t%s\n' "$id" "$desc" "$(flat "$want")" "$(flat "$got")" "$res" >>"$RESULTS/checks.tsv"
}

# --- accepted traffic: sent first and back to back so one queue batch (1 s flush
# window, 1000-item trigger in the example config) can carry both workspaces ----
send a1 /v1/logs   200 "ws A prod: two identical records"            "$(logs_json "$NO_ATTRS" "$NO_ATTRS" dup-body dup-body)" -H "X-Toudai-Key: $KEY_A_PROD"
send a2 /v1/logs   200 "ws A prod: manual resubmission of a1, same bytes (not an SDK retry)"  "$(logs_json "$NO_ATTRS" "$NO_ATTRS" dup-body dup-body)" -H "X-Toudai-Key: $KEY_A_PROD"
send a3 /v1/traces 200 "ws B prod: two identical spans"              "$(traces_json)" -H "X-Toudai-Key: $KEY_B_PROD"
send a4 /v1/logs   200 "ws A multi key, dataset staging"             "$(logs_json "$NO_ATTRS" "$NO_ATTRS" staging-body)" -H "X-Toudai-Key: $KEY_A_MULTI" -H 'X-Toudai-Dataset: staging'
send a5 /v1/logs   200 "ws B prod"                                   "$(logs_json "$NO_ATTRS" "$NO_ATTRS" b-body)" -H "X-Toudai-Key: $KEY_B_PROD"
FORGED_RECORD="[{\"key\":\"toudai.workspace_id\",\"value\":{\"stringValue\":\"$VICTIM\"}},{\"key\":\"toudai.event_id\",\"value\":{\"stringValue\":\"$FORGED_ID\"}},{\"key\":\"toudai.scope_source\",\"value\":{\"stringValue\":\"gateway\"}}]"
send a6 /v1/logs   200 "ws A prod: record-level toudai.* attributes"  "$(logs_json "$NO_ATTRS" "$FORGED_RECORD" record-forge-body)" -H "X-Toudai-Key: $KEY_A_PROD"

# --- refused traffic: bodies carry FORGED-REJECT so a leak is greppable --------
res_attr() { printf '[{"key":"%s","value":{"stringValue":"%s"}}]' "$1" "$2"; }
send r1 /v1/logs 403 "unknown dataset on a multi-dataset key"            "$(logs_json "$NO_ATTRS" "$NO_ATTRS" FORGED-REJECT-1)" -H "X-Toudai-Key: $KEY_A_MULTI" -H 'X-Toudai-Dataset: nope'
send r2 /v1/logs 403 "ws B key naming ws A's staging dataset"            "$(logs_json "$NO_ATTRS" "$NO_ATTRS" FORGED-REJECT-2)" -H "X-Toudai-Key: $KEY_B_PROD" -H 'X-Toudai-Dataset: staging'
send r3 /v1/logs 400 "multi-dataset key without a dataset"               "$(logs_json "$NO_ATTRS" "$NO_ATTRS" FORGED-REJECT-3)" -H "X-Toudai-Key: $KEY_A_MULTI"
send r4 /v1/logs 400 "override: resource toudai.workspace_id"            "$(logs_json "$(res_attr toudai.workspace_id $VICTIM)" "$NO_ATTRS" FORGED-REJECT-4)" -H "X-Toudai-Key: $KEY_A_PROD"
send r5 /v1/logs 400 "override: resource toudai.dataset"                 "$(logs_json "$(res_attr toudai.dataset staging)" "$NO_ATTRS" FORGED-REJECT-5)" -H "X-Toudai-Key: $KEY_A_PROD"
send r6 /v1/logs 400 "override: forged toudai.scope_source marker"       "$(logs_json "$(res_attr toudai.scope_source gateway)" "$NO_ATTRS" FORGED-REJECT-6)" -H "X-Toudai-Key: $KEY_A_PROD"
send r7 /v1/logs 401 "no key"                                            "$(logs_json "$NO_ATTRS" "$NO_ATTRS" FORGED-REJECT-7)"
send r8 /v1/logs 401 "unknown key"                                       "$(logs_json "$NO_ATTRS" "$NO_ATTRS" FORGED-REJECT-8)" -H 'X-Toudai-Key: tk_exp024_SECRETMARKER_not_a_key_0000'
send r9 /v1/logs 401 "read-only key"                                     "$(logs_json "$NO_ATTRS" "$NO_ATTRS" FORGED-REJECT-9)" -H "X-Toudai-Key: $KEY_A_RO"
send r10 /v1/logs 401 "valid key in Authorization instead of the key header" "$(logs_json "$NO_ATTRS" "$NO_ATTRS" FORGED-REJECT-10)" -H "Authorization: Bearer $KEY_A_PROD"
send r11 /v1/traces 401 "no key (traces)"                                "$(traces_json)"

# --- wait for the export (flush window 1 s), then read back ---------------------
for _ in $(seq 1 100); do
  n="$(ch 'SELECT (SELECT count() FROM toudai.canonical_events) + (SELECT count() FROM toudai.serving_logs) + (SELECT count() FROM toudai.serving_spans)')"
  [ "$n" -ge 18 ] && break
  sleep 0.2
done
ch 'SYSTEM FLUSH LOGS'

SL=toudai.serving_logs SS=toudai.serving_spans CE=toudai.canonical_events
ch "SELECT tenant_id, dataset, event_id, body, resource['toudai.workspace_id'], resource['toudai.dataset'] FROM $SL ORDER BY timestamp, event_id" >"$RESULTS/serving_logs.tsv"
ch "SELECT tenant_id, dataset, event_id, span_name FROM $SS ORDER BY event_id" >"$RESULTS/serving_spans.tsv"
ch "SELECT tenant_id, dataset, signal, event_id, JSONExtractString(payload,'identity','event_id') FROM $CE ORDER BY signal, event_id" >"$RESULTS/canonical_events.tsv"

# --- scope: key-derived workspace/dataset on every stored row -------------------
check s-logs-by-scope "serving_logs rows by workspace/dataset/body" \
  "$(printf '%s\n' "$WSA	prod	dup-body	4" "$WSA	prod	record-forge-body	1" "$WSA	staging	staging-body	1" "$WSB	prod	b-body	1")" \
  "$(ch "SELECT tenant_id, dataset, body, count() FROM $SL GROUP BY 1,2,3 ORDER BY 1,2,3" | sort)"
check s-spans-by-scope "serving_spans rows by workspace/dataset" "$WSB	prod	2" \
  "$(ch "SELECT tenant_id, dataset, count() FROM $SS GROUP BY 1,2")"
check s-canonical-by-scope "canonical_events rows by workspace/dataset/signal" \
  "$(printf '%s\n' "$WSA	prod	logs	5" "$WSA	staging	logs	1" "$WSB	prod	logs	1" "$WSB	prod	spans	2")" \
  "$(ch "SELECT tenant_id, dataset, signal, count() FROM $CE GROUP BY 1,2,3 ORDER BY 1,2,3" | sort)"
check s-resource-matches-column "serving_logs rows whose stored resource scope differs from the tenant_id/dataset columns" 0 \
  "$(ch "SELECT countIf(resource['toudai.workspace_id'] != tenant_id OR resource['toudai.dataset'] != dataset) FROM $SL")"

# --- refused requests wrote nothing ---------------------------------------------
dump() { for t in "$SL" "$SS" "$CE"; do ch "SELECT * FROM $t FORMAT TSVRaw"; done; }
dump >"$ROOT/all-rows.txt"
check n-rejected-bodies "stored rows containing a refused request's body marker" 0 "$(grep -c 'FORGED-REJECT' "$ROOT/all-rows.txt" || true)"
check n-victim "stored rows mentioning the forged victim workspace" 0 "$(grep -c '99999999-9999' "$ROOT/all-rows.txt" || true)"
check n-forged-id "stored rows containing the forged record event_id" 0 "$(grep -c "$FORGED_ID" "$ROOT/all-rows.txt" || true)"
check n-record-keys "stored rows containing the record-level toudai.event_id or toudai.scope_source keys" 0 \
  "$(grep -c -e 'toudai.event_id' -e 'toudai.scope_source' "$ROOT/all-rows.txt" || true)"

# --- identity ---------------------------------------------------------------------
UUID7='^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
check i-uuidv7 "canonical rows with a canonical-form UUIDv7 event_id / all rows" "9	9" \
  "$(ch "SELECT countIf(match(event_id, '$UUID7')), count() FROM $CE")"
check i-unique "distinct event_ids / canonical rows (no ID shared, retries and identical events included)" "9	9" \
  "$(ch "SELECT uniqExact(event_id), count() FROM $CE")"
check i-identical-content "dup-body rows / distinct event_ids / distinct content (same bytes sent four times)" "4	4	1" \
  "$(ch "SELECT count(), uniqExact(event_id), uniqExact(body, timestamp, observed_ts, severity_text, record_string) FROM $SL WHERE body = 'dup-body'")"
check i-identical-spans "identical spans: rows / distinct event_ids / distinct content" "2	2	1" \
  "$(ch "SELECT count(), uniqExact(event_id), uniqExact(span_name, timestamp, end_ts, trace_id, span_id) FROM $SS")"
check i-join-logs "serving_logs rows with exactly one canonical row sharing tenant, dataset, event_id, payload hash and payload identity" 7 \
  "$(ch "SELECT count() FROM $SL s INNER JOIN $CE c ON s.tenant_id=c.tenant_id AND s.dataset=c.dataset AND s.event_id=c.event_id WHERE c.signal='logs' AND s.payload_sha256=c.payload_sha256 AND JSONExtractString(c.payload,'identity','event_id')=s.event_id AND JSONExtractString(c.payload,'identity','tenant_id')=s.tenant_id")"
check i-join-spans "serving_spans rows with exactly one matching canonical row" 2 \
  "$(ch "SELECT count() FROM $SS s INNER JOIN $CE c ON s.tenant_id=c.tenant_id AND s.dataset=c.dataset AND s.event_id=c.event_id WHERE c.signal='spans' AND s.payload_sha256=c.payload_sha256 AND JSONExtractString(c.payload,'identity','event_id')=s.event_id")"

# --- rebatching: IDs are assigned before the queue batch, so they must survive it --
check b-one-insert-per-table "finished INSERTs into serving_logs / rows written by the largest" "1	7" \
  "$(ch "SELECT count(), max(written_rows) FROM system.query_log WHERE type='QueryFinish' AND query_kind='Insert' AND has(tables,'toudai.serving_logs')")"
check b-cross-workspace "workspaces among the rows that one insert wrote" 2 "$(ch "SELECT uniqExact(tenant_id) FROM $SL")"

# --- credentials ---------------------------------------------------------------------
check c-stored "stored rows containing SECRETMARKER" 0 "$(grep -c 'SECRETMARKER' "$ROOT/all-rows.txt" || true)"
check c-query-log "query_log statements containing SECRETMARKER" 0 "$(ch "SELECT count() FROM system.query_log WHERE position(query,'SECRETMARKER') > 0")"
check c-collector-log "Collector log lines containing SECRETMARKER" 0 "$(grep -c 'SECRETMARKER' "$ROOT/collector.log" || true)"
check c-responses "producer-visible response bodies containing SECRETMARKER" 0 "$(cat "$ROOT"/resp/*.txt | grep -c 'SECRETMARKER' || true)"

cp "$ROOT/collector.log" "$RESULTS/collector.log"
{
  echo "collector_revision=$REV"
  echo "collector_uncommitted_paths=$DIRTY"
  echo "clickhouse=$(cat "$RESULTS/clickhouse-version.txt")"
  echo "checks=$(($(wc -l <"$RESULTS/checks.tsv") - 1)) failed=$FAILS"
} >"$RESULTS/summary.txt"
cat "$RESULTS/summary.txt"
awk -F'\t' '$5 == "FAIL"' "$RESULTS/checks.tsv"
[ "$FAILS" -eq 0 ]
