#!/usr/bin/env bash
# Experiment 021 harness self-test: a run must fail closed.
#
# Usage:
#   test-run-failure-modes.sh fast   (default) synthetic aggregation checks and
#                                    verifier failure sensitivity; no collector run
#   test-run-failure-modes.sh full   fast checks plus an injected verifier crash
#                                    and a normal full run
#
# The fast mode drives run.sh's summarize() through `check-results` with
# synthetic results and covers: an all-pass set, a missing phase, an empty phase
# file, a nonzero phase exit status, a failed check line, a missing status entry,
# and a stale checks file. It also proves the fail-closed probe configuration
# cannot inject a trusted scope, that the verifier reports failures (not passes)
# against an empty database, that an unknown phase is rejected, that the
# trusted-scope configuration scan sees every workspace pipeline, and that a
# stale injector holding a phase port fails the phase instead of being reused.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH021_ROOT:-/home/user/lh021}"
PY="$ROOT/venv/bin/python"
MODE="${1:-fast}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
# Keep bytecode caches out of the tracked tree: this script imports the harness
# modules directly for the fixture regression check.
export PYTHONDONTWRITEBYTECODE=1
PASS=0
FAIL=0

ok() { printf '[PASS] %s\n' "$*"; PASS=$((PASS + 1)); }
no() { printf '[FAIL] %s\n' "$*"; FAIL=$((FAIL + 1)); }

new_results() { # new_results DIR
  local dir="$1"
  rm -rf "$dir"
  mkdir -p "$dir"
  date -u +"%Y-%m-%dT%H:%M:%SZ" >"$dir/run-start.txt"
  : >"$dir/phase-status.tsv"
}

write_checks() { # write_checks FILE OK_COUNT FAIL_COUNT
  local file="$1" okc="$2" badc="$3" i
  : >"$file"
  for ((i = 0; i < okc; i++)); do
    printf '{"phase":"x","check":"ok.%d","ok":true,"detail":""}\n' "$i" >>"$file"
  done
  for ((i = 0; i < badc; i++)); do
    printf '{"phase":"x","check":"bad.%d","ok":false,"detail":"injected"}\n' "$i" >>"$file"
  done
}

set_status() { # set_status DIR PHASE STATUS
  printf '%s\t%s\n' "$2" "$3" >>"$1/phase-status.tsv"
}

expect() { # expect NAME EXPECTED(zero|nonzero) DIR PHASES...
  local name="$1" want="$2" dir="$3"
  shift 3
  local rc=0
  set +e
  # The synthetic root only needs the interpreter the harness uses to aggregate
  # checks; everything else in check-results is pure bash. Without this the
  # aggregation would fail with exit 127 for the wrong reason.
  mkdir -p "$WORK/root"
  [[ -e "$WORK/root/venv" ]] || ln -s "$ROOT/venv" "$WORK/root/venv"
  LH021_ROOT="$WORK/root" LH021_RESULTS="$dir" "$HERE/run.sh" check-results "$@" \
    >"$dir/summary.log" 2>&1
  rc=$?
  set -e
  if { [[ "$want" == zero && $rc -eq 0 ]] || [[ "$want" == nonzero && $rc -ne 0 ]]; }; then
    ok "$name (exit $rc)"
  else
    no "$name (exit $rc, expected $want)"
    sed 's/^/    /' "$dir/summary.log"
  fi
}

# 1. A complete, all-pass result set passes.
new_results "$WORK/case1"
write_checks "$WORK/case1/checks-a.jsonl" 3 0
set_status "$WORK/case1" a 0
expect "complete all-pass results pass" zero "$WORK/case1" a

# 2. A missing phase is a coverage failure.
new_results "$WORK/case2"
write_checks "$WORK/case2/checks-a.jsonl" 3 0
set_status "$WORK/case2" a 0
expect "missing phase fails" nonzero "$WORK/case2" a b

# 3. An empty phase file is a coverage failure.
new_results "$WORK/case3"
write_checks "$WORK/case3/checks-b.jsonl" 0 0
set_status "$WORK/case3" b 0
expect "empty phase output fails" nonzero "$WORK/case3" b

# 4. A nonzero phase exit status is a failure even with a plausible file.
new_results "$WORK/case4"
write_checks "$WORK/case4/checks-c.jsonl" 2 0
set_status "$WORK/case4" c 1
expect "nonzero phase exit fails" nonzero "$WORK/case4" c

# 5. A failed check line is a failure.
new_results "$WORK/case5"
write_checks "$WORK/case5/checks-d.jsonl" 2 1
set_status "$WORK/case5" d 0
expect "failed check line fails" nonzero "$WORK/case5" d

# 6. A checks file with no recorded status for this run is a failure.
new_results "$WORK/case6"
write_checks "$WORK/case6/checks-e.jsonl" 1 0
expect "missing phase status fails" nonzero "$WORK/case6" e

# 7. A checks file older than the run marker cannot satisfy the run.
new_results "$WORK/case7"
write_checks "$WORK/case7/checks-f.jsonl" 1 0
set_status "$WORK/case7" f 0
touch -d '10 seconds ago' "$WORK/case7/checks-f.jsonl"
expect "stale checks file fails" nonzero "$WORK/case7" f

# 8. The fail-closed probe really has no trusted-scope injection.
if grep -q "lighthouse.workspace_id" "$HERE/collector-no-trust.yaml"; then
  no "collector-no-trust.yaml injects a trusted workspace; the probe would not fail closed"
else
  ok "fail-closed probe cannot inject a trusted scope"
fi
if grep -q "resource/ws" "$HERE/collector-no-trust.yaml"; then
  no "collector-no-trust.yaml references a trusted-scope processor"
else
  ok "fail-closed probe has no trusted-scope processor"
fi

# 9. The verifier must report failures (not silently pass) against an empty
#    database, proving the representation checks are wired to real rows.
TEST_DB="lh021_harselftest_$$"
curl -sS --fail-with-body "http://127.0.0.1:18224/?multiquery=1" \
  --data-binary "CREATE DATABASE IF NOT EXISTS $TEST_DB" >/dev/null 2>&1 || true
mkdir -p "$WORK/empty-checks"
rc=0
set +e
LH021_DATABASE="$TEST_DB" LH021_RESULTS="$WORK/empty-checks" \
  "$PY" "$HERE/verify.py" --phase semantics --results "$WORK/empty-checks" \
  --out checks-semantics.jsonl >"$WORK/empty-checks/out.log" 2>&1
rc=$?
set -e
if (( rc != 0 )); then
  ok "verifier fails against an empty database (exit $rc)"
else
  no "verifier passed against an empty database"
  sed 's/^/    /' "$WORK/empty-checks/out.log" | tail -5
fi
if [[ -s "$WORK/empty-checks/checks-semantics.jsonl" ]] &&
   grep -q '"ok": false' "$WORK/empty-checks/checks-semantics.jsonl"; then
  ok "verifier recorded explicit failing checks for the empty database"
else
  no "verifier recorded no failing check lines for the empty database"
fi
curl -sS --fail-with-body "http://127.0.0.1:18224/?multiquery=1" \
  --data-binary "DROP DATABASE IF EXISTS $TEST_DB" >/dev/null 2>&1 || true

# 10. An unknown phase is rejected instead of silently doing nothing.
rc=0
set +e
"$HERE/run.sh" not-a-phase >"$WORK/unknown.log" 2>&1
rc=$?
set -e
if (( rc != 0 )); then
  ok "unknown phase rejected (exit $rc)"
else
  no "unknown phase returned 0"
fi

# 11. The trusted-scope configuration scan must see every pipeline that reaches
#     the owned exporter. A leading `\n` in the scan pattern once skipped the
#     pipelines whose body ended where the next pipeline began, and that was
#     only visible inside a full run; this phase needs no services so the fast
#     self-test catches the regression directly.
mkdir -p "$WORK/trust-config"
rc=0
set +e
"$PY" "$HERE/verify.py" --phase trust-config --results "$WORK/trust-config" \
  --out checks-trust-config.jsonl >"$WORK/trust-config/out.log" 2>&1
rc=$?
set -e
if (( rc == 0 )); then
  ok "trusted-scope configuration scan passes (all four workspace pipelines)"
else
  no "trusted-scope configuration scan failed (exit $rc)"
  sed 's/^/    /' "$WORK/trust-config/out.log" | tail -5
fi

# 12. A stale injector holding the phase's port must fail the phase rather than
#     let it run against that process. A real full run once reused a leftover
#     lost-response proxy: the new proxy died with `Address already in use`, the
#     stale one had already spent its injection budget, and the phase produced
#     five false failures instead of failing closed at startup. The holder here
#     is deliberately a foreign process, not a harness injector, so the guard
#     must refuse to reap it.
TEST_INJ_PORT=$((19200 + $$ % 500))
mkdir -p "$WORK/root"
[[ -e "$WORK/root/venv" ]] || ln -s "$ROOT/venv" "$WORK/root/venv"
"$PY" -c "
import socket, sys, time
sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('127.0.0.1', int(sys.argv[2])))
sock.listen(1)
open(sys.argv[1], 'w').write('ready')
time.sleep(120)
" "$WORK/port-held" "$TEST_INJ_PORT" &
holder=$!
for _ in $(seq 1 40); do
  if [[ -s "$WORK/port-held" ]]; then break; fi
  sleep 0.1
done
mkdir -p "$WORK/injector-guard-results"
rc=0
set +e
LH021_ROOT="$WORK/root" LH021_RESULTS="$WORK/injector-guard-results" \
  LH021_LOST_PORT="$TEST_INJ_PORT" \
  "$HERE/run.sh" ambiguous-serving >"$WORK/injector-guard.log" 2>&1
rc=$?
set -e
kill "$holder" 2>/dev/null || true
wait "$holder" 2>/dev/null || true
if (( rc != 0 )) && grep -q "already in use" "$WORK/injector-guard.log"; then
  ok "held injector port fails the phase instead of reusing the injector (exit $rc)"
else
  no "held injector port did not fail closed (exit $rc)"
  sed 's/^/    /' "$WORK/injector-guard.log" | tail -5
fi

# 13. The span-timing fixture must keep missing timing, reversed timing, and a
#     valid zero duration distinct, and the mixed request must really carry one
#     valid and one reversed span. The integration phase asserts against this
#     surface, so a fixture regression must fail here instead of silently
#     weakening the semantics phase.
rc=0
set +e
PYTHONPATH="$HERE" "$PY" - <<'PYEOF' >"$WORK/span-timing.log" 2>&1
# Import the 021 verifier first: importing fixture021 puts Experiment 019's
# harness directory at the front of sys.path, and that directory also has a
# verify.py.
import verify
import fixture021

spans = {e["event_id"]: e for e in fixture021.ALL_SPAN_EVENTS}
statuses = {i: verify.span_timing_status(e) for i, e in spans.items()}
assert statuses["span-missing-time"] == "missing", statuses
assert statuses["span-negative"] == "end_before_start", statuses
assert statuses["span-zero"] == "ok", statuses
assert statuses["timing-mixed-valid"] == "ok", statuses
assert statuses["timing-mixed-reversed"] == "end_before_start", statuses
assert verify.span_duration_ns(spans["span-zero"]) == 0, "a zero duration stays observable"
assert verify.span_duration_ns(spans["span-negative"]) is None, "a reversed duration is unavailable"
assert verify.span_duration_ns(spans["span-missing-time"]) is None, "a missing duration is unavailable"
data, _ = fixture021.build_spans("timing-mixed")
req = fixture021.trace_service_pb2.ExportTraceServiceRequest()
req.ParseFromString(data)
spans_pb = [(s.start_time_unix_nano, s.end_time_unix_nano)
            for rl in req.resource_spans for sl in rl.scope_spans for s in sl.spans]
assert len(spans_pb) == 2, spans_pb
assert any(e >= s for s, e in spans_pb) and any(e < s for s, e in spans_pb), spans_pb
print("span timing fixture ok")
PYEOF
rc=$?
set -e
if (( rc == 0 )); then
  ok "span timing fixture keeps missing/reversed/zero distinct in a mixed batch"
else
  no "span timing fixture regression"
  sed 's/^/    /' "$WORK/span-timing.log" | tail -5
fi

if [[ "$MODE" == full ]]; then
  if [[ ! -x "$ROOT/bin/lighthouse-collector-021" || ! -x "$PY" ]]; then
    echo "full mode needs harness/setup.sh to have run first" >&2
    exit 2
  fi

  rc=0
  set +e
  LH021_TEST_CRASH_PHASE=semantics "$HERE/run.sh" all >"$WORK/injected.log" 2>&1
  rc=$?
  set -e
  if (( rc != 0 )); then
    ok "injected verifier crash fails run.sh all (exit $rc)"
  else
    no "injected verifier crash returned 0"
  fi
  if grep -q "coverage problems:.*semantics(exit=1)" "$WORK/injected.log"; then
    ok "summary reports the crashed semantics phase"
  else
    no "summary did not report the crashed semantics phase"
    tail -5 "$WORK/injected.log" | sed 's/^/    /'
  fi

  rc=0
  set +e
  "$HERE/run.sh" all >"$WORK/normal.log" 2>&1
  rc=$?
  set -e
  if (( rc == 0 )); then
    ok "normal full run exits 0"
  else
    no "normal full run exited $rc"
    tail -8 "$WORK/normal.log" | sed 's/^/    /'
  fi
  if grep -q "HARNESS PASSED" "$WORK/normal.log"; then
    ok "normal full run reports HARNESS PASSED"
  else
    no "normal full run did not report HARNESS PASSED"
  fi
  grep -E "phase coverage:|checks:|HARNESS" "$WORK/normal.log" | sed 's/^/    /'
fi

printf '\n%s: %d passed, %d failed\n' "$MODE" "$PASS" "$FAIL"
[[ "$FAIL" == 0 ]]
