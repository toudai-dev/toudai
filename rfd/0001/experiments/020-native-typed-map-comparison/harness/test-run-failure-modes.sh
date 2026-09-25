#!/usr/bin/env bash
# Experiment 020 harness self-test: a run must fail closed, and the semantics
# assertions must fail for a plausible wrong layout.
#
# Usage:
#   test-run-failure-modes.sh fast   (default) synthetic aggregation checks; no services
#   test-run-failure-modes.sh full   fast checks, an injected and a normal full run,
#                                    and the wrong-layout assertion-sensitivity checks
#
# The fast mode drives run.sh's summarize() through `check-results` with
# synthetic results and covers: a complete all-pass set, a missing phase, an
# empty phase file, a nonzero phase exit status, a failed check line, a missing
# status entry, and a stale checks file.
#
# The full mode needs `setup.sh` to have run. It runs `run.sh all` twice: once
# with an injected unexpected exception in the semantics phase
# (LH020_TEST_CRASH_PHASE, honoured by verify.py as a test-only hook) and once
# normally, asserting the process exit status and phase coverage in both cases.
# It then runs the native typed check set against the deliberately wrong
# Float64-collapsed table and asserts a nonzero exit, and against the correct
# table and asserts zero.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH020_ROOT:-/home/user/lh020}"
PY="$ROOT/venv/bin/python"
MODE="${1:-fast}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
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
    printf '{"name":"ok.%d","kind":"semantic","expected":"1","observed":"1","ok":true}\n' "$i" >>"$file"
  done
  for ((i = 0; i < badc; i++)); do
    printf '{"name":"bad.%d","kind":"semantic","expected":"1","observed":"2","ok":false}\n' "$i" >>"$file"
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
  LH020_ROOT="$WORK/root" LH020_RESULTS="$dir" "$HERE/run.sh" check-results "$@" >"$dir/summary.log" 2>&1
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

if [[ "$MODE" == full ]]; then
  if [[ ! -x "$ROOT/venv/bin/python" ]]; then
    echo "full mode needs harness/setup.sh to have run first" >&2
    exit 2
  fi

  # The candidate checks below query the tables a normal run loaded, but that
  # run stops its own ClickHouse on exit. Start a test server on the same data
  # directory and stop it when this script exits.
  export LH020_CH_PATH="$ROOT/run/clickhouse/data/"
  export LH020_CH_TMP="$ROOT/run/clickhouse/tmp/"
  export LH020_CH_USER_FILES="$ROOT/run/clickhouse/user_files/"
  export LH020_CH_FORMAT_SCHEMAS="$ROOT/run/clickhouse/format_schemas/"
  export LH020_CH_LOG="$ROOT/logs/clickhouse-server.log"
  export LH020_CH_ERRORLOG="$ROOT/logs/clickhouse-server.err.log"
  export LH020_CH_USERS="$HERE/clickhouse-users.xml"
  CH_BIN="$(cd "$HERE" && nix develop --command bash -c 'command -v clickhouse-server')"
  stop_test_server() {
    local pid
    pid="$(cat "$ROOT/run/clickhouse-test.pid" 2>/dev/null || true)"
    if [[ -n "$pid" ]]; then
      kill -TERM "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
      local deadline=$((SECONDS + 20))
      while (( SECONDS < deadline )) && (exec 3<>/dev/tcp/127.0.0.1/18125) 2>/dev/null; do sleep 0.5; done
      kill -KILL "-$pid" 2>/dev/null || true
      rm -f "$ROOT/run/clickhouse-test.pid"
    fi
  }
  trap 'stop_test_server; rm -rf "$WORK"' EXIT
  start_test_server() {
    if (exec 3<>/dev/tcp/127.0.0.1/18125) 2>/dev/null; then return 0; fi
    setsid "$CH_BIN" --config-file="$HERE/clickhouse-config.xml" >"$ROOT/logs/clickhouse-test.log" 2>&1 &
    echo $! >"$ROOT/run/clickhouse-test.pid"
    local deadline=$((SECONDS + 90))
    while (( SECONDS < deadline )); do
      if (exec 3<>/dev/tcp/127.0.0.1/18125) 2>/dev/null; then return 0; fi
      sleep 0.5
    done
    return 1
  }

  rc=0
  set +e
  LH020_TEST_CRASH_PHASE=semantics "$HERE/run.sh" all >"$WORK/injected.log" 2>&1
  rc=$?
  set -e
  if (( rc != 0 )); then
    ok "injected semantics exception fails run.sh all (exit $rc)"
  else
    no "injected semantics exception returned 0"
  fi
  if grep -q "coverage problems:.*semantics(exit=1)" "$WORK/injected.log"; then
    ok "summary reports the crashed semantics phase"
  else
    no "summary did not report the crashed semantics phase"
    tail -5 "$WORK/injected.log" | sed 's/^/    /'
  fi
  if grep -q "HARNESS FAILED" "$WORK/injected.log"; then
    ok "injected run reports HARNESS FAILED"
  else
    no "injected run did not report HARNESS FAILED"
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
    tail -5 "$WORK/normal.log" | sed 's/^/    /'
  fi
  if grep -q "HARNESS PASSED" "$WORK/normal.log"; then
    ok "normal full run reports HARNESS PASSED"
  else
    no "normal full run did not report HARNESS PASSED"
  fi
  if grep -q "phase coverage: capabilities semantics negative cost files" "$WORK/normal.log"; then
    ok "normal full run asserts all five phases"
  else
    no "normal full run did not assert all five phases"
  fi
  grep -E "phase coverage:|checks:|HARNESS" "$WORK/normal.log" | sed 's/^/    /'

  # The assertion set must fail for a plausible wrong layout and pass for the
  # correct one. This runs the same native typed checks against both tables.
  if ! start_test_server; then
    no "test ClickHouse did not start for the candidate checks"
  fi
  rc=0
  set +e
  "$PY" "$HERE/verify.py" --phase semantics --results "$HERE/results" \
    --candidate rep_wrong_float_only >"$WORK/wrong.log" 2>&1
  rc=$?
  set -e
  if (( rc != 0 )); then
    ok "wrong-layout candidate fails the semantics checks (exit $rc)"
  else
    no "wrong-layout candidate passed the semantics checks"
  fi
  if grep -q "FAIL.*semantics.native_typed.typed_filter_int" "$WORK/wrong.log"; then
    ok "wrong-layout run reports the discriminating typed_filter_int check"
  else
    no "wrong-layout run did not report the typed_filter_int failure"
    grep -E "FAIL" "$WORK/wrong.log" | head -3 | sed 's/^/    /'
  fi

  rc=0
  set +e
  "$PY" "$HERE/verify.py" --phase semantics --results "$HERE/results" \
    --candidate rep_native_typed >"$WORK/correct.log" 2>&1
  rc=$?
  set -e
  if (( rc == 0 )); then
    ok "correct candidate passes the same checks (exit 0)"
  else
    no "correct candidate failed the same checks (exit $rc)"
    grep -E "FAIL" "$WORK/correct.log" | head -3 | sed 's/^/    /'
  fi
fi

printf '\n%s: %d passed, %d failed\n' "$MODE" "$PASS" "$FAIL"
[[ "$FAIL" == 0 ]]
