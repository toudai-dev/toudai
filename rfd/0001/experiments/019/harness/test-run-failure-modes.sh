#!/usr/bin/env bash
# Experiment 019 harness self-test: a run must fail closed.
#
# Usage:
#   test-run-failure-modes.sh fast   (default) synthetic aggregation checks; no services
#   test-run-failure-modes.sh full   fast checks plus an injected and a normal full run
#
# The fast mode drives run.sh's summarize() through `check-results` with
# synthetic results and covers: a complete all-pass set, a missing phase, an
# empty phase file, a nonzero phase exit status, a failed check line, a missing
# status entry, and a stale checks file.
#
# The full mode runs `run.sh all` twice: once with an injected unexpected
# exception in the semantics phase (LH019_TEST_CRASH_PHASE, honoured by
# verify.py as a test-only hook) and once normally. It asserts the process exit
# status and the phase-coverage summary in both cases. It needs `setup.sh` to
# have run and starts its own disposable ClickHouse/Collector, so it takes a
# couple of minutes.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${LH019_ROOT:-/home/user/lh019}"
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
  LH019_ROOT="$WORK/root" LH019_RESULTS="$dir" "$HERE/run.sh" check-results "$@" >"$dir/summary.log" 2>&1
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
  if [[ ! -x "$ROOT/bin/otelcol-contrib" || ! -x "$ROOT/venv/bin/python" ]]; then
    echo "full mode needs harness/setup.sh to have run first" >&2
    exit 2
  fi

  rc=0
  set +e
  LH019_TEST_CRASH_PHASE=semantics "$HERE/run.sh" all >"$WORK/injected.log" 2>&1
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
  if grep -q "phase coverage: stock-map stock-json managed transform identity semantics cost files" "$WORK/normal.log"; then
    ok "normal full run asserts all eight phases"
  else
    no "normal full run did not assert all eight phases"
  fi
  grep -E "phase coverage:|checks:|HARNESS" "$WORK/normal.log" | sed 's/^/    /'
fi

printf '\n%s: %d passed, %d failed\n' "$MODE" "$PASS" "$FAIL"
[[ "$FAIL" == 0 ]]
