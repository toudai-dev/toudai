#!/usr/bin/env bash
#
# Regenerate the mapping goldens from the Go exporter of toudai-dev/collector.
#
#   ingest/tests/fixtures/mapping/regenerate.sh <collector checkout>
#
# The checkout must be at the reference commit below and have a Go toolchain
# (its `nix develop` shell provides one). The generator is copied into the
# checkout's toudaiexporter package for the run and removed afterwards; the
# checkout is otherwise untouched.

set -euo pipefail

reference=f780574
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
collector="$(cd "${1:?usage: regenerate.sh <collector checkout>}" && pwd)"

head="$(git -C "${collector}" rev-parse --short=7 HEAD)"
if [[ "${head}" != "${reference}" ]]; then
  printf 'collector checkout is at %s, want %s\n' "${head}" "${reference}" >&2
  exit 1
fi

generator="${collector}/toudaiexporter/zz_toudai_golden_test.go"
cp "${here}/golden/golden_test.go" "${generator}"
trap 'rm -f "${generator}"' EXIT
(cd "${collector}" && TOUDAI_GOLDEN_DIR="${here}" go test -count=1 -tags toudaigolden -run '^TestToudaiGolden$' ./toudaiexporter)
