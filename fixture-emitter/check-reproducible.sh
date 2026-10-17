#!/usr/bin/env bash
#
# Reproducibility and independence checks for the fixture emitter.
#
# 1. Two runs with one seed produce byte-identical expected files; another seed
#    does not.
# 2. The expected file matches the committed copy for the default seed.
# 3. Every payload checksum is recomputed outside the emitter (jq + sha256sum).
# 4. Over gRPC and over HTTP, a loopback OTLP receiver decodes what the SDK sent
#    and the rows computed from the wire equal the expected rows byte for byte,
#    on two runs each.
#
# No Collector, ClickHouse, or batch receipt is involved.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
committed="${FIXTURE_EXPECTED:-${here}/../rfd/0001/experiments/023-fixture-emitter-run/expected.jsonl}"
seed="${SEED:-20260901}"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

cargo build --quiet --manifest-path "${here}/Cargo.toml"
emitter="${CARGO_TARGET_DIR:-${here}/target}/debug/fixture-emitter"

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

"${emitter}" expected --seed "${seed}" --out "${work}/a.jsonl"
"${emitter}" expected --seed "${seed}" --out "${work}/b.jsonl"
cmp "${work}/a.jsonl" "${work}/b.jsonl" || fail "two runs with seed ${seed} differ"
"${emitter}" expected --seed "$((seed + 1))" --out "${work}/other.jsonl"
if cmp -s "${work}/a.jsonl" "${work}/other.jsonl"; then
  fail "a different seed produced identical expected contents"
fi
printf 'ok: expected file is byte-identical across runs (%s events) and changes with the seed\n' \
  "$(wc -l < "${work}/a.jsonl" | tr -d ' ')"

if [[ -f "${committed}" ]]; then
  cmp "${work}/a.jsonl" "${committed}" || fail "expected contents differ from ${committed}; regenerate it and review the diff"
  printf 'ok: matches the committed expected file\n'
fi

checked=0
while IFS= read -r line; do
  want="$(jq -r '.payload_sha256' <<<"${line}")"
  got="$(jq -j '.payload' <<<"${line}" | sha256sum | cut -d' ' -f1)"
  [[ "${want}" == "${got}" ]] || fail "payload checksum mismatch for $(jq -r '.fixture_key' <<<"${line}")"
  checked=$((checked + 1))
done < <(jq -c 'select(.payload != null)' "${work}/a.jsonl")
printf 'ok: %s payload checksums recomputed with sha256sum\n' "${checked}"

for protocol in grpc http; do
  for run in 1 2; do
    "${emitter}" selfcheck --seed "${seed}" --protocol "${protocol}" \
      --expected-out "${work}/expected-${protocol}-${run}.jsonl" \
      --observed-out "${work}/observed-${protocol}-${run}.jsonl"
    cmp "${work}/a.jsonl" "${work}/expected-${protocol}-${run}.jsonl" || fail "${protocol} run ${run}: expected file differs"
    cmp "${work}/a.jsonl" "${work}/observed-${protocol}-${run}.jsonl" || fail "${protocol} run ${run}: wire contents differ from expected"
  done
done
printf 'ok: wire contents equal expected contents over gRPC and HTTP on two runs each\n'
