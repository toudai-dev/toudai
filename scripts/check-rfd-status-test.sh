#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
checker="${repo_root}/scripts/check-rfd-status.sh"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/toudai-rfd-check.XXXXXX")"
trap 'rm -rf "${test_root}"' EXIT

write_valid() {
  rm -rf "${test_root}/rfd"
  mkdir -p "${test_root}/rfd/0001"
  printf '# Test RFDs\n' > "${test_root}/rfd/README.md"
  cat > "${test_root}/rfd/0001/README.adoc" <<'EOF'
:authors: Example Author <author@example.com>
:state: discussion
:discussion: https://example.com/discussion/1
:labels: test

= RFD 1 Valid fixture

See link:IMPLEMENTATION.org[the checklist].
EOF
  cat > "${test_root}/rfd/0001/IMPLEMENTATION.org" <<'EOF'
#+TITLE: RFD 0001 implementation checklist

[[file:README.adoc][Back to RFD 1]]

- [ ] Exercise the checker.
EOF
}

expect_failure() {
  local expected="$1"
  if RFD_DIR="${test_root}/rfd" "${checker}" > "${test_root}/output" 2>&1; then
    printf 'expected checker failure containing: %s\n' "${expected}" >&2
    exit 1
  fi
  grep -Fq "${expected}" "${test_root}/output" || {
    cat "${test_root}/output" >&2
    exit 1
  }
}

write_valid
RFD_DIR="${test_root}/rfd" "${checker}" >/dev/null

write_valid
sed -i 's/:state: discussion/:state: draft/' "${test_root}/rfd/0001/README.adoc"
expect_failure 'invalid state: draft'

write_valid
sed -i 's#https://example.com/discussion/1##' "${test_root}/rfd/0001/README.adoc"
expect_failure 'requires a discussion URL'

write_valid
printf '\n* [ ] Proposal checkbox.\n' >> "${test_root}/rfd/0001/README.adoc"
expect_failure 'implementation checkboxes belong in a separate implementation document'

write_sections() {
  write_valid
  rm "${test_root}/rfd/0001/IMPLEMENTATION.org"
  sed -i '/See link:IMPLEMENTATION.org/d' "${test_root}/rfd/0001/README.adoc"
  for section in events experiments; do
    printf '\nSee link:%s.adoc[design] and link:%s-impl.adoc[implementation].\n' "${section}" "${section}" >> "${test_root}/rfd/0001/README.adoc"
    printf '= %s\n\nDesign context.\n' "${section}" > "${test_root}/rfd/0001/${section}.adoc"
    printf '= %s implementation\n\nlink:%s.adoc[Design]\n\n* [ ] Exercise the checker.\n' "${section}" "${section}" > "${test_root}/rfd/0001/${section}-impl.adoc"
  done
}

write_sections
RFD_DIR="${test_root}/rfd" "${checker}" >/dev/null

for scenario in missing-design missing-plan missing-link missing-backlink checkbox mixed; do
  write_sections
  case "${scenario}" in
    missing-design)
      rm "${test_root}/rfd/0001/events.adoc"
      expected='missing design companion' ;;
    missing-plan)
      rm "${test_root}/rfd/0001/events-impl.adoc"
      expected='missing implementation companion' ;;
    missing-link)
      sed -i '/See link:events.adoc/d' "${test_root}/rfd/0001/README.adoc"
      expected='RFD must link to its implementation checklist' ;;
    missing-backlink)
      sed -i '/link:events.adoc/d' "${test_root}/rfd/0001/events-impl.adoc"
      expected='implementation must link to its design' ;;
    checkbox)
      printf '\n* [ ] Misplaced task.\n' >> "${test_root}/rfd/0001/events.adoc"
      expected='implementation checkboxes belong in a separate implementation document' ;;
    mixed)
      touch "${test_root}/rfd/0001/IMPLEMENTATION.org"
      expected='mixed legacy and section implementation checklists' ;;
  esac
  expect_failure "${expected}"
done

printf 'RFD checker tests passed.\n'
