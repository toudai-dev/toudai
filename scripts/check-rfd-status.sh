#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rfd_root="${RFD_DIR:-${repo_root}/rfd}"
allowed_states='prediscussion|ideation|discussion|published|committed|abandoned'
failures=0
found=0

problem() {
  printf '  %s\n' "$1" >&2
  failures=$((failures + 1))
}

if [[ ! -d "${rfd_root}" ]]; then
  printf 'RFD directory not found: %s\n' "${rfd_root}" >&2
  exit 1
fi

printf '%-4s  %-13s  %s\n' RFD State Title
printf '%-4s  %-13s  %s\n' ---- ------------- -----------------------------------

shopt -s nullglob
entries=("${rfd_root}"/*)
shopt -u nullglob

for entry in "${entries[@]}"; do
  name="$(basename "${entry}")"
  [[ "${name}" == README.md ]] && continue

  if [[ ! -d "${entry}" || ! "${name}" =~ ^[0-9]{4}$ ]]; then
    problem "invalid RFD entry: ${name}"
    continue
  fi
  found=1

  source="${entry}/README.adoc"
  if [[ ! -f "${source}" ]]; then
    problem "missing canonical RFD source: ${name}/README.adoc"
    continue
  fi

  authors_count="$(grep -Ec '^:authors: .+' "${source}" || true)"
  state_count="$(grep -Ec '^:state: .+' "${source}" || true)"
  discussion_count="$(grep -Ec '^:discussion:' "${source}" || true)"
  labels_count="$(grep -Ec '^:labels: .+' "${source}" || true)"
  title_count="$(grep -Ec '^= RFD [0-9]+ .+' "${source}" || true)"
  state="$(sed -n 's/^:state:[[:space:]]*//p' "${source}")"
  discussion="$(sed -n 's/^:discussion:[[:space:]]*//p' "${source}")"
  title_line="$(sed -n 's/^= RFD //p' "${source}")"
  title_number="${title_line%% *}"
  title="${title_line#* }"
  expected_number="$((10#${name}))"

  [[ "${authors_count}" == 1 ]] || problem "${name}/README.adoc must contain exactly one non-empty authors attribute"
  grep -Eq '^:authors: .+<[^>]+>' "${source}" || problem "${name}/README.adoc authors must include an address in angle brackets"
  [[ "${state_count}" == 1 ]] || problem "${name}/README.adoc must contain exactly one non-empty state attribute"
  [[ "${state}" =~ ^(${allowed_states})$ ]] || problem "${name}/README.adoc has invalid state: ${state:-missing}"
  [[ "${discussion_count}" == 1 ]] || problem "${name}/README.adoc must contain exactly one discussion attribute"
  if [[ -n "${discussion}" && ! "${discussion}" =~ ^https?:// ]]; then
    problem "${name}/README.adoc discussion must be empty or an HTTP(S) URL"
  fi
  if [[ "${state}" =~ ^(discussion|published|committed)$ && -z "${discussion}" ]]; then
    problem "${name}/README.adoc state ${state} requires a discussion URL"
  fi
  [[ "${labels_count}" == 1 ]] || problem "${name}/README.adoc must contain exactly one non-empty labels attribute"
  [[ "${title_count}" == 1 ]] || problem "${name}/README.adoc must contain exactly one RFD title"
  [[ "${title_number}" == "${expected_number}" ]] || problem "${name}/README.adoc title number ${title_number:-missing} does not match directory number ${expected_number}"

  implementations=()
  [[ -f "${entry}/IMPLEMENTATION.org" ]] && implementations+=(IMPLEMENTATION.org)
  [[ -f "${entry}/IMPLEMENTATION.md" ]] && implementations+=(IMPLEMENTATION.md)
  if [[ "${#implementations[@]}" == 0 ]]; then
    problem "missing implementation checklist: ${name}/IMPLEMENTATION.org or IMPLEMENTATION.md"
  elif [[ "${#implementations[@]}" -gt 1 ]]; then
    problem "multiple implementation checklist formats: ${name}"
  else
    implementation="${implementations[0]}"
    if [[ "${implementation}" == IMPLEMENTATION.org ]]; then
      expected_heading="#+TITLE: RFD ${name} implementation checklist"
      backlink='[[file:README.adoc]['
    else
      expected_heading="# RFD ${name} implementation checklist"
      backlink='](README.adoc)'
    fi
    [[ "$(head -n 1 "${entry}/${implementation}")" == "${expected_heading}" ]] || problem "invalid implementation checklist heading: ${name}/${implementation}"
    grep -Fq "${backlink}" "${entry}/${implementation}" || problem "implementation checklist must link to its RFD: ${name}/${implementation}"
    grep -Fq "link:${implementation}[" "${source}" || problem "RFD must link to its implementation checklist: ${name}/README.adoc"
  fi

  if grep -Eq '^\* \[[ xX]\]' "${source}"; then
    problem "implementation checkboxes belong in a separate implementation document: ${name}/README.adoc"
  fi

  printf '%-4s  %-13s  %s\n' "${name}" "${state:-missing}" "${title:-missing}"
done

[[ "${found}" == 1 ]] || problem "no RFDs found in ${rfd_root}"
if [[ "${failures}" -gt 0 ]]; then
  printf 'RFD status check failed with %s issue(s).\n' "${failures}" >&2
  exit 1
fi

printf 'RFD status check passed.\n'
