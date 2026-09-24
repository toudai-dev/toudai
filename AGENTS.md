# Agent Guide

This file provides guidance to AI coding agents working in this repository.

## Commit Titles

Use conventional commit titles for any commit you create. This repository's changelog configuration in `cliff.toml` enables `conventional_commits = true` and `filter_unconventional = true`, so non-conventional titles are easy to lose or misclassify.

Prefer these commit types because they are the ones grouped or validated in the repo today:

- `feat`
- `fix`
- `doc`
- `perf`
- `refactor`
- `style`
- `test`
- `chore`
- `ci`
- `revert`
- `security`

Use standard conventional commit formatting such as `fix(promapi): handle empty query` or `chore(ci): align changelog workflow permissions`.

## Version Control

- Use `jj` for every version-control operation it supports whenever the workspace
  is colocated with a Jujutsu repository. This includes status, diffs, history,
  commits, rebases, bookmarks, and pushes.
- Fall back to `git` only when `jj` is unavailable or does not provide the
  required interoperability or check. Commands such as `git diff --check` and
  GitHub CLI operations are acceptable because they are not repository-history
  mutations that `jj` replaces.

## Development Commands

### Building and Running

- `cargo run` runs the main CLI application
- `make dev` is an alias for `cargo run`
- `make build` builds the debug binary

### Testing and Quality

- `make test` or `cargo test --all-targets` runs the full test suite
- `make lint` or `cargo clippy --all-targets --all-features -- -D warnings` runs linting
- `make fmt` or `cargo fmt --check` checks formatting

### Docker Environment

- `make up` starts the development environment with docker-compose
- `make down` stops the development environment

## Project Architecture

This is a ClickHouse-backed telemetry service designed to be compatible with the Prometheus API, PromQL, LogQL, and TraceQL. The project is still in proof-of-concept shape, so some endpoints are present before full backend behavior exists.

## Working Style

- Prefer minimal, targeted changes that preserve the existing code style.
- Run relevant tests for the area you touch when practical.
- Do not introduce a second commit-title convention; keep commit types aligned with `cliff.toml` and `.github/workflows/commits.yml`.
- Commit in small logical chunks. This is a hard requirement when creating commits.
- Each commit must be self-reviewable: it should contain one coherent change, include its relevant tests/docs, and avoid mixing unrelated cleanup or follow-up work.
- If working from an RFD, update its implementation checklist incrementally as part of implementation, not only at the end.
- Prefer writing table tests over individual tests whenever possible
- Add comments that describe intent for non-obvious logic. This is required when code encodes a production constraint, workaround, performance tradeoff, safety guardrail, or surprising behavior.
- Keep comments succinct and useful for review. Avoid comments that merely restate what the next line of code does.

### Working From RFDs

When implementing an RFD in `rfd/NNNN/`:

- Keep `README.adoc` as the umbrella and stable design context in flat
  `<section>.adoc` files; keep checkboxes out of design documents.
- Treat each `<section>-impl.adoc` as the source of truth for that section's
  progress. Each checklist item has one owner. Legacy unsplit RFDs may use
  `IMPLEMENTATION.org` or `IMPLEMENTATION.md` instead.
- Before code changes, identify the phase/checklist items being worked.
- Update checklist items incrementally as work completes.
- Do not mark an item complete unless the named code/test/docs work is actually done.
- If implementation differs from the checklist wording, update the wording instead of marking an inaccurate item complete.
- Store experiments that inform an RFD under that RFD's `experiments/`
  directory and reference their measured results from the proposal.
- In the final response, summarize which checklist items changed and which remain open.
