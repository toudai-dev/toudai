#!/usr/bin/env bash
#
# Apply clickhouse/schema/*.sql in order to one deployment database.
#
# The database name is configuration (Decision 0002 D1): it comes from
# TOUDAI_CLICKHOUSE_DATABASE, the variable the query service reads, and defaults
# to `toudai`. Arguments are passed to clickhouse-client, for example
# `--host`, `--port`, `--user`, and `--password`. Every statement is
# `IF NOT EXISTS`, so re-running is safe.
#
# Usage: TOUDAI_CLICKHOUSE_DATABASE=<name> clickhouse/apply-schema.sh [client args...]

set -euo pipefail

schema_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/schema"
database="${TOUDAI_CLICKHOUSE_DATABASE:-toudai}"

for file in "${schema_dir}"/*.sql; do
  clickhouse-client --param_database="${database}" --multiquery "$@" < "${file}"
  printf 'applied %s to %s\n' "$(basename "${file}")" "${database}"
done
