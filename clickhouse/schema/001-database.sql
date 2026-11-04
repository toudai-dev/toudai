-- Decision 0002 D1: the database is deployment configuration, not part of the
-- schema. Every file names it as the `database` query parameter; apply them with
-- `clickhouse/apply-schema.sh` (default `toudai`) or pass
-- `--param_database=<name>` to clickhouse-client yourself.
CREATE DATABASE IF NOT EXISTS {database:Identifier};
