#!/usr/bin/env bash
# The printf formats below build byte escapes at run time.
# shellcheck disable=SC2059

# Seed each target's corpus from the ingest test fixtures, so fuzzing starts
# from valid requests instead of searching for the protobuf framing. Each seed
# is the target's selector byte followed by the input.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
fixtures="$here/../tests/fixtures"
corpus="$here/corpus"
mkdir -p "$corpus"/{http_body,grpc_body,json_round_trip}

seed() { # target name selector-hex file
  { printf "\\x$3"; cat "$4"; } > "$corpus/$1/seed-$2"
}

m="$fixtures/mapping"
# http_body: low nibble encoding (0 none, 1 gzip), bit 4 JSON, bit 5 traces.
seed http_body logs-pb 00 "$m/logs.otlp.pb"
seed http_body logs-json 10 "$m/logs.otlp.json"
seed http_body traces-pb 20 "$m/traces.otlp.pb"
seed http_body traces-json 30 "$m/traces.otlp.json"
gzip -nc "$m/logs.otlp.pb" > "$corpus/logs.otlp.pb.gz"
seed http_body logs-pb-gzip 01 "$corpus/logs.otlp.pb.gz"
rm "$corpus/logs.otlp.pb.gz"

# json_round_trip: bit 0 traces, bit 1 decimal-string doubles.
seed json_round_trip logs 00 "$m/logs.otlp.pb"
seed json_round_trip logs-decimal 02 "$m/logs.otlp.pb"
seed json_round_trip traces 01 "$m/traces.otlp.pb"

# grpc_body: one snappy-compressed message, as golang/snappy writes it.
sz="$fixtures/snappy/logs.otlp.pb.sz"
size=$(wc -c < "$sz")
{
  printf '\x01'
  printf "\\x$(printf %02x $((size >> 24 & 255)))\\x$(printf %02x $((size >> 16 & 255)))"
  printf "\\x$(printf %02x $((size >> 8 & 255)))\\x$(printf %02x $((size & 255)))"
  cat "$sz"
} > "$corpus/grpc_body/seed-logs-snappy"
