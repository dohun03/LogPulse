#!/bin/bash
set -euo pipefail

: "${CLICKHOUSE_WRITER_PASSWORD:?CLICKHOUSE_WRITER_PASSWORD is required}"
: "${CLICKHOUSE_READER_PASSWORD:?CLICKHOUSE_READER_PASSWORD is required}"

sed \
  -e "s|{{WRITER_PASSWORD}}|${CLICKHOUSE_WRITER_PASSWORD}|g" \
  -e "s|{{READER_PASSWORD}}|${CLICKHOUSE_READER_PASSWORD}|g" \
  /infra/clickhouse/init.sql \
  | clickhouse-client --multiquery