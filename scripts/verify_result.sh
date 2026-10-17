#!/usr/bin/env bash
set -euo pipefail

query_count() {
  docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --query "$1"
}

docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --multiquery < scripts/verify_clickhouse.sql

dwd_rows=$(query_count "SELECT count() FROM dwd_user_behavior FINAL")
overview_rows=$(query_count "SELECT count() FROM ads_realtime_overview")
rank_rows=$(query_count "SELECT count() FROM ads_product_rank")

if [ "$dwd_rows" -le 0 ]; then
  echo "verify failed: dwd_user_behavior has no rows" >&2
  exit 1
fi

if [ "$overview_rows" -le 0 ]; then
  echo "verify failed: ads_realtime_overview has no rows" >&2
  exit 1
fi

if [ "$rank_rows" -le 0 ]; then
  echo "verify failed: ads_product_rank has no rows" >&2
  exit 1
fi

echo "verify passed: dwd_rows=$dwd_rows, overview_rows=$overview_rows, rank_rows=$rank_rows"
