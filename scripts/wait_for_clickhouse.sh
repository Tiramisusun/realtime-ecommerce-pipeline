#!/usr/bin/env bash
# 等待 ClickHouse Kafka Engine 把 Flink 输出消费进结果表
set -euo pipefail

expected_dwd_rows="${1:-1}"
timeout_sec="${2:-90}"

query() {
  docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --query "$1"
}

for _ in $(seq 1 "$timeout_sec"); do
  dwd_rows=$(query "SELECT count() FROM dwd_user_behavior FINAL")
  overview_rows=$(query "SELECT count() FROM ads_realtime_overview")
  rank_rows=$(query "SELECT count() FROM ads_product_rank")
  if [ "$dwd_rows" -ge "$expected_dwd_rows" ] && [ "$overview_rows" -gt 0 ] && [ "$rank_rows" -gt 0 ]; then
    echo "clickhouse ready: dwd_rows=$dwd_rows, overview_rows=$overview_rows, rank_rows=$rank_rows"
    exit 0
  fi
  sleep 1
done

echo "timeout waiting for clickhouse: dwd_rows=$dwd_rows, overview_rows=$overview_rows, rank_rows=$rank_rows" >&2
exit 1
