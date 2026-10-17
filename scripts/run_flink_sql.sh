#!/usr/bin/env bash
set -euo pipefail

cat \
  flink-sql/01_create_source_tables.sql \
  flink-sql/02_create_dwd_tables.sql \
  flink-sql/03_create_dws_ads_tables.sql \
  flink-sql/04_submit_jobs.sql \
  | docker exec -i rtdw_flink_jobmanager /opt/flink/bin/sql-client.sh
