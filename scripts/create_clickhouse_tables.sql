CREATE DATABASE IF NOT EXISTS default;

DROP TABLE IF EXISTS mv_dwd_user_behavior;
DROP TABLE IF EXISTS mv_ads_realtime_overview;
DROP TABLE IF EXISTS mv_ads_product_rank;
DROP TABLE IF EXISTS kafka_dwd_user_behavior;
DROP TABLE IF EXISTS kafka_ads_realtime_overview;
DROP TABLE IF EXISTS kafka_ads_product_rank;
DROP TABLE IF EXISTS dwd_user_behavior;
DROP TABLE IF EXISTS ads_realtime_overview;
DROP TABLE IF EXISTS ads_product_rank;

-- 结果表

-- ORDER BY 包含 event_id，重复消费的同一事件会在合并时去重
CREATE TABLE dwd_user_behavior (
  event_id String,
  user_id Int64,
  product_id Int64,
  product_name String,
  category_id Int64,
  category_name String,
  shop_id Int64,
  shop_name String,
  event_type String,
  channel String,
  amount Decimal(10, 2),
  event_ts DateTime
) ENGINE = ReplacingMergeTree
ORDER BY (event_ts, event_type, product_id, event_id);

CREATE TABLE ads_realtime_overview (
  window_start DateTime,
  window_end DateTime,
  pv Int64,
  uv Int64,
  cart_users Int64,
  order_users Int64,
  pay_users Int64,
  pay_amount Decimal(18, 2)
) ENGINE = ReplacingMergeTree
ORDER BY (window_start, window_end);

CREATE TABLE ads_product_rank (
  window_start DateTime,
  window_end DateTime,
  product_id Int64,
  product_name String,
  category_name String,
  pay_count Int64,
  pay_amount Decimal(18, 2)
) ENGINE = ReplacingMergeTree
ORDER BY (window_start, window_end, product_id);

-- Kafka Engine 表：ClickHouse 直接消费 Flink 输出的 Topic，按批写入

CREATE TABLE kafka_dwd_user_behavior (
  event_id String,
  user_id Int64,
  product_id Int64,
  product_name Nullable(String),
  category_id Nullable(Int64),
  category_name Nullable(String),
  shop_id Int64,
  shop_name Nullable(String),
  event_type String,
  channel String,
  amount Decimal(10, 2),
  event_ts String
) ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:29092',
  kafka_topic_list = 'dwd_user_behavior',
  kafka_group_name = 'clickhouse_dwd_user_behavior',
  kafka_format = 'JSONEachRow',
  kafka_skip_broken_messages = 100;

CREATE TABLE kafka_ads_realtime_overview (
  window_start String,
  window_end String,
  pv Int64,
  uv Int64,
  cart_users Int64,
  order_users Int64,
  pay_users Int64,
  pay_amount Decimal(18, 2)
) ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:29092',
  kafka_topic_list = 'ads_realtime_overview',
  kafka_group_name = 'clickhouse_ads_realtime_overview',
  kafka_format = 'JSONEachRow',
  kafka_skip_broken_messages = 100;

CREATE TABLE kafka_ads_product_rank (
  window_start String,
  window_end String,
  product_id Int64,
  product_name Nullable(String),
  category_name Nullable(String),
  pay_count Int64,
  pay_amount Decimal(18, 2)
) ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:29092',
  kafka_topic_list = 'ads_product_rank',
  kafka_group_name = 'clickhouse_ads_product_rank',
  kafka_format = 'JSONEachRow',
  kafka_skip_broken_messages = 100;

-- 物化视图：把 Kafka Engine 读到的数据转换后写入结果表

CREATE MATERIALIZED VIEW mv_dwd_user_behavior TO dwd_user_behavior AS
SELECT
  event_id,
  user_id,
  product_id,
  ifNull(product_name, '') AS product_name,
  ifNull(category_id, 0) AS category_id,
  ifNull(category_name, '') AS category_name,
  shop_id,
  ifNull(shop_name, '') AS shop_name,
  event_type,
  channel,
  amount,
  parseDateTimeBestEffort(event_ts) AS event_ts
FROM kafka_dwd_user_behavior;

CREATE MATERIALIZED VIEW mv_ads_realtime_overview TO ads_realtime_overview AS
SELECT
  parseDateTimeBestEffort(window_start) AS window_start,
  parseDateTimeBestEffort(window_end) AS window_end,
  pv,
  uv,
  cart_users,
  order_users,
  pay_users,
  pay_amount
FROM kafka_ads_realtime_overview;

CREATE MATERIALIZED VIEW mv_ads_product_rank TO ads_product_rank AS
SELECT
  parseDateTimeBestEffort(window_start) AS window_start,
  parseDateTimeBestEffort(window_end) AS window_end,
  product_id,
  ifNull(product_name, '') AS product_name,
  ifNull(category_name, '') AS category_name,
  pay_count,
  pay_amount
FROM kafka_ads_product_rank;
