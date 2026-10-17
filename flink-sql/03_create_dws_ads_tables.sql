CREATE TABLE ads_realtime_overview_kafka (
  window_start STRING,
  window_end STRING,
  pv BIGINT,
  uv BIGINT,
  cart_users BIGINT,
  order_users BIGINT,
  pay_users BIGINT,
  pay_amount DECIMAL(18,2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ads_realtime_overview',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE ads_product_rank_kafka (
  window_start STRING,
  window_end STRING,
  product_id BIGINT,
  product_name STRING,
  category_name STRING,
  pay_count BIGINT,
  pay_amount DECIMAL(18,2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ads_product_rank',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL'
);
