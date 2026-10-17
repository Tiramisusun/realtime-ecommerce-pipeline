-- 用 STATEMENT SET 把 DWD、实时概览、商品排行三条 INSERT 合并成一个 Flink 任务。
-- 三条 INSERT 共用 dwd_user_behavior_view，Flink 会复用同一份 Kafka 读取和维表关联，
-- 每条事件只做一次维表查询，而不是每个任务各做一次。
EXECUTE STATEMENT SET
BEGIN

  INSERT INTO dwd_user_behavior_kafka
  SELECT
    event_id,
    user_id,
    product_id,
    product_name,
    category_id,
    category_name,
    shop_id,
    shop_name,
    event_type,
    channel,
    amount,
    CAST(event_ts AS STRING) AS event_ts
  FROM dwd_user_behavior_view;

  INSERT INTO ads_realtime_overview_kafka
  SELECT
    CAST(window_start AS STRING) AS window_start,
    CAST(window_end AS STRING) AS window_end,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS cart_users,
    COUNT(DISTINCT CASE WHEN event_type = 'order' THEN user_id END) AS order_users,
    COUNT(DISTINCT CASE WHEN event_type = 'pay' THEN user_id END) AS pay_users,
    SUM(CASE WHEN event_type = 'pay' THEN amount ELSE CAST(0 AS DECIMAL(10,2)) END) AS pay_amount
  FROM TABLE(
    TUMBLE(TABLE dwd_user_behavior_view, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
  )
  GROUP BY window_start, window_end;

  INSERT INTO ads_product_rank_kafka
  SELECT
    CAST(window_start AS STRING) AS window_start,
    CAST(window_end AS STRING) AS window_end,
    product_id,
    product_name,
    category_name,
    COUNT(*) AS pay_count,
    SUM(amount) AS pay_amount
  FROM TABLE(
    TUMBLE(TABLE dwd_user_behavior_view, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
  )
  WHERE event_type = 'pay'
  GROUP BY window_start, window_end, product_id, product_name, category_name;

END;
