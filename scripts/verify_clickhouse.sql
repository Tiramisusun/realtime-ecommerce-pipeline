SELECT count() AS dwd_rows FROM dwd_user_behavior FINAL;

SELECT *
FROM ads_realtime_overview
ORDER BY window_start DESC
LIMIT 10;

SELECT
  window_start,
  window_end,
  product_name,
  category_name,
  pay_count,
  pay_amount
FROM ads_product_rank
ORDER BY window_start DESC, pay_amount DESC
LIMIT 10;
