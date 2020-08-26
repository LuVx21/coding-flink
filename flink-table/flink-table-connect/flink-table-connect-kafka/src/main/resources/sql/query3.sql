INSERT INTO pvuv_sink
SELECT
  DATE_FORMAT(FROM_UNIXTIME(ts), 'yyyy-MM-dd HH:00') as dt,
  COUNT(*) AS pv,
  COUNT(DISTINCT user_id) AS uv
FROM user_log
GROUP BY
    DATE_FORMAT(FROM_UNIXTIME(ts), 'yyyy-MM-dd HH:00');
