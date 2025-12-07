CREATE OR REPLACE VIEW v_daily_revenue AS
SELECT
  pickup_date,
  SUM(trip_count)      AS total_trips,
  SUM(total_revenue)   AS total_revenue,
  AVG(avg_total_amount) AS avg_fare
FROM daily_zone_metrics_gold
GROUP BY pickup_date
ORDER BY pickup_date;
