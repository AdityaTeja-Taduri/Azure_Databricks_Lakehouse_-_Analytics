CREATE OR REPLACE VIEW v_borough_revenue_trend AS
SELECT
  pickup_date,
  Borough,
  SUM(trip_count)    AS total_trips,
  SUM(total_revenue) AS total_revenue
FROM daily_zone_metrics_gold
GROUP BY pickup_date, Borough
ORDER BY pickup_date, Borough;
