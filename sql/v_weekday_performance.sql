CREATE OR REPLACE VIEW v_weekday_performance AS
SELECT
  date_format(pickup_date, 'E') AS weekday,
  SUM(trip_count)               AS total_trips,
  SUM(total_revenue)            AS total_revenue,
  AVG(avg_total_amount)         AS avg_fare
FROM daily_zone_metrics_gold
GROUP BY date_format(pickup_date, 'E')
ORDER BY total_trips DESC;
