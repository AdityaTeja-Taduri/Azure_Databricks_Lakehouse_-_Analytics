CREATE OR REPLACE VIEW v_top_zones_by_revenue AS
SELECT
  Zone,
  Borough,
  SUM(total_revenue) AS total_revenue,
  SUM(trip_count)    AS total_trips
FROM daily_zone_metrics_gold
GROUP BY Zone, Borough
ORDER BY total_revenue DESC;
