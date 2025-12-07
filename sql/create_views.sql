USE CATALOG dbw_p3_lakehouse;
USE SCHEMA nyc_taxi;

CREATE OR REPLACE VIEW v_daily_revenue AS
SELECT
  pickup_date,
  SUM(trip_count)      AS total_trips,
  SUM(total_revenue)   AS total_revenue,
  AVG(avg_total_amount) AS avg_fare
FROM daily_zone_metrics_gold
GROUP BY pickup_date
ORDER BY pickup_date;

CREATE OR REPLACE VIEW v_top_zones_by_revenue AS
SELECT
  Zone,
  Borough,
  SUM(total_revenue) AS total_revenue,
  SUM(trip_count)    AS total_trips
FROM daily_zone_metrics_gold
GROUP BY Zone, Borough
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW v_weekday_performance AS
SELECT
  date_format(pickup_date, 'E') AS weekday,
  SUM(trip_count)               AS total_trips,
  SUM(total_revenue)            AS total_revenue,
  AVG(avg_total_amount)         AS avg_fare
FROM daily_zone_metrics_gold
GROUP BY date_format(pickup_date, 'E')
ORDER BY total_trips DESC;

CREATE OR REPLACE VIEW v_borough_revenue_trend AS
SELECT
  pickup_date,
  Borough,
  SUM(trip_count)    AS total_trips,
  SUM(total_revenue) AS total_revenue
FROM daily_zone_metrics_gold
GROUP BY pickup_date, Borough
ORDER BY pickup_date, Borough;
