# Databricks notebook source
from pyspark.sql.functions import col, to_date, count, avg, sum

# Use UC catalog + schema
spark.sql("USE CATALOG dbw_p3_lakehouse")
spark.sql("USE SCHEMA nyc_taxi")

# Load Silver tables
trips_silver = spark.table("trips_silver")
zones_silver = spark.table("zones_silver")

print("Trips Silver sample:")
trips_silver.show(5, truncate=False)

print("Zones Silver sample:")
zones_silver.show(5, truncate=False)


# COMMAND ----------

from pyspark.sql.functions import to_date

# Add a clean date column from pickup_time
trips_with_date = trips_silver.withColumn(
    "pickup_date",
    to_date(col("pickup_time"))
)

# Join with zones on PULocationID
trips_with_zones = (
    trips_with_date.join(
        zones_silver,
        trips_with_date.PULocationID == zones_silver.LocationID,
        "left"
    )
)

print("Joined sample:")
trips_with_zones.select(
    "pickup_date", "Borough", "Zone", "trip_distance", "total_amount"
).show(10, truncate=False)

# Aggregate to daily zone metrics
daily_zone_metrics = (
    trips_with_zones
    .groupBy("pickup_date", "Borough", "Zone")
    .agg(
        count("*").alias("trip_count"),
        avg("trip_distance").alias("avg_trip_distance"),
        avg("total_amount").alias("avg_total_amount"),
        sum("total_amount").alias("total_revenue")
    )
)

print("Gold metrics sample:")
daily_zone_metrics.show(10, truncate=False)


# COMMAND ----------

gold_daily_metrics_path = "abfss://gold@stp3taxilakehouse.dfs.core.windows.net/daily_zone_metrics/"

# Overwrite Gold folder
dbutils.fs.rm(gold_daily_metrics_path, True)

(
    daily_zone_metrics
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_daily_metrics_path)
)

print("✅ Gold Delta written to:", gold_daily_metrics_path)


# COMMAND ----------

# Make sure we're in the right catalog/schema
spark.sql("USE CATALOG dbw_p3_lakehouse")
spark.sql("USE SCHEMA nyc_taxi")

# Register Gold table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS daily_zone_metrics_gold
USING DELTA
LOCATION '{gold_daily_metrics_path}'
""")

print("✅ Gold table registered: dbw_p3_lakehouse.nyc_taxi.daily_zone_metrics_gold")

# Quick preview via SQL
display(spark.sql("""
SELECT *
FROM daily_zone_metrics_gold
ORDER BY pickup_date, Borough, Zone
LIMIT 100
"""))

# Check years again (should be only 2024)
display(spark.sql("""
SELECT DISTINCT year(pickup_date) AS year
FROM daily_zone_metrics_gold
ORDER BY year
"""))
