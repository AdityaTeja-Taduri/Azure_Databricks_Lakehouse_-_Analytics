# Databricks notebook source
from pyspark.sql.functions import col

raw_trips_path = "abfss://raw@stp3taxilakehouse.dfs.core.windows.net/nyc_taxi/trips/"
raw_zones_path = "abfss://raw@stp3taxilakehouse.dfs.core.windows.net/nyc_taxi/lookup/"

# Read raw parquet files

trips_raw = (
    spark.read
    .format("parquet")
    .load(raw_trips_path)
)

print("Raw sample:")
trips_raw.show(5, truncate=False)

print("Raw Schema:")
trips_raw.printSchema()

zones_raw = (
    spark.read
    .format("csv")
    .load(raw_zones_path)
)

print("Zones Schema:")
zones_raw.printSchema()

print("Zones sample:")
zones_raw.show(5, truncate=False)

# COMMAND ----------

bronze_trips_csv_path = "abfss://bronze@stp3taxilakehouse.dfs.core.windows.net/nyc_taxi/trips_csv/"
bronze_zones_csv_path = "abfss://bronze@stp3taxilakehouse.dfs.core.windows.net/nyc_taxi/zones_csv/"

trips_bronze = (
     trips_raw
    .write
    .format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save(bronze_trips_csv_path)
)

(
    zones_raw
    .write
    .mode("overwrite")
      .option("header", "true")
      .csv(bronze_zones_csv_path)
)

print("✅ Bronze CSV written:")
print(f"Trips  → {bronze_trips_csv_path}")
print(f"Zones  → {bronze_zones_csv_path}")
