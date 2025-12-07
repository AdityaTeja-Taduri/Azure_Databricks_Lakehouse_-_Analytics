# Databricks notebook source
from pyspark.sql.functions import col, to_timestamp

# Bronze CSV paths (adjust only if your folders are different)
bronze_trips_csv_path = "abfss://bronze@stp3taxilakehouse.dfs.core.windows.net/nyc_taxi/trips_csv/"
bronze_zones_csv_path = "abfss://bronze@stp3taxilakehouse.dfs.core.windows.net/nyc_taxi/zones_csv/"

# Load Bronze Trips (CSV)
trips_bronze_csv = (
    spark.read
    .option("header", "true")
    .csv(bronze_trips_csv_path)
)

print("Bronze Trips sample:")
trips_bronze_csv.show(5, truncate=False)
print("Bronze Trips columns:", trips_bronze_csv.columns)

# Load Bronze Zones (CSV)
zones_bronze_csv = (
    spark.read
    .option("header", "true")
    .csv(bronze_zones_csv_path)
)

print("Bronze Zones sample:")
zones_bronze_csv.show(5, truncate=False)
print("Bronze Zones columns:", zones_bronze_csv.columns)


# COMMAND ----------

from pyspark.sql.functions import (
    col, to_timestamp, year, unix_timestamp
)

# 1) Parse timestamps & cast numeric fields from Bronze CSV
trips_typed = (
    trips_bronze_csv
    # parse timestamps from ORIGINAL columns
    .withColumn("pickup_time",  to_timestamp(col("tpep_pickup_datetime")))
    .withColumn("dropoff_time", to_timestamp(col("tpep_dropoff_datetime")))
    .withColumn("passenger_count", col("passenger_count").cast("int"))
    .withColumn("trip_distance",   col("trip_distance").cast("double"))
    .withColumn("PULocationID",    col("PULocationID").cast("int"))
    .withColumn("DOLocationID",    col("DOLocationID").cast("int"))
    .withColumn("payment_type",    col("payment_type").cast("int"))
    .withColumn("fare_amount",     col("fare_amount").cast("double"))
    .withColumn("tip_amount",      col("tip_amount").cast("double"))
    .withColumn("tolls_amount",    col("tolls_amount").cast("double"))
    .withColumn("total_amount",    col("total_amount").cast("double"))
)

# 2) Drop rows with null timestamps
trips_clean = (
    trips_typed
    .filter(col("pickup_time").isNotNull() & col("dropoff_time").isNotNull())
)

# 3) Keep ONLY 2024 trips
trips_clean = trips_clean.filter(year(col("pickup_time")) == 2024)

# 4) Remove rows where dropoff < pickup
trips_clean = trips_clean.filter(col("dropoff_time") >= col("pickup_time"))

# 5) Add trip duration (minutes)
trips_clean = trips_clean.withColumn(
    "trip_duration_min",
    (unix_timestamp(col("dropoff_time")) - unix_timestamp(col("pickup_time"))) / 60.0
)

# 6) Keep only reasonable durations (0 < duration <= 240 mins)
trips_clean = trips_clean.filter(
    (col("trip_duration_min") > 0) & (col("trip_duration_min") <= 240)
)

# 7) Remove negative and crazy values
trips_clean = (
    trips_clean
    .filter(col("trip_distance") >= 0)
    .filter(col("fare_amount")   >= 0)
    .filter(col("total_amount")  >= 0)
    .filter(col("total_amount")  <= 1000)  # simple sanity cap
)

print("Silver Trips sample (cleaned):")
trips_clean.show(10, truncate=False)

from pyspark.sql.functions import year as spark_year

print("Distinct years in Silver pickup_time:")
(
    trips_clean
    .select(spark_year("pickup_time").alias("year"))
    .distinct()
    .orderBy("year")
    .show()
)

print("Row count in trips_clean:", trips_clean.count())


# COMMAND ----------

from pyspark.sql.functions import col

# Clean / typecast zones lookup
zones_silver = (
    zones_bronze_csv
    .withColumnRenamed("_c0", "LocationID")
    .withColumnRenamed("_c1", "Borough")
    .withColumnRenamed("_c2", "Zone")
    .withColumnRenamed("_c3", "service_zone")
    .withColumn("LocationID", col("LocationID").cast("int"))
)
zones_silver = zones_silver.filter(col("LocationID").isNotNull())
print("Silver Zones sample:")
zones_silver.show(10, truncate=False)

print("Row count in zones_silver:", zones_silver.count())


# COMMAND ----------

silver_trips_path = "abfss://silver@stp3taxilakehouse.dfs.core.windows.net/trips_clean/"
silver_zones_path = "abfss://silver@stp3taxilakehouse.dfs.core.windows.net/zones_clean/"

# Overwrite Silver folders
dbutils.fs.rm(silver_trips_path, True)
dbutils.fs.rm(silver_zones_path, True)

(
    trips_clean
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_trips_path)
)

(
    zones_silver
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_zones_path)
)

print("✅ Silver Delta written to:")
print(silver_trips_path)
print(silver_zones_path)


# COMMAND ----------

spark.sql("USE CATALOG dbw_p3_lakehouse")
spark.sql("USE SCHEMA nyc_taxi")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS trips_silver
USING DELTA
LOCATION '{silver_trips_path}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS zones_silver
USING DELTA
LOCATION '{silver_zones_path}'
""")

print("✅ Silver tables registered in dbw_p3_lakehouse.nyc_taxi")

display(spark.sql("SELECT * FROM trips_silver LIMIT 10"))
display(spark.sql("SELECT * FROM zones_silver LIMIT 10"))
