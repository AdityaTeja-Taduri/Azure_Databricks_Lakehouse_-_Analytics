---

🚖 NYC Taxi Lakehouse Analytics — End-to-End Azure + Databricks Project
Modern Data Engineering | Analytics Engineering | Lakehouse Architecture

---

This project implements a full Bronze → Silver → Gold Lakehouse pipeline using Azure Databricks, Azure Data Lake Storage Gen2, and Power BI, built on top of the NYC Yellow Taxi 2024 dataset.
It demonstrates production-grade data ingestion, transformation, quality improvements, aggregation, SQL analytics, and dashboarding, all within the Databricks Lakehouse Platform.

---

⭐ Project Highlights

  - Complete Lakehouse architecture built from scratch.
  - Raw data ingestion from Kaggle → ADLS → Databricks.
  - Bronze Layer: Raw→CSV standardization.
  - Silver Layer: Cleansed and normalized taxi trip datasets.
  - Gold Layer: Business-ready metrics (revenue, trip volume, fares).
  - SQL Views powering dashboards in Databricks + Power BI.
  - Full GitHub repo with:
    - Notebooks (.py, .sql)
    - Exported Databricks notebooks (.dbc)
    - Dashboards (.pbix, .json)
    - Architecture diagram
    - Screenshots

---

🏗️ Architecture Overview

  ```
                      Raw Parquet (Kaggle NYC Taxi)
                               │
                               ▼
                  Azure Data Lake Storage (ADLS Gen2)
                               │
                     Raw Parquet → Bronze CSV
                               ▼
        ┌──────────────────────────────────────────────┐
        │                 BRONZE LAYER                 │
        │ Raw files standardized to CSV                │
        │ • trips_csv                                  │
        │ • zones_csv                                  │
        └───────────────────────┬──────────────────────┘
                                ▼
        ┌──────────────────────────────────────────────┐
        │                 SILVER LAYER                 │
        │ Cleaned + normalized tables                  │
        │ • trips_silver                               │
        │ • zones_silver                               │
        └───────────────────────┬──────────────────────┘
                                ▼
        ┌──────────────────────────────────────────────┐
        │                  GOLD LAYER                  │
        │ Aggregated business metrics                  │
        │ • daily_zone_metrics_gold                    │
        └───────────────────────┬──────────────────────┘
                                ▼
              Databricks SQL Views (Analytics Layer)
              • v_daily_revenue  
              • v_borough_revenue_trend
              • v_top_zones_by_revenue
              • v_weekday_performance
                                ▼
        ┌──────────────────────────────────────────────┐
        │         Databricks Dashboards + Power BI     │
        └──────────────────────────────────────────────┘
```

---

📂 Repository Structure

```
📦 Azure_Databricks_Lakehouse_Analytics
│
├── dashboards/
│   ├── NYC_Taxi_Lakehouse_Analytics.pbix
│   └── NYC Taxi Lakehouse Analytics.lvdash.json
│
├── databricks/
│   ├── 01_bronze_layer.dbc
│   ├── 02_silver_layer.dbc
│   ├── 03_gold_layer.dbc
│   └── 04_sql_views_and_dashboards.dbc
│
├── notebooks/
│   ├── 01_bronze_layer.py
│   ├── 02_silver_layer.py
│   ├── 03_gold_layer.py
│   └── 04_sql_views_and_dashboards.sql
│
├── sql/
│   ├── v_daily_revenue.sql
│   ├── v_borough_revenue_trend.sql
│   ├── v_top_zones_by_revenue.sql
│   └── v_weekday_performance.sql
│
├── screenshots/
│   ├── 01_Bronze_Trips_Preview.png
|   ├── 02_Bronze_Zones_Preview.png
│   ├── 03_Silver_Trips_Preview.png
│   ├── 04_Silver_Zones_Preview.png
│   ├── 05_Gold-Daily_Zone_Metrics_Preview.png
│   ├── 06_Gold_Tables_Catalog.png
│   ├── 001_Databricks_Dashboard_Preview.png
│   ├── 002_Databricks_Dashboard_Preview.png
│   └── powerbi_dashboard.png
│ 
└── README.md
```

---

🥉 BRONZE Layer — Raw → CSV Standardization
Objective:
Convert raw parquet files from ADLS into consistent CSV format for downstream processing.

Key Steps:
```
✔ Load raw parquet
✔ Convert to CSV
✔ Store in /bronze/nyc_taxi/
```
Output Tables:
```
  - trips_csv
  - zones_csv
```

---

🥈 SILVER Layer — Data Cleaning & Normalization
Cleaning Performed:
```
  - Timestamp conversion (pickup_time, dropoff_time)
  - Removed negative distances, invalid fares
  - Standardized LocationID
  - Removed null / corrupted zone rows
```
Output Tables:
```
  - trips_silver
  - zones_silver
```

---

🥇 GOLD Layer — Business Aggregations
Gold Table: daily_zone_metrics_gold

Metrics computed:
```
  - trip_count
  - avg_trip_distance
  - avg_total_amount
  - total_revenue
```
Purpose:
Provide analytical building blocks for BI dashboards.

---

📊 SQL Analytics Layer

1️⃣ Daily Revenue Trend:
```
v_daily_revenue.sql
```
Shows 2024 revenue patterns including dips/spikes.

2️⃣ Revenue by Borough:
```
v_borough_revenue_trend.sql
```
Identifies highest revenue-generating boroughs (Manhattan dominates).

3️⃣ Top 10 Zones by Revenue:
```
v_top_zones_by_revenue.sql
```
JFK & LaGuardia clearly lead.

4️⃣ Weekday Performance (Trips & Avg Fare):
```
v_weekday_performance.sql
```
Revenue peaks on Thursdays, dips on Fridays.

---

📈 Databricks Dashboard

Includes:

  - Daily revenue line chart
  - Borough comparison bar chart
  - Top revenue zones bar chart
  - Weekday performance (Trips + Avg Fare)

Located in:
```
/screenshots/001_Databricks_Dashborad_Preview.png
/screenshots/002_Databricks_Dashboard_Preview.png
```

---

🖥️ Power BI Dashboard

Visuals:

  - Daily Revenue Trend (2024)
  - Revenue by Borough
  - Top 10 Zones by Revenue
  - Daily Trips & Average Fare (2024)

File:
```
dashboards/NYC_Taxi_Lakehouse_Analytics.pbix
```

---

🔥 Skills Demonstrated

Data Engineering:

  - ADLS Gen2 setup
  - Databricks ingestion pipelines
  - Schema enforcement
  - Bronze → Silver → Gold transformations
  - Delta table optimization

Analytics Engineering:

  - SQL modeling
  - Business metrics pipeline
  - Reusable SQL views

BI & Visualization:

  - Databricks SQL Dashboards
  - Power BI – Direct Lake connections

Cloud & DevOps:

  - Azure Databricks
  - GitHub repo structure
  - Versioned notebooks & SQL files

---
