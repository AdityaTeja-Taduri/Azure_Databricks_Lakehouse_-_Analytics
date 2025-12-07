```
                    ┌──────────────────────────────┐
                    │        Kaggle NYC Taxi        │
                    │         Raw Parquet           │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                ┌──────────────────────────────────────────┐
                │         Azure Data Lake Storage           │
                │           (stp3taxilakehouse)             │
                └──────────────────┬────────────────────────┘
                                   │
                         Raw Parquet Zone
                                   │
                                   ▼
        ┌────────────────────────────────────────────────────────┐
        │                   BRONZE LAYER                         │
        │ Raw → CSV Conversion                                   │
        │ • trips_csv                                            │
        │ • zones_csv                                            │
        └──────────────┬─────────────────────────────────────────┘
                       │ Cleaned schema, no transforms
                       ▼
        ┌────────────────────────────────────────────────────────┐
        │                   SILVER LAYER                         │
        │ Clean + Normalize                                      │
        │ • trips_silver: cleaned timestamps, distance filters    │
        │ • zones_silver: valid LocationID, removed NULL rows     │
        └──────────────┬─────────────────────────────────────────┘
                       │ Aggregations + Business Metrics
                       ▼
        ┌────────────────────────────────────────────────────────┐
        │                     GOLD LAYER                          │
        │ daily_zone_metrics_gold                                 │
        │ • trip_count                                            │
        │ • avg_fare                                              │
        │ • avg_trip_distance                                     │
        │ • total_revenue                                         │
        └──────────────┬─────────────────────────────────────────┘
                       │ Exposed via SQL Views
                       ▼
        ┌────────────────────────────────────────────────────────┐
        │                SQL VIEWS (Databricks SQL)              │
        │ v_daily_revenue                                        │
        │ v_borough_revenue_trend                                │
        │ v_top_zones_by_revenue                                 │
        │ v_weekday_performance                                  │
        └──────────────┬─────────────────────────────────────────┘
                       │
                       ▼
        ┌────────────────────────────────────────────────────────┐
        │                 Dashboard Consumers                     │
        │ 1. Databricks Dashboards (in-warehouse)                 │
        │ 2. Power BI (Direct Lake/Spark Connect)                │
        │    • Daily Revenue Trend                               │
        │    • Revenue by Borough                                │
        │    • Top 10 Zones by Revenue                           │
        │    • Daily Trips vs Avg Fare                           │
        └────────────────────────────────────────────────────────┘
```
