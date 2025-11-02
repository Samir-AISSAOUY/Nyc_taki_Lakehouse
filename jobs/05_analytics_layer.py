#!/usr/bin/env python3
"""
Analytics Layer - Advanced Business Intelligence
Prepares data for Tableau Desktop

Input: s3a://lake/gold/*
Output: s3a://lake/analytics/* + CSV exports for Tableau
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    round as spark_round, current_timestamp, lit, when, dayofweek,
    lag, lead, percent_rank, dense_rank, row_number
)
from pyspark.sql.window import Window

# Data paths
GOLD_DAILY = "s3a://lake/gold/trips_daily_metrics"
GOLD_HOURLY = "s3a://lake/gold/trips_hourly_metrics"
GOLD_BOROUGH = "s3a://lake/gold/trips_borough_metrics"
GOLD_ROUTES = "s3a://lake/gold/trips_popular_routes"
SILVER_TRIPS = "s3a://lake/silver/trips_cleaned"

# Analytics output paths
ANALYTICS_KPI = "s3a://lake/analytics/kpi_dashboard"
ANALYTICS_TRENDS = "s3a://lake/analytics/time_series_trends"
ANALYTICS_HEATMAP = "s3a://lake/analytics/hourly_heatmap"
ANALYTICS_TOP_ROUTES = "s3a://lake/analytics/top_routes_analysis"

# CSV export directory for Tableau
CSV_EXPORT_DIR = "/data/tableau_exports"

print("\nANALYTICS LAYER - Business Intelligence\n")

# Initialize Spark session
spark = (SparkSession.builder
    .appName("analytics_layer")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate())

# ============================================
# 1. KPI Dashboard Data
# ============================================
print("1. KPI DASHBOARD\n")

print("Calculating global KPIs...")

# Load data
df_daily = spark.read.format("delta").load(GOLD_DAILY)
df_silver = spark.read.format("delta").load(SILVER_TRIPS)

# Calculate global KPIs
kpi_data = df_daily.agg(
    spark_sum("total_trips").alias("total_trips_ytd"),
    spark_round(spark_sum("total_revenue"), 2).alias("total_revenue_ytd"),
    spark_round(avg("avg_fare"), 2).alias("avg_fare_ytd"),
    spark_round(avg("avg_distance"), 2).alias("avg_distance_ytd"),
    spark_round(avg("avg_duration"), 2).alias("avg_duration_ytd"),
    count("pickup_date").alias("days_in_operation")
).withColumn("avg_trips_per_day", 
    spark_round(col("total_trips_ytd") / col("days_in_operation"), 0))

# Calculate Month over Month Growth
df_daily_with_month = df_daily.withColumn(
    "year_month", 
    col("pickup_date").substr(1, 7)
)

monthly_metrics = df_daily_with_month.groupBy("year_month").agg(
    spark_sum("total_trips").alias("monthly_trips"),
    spark_round(spark_sum("total_revenue"), 2).alias("monthly_revenue"),
    spark_round(avg("avg_fare"), 2).alias("monthly_avg_fare")
).orderBy("year_month")

# Add month-over-month growth calculations
window_spec = Window.orderBy("year_month")
monthly_metrics = monthly_metrics.withColumn(
    "prev_month_trips", lag("monthly_trips").over(window_spec)
).withColumn(
    "mom_growth_pct",
    when(col("prev_month_trips").isNotNull(),
         spark_round(((col("monthly_trips") - col("prev_month_trips")) / col("prev_month_trips")) * 100, 2)
    ).otherwise(0)
)

# Find best and worst performing days
best_days = df_daily.orderBy(col("total_revenue").desc()).limit(10).select(
    "pickup_date", "total_trips", "total_revenue", "avg_fare"
).withColumn("rank_type", lit("Top 10 Days"))

worst_days = df_daily.orderBy(col("total_revenue").asc()).limit(10).select(
    "pickup_date", "total_trips", "total_revenue", "avg_fare"
).withColumn("rank_type", lit("Bottom 10 Days"))

performance_ranking = best_days.union(worst_days)

print("KPI metrics calculated\n")
kpi_data.show(truncate=False)
monthly_metrics.show(5, truncate=False)

# Save to Delta Lake
print(f"Writing to: {ANALYTICS_KPI}")
monthly_metrics.write.format("delta").mode("overwrite").save(ANALYTICS_KPI)

# Export to CSV for Tableau
print(f"Exporting KPI data to CSV...")
monthly_metrics.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/kpi_monthly")
performance_ranking.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/performance_ranking")

# ============================================
# 2. Time Series Trends Analysis
# ============================================
print("\n2. TIME SERIES TRENDS\n")

print("Analyzing temporal patterns...")

# Create daily trends with moving averages
window_7d = Window.orderBy("pickup_date").rowsBetween(-6, 0)
window_30d = Window.orderBy("pickup_date").rowsBetween(-29, 0)

trends = df_daily.select(
    "pickup_date",
    "total_trips",
    "total_revenue",
    "avg_fare",
    "avg_distance"
).withColumn(
    "trips_ma7",
    spark_round(avg("total_trips").over(window_7d), 0)
).withColumn(
    "trips_ma30",
    spark_round(avg("total_trips").over(window_30d), 0)
).withColumn(
    "revenue_ma7",
    spark_round(avg("total_revenue").over(window_7d), 2)
).withColumn(
    "revenue_ma30",
    spark_round(avg("total_revenue").over(window_30d), 2)
).withColumn(
    "day_of_week",
    dayofweek("pickup_date")
).withColumn(
    "is_weekend",
    when(col("day_of_week").isin([1, 7]), "Weekend").otherwise("Weekday")
)

# Compare weekday vs weekend performance
weekday_comparison = trends.groupBy("is_weekend").agg(
    spark_round(avg("total_trips"), 0).alias("avg_daily_trips"),
    spark_round(avg("total_revenue"), 2).alias("avg_daily_revenue"),
    spark_round(avg("avg_fare"), 2).alias("avg_fare")
)

print("Weekday vs Weekend:")
weekday_comparison.show(truncate=False)

# Save to Delta Lake
print(f"Writing to: {ANALYTICS_TRENDS}")
trends.write.format("delta").mode("overwrite").save(ANALYTICS_TRENDS)

# Export to CSV
trends.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/daily_trends")
weekday_comparison.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/weekday_comparison")

# ============================================
# 3. Hourly Heatmap Data
# ============================================
print("\n3. HOURLY HEATMAP\n")

print("Creating heatmap data...")

df_hourly = spark.read.format("delta").load(GOLD_HOURLY)

# Pivot by hour for heatmap visualization
heatmap = df_hourly.withColumn(
    "day_of_week",
    dayofweek("pickup_date")
).withColumn(
    "day_name",
    when(col("day_of_week") == 1, "Sunday")
    .when(col("day_of_week") == 2, "Monday")
    .when(col("day_of_week") == 3, "Tuesday")
    .when(col("day_of_week") == 4, "Wednesday")
    .when(col("day_of_week") == 5, "Thursday")
    .when(col("day_of_week") == 6, "Friday")
    .otherwise("Saturday")
).groupBy("day_name", "day_of_week", "pickup_hour").agg(
    spark_round(avg("total_trips"), 0).alias("avg_trips"),
    spark_round(avg("avg_fare"), 2).alias("avg_fare"),
    spark_round(avg("avg_speed"), 2).alias("avg_speed")
).orderBy("day_of_week", "pickup_hour")

# Identify peak hours
peak_hours = df_hourly.groupBy("pickup_hour").agg(
    spark_sum("total_trips").alias("total_trips_by_hour"),
    spark_round(avg("avg_fare"), 2).alias("avg_fare_by_hour")
).orderBy(col("total_trips_by_hour").desc())

print("Top 5 busiest hours:")
peak_hours.show(5, truncate=False)

# Save to Delta Lake
print(f"Writing to: {ANALYTICS_HEATMAP}")
heatmap.write.format("delta").mode("overwrite").save(ANALYTICS_HEATMAP)

# Export to CSV
heatmap.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/hourly_heatmap")
peak_hours.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/peak_hours")

# ============================================
# 4. Top Routes Deep Dive
# ============================================
print("\n4. TOP ROUTES ANALYSIS\n")

print("Analyzing route profitability...")

df_routes = spark.read.format("delta").load(GOLD_ROUTES)

# Find top 100 routes by volume and revenue
top_routes_volume = df_routes.orderBy(col("total_trips").desc()).limit(100).withColumn(
    "total_revenue", spark_round(col("total_trips") * col("avg_fare"), 2)
).withColumn(
    "rank_by_volume", row_number().over(Window.orderBy(col("total_trips").desc()))
)

# Identify most profitable routes (revenue per trip)
top_routes_profit = df_routes.withColumn(
    "revenue_per_trip", col("avg_fare")
).orderBy(col("revenue_per_trip").desc()).limit(100).withColumn(
    "rank_by_profit", row_number().over(Window.orderBy(col("revenue_per_trip").desc()))
)

# Calculate route efficiency (revenue per mile)
route_efficiency = df_routes.withColumn(
    "revenue_per_mile", 
    spark_round(col("avg_fare") / col("avg_distance"), 2)
).filter(col("avg_distance") > 0).orderBy(col("revenue_per_mile").desc()).limit(100)

# Compare inter-borough vs intra-borough trips
borough_flow = df_routes.withColumn(
    "flow_type",
    when(col("PU_Borough") == col("DO_Borough"), "Intra-Borough")
    .otherwise("Inter-Borough")
).groupBy("flow_type").agg(
    spark_sum("total_trips").alias("total_trips"),
    spark_round(avg("avg_fare"), 2).alias("avg_fare"),
    spark_round(avg("avg_distance"), 2).alias("avg_distance")
)

print("Inter vs Intra Borough:")
borough_flow.show(truncate=False)

# Save to Delta Lake
print(f"Writing to: {ANALYTICS_TOP_ROUTES}")
top_routes_volume.write.format("delta").mode("overwrite").save(ANALYTICS_TOP_ROUTES)

# Export to CSV
top_routes_volume.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/top_routes_volume")
top_routes_profit.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/top_routes_profit")
route_efficiency.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/route_efficiency")
borough_flow.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/borough_flow")

# ============================================
# 5. Borough Performance Comparison
# ============================================
print("\n5. BOROUGH COMPARISON\n")

df_borough = spark.read.format("delta").load(GOLD_BOROUGH)

# Summarize performance by borough
borough_summary = df_borough.groupBy("PU_Borough").agg(
    spark_sum("total_trips").alias("total_trips"),
    spark_round(spark_sum("total_revenue"), 2).alias("total_revenue"),
    spark_round(avg("avg_fare"), 2).alias("avg_fare"),
    spark_round(avg("avg_distance"), 2).alias("avg_distance")
).withColumn(
    "revenue_rank",
    dense_rank().over(Window.orderBy(col("total_revenue").desc()))
).orderBy("revenue_rank")

print("Borough Performance:")
borough_summary.show(truncate=False)

# Export to CSV
borough_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{CSV_EXPORT_DIR}/borough_summary")

# ============================================
# Summary
# ============================================
print("\nANALYTICS LAYER COMPLETED!")
print(f"\nExports created in: {CSV_EXPORT_DIR}")
print(f"\nTableau-ready CSV files:")
print(f"  1. kpi_monthly/")
print(f"  2. daily_trends/")
print(f"  3. hourly_heatmap/")
print(f"  4. top_routes_volume/")
print(f"  5. borough_summary/")
print(f"  6. performance_ranking/")
print(f"  7. weekday_comparison/")
print(f"  8. peak_hours/")
print(f"  9. route_efficiency/")
print(f" 10. borough_flow/")
print(f"\nNext: Import CSV files into Tableau Desktop")

spark.stop()
sys.exit(0)