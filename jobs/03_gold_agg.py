#!/usr/bin/env python3
"""
Gold Layer - Business Metrics Aggregation

Input: s3a://lake/silver/trips_cleaned
Output: s3a://lake/gold/* (4 tables)
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    round as spark_round, current_timestamp, lit
)

SILVER_TRIPS = "s3a://lake/silver/trips_cleaned"
GOLD_DAILY = "s3a://lake/gold/trips_daily_metrics"
GOLD_HOURLY = "s3a://lake/gold/trips_hourly_metrics"
GOLD_BOROUGH = "s3a://lake/gold/trips_borough_metrics"
GOLD_ROUTES = "s3a://lake/gold/trips_popular_routes"

print("\nGOLD LAYER - Business Aggregations\n")

spark = (SparkSession.builder
    .appName("gold_aggregate")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate())

# Read Silver data
try:
    print(f"Reading: {SILVER_TRIPS}")
    df = spark.read.format("delta").load(SILVER_TRIPS)
    total_count = df.count()
    print(f"Loaded: {total_count:,} records\n")
except Exception as e:
    print(f"ERROR: {str(e)}")
    spark.stop()
    sys.exit(1)

results = []

# 1. Daily Metrics
print("DAILY METRICS\n")

print("Calculating...")
daily = (df
    .groupBy("pickup_date")
    .agg(
        count("*").alias("total_trips"),
        spark_round(spark_sum("fare_amount"), 2).alias("total_fare"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 3).alias("avg_distance"),
        spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration"),
        spark_round(avg("tip_percentage"), 2).alias("avg_tip_pct"),
        spark_sum("passenger_count").alias("total_passengers")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
    .withColumn("_layer", lit("gold"))
    .orderBy("pickup_date")
)

daily_count = daily.count()
print(f"Generated: {daily_count} days\n")

print(f"Writing to: {GOLD_DAILY}")
try:
    (daily
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_DAILY)
    )
    print(f"Daily metrics saved\n")
    daily.show(5, truncate=False)
    results.append(("Daily Metrics", GOLD_DAILY, daily_count))
except Exception as e:
    print(f"ERROR: {str(e)}")
    spark.stop()
    sys.exit(1)

# 2. Hourly Metrics
print("HOURLY METRICS\n")

print("Calculating...")
hourly = (df
    .groupBy("pickup_date", "pickup_hour")
    .agg(
        count("*").alias("total_trips"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 3).alias("avg_distance"),
        spark_round(avg("avg_speed_mph"), 2).alias("avg_speed")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
    .withColumn("_layer", lit("gold"))
    .orderBy("pickup_date", "pickup_hour")
)

hourly_count = hourly.count()
print(f"Generated: {hourly_count} hours\n")

print(f"Writing to: {GOLD_HOURLY}")
try:
    (hourly
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("pickup_date")
        .option("overwriteSchema", "true")
        .save(GOLD_HOURLY)
    )
    print(f"Hourly metrics saved\n")
    results.append(("Hourly Metrics", GOLD_HOURLY, hourly_count))
except Exception as e:
    print(f"ERROR: {str(e)}")
    spark.stop()
    sys.exit(1)

# 3. Borough Metrics
print("BOROUGH METRICS\n")

print("Calculating...")
borough = (df
    .filter(col("PU_Borough").isNotNull())
    .groupBy("pickup_date", "PU_Borough")
    .agg(
        count("*").alias("total_trips"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 3).alias("avg_distance")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
    .withColumn("_layer", lit("gold"))
    .orderBy("pickup_date", "PU_Borough")
)

borough_count = borough.count()
print(f"Generated: {borough_count} borough-days\n")

print(f"Writing to: {GOLD_BOROUGH}")
try:
    (borough
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("pickup_date")
        .option("overwriteSchema", "true")
        .save(GOLD_BOROUGH)
    )
    print(f"Borough metrics saved\n")
    
    # Show top boroughs
    print("Top boroughs by revenue:")
    borough_summary = (borough
        .groupBy("PU_Borough")
        .agg(spark_round(spark_sum("total_revenue"), 2).alias("revenue"))
        .orderBy(col("revenue").desc())
    )
    borough_summary.show(5, truncate=False)
    
    results.append(("Borough Metrics", GOLD_BOROUGH, borough_count))
except Exception as e:
    print(f"ERROR: {str(e)}")
    spark.stop()
    sys.exit(1)

# 4. Popular Routes
print("POPULAR ROUTES\n")

print("Calculating...")
routes = (df
    .filter(col("PU_Zone").isNotNull() & col("DO_Zone").isNotNull())
    .groupBy("PU_Borough", "PU_Zone", "DO_Borough", "DO_Zone")
    .agg(
        count("*").alias("total_trips"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 3).alias("avg_distance")
    )
    .withColumn("_aggregation_timestamp", current_timestamp())
    .withColumn("_layer", lit("gold"))
    .orderBy(col("total_trips").desc())
)

routes_count = routes.count()
print(f"Generated: {routes_count} routes\n")

print(f"Writing to: {GOLD_ROUTES}")
try:
    (routes
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_ROUTES)
    )
    print(f"Routes saved\n")
    
    print("Top 10 routes:")
    routes.select("PU_Zone", "DO_Zone", "total_trips", "avg_fare").show(10, truncate=False)
    
    results.append(("Popular Routes", GOLD_ROUTES, routes_count))
except Exception as e:
    print(f"ERROR: {str(e)}")
    spark.stop()
    sys.exit(1)

# Summary
print("\nAGGREGATION SUMMARY\n")

print(f"{'Table':<25} {'Records':>15} {'Location':<50}")
print("-" * 90)
for name, path, count in results:
    print(f"{name:<25} {count:>15,} {path:<50}")

total_gold = sum([count for _, _, count in results])
print("-" * 90)
print(f"{'TOTAL GOLD RECORDS':<25} {total_gold:>15,}")

print("\nGOLD LAYER COMPLETED!")
print(f"\nSummary:")
print(f"   Input (Silver):  {total_count:>15,} records")
print(f"   Output (Gold):   {total_gold:>15,} records")
print(f"   Tables created:  {len(results):>15}")
print(f"\nNext: Run Delta maintenance\n")

spark.stop()
sys.exit(0)