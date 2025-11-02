#!/usr/bin/env python3
"""
Silver Layer - Data Cleaning & Transformation

Input: s3a://lake/bronze/trips, s3a://lake/bronze/taxi_zone_lookup
Output: s3a://lake/silver/trips_cleaned
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, round as spark_round,
    upper, trim, when, hour, dayofweek, current_timestamp, 
    lit, year, month, dayofmonth
)
from pyspark.sql.types import IntegerType, DoubleType

BRONZE_TRIPS = "s3a://lake/bronze/trips"
BRONZE_ZONES = "s3a://lake/bronze/taxi_zone_lookup"
SILVER_TRIPS = "s3a://lake/silver/trips_cleaned"

print("\nSILVER LAYER - Data Transformation\n")

spark = (SparkSession.builder
    .appName("silver_transform")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate())

print(f"Spark version: {spark.version}\n")

# Step 1: Read Bronze Data
print("READING BRONZE DATA\n")

try:
    print(f"Reading trips: {BRONZE_TRIPS}")
    df_trips = spark.read.format("delta").load(BRONZE_TRIPS)
    initial_count = df_trips.count()
    print(f"Loaded: {initial_count:,} records\n")
    
    print(f"Reading zones: {BRONZE_ZONES}")
    df_zones = spark.read.format("delta").load(BRONZE_ZONES)
    zones_count = df_zones.count()
    print(f"Loaded: {zones_count:,} zones\n")
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    spark.stop()
    sys.exit(1)

# Step 2: Type Conversions
print("TYPE CONVERSIONS\n")

print("Converting data types...")
df_typed = (df_trips
    .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
    .withColumn("fare_amount", col("fare_amount").cast(DoubleType()))
    .withColumn("total_amount", col("total_amount").cast(DoubleType()))
    .withColumn("tip_amount", col("tip_amount").cast(DoubleType()))
    .withColumn("trip_distance", col("trip_distance").cast(DoubleType()))
    .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
    .withColumn("PULocationID", col("PULocationID").cast(IntegerType()))
    .withColumn("DOLocationID", col("DOLocationID").cast(IntegerType()))
)
print("Type conversions completed\n")

# Step 3: Data Quality Filters
print("QUALITY FILTERS\n")

before_filter = df_typed.count()
print(f"Before filtering: {before_filter:,}\n")

print("Applying filters...")
df_filtered = (df_typed
    .filter(col("fare_amount") > 0)
    .filter(col("total_amount") > 0)
    .filter(col("trip_distance") > 0)
    .filter(col("trip_distance") < 100)
    .filter((col("passenger_count") >= 1) & (col("passenger_count") <= 6))
    .filter(col("tpep_pickup_datetime") < col("tpep_dropoff_datetime"))
    .filter(col("PULocationID").isNotNull())
    .filter(col("DOLocationID").isNotNull())
)

after_filter = df_filtered.count()
removed = before_filter - after_filter
removal_pct = (removed / before_filter * 100) if before_filter > 0 else 0

print(f"After filtering: {after_filter:,}")
print(f"Removed: {removed:,} ({removal_pct:.2f}%)\n")

# Step 4: Feature Engineering
print("FEATURE ENGINEERING\n")

print("Creating features...")
df_features = (df_filtered
    .withColumn("trip_duration_minutes", 
        (col("tpep_dropoff_datetime").cast("long") - 
         col("tpep_pickup_datetime").cast("long")) / 60)
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .withColumn("pickup_dayofweek", dayofweek(col("tpep_pickup_datetime")))
    .withColumn("pickup_day", dayofmonth(col("tpep_pickup_datetime")))
    .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
    .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
    .withColumn("avg_speed_mph",
        when(col("trip_duration_minutes") > 0,
             spark_round(col("trip_distance") / (col("trip_duration_minutes") / 60), 2))
        .otherwise(0))
    .withColumn("tip_percentage",
        when(col("fare_amount") > 0,
             spark_round((col("tip_amount") / col("fare_amount")) * 100, 2))
        .otherwise(0))
    .withColumn("fare_per_mile",
        when(col("trip_distance") > 0,
             spark_round(col("fare_amount") / col("trip_distance"), 2))
        .otherwise(0))
    .withColumn("fare_amount", spark_round(col("fare_amount"), 2))
    .withColumn("total_amount", spark_round(col("total_amount"), 2))
    .withColumn("time_period",
        when((col("pickup_hour") >= 6) & (col("pickup_hour") < 12), "Morning")
        .when((col("pickup_hour") >= 12) & (col("pickup_hour") < 18), "Afternoon")
        .when((col("pickup_hour") >= 18) & (col("pickup_hour") < 22), "Evening")
        .otherwise("Night"))
    .withColumn("day_type",
        when(col("pickup_dayofweek").isin([1, 7]), "Weekend")
        .otherwise("Weekday"))
)

print("Features created\n")

# Step 5: Zone Enrichment
print("ZONE ENRICHMENT\n")

print("Joining with zones...")
# Prepare zones dataframe with clean column names
df_zones_clean = (df_zones
    .withColumn("LocationID", col("LocationID").cast(IntegerType()))
    .withColumn("Borough", upper(trim(col("Borough"))))
    .withColumn("Zone", trim(col("Zone"))))

# Debug: Print schema before joins
print("\nZones Schema:")
df_zones_clean.printSchema()

print("\nFeatures Schema:")
df_features.printSchema()

# Avoid duplicate column names by using explicit selection and aliases
df_with_pu = (df_features
    .join(
        df_zones_clean.select(
            col("LocationID").alias("pu_LocationID"),
            col("Borough").alias("PU_Borough"),
            col("Zone").alias("PU_Zone")
        ),
        col("PULocationID") == col("pu_LocationID"),
        "left"
    )
    .drop("pu_LocationID")
)

# Add DO location info
df_final = (df_with_pu
    .join(
        df_zones_clean.select(
            col("LocationID").alias("do_LocationID"),
            col("Borough").alias("DO_Borough"),
            col("Zone").alias("DO_Zone")
        ),
        col("DOLocationID") == col("do_LocationID"),
        "left"
    )
    .drop("do_LocationID")
    .withColumn("_transformation_timestamp", current_timestamp())
    .withColumn("_layer", lit("silver"))
)

final_count = df_final.count()
print(f"Zone enrichment completed: {final_count:,}\n")

# Step 6: Write to Delta Lake
print("WRITING TO DELTA LAKE\n")

print(f"Writing to: {SILVER_TRIPS}")
print("   Partitioning by: pickup_date\n")

try:
    write_start = datetime.now()
    
    (df_final
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("pickup_date")
        .option("overwriteSchema", "true")
        .option("maxRecordsPerFile", 1000000)
        .save(SILVER_TRIPS)
    )
    
    write_duration = (datetime.now() - write_start).total_seconds()
    
    df_verify = spark.read.format("delta").load(SILVER_TRIPS)
    verified_count = df_verify.count()
    
    print(f"SUCCESS!")
    print(f"   Records: {verified_count:,}")
    print(f"   Duration: {write_duration:.2f}s\n")
    
    print("Sample data:")
    df_verify.select(
        "pickup_date", "pickup_hour", "PU_Borough", "DO_Borough",
        "trip_distance", "fare_amount", "time_period"
    ).show(5, truncate=False)
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    spark.stop()
    sys.exit(1)

# Summary
print("SILVER LAYER COMPLETED!")
print(f"\nSummary:")
print(f"   Input:    {initial_count:>15,}")
print(f"   Output:   {verified_count:>15,}")
print(f"   Filtered: {removed:>15,} ({removal_pct:.2f}%)")
print(f"   Duration: {write_duration:>15.2f}s")
print(f"\nNext: Run quality checks\n")

spark.stop()
sys.exit(0)