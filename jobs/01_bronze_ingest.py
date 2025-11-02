#!/usr/bin/env python3
"""
Bronze Layer - Raw Data Ingestion

Input: /data/raw/*.parquet + taxi_zone_lookup.csv
Output: s3a://lake/bronze/trips + s3a://lake/bronze/taxi_zone_lookup
"""

import sys
import glob
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, lit

RAW_DIR = "/data/raw"
BRONZE_TRIPS = "s3a://lake/bronze/trips"
BRONZE_ZONES = "s3a://lake/bronze/taxi_zone_lookup"

print("\nBRONZE LAYER - Raw Data Ingestion\n")

# Create Spark session
spark = (SparkSession.builder
    .appName("bronze_ingest")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.parquet.mergeSchema", "true")
    .getOrCreate())

print(f"Spark version: {spark.version}")
print(f"Raw data directory: {RAW_DIR}\n")

# Part 1: Ingest Trip Data (Parquet)
print("INGESTING TRIP DATA\n")

try:
    print("Searching for Parquet files...")
    trip_files = sorted(glob.glob(f"{RAW_DIR}/yellow_tripdata_2023-*.parquet"))
    
    if not trip_files:
        raise FileNotFoundError(f"No Parquet files found in {RAW_DIR}")
    
    print(f"Found {len(trip_files)} Parquet files:")
    for i, file in enumerate(trip_files, 1):
        file_name = file.split('/')[-1]
        print(f"   {i:2d}. {file_name}")
    
    print(f"\nReading Parquet files...")
    df_trips = spark.read.parquet(trip_files[0])

    # Read and union other files one by one
    for file in trip_files[1:]:
        df_temp = spark.read.parquet(file)
        df_trips = df_trips.unionByName(df_temp, allowMissingColumns=True)
    
    print("Counting records...")
    initial_count = df_trips.count()
    print(f"Total records: {initial_count:,}\n")
    
    print("Data schema:")
    df_trips.printSchema()
    
    print("\nAdding metadata columns...")
    df_trips_enriched = (df_trips
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_ingestion_date", lit(datetime.now().date()))
        .withColumn("_layer", lit("bronze"))
    )
    
    print(f"Writing to Delta Lake: {BRONZE_TRIPS}")
    print("   This may take several minutes...\n")
    
    write_start = datetime.now()
    
    (df_trips_enriched
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("dataChange", "true")
        .save(BRONZE_TRIPS)
    )
    
    write_duration = (datetime.now() - write_start).total_seconds()
    
    # Verify write
    df_verify = spark.read.format("delta").load(BRONZE_TRIPS)
    final_count = df_verify.count()
    
    print(f"SUCCESS - Trip data ingestion completed!")
    print(f"   Records written: {final_count:,}")
    print(f"   Duration: {write_duration:.2f} seconds")
    print(f"   Throughput: {final_count/write_duration:,.0f} records/second\n")
    
    print("Sample data (first 5 rows):")
    df_verify.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount"
    ).show(5, truncate=False)
    
except Exception as e:
    print(f"\nERROR: {str(e)}")
    spark.stop()
    sys.exit(1)

# Part 2: Ingest Zone Lookup (CSV)
print("\nINGESTING ZONE LOOKUP DATA\n")

try:
    zone_csv = f"{RAW_DIR}/taxi_zone_lookup.csv"
    print(f"Reading CSV: {zone_csv}")
    
    df_zones = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(zone_csv)
    )
    
    zones_count = df_zones.count()
    print(f"Zones loaded: {zones_count:,}\n")
    
    print("Zone schema:")
    df_zones.printSchema()
    
    print("\nAdding metadata...")
    df_zones_enriched = (df_zones
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_ingestion_date", lit(datetime.now().date()))
        .withColumn("_layer", lit("bronze"))
    )
    
    print(f"\nWriting to Delta Lake: {BRONZE_ZONES}")
    
    (df_zones_enriched
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_ZONES)
    )
    
    df_zones_verify = spark.read.format("delta").load(BRONZE_ZONES)
    zones_final = df_zones_verify.count()
    
    print(f"SUCCESS - Zone data ingestion completed!")
    print(f"   Records written: {zones_final:,}\n")
    
    print("Sample zones:")
    df_zones_verify.select("LocationID", "Borough", "Zone").show(10, truncate=False)
    
except Exception as e:
    print(f"\nERROR: {str(e)}")
    spark.stop()
    sys.exit(1)

# Summary
print("\nBRONZE LAYER COMPLETED!")
print(f"\nSummary:")
print(f"   Trip records:  {final_count:>15,} -> {BRONZE_TRIPS}")
print(f"   Zone records:  {zones_final:>15,} -> {BRONZE_ZONES}")
print(f"   Duration:      {write_duration:>15.2f}s")
print(f"\nNext: Run Silver layer transformation\n")

spark.stop()
sys.exit(0)