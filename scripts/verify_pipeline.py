#!/usr/bin/env python3
"""
Full Pipeline Verification Script

This script verifies the entire data pipeline from Bronze to Analytics.
Run after completing all pipeline stages to validate data quality and completeness.

Usage: spark-submit verify_pipeline.py
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

# Data layer paths
BRONZE_TRIPS = "s3a://lake/bronze/trips"
BRONZE_ZONES = "s3a://lake/bronze/taxi_zone_lookup"
SILVER_TRIPS = "s3a://lake/silver/trips_cleaned"
GOLD_DAILY = "s3a://lake/gold/trips_daily_metrics"
GOLD_HOURLY = "s3a://lake/gold/trips_hourly_metrics"
GOLD_BOROUGH = "s3a://lake/gold/trips_borough_metrics"
GOLD_ROUTES = "s3a://lake/gold/trips_popular_routes"
ANALYTICS_KPI = "s3a://lake/analytics/kpi_dashboard"
ANALYTICS_TRENDS = "s3a://lake/analytics/time_series_trends"
ANALYTICS_HEATMAP = "s3a://lake/analytics/hourly_heatmap"
ANALYTICS_TOP_ROUTES = "s3a://lake/analytics/top_routes_analysis"

# Define expected table properties
LAYER_INFO = {
    "Bronze Trips": {"path": BRONZE_TRIPS, "layer": "bronze", "min_records": 10000000},
    "Bronze Zones": {"path": BRONZE_ZONES, "layer": "bronze", "min_records": 200},
    "Silver Trips": {"path": SILVER_TRIPS, "layer": "silver", "min_records": 8000000},
    "Gold Daily": {"path": GOLD_DAILY, "layer": "gold", "min_records": 10},
    "Gold Hourly": {"path": GOLD_HOURLY, "layer": "gold", "min_records": 100},
    "Gold Borough": {"path": GOLD_BOROUGH, "layer": "gold", "min_records": 20},
    "Gold Routes": {"path": GOLD_ROUTES, "layer": "gold", "min_records": 100},
    "Analytics KPI": {"path": ANALYTICS_KPI, "layer": "analytics", "min_records": 5},
    "Analytics Trends": {"path": ANALYTICS_TRENDS, "layer": "analytics", "min_records": 10},
    "Analytics Heatmap": {"path": ANALYTICS_HEATMAP, "layer": "analytics", "min_records": 100},
    "Analytics Routes": {"path": ANALYTICS_TOP_ROUTES, "layer": "analytics", "min_records": 20}
}

print("\n" + "=" * 70)
print("NYC TAXI DATA PIPELINE VERIFICATION")
print("=" * 70)
print("\nChecking all data layers for completeness and quality...\n")

# Create Spark session
spark = (SparkSession.builder
    .appName("verify_pipeline")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate())

print(f"Spark version: {spark.version}")
start_time = datetime.now()

# Track verification status
all_passed = True
layers_status = {"bronze": True, "silver": True, "gold": True, "analytics": True}
verification_results = []

# Check each table
for table_name, info in LAYER_INFO.items():
    path = info["path"]
    expected_layer = info["layer"]
    min_records = info["min_records"]
    
    print(f"\nVerifying {table_name}:")
    print(f"Path: {path}")
    print("-" * 50)
    
    try:
        # Check if table exists
        df = spark.read.format("delta").load(path)
        
        # Count records
        record_count = df.count()
        print(f"Records: {record_count:,}")
        
        # Validate record count
        if record_count < min_records:
            status = "WARNING"
            message = f"Record count {record_count:,} is below minimum expected ({min_records:,})"
            print(f"⚠️ {message}")
            layers_status[expected_layer] = False
        else:
            status = "PASSED"
            message = f"Record count {record_count:,} meets minimum requirement"
            print(f"✅ {message}")
            
        # Check for layer metadata if applicable
        has_layer_column = "_layer" in df.columns
        if has_layer_column:
            layer_values = df.select("_layer").distinct().collect()
            actual_layers = [r["_layer"] for r in layer_values]
            
            if expected_layer not in actual_layers:
                status = "WARNING"
                message = f"Layer value mismatch: expected '{expected_layer}', found {actual_layers}"
                print(f"⚠️ {message}")
                layers_status[expected_layer] = False
            else:
                message = f"Layer value verified: {expected_layer}"
                print(f"✅ {message}")
                
        # For Silver layer, check data quality
        if expected_layer == "silver":
            null_pickup = df.filter(col("tpep_pickup_datetime").isNull()).count()
            null_dropoff = df.filter(col("tpep_dropoff_datetime").isNull()).count()
            null_distance = df.filter(col("trip_distance").isNull()).count()
            null_fare = df.filter(col("fare_amount").isNull()).count()
            
            null_summary = f"Null values: pickup={null_pickup}, dropoff={null_dropoff}, distance={null_distance}, fare={null_fare}"
            print(f"Quality check: {null_summary}")
            
            if null_pickup + null_dropoff + null_distance + null_fare > 0:
                status = "WARNING"
                message = "Found null values in critical columns"
                print(f"⚠️ {message}")
                layers_status[expected_layer] = False
        
        # For Gold layer, check aggregations
        if expected_layer == "gold" and table_name == "Gold Daily":
            total_trips = df.agg(spark_sum("total_trips").alias("sum")).collect()[0]["sum"]
            total_revenue = df.agg(spark_sum("total_revenue").alias("sum")).collect()[0]["sum"]
            
            print(f"Aggregation check: {total_trips:,} total trips, ${total_revenue:,.2f} total revenue")
            
            if total_trips < min_records * 1000 or total_revenue < min_records * 1000:
                status = "WARNING"
                message = "Aggregation values lower than expected"
                print(f"⚠️ {message}")
                layers_status[expected_layer] = False
                
        verification_results.append({
            "table": table_name,
            "status": status,
            "records": record_count,
            "message": message
        })
                
    except Exception as e:
        status = "FAILED"
        message = str(e)
        print(f"❌ ERROR: {message}")
        verification_results.append({
            "table": table_name,
            "status": status,
            "records": 0,
            "message": message
        })
        all_passed = False
        layers_status[expected_layer] = False

# Summarize verification results
print("\n" + "=" * 70)
print("PIPELINE VERIFICATION SUMMARY")
print("=" * 70)

# Calculate statistics
total_tables = len(verification_results)
passed_tables = sum(1 for r in verification_results if r["status"] == "PASSED")
warning_tables = sum(1 for r in verification_results if r["status"] == "WARNING")
failed_tables = sum(1 for r in verification_results if r["status"] == "FAILED")

pass_rate = (passed_tables / total_tables) * 100 if total_tables > 0 else 0
total_records = sum(r["records"] for r in verification_results)

# Display summary table
print(f"\nVerified {total_tables} tables across all layers")
print(f"Pass rate: {pass_rate:.1f}%")
print(f"Total records: {total_records:,}")
print(f"\nResults by status:")
print(f"  Passed:  {passed_tables}")
print(f"  Warning: {warning_tables}")
print(f"  Failed:  {failed_tables}")

print(f"\nResults by layer:")
for layer, status in layers_status.items():
    icon = "✅" if status else "❌"
    print(f"  {icon} {layer.capitalize()} Layer: {'PASSED' if status else 'ISSUES FOUND'}")

# Show details for issues
if warning_tables + failed_tables > 0:
    print("\nIssues found:")
    for i, result in enumerate([r for r in verification_results if r["status"] != "PASSED"], 1):
        print(f"  {i}. {result['table']}: {result['message']}")

# Execution time
end_time = datetime.now()
duration = (end_time - start_time).total_seconds()
print(f"\nVerification completed in {duration:.2f} seconds")

# Final verdict
print("\n" + "=" * 70)
if all_passed and all(status for status in layers_status.values()):
    print("✅ PIPELINE VERIFICATION SUCCESSFUL")
    print("All layers passed validation checks.")
    print("\nThe data pipeline is ready for production use!")
else:
    print("⚠️ PIPELINE VERIFICATION COMPLETED WITH ISSUES")
    print("Some layers have warnings or errors that should be addressed.")
    print("\nRecommendations:")
    
    if not layers_status["bronze"]:
        print("- Review bronze_ingest.py and check data sources")
    
    if not layers_status["silver"]:
        print("- Validate silver_transform.py and quality checks")
    
    if not layers_status["gold"]:
        print("- Examine gold_agg.py aggregation logic")
    
    if not layers_status["analytics"]:
        print("- Verify analytics_layer.py calculations")

# Exit with appropriate status code
spark.stop()
sys.exit(0 if all_passed else 1)