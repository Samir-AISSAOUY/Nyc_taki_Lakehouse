"""
Bronze Layer Verification Script

This script verifies the Bronze layer data quality and completeness.
Use after bronze_ingest.py to ensure data was loaded correctly.

Usage: spark-submit verify_bronze.py
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull

# Paths
BRONZE_TRIPS = "s3a://lake/bronze/trips"
BRONZE_ZONES = "s3a://lake/bronze/taxi_zone_lookup"

print("\n" + "=" * 60)
print("BRONZE LAYER VERIFICATION")
print("=" * 60)

# Create Spark session
spark = (SparkSession.builder
    .appName("verify_bronze")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate())

print(f"Spark version: {spark.version}")

# Verification results
verification_passed = True
issues_found = []

# Verify Trips data
print("\nVerifying Bronze Trips:")
print("-" * 30)

try:
    # Load Bronze trips data
    df_trips = spark.read.format("delta").load(BRONZE_TRIPS)
    trip_count = df_trips.count()
    
    print(f"Trip records found: {trip_count:,}")
    
    if trip_count == 0:
        verification_passed = False
        issues_found.append("No trip records found in Bronze layer")
        print("  ERROR: No records found")
    
    # Check for expected columns
    required_columns = [
        "tpep_pickup_datetime", 
        "tpep_dropoff_datetime",
        "passenger_count", 
        "trip_distance",
        "fare_amount",
        "PULocationID",
        "DOLocationID"
    ]
    
    missing_columns = [col for col in required_columns if col not in df_trips.columns]
    if missing_columns:
        verification_passed = False
        issues_found.append(f"Missing columns: {', '.join(missing_columns)}")
        print(f"  ERROR: Missing columns: {', '.join(missing_columns)}")
    else:
        print("  All required columns present")
    
    # Check for null values in critical columns
    if not missing_columns:
        null_counts = {}
        for column in required_columns:
            null_count = df_trips.filter(
                isnull(col(column)) | isnan(col(column))
            ).count()
            null_counts[column] = null_count
            
            if null_count > 0:
                pct_null = (null_count / trip_count) * 100
                if pct_null > 5:  # Alert if more than 5% nulls
                    issues_found.append(f"High null rate in {column}: {pct_null:.2f}%")
                    print(f"  WARNING: {column} has {null_count:,} null values ({pct_null:.2f}%)")
        
        # Print null summary
        print("\n  Null value summary:")
        for column, count in null_counts.items():
            if count > 0:
                print(f"    - {column}: {count:,} ({(count/trip_count)*100:.2f}%)")
            else:
                print(f"    - {column}: 0")
    
    # Check metadata columns
    metadata_columns = ["_source_file", "_ingestion_timestamp", "_ingestion_date", "_layer"]
    missing_metadata = [col for col in metadata_columns if col not in df_trips.columns]
    
    if missing_metadata:
        verification_passed = False
        issues_found.append(f"Missing metadata columns: {', '.join(missing_metadata)}")
        print(f"  ERROR: Missing metadata columns: {', '.join(missing_metadata)}")
    else:
        print("  All metadata columns present")
        
        # Verify layer value
        layer_values = df_trips.select("_layer").distinct().collect()
        if len(layer_values) != 1 or layer_values[0]["_layer"] != "bronze":
            verification_passed = False
            issues_found.append("Invalid _layer value (should be 'bronze')")
            print(f"  ERROR: Invalid _layer value: {[r['_layer'] for r in layer_values]}")
        else:
            print("  Layer value verified: bronze")
    
except Exception as e:
    verification_passed = False
    issues_found.append(f"Error loading bronze trips: {str(e)}")
    print(f"  ERROR: {str(e)}")

# Verify Zones data
print("\nVerifying Bronze Zones:")
print("-" * 30)

try:
    # Load Bronze zones data
    df_zones = spark.read.format("delta").load(BRONZE_ZONES)
    zone_count = df_zones.count()
    
    print(f"Zone records found: {zone_count:,}")
    
    if zone_count == 0:
        verification_passed = False
        issues_found.append("No zone records found in Bronze layer")
        print("  ERROR: No zone records found")
    
    # Check for expected columns
    required_zone_cols = ["LocationID", "Borough", "Zone"]
    
    missing_zone_cols = [col for col in required_zone_cols if col not in df_zones.columns]
    if missing_zone_cols:
        verification_passed = False
        issues_found.append(f"Missing zone columns: {', '.join(missing_zone_cols)}")
        print(f"  ERROR: Missing zone columns: {', '.join(missing_zone_cols)}")
    else:
        print("  All required zone columns present")
    
    # Count boroughs
    if "Borough" in df_zones.columns:
        borough_count = df_zones.select("Borough").distinct().count()
        print(f"  Distinct boroughs: {borough_count}")
        
        if borough_count < 5:  # NYC has 5 boroughs
            issues_found.append(f"Unexpected borough count: {borough_count} (expected 5+)")
            print("  WARNING: Fewer boroughs than expected")
    
        # Show borough distribution
        borough_dist = df_zones.groupBy("Borough").count().orderBy("count", ascending=False)
        print("\n  Borough distribution:")
        borough_dist.show(truncate=False)
    
except Exception as e:
    verification_passed = False
    issues_found.append(f"Error loading bronze zones: {str(e)}")
    print(f"  ERROR: {str(e)}")

# Overall verification results
print("\n" + "=" * 60)
print("VERIFICATION SUMMARY")
print("=" * 60)

if verification_passed:
    print("\n✅ BRONZE LAYER VERIFICATION PASSED")
    print("All checks completed successfully.")
else:
    print("\n❌ BRONZE LAYER VERIFICATION FAILED")
    print("Issues found:")
    for i, issue in enumerate(issues_found, 1):
        print(f"  {i}. {issue}")

print("\nNext steps:")
if verification_passed:
    print("- Proceed to Silver layer transformation")
else:
    print("- Fix identified issues in the Bronze layer")
    print("- Re-run bronze_ingest.py")
    print("- Verify again with this script")

# Exit with status code
spark.stop()
sys.exit(0 if verification_passed else 1)