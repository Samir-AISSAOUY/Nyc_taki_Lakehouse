#!/usr/bin/env python3
"""
Delta Lake Maintenance - OPTIMIZE & VACUUM

Operations:
- OPTIMIZE: Compact small files
- VACUUM: Remove old versions (7-day retention)
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

DELTA_TABLES = {
    "Bronze Trips": "s3a://lake/bronze/trips",
    "Bronze Zones": "s3a://lake/bronze/taxi_zone_lookup",
    "Silver Trips": "s3a://lake/silver/trips_cleaned",
    "Gold Daily": "s3a://lake/gold/trips_daily_metrics",
    "Gold Hourly": "s3a://lake/gold/trips_hourly_metrics",
    "Gold Borough": "s3a://lake/gold/trips_borough_metrics",
    "Gold Routes": "s3a://lake/gold/trips_popular_routes",
}

RETENTION_HOURS = 168  # 7 days

print("\nDELTA LAKE MAINTENANCE\n")

spark = (SparkSession.builder
    .appName("delta_maintenance")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate())

print(f"Spark version: {spark.version}")
print(f"Retention: {RETENTION_HOURS}h ({RETENTION_HOURS/24:.0f} days)\n")

success_count = 0
failure_count = 0
skipped_count = 0
results = []

total_tables = len(DELTA_TABLES)

print(f"PROCESSING {total_tables} TABLES\n")

for idx, (table_name, table_path) in enumerate(DELTA_TABLES.items(), 1):
    print(f"[{idx}/{total_tables}] {table_name}")
    print(f"Path: {table_path}\n")
    
    start_time = datetime.now()
    
    try:
        # Check if table exists
        try:
            delta_table = DeltaTable.forPath(spark, table_path)
        except Exception:
            print(f"  Table not found - skipping\n")
            skipped_count += 1
            results.append({
                "table": table_name,
                "status": "SKIPPED",
                "reason": "Not found"
            })
            continue
        
        # Get table info
        print("Table info:")
        df = spark.read.format("delta").load(table_path)
        record_count = df.count()
        files_before = len(df.inputFiles())
        print(f"   Records: {record_count:,}")
        print(f"   Files: {files_before:,}\n")
        
        # OPTIMIZE
        print("Running OPTIMIZE...")
        optimize_start = datetime.now()
        
        delta_table.optimize().executeCompaction()
        
        optimize_duration = (datetime.now() - optimize_start).total_seconds()
        print(f"   Completed in {optimize_duration:.2f}s")
        
        # VACUUM
        print(f"\nRunning VACUUM ({RETENTION_HOURS}h)...")
        vacuum_start = datetime.now()
        
        delta_table.vacuum(RETENTION_HOURS)
        
        vacuum_duration = (datetime.now() - vacuum_start).total_seconds()
        print(f"   Completed in {vacuum_duration:.2f}s")
        
        # Get final stats
        df_final = spark.read.format("delta").load(table_path)
        files_after = len(df_final.inputFiles())
        
        total_duration = (datetime.now() - start_time).total_seconds()
        
        print(f"\nSummary:")
        print(f"   Files: {files_before:,} -> {files_after:,}")
        print(f"   Reduction: {files_before - files_after:,}")
        print(f"   Time: {total_duration:.2f}s")
        print(f"   SUCCESS\n")
        
        success_count += 1
        results.append({
            "table": table_name,
            "status": "SUCCESS",
            "records": record_count,
            "files_before": files_before,
            "files_after": files_after,
            "duration": total_duration
        })
        
    except Exception as e:
        failure_count += 1
        duration = (datetime.now() - start_time).total_seconds()
        
        print(f"   ERROR: {str(e)}")
        print(f"   Duration: {duration:.2f}s\n")
        
        results.append({
            "table": table_name,
            "status": "FAILED",
            "error": str(e),
            "duration": duration
        })

# Final Summary
print("MAINTENANCE SUMMARY\n")

print(f"Tables processed: {total_tables}")
print(f"Successful: {success_count}")
print(f"Failed: {failure_count}")
print(f"Skipped: {skipped_count}\n")

if success_count > 0:
    print("Successful tables:")
    print(f"{'Table':<30} {'Records':>15} {'Files Before':>15} {'Files After':>15} {'Time':>10}")
    print("-" * 85)
    
    for result in results:
        if result["status"] == "SUCCESS":
            print(f"{result['table']:<30} {result['records']:>15,} "
                  f"{result['files_before']:>15,} {result['files_after']:>15,} "
                  f"{result['duration']:>9.2f}s")
    print()

if failure_count > 0:
    print(f"\nFailed tables:")
    for result in results:
        if result["status"] == "FAILED":
            print(f"   {result['table']}: {result.get('error', 'Unknown')}")
    print()

# Best Practices
print("BEST PRACTICES")
print("""
1. Run OPTIMIZE after large ingestions
2. VACUUM retention: 7 days allows time travel
3. Schedule during off-peak hours
4. Monitor file count trends
5. Target file size: 128MB - 1GB
""")

if failure_count == 0:
    print("MAINTENANCE COMPLETED!\n")
    spark.stop()
    sys.exit(0)
else:
    print(f"COMPLETED WITH {failure_count} ERROR(S)\n")
    spark.stop()
    sys.exit(0)  # Exit 0 to not block pipeline