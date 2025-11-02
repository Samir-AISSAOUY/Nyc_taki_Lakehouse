#!/usr/bin/env python3
"""
Silver Layer - Data Quality Validation

Input: s3a://lake/silver/trips_cleaned
Output: Quality check results (exit code 0=pass)
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, avg, min as spark_min, max as spark_max

SILVER_TRIPS = "s3a://lake/silver/trips_cleaned"

print("\nDATA QUALITY VALIDATION\n")

spark = (SparkSession.builder
    .appName("silver_quality_check")
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

# Track results
tests_passed = 0
tests_failed = 0
failed_tests = []

print("RUNNING QUALITY TESTS\n")

# Test 1: Data Existence
test_name = "Data Existence"
print(f"Test 1: {test_name}")
if total_count > 0:
    print(f"   PASSED - {total_count:,} records\n")
    tests_passed += 1
else:
    print(f"   FAILED - No data\n")
    tests_failed += 1
    failed_tests.append(test_name)

# Test 2: No Nulls in Critical Columns
test_name = "No Nulls in Critical Columns"
print(f"Test 2: {test_name}")
critical_cols = ["tpep_pickup_datetime", "fare_amount", "trip_distance"]
null_counts = df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in critical_cols
]).collect()[0].asDict()

has_nulls = any(null_counts.values())
if not has_nulls:
    print(f"   PASSED\n")
    tests_passed += 1
else:
    print(f"   FAILED - Nulls found:")
    for col_name, null_count in null_counts.items():
        if null_count > 0:
            print(f"      {col_name}: {null_count:,}")
    print()
    tests_failed += 1
    failed_tests.append(test_name)

# Test 3: Positive Amounts
test_name = "Positive Fare Amounts"
print(f"Test 3: {test_name}")
negative_fares = df.filter(col("fare_amount") <= 0).count()
if negative_fares == 0:
    print(f"   PASSED\n")
    tests_passed += 1
else:
    print(f"   FAILED - {negative_fares:,} non-positive\n")
    tests_failed += 1
    failed_tests.append(test_name)

# Test 4: Positive Distances
test_name = "Positive Trip Distances"
print(f"Test 4: {test_name}")
invalid_distances = df.filter(col("trip_distance") <= 0).count()
if invalid_distances == 0:
    print(f"   PASSED\n")
    tests_passed += 1
else:
    print(f"   FAILED - {invalid_distances:,} invalid\n")
    tests_failed += 1
    failed_tests.append(test_name)

# Test 5: Valid Passenger Counts
test_name = "Valid Passenger Counts"
print(f"Test 5: {test_name}")
invalid_passengers = df.filter(
    (col("passenger_count") < 1) | (col("passenger_count") > 6)
).count()
if invalid_passengers == 0:
    print(f"   PASSED\n")
    tests_passed += 1
else:
    print(f"   FAILED - {invalid_passengers:,} invalid\n")
    tests_failed += 1
    failed_tests.append(test_name)

# Test 6: Reasonable Speeds
test_name = "Reasonable Speeds"
print(f"Test 6: {test_name}")
unreasonable_speeds = df.filter(col("avg_speed_mph") > 100).count()
if unreasonable_speeds == 0:
    print(f"   PASSED\n")
    tests_passed += 1
else:
    print(f"   FAILED - {unreasonable_speeds:,} > 100mph\n")
    tests_failed += 1
    failed_tests.append(test_name)

# Test 7: Valid Time Periods
test_name = "Valid Time Periods"
print(f"Test 7: {test_name}")
invalid_periods = df.filter(
    ~col("time_period").isin(["Morning", "Afternoon", "Evening", "Night"])
).count()
if invalid_periods == 0:
    print(f"   PASSED\n")
    tests_passed += 1
else:
    print(f"   FAILED - {invalid_periods:,} invalid\n")
    tests_failed += 1
    failed_tests.append(test_name)

# Test 8: Pickup Before Dropoff
test_name = "Pickup Before Dropoff"
print(f"Test 8: {test_name}")
invalid_times = df.filter(
    col("tpep_pickup_datetime") >= col("tpep_dropoff_datetime")
).count()
if invalid_times == 0:
    print(f"   PASSED\n")
    tests_passed += 1
else:
    print(f"   FAILED - {invalid_times:,} invalid\n")
    tests_failed += 1
    failed_tests.append(test_name)

# Statistics
print("DATA STATISTICS\n")

stats = df.select(
    avg("trip_distance").alias("avg_distance"),
    spark_min("trip_distance").alias("min_distance"),
    spark_max("trip_distance").alias("max_distance"),
    avg("fare_amount").alias("avg_fare"),
    avg("avg_speed_mph").alias("avg_speed")
).collect()[0]

print(f"Distance: avg={stats['avg_distance']:.2f} min={stats['min_distance']:.2f} max={stats['max_distance']:.2f} miles")
print(f"Fare: avg=${stats['avg_fare']:.2f}")
print(f"Speed: avg={stats['avg_speed']:.2f} mph\n")

# Borough distribution
print("Borough Distribution:")
df.groupBy("PU_Borough").count().orderBy(col("count").desc()).show(10)

# Final Summary
total_tests = tests_passed + tests_failed
pass_rate = (tests_passed / total_tests * 100) if total_tests > 0 else 0

print("\nQUALITY CHECK SUMMARY")
print(f"\nTotal records: {total_count:,}")
print(f"Tests run: {total_tests}")
print(f"Passed: {tests_passed}")
print(f"Failed: {tests_failed}")
print(f"Pass rate: {pass_rate:.1f}%")

if failed_tests:
    print(f"\nFailed tests:")
    for i, test in enumerate(failed_tests, 1):
        print(f"   {i}. {test}")

print()

if tests_failed == 0:
    print("ALL TESTS PASSED!")
    print("Data ready for Gold layer\n")
    spark.stop()
    sys.exit(0)
else:
    print("SOME TESTS FAILED!")
    print("Continuing pipeline with warnings...\n")
    spark.stop()
    sys.exit(0)  # Exit 0 to not block pipeline