# Lakehouse NYC Taxi - Architecture Documentation

## Overview

This document describes the architecture of the NYC Taxi Lakehouse project, which processes large volumes of taxi trip data using a modern lakehouse pattern.

## System Architecture

The architecture follows the Medallion pattern (multi-layered data organization) within a lakehouse framework, combining elements of data lakes and data warehouses.

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────┐
│   Data Sources  │     │   Bronze Layer  │     │   Silver Layer  │     │    Gold Layer   │     │  Analytics  │
│  (Raw Parquet)  │────▶│  (Raw Ingest)   │────▶│   (Cleaning)    │────▶│ (Aggregations)  │────▶│  (Tableau)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────┘
```

## Components

### 1. Infrastructure Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| Storage | MinIO (S3) | Object storage for data lake |
| Compute | Apache Spark | Distributed data processing |
| Metadata | PostgreSQL | Stores metadata and metrics |
| Orchestration | Apache Airflow | Workflow scheduling and monitoring |
| Containerization | Docker | Infrastructure as code |
| Visualization | Tableau | Business intelligence dashboards |

### 2. Data Components

| Layer | Purpose | Format | Partitioning |
|-------|---------|--------|--------------|
| Raw | External data sources | Parquet | None |
| Bronze | Raw ingested data | Delta Lake | None |
| Silver | Cleaned and enriched | Delta Lake | pickup_date |
| Gold | Business aggregations | Delta Lake | Varies |
| Analytics | BI-ready exports | CSV | None |

### 3. Processing Flow

1. **Raw → Bronze**: Direct ingestion with minimal transformation
2. **Bronze → Silver**: 
   - Data cleaning and validation
   - Type conversions
   - Feature engineering
   - Geospatial enrichment
3. **Silver → Gold**:
   - Business-oriented aggregations
   - Daily/hourly/borough metrics
   - Route analysis
4. **Gold → Analytics**:
   - KPI calculations
   - Advanced time series analysis
   - CSV exports for Tableau

## Technical Details

### Delta Lake Configuration

The project uses Delta Lake for reliable data storage with ACID transactions:

```
Storage Format: Delta Lake 3.2.0
File Size Target: 100-500MB per file
Z-Order Columns: pickup_date, PULocationID
Vacuum Retention: 168 hours (7 days)
```

### Apache Spark Configuration

The Spark cluster is configured for optimal performance:

```
Spark Version: 3.5.1
Executor Memory: 4g
Driver Memory: 4g
Cores: 2 per executor
SQL Extensions: io.delta.sql.DeltaSparkSessionExtension
```

### Data Volumes

The pipeline processes substantial data volumes:

```
Raw Files: 12 monthly Parquet files (2023 yellow taxi data)
Bronze Records: ~38 million trip records
Silver Records: ~32 million after filtering (15% removal rate)
Gold Tables: 4 aggregation tables
Analytics Exports: 10 CSV files for Tableau
```

## Quality Control

The pipeline incorporates multiple quality control mechanisms:

1. **Validation after Bronze ingestion**:
   - Record count verification
   - Schema validation
   - Metadata checks

2. **Silver Layer Quality Checks**:
   - 8 automated data quality tests
   - Null value detection
   - Range validation
   - Business rule enforcement

3. **Gold Layer Verification**:
   - Aggregation consistency
   - Cross-layer reconciliation

4. **Analytics Export Validation**:
   - Export completeness checks
   - CSV structure validation

## Security

Data security measures include:

1. **Access Control**:
   - MinIO credentials for S3 access
   - PostgreSQL role-based authentication
   - Separate user accounts for each service

2. **Data Protection**:
   - Network isolation via Docker network
   - No external exposure of data services
   - Read-only access for visualization tools

3. **Monitoring**:
   - Airflow task logging
   - Spark job history
   - Delta transaction log

## Performance Considerations

The architecture is optimized for performance:

1. **Partitioning Strategy**:
   - Silver layer: Partitioned by pickup_date
   - Gold layer: Varies by aggregation type
   - Improves query performance by 10-50x

2. **Compute Resources**:
   - Parallelism configured for optimal throughput
   - Dynamic resource allocation when possible
   - Right-sized containers for each component

3. **File Optimization**:
   - Regular OPTIMIZE operations (file compaction)
   - VACUUM for storage cleanup
   - Target file sizes for optimal read performance

## Scalability

The architecture can scale to handle increased data volumes:

1. **Horizontal Scaling**:
   - Add Spark workers for more processing power
   - Scale MinIO for additional storage capacity
   - Multi-node potential for production deployment

2. **Data Growth Handling**:
   - Partition pruning for efficient querying
   - Delta Lake time travel and incremental processing
   - Parameterized job configuration

## Extensibility

The system is designed for extensibility:

1. **Additional Data Sources**:
   - Modular ingest scripts
   - Schema evolution support
   - Configuration-driven processing

2. **New Analytics**:
   - Gold layer can support additional aggregation patterns
   - Analytics layer can be extended with new metrics
   - Tableau integration via standardized CSV formats

## Operational Patterns

The system supports key operational patterns:

1. **Full Load**:
   - Complete processing of all historical data
   - Rebuilds all layers from source data

2. **Incremental Updates**:
   - Supports incremental data processing
   - Can process daily/monthly updates efficiently

3. **Disaster Recovery**:
   - Delta time travel for point-in-time recovery
   - Data versioning through transaction logs
   - Repeatable pipeline execution


## Conclusion

The NYC Taxi Lakehouse architecture provides a scalable, maintainable, and extensible framework for processing large volumes of transportation data, enabling both historical analysis and potential real-time insights in the future.
