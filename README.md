# Lakehouse NYC Taxi - Data Engineering + Analytics (Tableau)

![1__doMZNKaOaDPo6qWojbxZw](https://github.com/user-attachments/assets/6ad43a64-4e68-4322-b983-a38a01999be0)

Complete data pipeline from raw data ingestion to Tableau dashboards.

## Overview

Pipeline for processing 50M+ NYC taxi trip records using Medallion architecture (Bronze → Silver → Gold → Analytics).

## Architecture
![Architecture NYC Taxi](docs/Imagees/Nyk%20Taxi%20Architecture..png)

## Tech Stack

- Python 3.11 + PySpark 3.5.1
- Apache Spark (Distributed processing)
- Delta Lake 3.2.0 (ACID transactions)
- Apache Airflow 2.9.2 (Orchestration)
- MinIO (S3-compatible storage)
- PostgreSQL 14 (Metadata)
- Docker Compose (Infrastructure)
- Tableau Desktop (Visualization)

## Quick Start

### Prerequisites

- Docker Desktop 24.0+ with WSL2
- 16 GB RAM minimum
- 50 GB disk space
- PowerShell 7+ (Windows)

### Installation
```bash
# Clone repository
git clone https://github.com/[your-username]/lakehouse-nyc-taxi.git
cd lakehouse-nyc-taxi

# Download data
.\download_data.ps1

# Start infrastructure
docker compose build
docker compose up -d

# Wait 2 minutes
sleep 120

# Check services
docker compose ps
```

### Run Pipeline

1. Open Airflow: http://localhost:8088 (admin/admin)
2. Activate DAG: nyc_taxi_lakehouse
3. Trigger DAG
4. Duration: ~55 minutes

### Export for Tableau
```bash
# Create local folder
mkdir tableau_data

# Copy CSV exports
docker cp spark-master:/data/tableau_exports/. tableau_data/
```

## Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8088 | admin / admin |
| Spark Master | http://localhost:8080 | - |
| Spark Worker | http://localhost:8081 | - |
| MinIO | http://localhost:9001 | minio / minio12345 |

## Project Structure
```
lakehouse-nyc-taxi/
├── docker-compose.yml
├── download_data.ps1
├── dags/
│   └── nyc_taxi_lakehouse.py
├── jobs/
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_transform.py
│   ├── 02b_silver_quality_check.py
│   ├── 03_gold_agg.py
│   ├── 04_delta_maintenance.py
│   └── 05_analytics_layer.py
├── scripts/
│   ├── verify_bronze.py
│   └── verify_pipeline.py
├── spark/
│   ├── Dockerfile
│   └── conf/
│       └── spark-defaults.conf
├── postgres/
│   └── init.sql
└── docs/
    ├── architecture.md
    ├── setup_guide.md
    └── tableau_dashboard_guide.md
```

## Pipeline Stages

### Bronze Layer (20 min)
- Raw data ingestion
- 12 Parquet files → 38M records
- Delta Lake storage

### Silver Layer (15 min)
- Data cleaning (15% filtered)
- Feature engineering
- Geographic enrichment
- Output: 32M records

### Quality Check (3 min)
- 8 automated tests
- Data validation

### Gold Layer (5 min)
- Business aggregations
- 4 tables: Daily, Hourly, Borough, Routes

### Analytics Layer (10 min)
- KPI calculations
- Time series analysis
- CSV exports for Tableau

### Maintenance (3 min)
- OPTIMIZE + VACUUM

## Metrics

| Metric | Value |
|--------|-------|
| Data volume | 5 GB (50M records) |
| Pipeline duration | ~55 minutes |
| Bronze records | 38M |
| Silver records | 32M |
| Gold tables | 4 |
| Analytics exports | 10 CSV files |
| Dashboard pages | 4 |

## Tableau Dashboard

### Pages
1. Executive Overview (KPIs, trends)
2. Time Series Analysis (Moving averages)
3. Hourly Patterns (Heatmaps)
4. Geographic Insights (Borough, routes)

### Setup
See: docs/tableau_dashboard_guide.md

## Troubleshooting
```bash
# View logs
docker compose logs -f [service-name]

# Restart service
docker compose restart [service-name]

# Stop all
docker compose down

# Clean everything
docker compose down -v
```

## Documentation

- Architecture: docs/architecture.md
- Setup Guide: docs/setup_guide.md
- Tableau Guide: docs/tableau_dashboard_guide.md


## Author

Samir AISSAOUY
|| email: elaissaouy.samir12@gmail.com 
|| LinkedIn: https://www.linkedin.com/in/samir-el-aissaouy/
