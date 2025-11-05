# Lakehouse NYC Taxi - Setup Guide

This document provides step-by-step instructions to set up and run the NYC Taxi Lakehouse project on your local machine.

## Prerequisites

Before getting started, ensure you have the following prerequisites installed:

- Docker Desktop 24.0+ with WSL2 backend enabled
- 16 GB RAM minimum (32 GB recommended)
- 50 GB available disk space
- PowerShell 7+ (Windows) or Terminal (macOS/Linux)
- Git

## System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 4 cores | 8+ cores |
| RAM | 16 GB | 32 GB |
| Disk | 50 GB SSD | 100 GB SSD |
| Network | Broadband | High-speed connection |
| Docker | Docker Desktop 24.0+ | Latest version |

## Installation Steps

Follow these steps to set up the project environment:

### 1. Clone the Repository

```bash
# Clone the repository
git clone https://github.com/[your-username]/lakehouse-nyc-taxi.git

# Navigate to the project directory
cd lakehouse-nyc-taxi
```

### 2. Download Data

The project processes NYC Taxi trip data from 2023. Use the provided script to download the required files:

```powershell
# Windows
.\download_data.ps1

# macOS/Linux (if using PowerShell Core)
pwsh ./download_data.ps1

# Alternative for macOS/Linux without PowerShell
# Use the Python downloader script instead
python download_data.py
```

This script will:
- Create a `/data/raw` directory
- Download 12 monthly Parquet files (approximately 5 GB total)
- Download the taxi zone lookup CSV file
- Display progress and validation information

> Note: The download may take 5-20 minutes depending on your internet connection.

### 3. Build and Start Infrastructure

Use Docker Compose to build and start the required services:

```bash
# Build Docker images
docker compose build

# Start all services in detached mode
docker compose up -d
```

This will start the following services:
- MinIO (S3-compatible storage)
- PostgreSQL (Metadata database)
- Apache Spark (Master and Worker)
- Apache Airflow (Workflow orchestration)

### 4. Wait for Services to Initialize

The first startup requires time for services to initialize:

```bash
# Wait for services to be ready (at least 2 minutes)
sleep 120  # Windows/Linux
# or just wait approximately 2 minutes

# Check service status
docker compose ps
```

All services should show as "running" with no restarts or errors.

### 5. Access Web Interfaces

The following web interfaces are available:

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8088 | admin / admin |
| Spark Master | http://localhost:8080 | - |
| Spark Worker | http://localhost:8081 | - |
| MinIO Console | http://localhost:9001 | minio / minio12345 |

Verify you can access each of these interfaces in your browser.

## Running the Pipeline

Once the environment is set up, you can run the data pipeline:

### 1. Start the Airflow DAG

Navigate to the Airflow UI at http://localhost:8088 and:

1. Log in using credentials: admin / admin
2. Go to the DAGs view
3. Locate the "nyc_taxi_lakehouse" DAG
4. Toggle the switch to activate the DAG
5. Click the "Play" button to trigger the DAG manually

### 2. Monitor Execution

Monitor the pipeline execution through:

- Airflow UI: View task status and logs
- Spark UI: Monitor job execution and performance
- Docker logs: Check for any errors or warnings

```bash
# View logs for specific services
docker compose logs -f airflow
docker compose logs -f spark-master
```

The complete pipeline takes approximately 55 minutes to process all data.

### 3. Verify Results

After pipeline completion, verify the results:

```bash
# Run verification script
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/scripts/verify_pipeline.py
```

This script will validate that all layers (Bronze, Silver, Gold, Analytics) were created correctly with the expected data.

### 4. Export for Tableau

To use the data in Tableau Desktop:

```bash
# Create a local folder for Tableau data
mkdir tableau_data

# Copy CSV exports from the container
docker cp spark-master:/data/tableau_exports/. tableau_data/
```

Follow the instructions in `docs/tableau_dashboard_guide.md` to create the Tableau dashboard.

## Troubleshooting

Here are solutions to common issues you might encounter:

### Insufficient Memory

**Symptom**: Services restart continuously or Spark jobs fail with memory errors.

**Solution**: 
1. Increase Docker Desktop memory allocation (Preferences > Resources)
2. Reduce Spark executor/driver memory in docker-compose.yml
3. Ensure no other memory-intensive applications are running

### Port Conflicts

**Symptom**: Services fail to start due to port already in use.

**Solution**:
1. Check for other applications using the same ports
2. Modify port mappings in docker-compose.yml
3. Stop conflicting services

### Data Download Failures

**Symptom**: The data download script fails to retrieve some files.

**Solution**:
1. Check your internet connection
2. Try running the script again (it will skip already downloaded files)
3. Manually download files from the NYC TLC website

### Slow Performance

**Symptom**: Pipeline execution is significantly slower than expected.

**Solution**:
1. Check CPU utilization and Docker resource limits
2. Increase Docker CPU allocation if needed
3. Close other resource-intensive applications
4. Consider using fewer months of data for testing

### Container Communication Issues

**Symptom**: Services can't communicate with each other.

**Solution**:
1. Verify all containers are on the same Docker network
2. Check Docker network settings
3. Restart Docker Desktop
4. Rebuild containers with `docker compose build`

## Maintenance

### Stopping the Environment

When not in use, stop the environment to free up resources:

```bash
# Stop all containers
docker compose stop

# Or, stop and remove containers
docker compose down
```

### Cleanup

To completely clean up the environment and start fresh:

```bash
# Remove containers, networks, and volumes
docker compose down -v

# Remove data directory
rm -rf data/

# Rebuild and restart
./download_data.ps1
docker compose build
docker compose up -d
```

### Updating

To update the project:

```bash
# Pull latest changes
git pull

# Rebuild containers
docker compose build

# Restart services
docker compose down
docker compose up -d
```

## Next Steps

After successful setup and execution:

1. Explore the data through Tableau visualizations
2. Modify the pipeline to include additional transformations
3. Extend the gold layer with custom aggregations
4. Optimize performance based on your hardware
5. Consider implementing incremental processing

## Support

For issues or questions:

1. Check the troubleshooting section above
2. Review logs with `docker compose logs -f`
3. Create an issue on the GitHub repository
4. Consult the detailed documentation in the `docs/` directory

## Security Notes

This setup is designed for local development and learning purposes:

1. Default credentials are used for services
2. No encryption is implemented for data at rest
3. Services are not hardened for production use

If deploying to a shared environment, ensure you implement proper security measures!!!
