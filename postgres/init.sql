-- PostgreSQL initialization script for NYC Taxi Lakehouse
-- Creates databases and tables needed

-- Create Airflow database
CREATE DATABASE airflow;
\c airflow

CREATE TABLE IF NOT EXISTS alembic_version (
    version_num VARCHAR(32) NOT NULL, 
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Create Spark History database
CREATE DATABASE spark_history;
\c spark_history

CREATE TABLE IF NOT EXISTS spark_jobs (
    job_id VARCHAR(50) PRIMARY KEY,
    app_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_sec INTEGER,
    status VARCHAR(20),
    user_name VARCHAR(100),
    executor_cores INTEGER,
    executor_memory VARCHAR(20),
    total_tasks INTEGER,
    failed_tasks INTEGER,
    input_bytes BIGINT,
    output_bytes BIGINT
);

CREATE INDEX spark_jobs_app_name_idx ON spark_jobs (app_name);
CREATE INDEX spark_jobs_start_time_idx ON spark_jobs (start_time);
CREATE INDEX spark_jobs_status_idx ON spark_jobs (status);

-- Create metadata database for the data lake
CREATE DATABASE lakehouse;
\c lakehouse

-- Data Sources table
CREATE TABLE IF NOT EXISTS data_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    description TEXT,
    connection_info JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT TRUE
);

-- Tables metadata
CREATE TABLE IF NOT EXISTS lake_tables (
    table_id SERIAL PRIMARY KEY,
    layer VARCHAR(20) NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold', 'analytics')),
    table_name VARCHAR(100) NOT NULL,
    s3_location VARCHAR(200) NOT NULL,
    format VARCHAR(20) NOT NULL DEFAULT 'delta',
    partitioning_cols VARCHAR(200),
    record_count BIGINT,
    size_bytes BIGINT,
    last_updated TIMESTAMP,
    description TEXT,
    schema_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(layer, table_name)
);

-- Jobs tracking
CREATE TABLE IF NOT EXISTS pipeline_jobs (
    job_id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    job_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_sec INTEGER,
    records_processed BIGINT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality results
CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES pipeline_jobs(job_id),
    table_id INTEGER REFERENCES lake_tables(table_id),
    check_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    expected_value TEXT,
    actual_value TEXT,
    threshold FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create dashboard user for Tableau
CREATE USER dashboard_user WITH PASSWORD 'tableau123';
GRANT CONNECT ON DATABASE lakehouse TO dashboard_user;
GRANT USAGE ON SCHEMA public TO dashboard_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dashboard_user;

-- Insert default data
INSERT INTO data_sources (source_name, source_type, description, connection_info) 
VALUES ('NYC TLC Trip Data', 'web_api', 'NYC Taxi & Limousine Commission trip records', 
        '{"base_url": "https://d37ci6vzurychx.cloudfront.net/trip-data/", "format": "parquet"}');

-- Add baseline table definitions
INSERT INTO lake_tables (layer, table_name, s3_location, format, description) 
VALUES 
('bronze', 'trips', 's3a://lake/bronze/trips', 'delta', 'Raw NYC taxi trip data'),
('bronze', 'taxi_zone_lookup', 's3a://lake/bronze/taxi_zone_lookup', 'delta', 'Lookup table for taxi zones'),
('silver', 'trips_cleaned', 's3a://lake/silver/trips_cleaned', 'delta', 'Cleaned and transformed trip data'),
('gold', 'trips_daily_metrics', 's3a://lake/gold/trips_daily_metrics', 'delta', 'Daily trip metrics aggregations'),
('gold', 'trips_hourly_metrics', 's3a://lake/gold/trips_hourly_metrics', 'delta', 'Hourly trip metrics aggregations'),
('gold', 'trips_borough_metrics', 's3a://lake/gold/trips_borough_metrics', 'delta', 'Borough-level metrics aggregations'),
('gold', 'trips_popular_routes', 's3a://lake/gold/trips_popular_routes', 'delta', 'Popular routes metrics'),
('analytics', 'kpi_dashboard', 's3a://lake/analytics/kpi_dashboard', 'delta', 'KPI metrics for dashboard'),
('analytics', 'time_series_trends', 's3a://lake/analytics/time_series_trends', 'delta', 'Time series trend data'),
('analytics', 'hourly_heatmap', 's3a://lake/analytics/hourly_heatmap', 'delta', 'Hourly pattern heatmap data'),
('analytics', 'top_routes_analysis', 's3a://lake/analytics/top_routes_analysis', 'delta', 'Top routes analysis data');

-- Create notification function for updates
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add triggers
CREATE TRIGGER update_lake_tables_modtime
BEFORE UPDATE ON lake_tables
FOR EACH ROW EXECUTE PROCEDURE update_modified_column();

CREATE TRIGGER update_data_sources_modtime
BEFORE UPDATE ON data_sources
FOR EACH ROW EXECUTE PROCEDURE update_modified_column();

-- Add permissions for Airflow
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE lakehouse TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

-- Set permissions
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, UPDATE ON SEQUENCES TO airflow;
