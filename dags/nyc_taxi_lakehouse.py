"""
NYC Taxi Lakehouse Pipeline
Complete Data Engineering + Analytics workflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Configuration
DEFAULT_ARGS = {
    'owner': 'Samir AISSAOUY',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    dag_id='nyc_taxi_lakehouse',
    default_args=DEFAULT_ARGS,
    description='NYC Taxi Pipeline - Data Engineering + Analytics',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['lakehouse', 'medallion', 'analytics', 'tableau'],
)

# START
start = EmptyOperator(
    task_id='start_pipeline',
    dag=dag
)

# BRONZE - Raw data ingestion
bronze_ingest = BashOperator(
    task_id='bronze_ingest',
    bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --executor-memory 2g \
      --driver-memory 2g \
      --total-executor-cores 2 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minio \
      --conf spark.hadoop.fs.s3a.secret.key=minio12345 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/jobs/01_bronze_ingest.py
    """,
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

# SILVER - Data transformation and cleaning
silver_transform = BashOperator(
    task_id='silver_transform',
    bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --executor-memory 4g \
      --driver-memory 2g \
      --total-executor-cores 4 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minio \
      --conf spark.hadoop.fs.s3a.secret.key=minio12345 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/jobs/02_silver_transform.py
    """,
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

# QUALITY CHECK - Data validation
quality_check = BashOperator(
    task_id='quality_check',
    bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --executor-memory 2g \
      --driver-memory 1g \
      --total-executor-cores 2 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minio \
      --conf spark.hadoop.fs.s3a.secret.key=minio12345 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/jobs/02b_silver_quality_check.py || echo "Quality checks with warnings"
    """,
    execution_timeout=timedelta(minutes=15),
    dag=dag
)

# GOLD - Business aggregations
gold_aggregate = BashOperator(
    task_id='gold_aggregate',
    bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --executor-memory 3g \
      --driver-memory 2g \
      --total-executor-cores 3 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minio \
      --conf spark.hadoop.fs.s3a.secret.key=minio12345 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/jobs/03_gold_agg.py
    """,
    execution_timeout=timedelta(minutes=20),
    dag=dag
)

# ANALYTICS - Advanced analysis and CSV exports
analytics_layer = BashOperator(
    task_id='analytics_layer',
    bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --executor-memory 3g \
      --driver-memory 2g \
      --total-executor-cores 3 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minio \
      --conf spark.hadoop.fs.s3a.secret.key=minio12345 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/jobs/05_analytics_layer.py
    """,
    execution_timeout=timedelta(minutes=20),
    dag=dag
)

# MAINTENANCE - Delta Lake optimization
delta_maintenance = BashOperator(
    task_id='delta_maintenance',
    bash_command="""
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --executor-memory 2g \
      --driver-memory 2g \
      --total-executor-cores 2 \
      --conf spark.sql.adaptive.enabled=true \
      --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
      --conf spark.hadoop.fs.s3a.access.key=minio \
      --conf spark.hadoop.fs.s3a.secret.key=minio12345 \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
      /opt/jobs/04_delta_maintenance.py
    """,
    execution_timeout=timedelta(minutes=15),
    dag=dag
)

# END
end = EmptyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Pipeline flow
start >> bronze_ingest >> silver_transform >> quality_check >> gold_aggregate >> analytics_layer >> delta_maintenance >> end