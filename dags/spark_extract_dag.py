from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments untuk DAG
default_args = {
    "owner": "dibimbing",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Definisi DAG
spark_dag = DAG(
    dag_id="spark_extract_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Jalankan setiap hari
    catchup=False,
    description="DAG spark",
)

# Task untuk menjalankan SparkSubmitOperator
extract_task = SparkSubmitOperator(
    task_id="extract_csv_to_parquet",
    application="/spark-scripts/extract.py",  # Path ke script PySpark
    conn_id="spark_main",  # Koneksi Spark
    application_args=[
        "/opt/airflow/data",  # Argumen input_path
        "/opt/airflow/data/parquet"  # Argumen output_path
    ],
    dag=spark_dag,
)


# Set urutan task
extract_task
