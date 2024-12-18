from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 18),
}

dag = DAG(
    'csv_to_parquet_dag',
    default_args=default_args,
    description='Convert CSV to Parquet using Spark',
    schedule_interval=None,
    catchup=False,
)

convert_csv_to_parquet = SparkSubmitOperator(
    task_id='convert_csv_to_parquet',
    application='/spark-scripts/convert_csv_to_parquet.py',
    conn_id='spark_main',
    name='convert_csv_to_parquet_task',
    conf={
        "spark.master": "spark://dataeng-spark-master:7077",
        "spark.jars": "/scripts/postgresql-42.7.4.jar",
        "spark.hadoop.parquet.enable.summary-metadata": "false"
    },
    verbose=True,
    dag=dag,
)

convert_csv_to_parquet
