from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Use days_ago for easier debugging
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark-etl-dag',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger or set as needed
    catchup=False,
    max_active_runs=1  # Prevent multiple concurrent runs
)

# Use Airflow Variables for path configuration
SPARK_SCRIPTS_PATH = Variable.get("spark_scripts_path", "/spark-scripts")

# Extract task with additional parameters
extract_task = SparkSubmitOperator(
    task_id='extract_data',
    application=f'{SPARK_SCRIPTS_PATH}/extract.py',
    conn_id='spark_main',  # Ensure this connection is correctly configured
    dag=dag,
    py_files=[f'{SPARK_SCRIPTS_PATH}/common_utils.py'],  # Optional: additional Python files
    application_args=[  # Pass arguments to the script if needed
        '--input-path', '/opt/airflow/data',
        '--output-path', '/opt/airflow/data/parquet'
    ],
    conf={
        # Optional Spark configurations
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g'
    }
)

# Transform task
transform_task = SparkSubmitOperator(
    task_id='transform_data',
    application=f'{SPARK_SCRIPTS_PATH}/transform.py',
    conn_id='spark_main',
    dag=dag
)

# Load task
load_task = SparkSubmitOperator(
    task_id='load_data',
    application=f'{SPARK_SCRIPTS_PATH}/load.py',
    conn_id='spark_main',
    dag=dag
)

# Dependencies
extract_task >> transform_task >> load_task