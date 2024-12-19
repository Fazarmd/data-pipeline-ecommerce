import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

def create_spark_session():

    spark = SparkSession.builder \
        .appName("ETL_Process") \
        .config("spark.jars", "/scripts/postgresql-42.7.4.jar") \
        .config("spark.driver.extraClassPath", "/scripts/postgresql-42.7.4.jar") \
        .getOrCreate()
    return spark

def run_extract():

    spark = create_spark_session()
    
    try:
        exec(open("/spark-scripts/extract.py").read())
    except Exception as e:
        print(f"Error dalam proses ekstraksi: {e}")
    finally:
        spark.stop()

def run_transform():

    spark = create_spark_session()
    
    try:
        exec(open("/spark-scripts/transform.py").read())
    except Exception as e:
        print(f"Error dalam proses transformasi: {e}")
    finally:
        spark.stop()

def run_load():

    spark = create_spark_session()
    
    try:
        exec(open("/spark-scripts/load.py").read())
    except Exception as e:
        print(f"Error dalam proses loading: {e}")
    finally:
        spark.stop()

default_args = {
    'owner': 'dibimbing',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_spark_dag',
    default_args=default_args,
    description='ETL Pipeline menggunakan Spark',
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=run_extract,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=run_transform,
        dag=dag,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=run_load,
        dag=dag,
    )

    # Definisikan urutan task
    extract_task >> transform_task >> load_task