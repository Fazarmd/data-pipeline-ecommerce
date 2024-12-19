from pyspark.sql import SparkSession
import os

def load_to_postgres(input_path, table_name, postgres_config, schema="public"):

    # Inisialisasi SparkSession dengan PostgreSQL driver
    spark = SparkSession.builder \
        .appName("Load to PostgreSQL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .getOrCreate()
    
    try:
        df = spark.read.parquet(input_path)
        
        # Konfigurasi koneksi PostgreSQL
        postgres_url = (
            f"jdbc:postgresql://{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
        )
        
        dbtable = f"{schema}.{table_name}"
        df.write \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", dbtable) \
            .option("user", postgres_config['user']) \
            .option("password", postgres_config['password']) \
            .mode("overwrite") \
            .save()
        
        print(f"Berhasil memuat data ke tabel {dbtable}")
        print(f"Jumlah baris yang dimuat: {df.count()}")
    
    except Exception as e:
        print(f"Terjadi kesalahan dalam proses load: {str(e)}")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    postgres_config = {
        'host': 'dataeng-postgres', 
        'port': 5432,
        'database': 'postgres_db',   
        'user': 'user',              
        'password': 'password'       
    }
    
    input_paths = {
        "fact_orders": "/opt/airflow/data/transformed/fact_orders",
        "dim_products": "/opt/airflow/data/transformed/dim_products",
        "dim_customers": "/opt/airflow/data/transformed/dim_customers"
    }
    
    schema = "data_pipeline"
    
    for table_name, input_path in input_paths.items():
        load_to_postgres(input_path, table_name, postgres_config, schema)
