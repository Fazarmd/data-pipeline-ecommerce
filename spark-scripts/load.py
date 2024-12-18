from pyspark.sql import SparkSession
import os

def load_to_postgres(input_path, table_name, postgres_config, schema="public"):
    """
    Fungsi untuk memuat data dari Parquet ke PostgreSQL.
    
    :param input_path: Path folder input berisi file Parquet
    :param table_name: Nama tabel di PostgreSQL
    :param postgres_config: Konfigurasi koneksi PostgreSQL
    :param schema: Nama schema di PostgreSQL (default: "public")
    """
    # Inisialisasi SparkSession dengan PostgreSQL driver
    spark = SparkSession.builder \
        .appName("Load to PostgreSQL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .getOrCreate()
    
    try:
        # Baca data dari file Parquet
        df = spark.read.parquet(input_path)
        
        # Konfigurasi koneksi PostgreSQL
        postgres_url = (
            f"jdbc:postgresql://{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
        )
        
        # Tulis DataFrame ke PostgreSQL
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
        # Tutup SparkSession
        spark.stop()

if __name__ == "__main__":
    # Konfigurasi PostgreSQL
    postgres_config = {
        'host': 'dataeng-postgres',  # Sesuaikan dengan service name PostgreSQL di Docker Compose
        'port': 5432,
        'database': 'postgres_db',   # Sesuaikan dengan nama database Anda
        'user': 'user',              # Sesuaikan dengan username PostgreSQL
        'password': 'password'       # Sesuaikan dengan password PostgreSQL
    }
    
    # Path hasil transformasi
    input_paths = {
        "fact_orders": "/opt/airflow/data/transformed/fact_orders",
        "dim_products": "/opt/airflow/data/transformed/dim_products",
        "dim_customers": "/opt/airflow/data/transformed/dim_customers"
    }
    
    # Schema di PostgreSQL
    schema = "data_pipeline"
    
    # Muat setiap dataset ke tabel PostgreSQL
    for table_name, input_path in input_paths.items():
        load_to_postgres(input_path, table_name, postgres_config, schema)
