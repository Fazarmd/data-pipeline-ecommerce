from pyspark.sql import SparkSession
import os

def extract_csv_to_parquet(input_path, output_path):
    """
    Fungsi untuk mengekstrak file CSV dan menyimpannya dalam format Parquet
    
    :param input_path: Path folder input yang berisi file CSV
    :param output_path: Path folder output untuk file Parquet
    """
    # Inisialisasi SparkSession
    spark = SparkSession.builder \
        .appName("CSV to Parquet Extraction") \
        .getOrCreate()
    
    # Daftar semua file CSV di folder input
    csv_files = [f for f in os.listdir(input_path) if f.endswith('.csv')]
    
    # Proses setiap file CSV
    for csv_file in csv_files:
        # Buat path lengkap untuk input dan output
        input_file_path = os.path.join(input_path, csv_file)
        output_file_path = os.path.join(output_path, csv_file.replace('.csv', '.parquet'))
        
        # Baca file CSV
        df = spark.read.csv(input_file_path, header=True, inferSchema=True)
        
        # Simpan sebagai Parquet
        df.write.mode('overwrite').parquet(output_file_path)
        
        print(f"Berhasil mengonversi {csv_file} ke Parquet")
    
    # Tutup SparkSession
    spark.stop()

if __name__ == "__main__":
    input_path = "/opt/airflow/data"  # Path di dalam container
    output_path = "/opt/airflow/data/parquet"
    os.makedirs(output_path, exist_ok=True)
    
    extract_csv_to_parquet(input_path, output_path)