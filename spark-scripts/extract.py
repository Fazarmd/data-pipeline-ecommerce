from pyspark.sql import SparkSession
import os

def extract_csv_to_parquet(input_path, output_path):

    # Inisialisasi SparkSession
    spark = SparkSession.builder \
        .appName("CSV to Parquet Extraction") \
        .getOrCreate()
    
    # Daftar semua file CSV 
    csv_files = [f for f in os.listdir(input_path) if f.endswith('.csv')]
    
    # Proses setiap file CSV
    for csv_file in csv_files:

        input_file_path = os.path.join(input_path, csv_file)
        output_file_path = os.path.join(output_path, csv_file.replace('.csv', '.parquet'))
        
        df = spark.read.csv(input_file_path, header=True, inferSchema=True)
        
        df.write.mode('overwrite').parquet(output_file_path)
        
        print(f"Berhasil mengonversi {csv_file} ke Parquet")
    
    spark.stop()

if __name__ == "__main__":
    input_path = "/opt/airflow/data" 
    output_path = "/opt/airflow/data/parquet"
    os.makedirs(output_path, exist_ok=True)
    
    extract_csv_to_parquet(input_path, output_path)