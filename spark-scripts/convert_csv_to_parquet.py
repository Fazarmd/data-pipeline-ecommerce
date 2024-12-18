import pyspark
from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("CSV to Parquet Converter") \
    .getOrCreate()

# Path input dan output
input_path = "/opt/airflow/data/olist_customers_dataset.csv"
output_path = "/opt/airflow/data/staging/costumers-parquet"

# Baca file CSV
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Tulis ke format Parquet
df.write.mode('overwrite').parquet(output_path)

# Hentikan SparkSession
spark.stop()
