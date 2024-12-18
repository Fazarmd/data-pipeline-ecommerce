from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace, when, lit
from pyspark.sql.types import FloatType, TimestampType

def transform_data(input_dir, output_dir):
    """
    Fungsi untuk melakukan transformasi data dari Parquet ke format sesuai schema.
    
    :param input_dir: Path folder input berisi file Parquet.
    :param output_dir: Path folder output untuk file Parquet hasil transformasi.
    """
    # Inisialisasi SparkSession
    spark = SparkSession.builder.appName("Data Transformation").getOrCreate()

    try:
        # Load datasets
        orders = spark.read.parquet(f"{input_dir}/olist_orders_dataset.parquet")
        products = spark.read.parquet(f"{input_dir}/olist_products_dataset.parquet")
        customers = spark.read.parquet(f"{input_dir}/olist_customers_dataset.parquet")
        order_items = spark.read.parquet(f"{input_dir}/olist_order_items_dataset.parquet")
        payments = spark.read.parquet(f"{input_dir}/olist_order_payments_dataset.parquet")

        # Transformasi untuk tabel `fact_orders`
        fact_orders = (
            orders.join(order_items, "order_id")
                  .join(payments, "order_id")
                  .select(
                      col("order_id"),
                      col("customer_id"),
                      col("order_purchase_timestamp").cast(TimestampType()),
                      col("order_delivered_customer_date").cast(TimestampType()),
                      col("payment_value").cast(FloatType()),
                      col("product_id"),
                      col("price").cast(FloatType()),
                      col("freight_value").cast(FloatType())
                  )
        )
        fact_orders.write.mode("overwrite").parquet(f"{output_dir}/fact_orders")

        # Transformasi untuk tabel `dim_products`
        dim_products = (
            products.select(
                col("product_id"),
                trim(col("product_category_name")).alias("product_category_name"),
                col("product_weight_g").cast(FloatType()),
                col("product_length_cm").cast(FloatType()),
                col("product_height_cm").cast(FloatType()),
                col("product_width_cm").cast(FloatType())
            )
        )
        dim_products.write.mode("overwrite").parquet(f"{output_dir}/dim_products")

        # Transformasi untuk tabel `dim_customers`
        dim_customers = (
            customers.select(
                col("customer_id"),
                col("customer_unique_id"),
                trim(col("customer_city")).alias("customer_city"),
                trim(col("customer_state")).alias("customer_state")
            )
        )
        dim_customers.write.mode("overwrite").parquet(f"{output_dir}/dim_customers")

        # Print statistik dasar
        print("Transformasi berhasil:")
        print("Schema fact_orders:")
        fact_orders.printSchema()
        print(f"Jumlah baris fact_orders: {fact_orders.count()}")

        print("Schema dim_products:")
        dim_products.printSchema()
        print(f"Jumlah baris dim_products: {dim_products.count()}")

        print("Schema dim_customers:")
        dim_customers.printSchema()
        print(f"Jumlah baris dim_customers: {dim_customers.count()}")

    except Exception as e:
        print(f"Terjadi kesalahan dalam transformasi: {str(e)}")

    finally:
        # Tutup SparkSession
        spark.stop()

if __name__ == "__main__":
    input_dir = "/opt/airflow/data/parquet"
    output_dir = "/opt/airflow/data/transformed"

    transform_data(input_dir, output_dir)
