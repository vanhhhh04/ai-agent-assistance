from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Gold-Transform-Orders") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read Silver orders
print("=== Reading Silver layer ===")
df_orders = spark.read.parquet("hdfs://namenode:9000/datalake/silver/orders")

# Read directly from PostgreSQL for dimension tables
jdbc_url = "jdbc:postgresql://postgres:5432/ecommerce"
jdbc_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

print("=== Reading dimension tables from PostgreSQL ===")
df_customers = spark.read.jdbc(jdbc_url, "customers", properties=jdbc_props) \
    .select(
        col("id").alias("customer_id"),
        concat(col("first_name"), lit(" "), col("last_name")).alias("customer_name"),
        col("email"),
        col("gender"),
        col("segment") if "segment" in [f.name for f in spark.read.jdbc(jdbc_url, "customers", properties=jdbc_props).schema] else lit("N/A").alias("segment")
    )

df_products = spark.read.jdbc(jdbc_url, "products", properties=jdbc_props) \
    .select(
        col("id").alias("product_id"),
        col("name").alias("product_name"),
        col("brand"),
        col("price"),
        col("category_id")
    )

df_order_items = spark.read.jdbc(jdbc_url, "order_items", properties=jdbc_props) \
    .select(
        col("order_id"),
        col("product_id"),
        col("quantity"),
        col("unit_price"),
        col("total_price")
    )

print("=== Building Gold layer (Star Schema) ===")
df_gold = df_orders \
    .join(df_customers, "customer_id", "left") \
    .join(df_order_items, df_orders["id"] == df_order_items["order_id"], "left") \
    .join(df_products, "product_id", "left") \
    .select(
        df_orders["id"].alias("order_id"),
        col("customer_id"),
        col("customer_name"),
        col("email"),
        col("status"),
        col("subtotal"),
        col("discount_amount"),
        col("tax_amount"),
        col("shipping_cost"),
        col("total_amount").alias("ORDER_TOTAL_AMT"),
        col("order_date"),
        year(col("order_date")).alias("order_year"),
        month(col("order_date")).alias("order_month"),
        col("product_name"),
        col("brand"),
        col("quantity"),
        col("unit_price")
    )

print(f"=== Gold records: {df_gold.count()} ===")
df_gold.show(5)

print("=== Writing Gold layer ===")
df_gold.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/datalake/gold/orders")

print("=== Gold transformation complete! ===")
spark.stop()
