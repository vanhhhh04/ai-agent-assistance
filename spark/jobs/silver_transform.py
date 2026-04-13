from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Silver-Transform-Orders") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema matching your orders table
order_schema = StructType([
    StructField("id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("shipping_address_id", IntegerType()),
    StructField("billing_address_id", IntegerType()),
    StructField("coupon_id", IntegerType()),
    StructField("status", StringType()),
    StructField("subtotal", StringType()),
    StructField("discount_amount", StringType()),
    StructField("tax_amount", StringType()),
    StructField("shipping_cost", StringType()),
    StructField("total_amount", StringType()),
    StructField("order_date", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType())
])

print("=== Reading Bronze layer ===")
df_bronze = spark.read.parquet("hdfs://namenode:9000/datalake/bronze/orders")

print("=== Parsing JSON to structured columns ===")
df_silver = df_bronze \
    .select(from_json(col("raw_data"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("subtotal", col("subtotal").cast("decimal(12,2)")) \
    .withColumn("discount_amount", col("discount_amount").cast("decimal(12,2)")) \
    .withColumn("tax_amount", col("tax_amount").cast("decimal(12,2)")) \
    .withColumn("shipping_cost", col("shipping_cost").cast("decimal(12,2)")) \
    .withColumn("total_amount", col("total_amount").cast("decimal(12,2)")) \
    .withColumn("order_date", to_timestamp(col("order_date"))) \
    .withColumn("created_at", to_timestamp(col("created_at"))) \
    .dropna(subset=["id", "customer_id", "total_amount"])

print(f"=== Silver records: {df_silver.count()} ===")
df_silver.show(5)

print("=== Writing Silver layer ===")
df_silver.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/datalake/silver/orders")

print("=== Silver transformation complete! ===")
spark.stop()
