from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder \
    .appName("Bronze-Ingestion-Orders") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=== Reading from Kafka ===")

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("kafka.security.protocol", "PLAINTEXT") \
    .option("subscribe", "ecommerce-orders") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df.select(
    col("value").cast("string").alias("raw_data"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingestion_time")
)

count = df_parsed.count()
print(f"=== Total records: {count} ===")
df_parsed.show(5, truncate=False)

df_parsed.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/datalake/bronze/orders")

print("=== Bronze ingestion complete! ===")
spark.stop()
