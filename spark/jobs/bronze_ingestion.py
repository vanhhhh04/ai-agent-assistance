"""
Bronze Ingestion Job
====================
Reads raw events from all 5 source topics and writes partitioned Parquet to HDFS.

Topic sources:
  ERP (Debezium CDC) ── erp.public.customers
                        erp.public.orders          → hdfs/.../bronze/erp_raw
                        erp.public.order_items
                        erp.public.coupons
  Warehouse (NiFi CSV→JSON) ── warehouse.events    → hdfs/.../bronze/wh_raw
  Payment (NiFi HTTP) ──────── payment.events      → hdfs/.../bronze/pay_raw

Bronze rule: store raw data as-is, only add Kafka lineage metadata.
No business logic. No transformation.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP = "kafka:29092"
HDFS_BASE       = "hdfs://namenode:9000/datalake/bronze"

spark = SparkSession.builder \
    .appName("Bronze-Ingestion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def ingest_topics(topics: list, hdfs_path: str) -> int:
    """
    Read one or more Kafka topics (earliest→latest) and write raw Parquet to HDFS.
    Multiple topics are merged into one bronze table — _source_topic identifies origin.
    """
    topic_str = ",".join(topics)
    print(f"\n[BRONZE] topics={topics}")
    print(f"[BRONZE] → {hdfs_path}")

    df_raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic_str)
        .option("startingOffsets", "earliest")
        .option("endingOffsets",   "latest")
        .option("failOnDataLoss",  "false")
        .load()
    )

    df_bronze = df_raw.select(
        F.col("value").cast("string").alias("raw_data"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("_source_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.current_timestamp().alias("_bronze_ingested_at"),
    )

    count = df_bronze.count()
    print(f"[BRONZE]   records ingested: {count:,}")

    (
        df_bronze.write
        .mode("overwrite")
        .partitionBy("_source_topic", "_kafka_partition")
        .parquet(hdfs_path)
    )
    print(f"[BRONZE]   written ✓")
    return count


# ERP: all 4 Debezium topics → single erp_raw bronze table
erp_count = ingest_topics(
    topics=[
        "erp.public.customers",
        "erp.public.orders",
        "erp.public.order_items",
        "erp.public.coupons",
    ],
    hdfs_path=f"{HDFS_BASE}/erp_raw",
)

# Warehouse: NiFi converts CSV → JSON → one message per record
wh_count = ingest_topics(
    topics=["warehouse.events"],
    hdfs_path=f"{HDFS_BASE}/wh_raw",
)

# Payment: NiFi passes through HTTP POST JSON as-is
pay_count = ingest_topics(
    topics=["payment.events"],
    hdfs_path=f"{HDFS_BASE}/pay_raw",
)

total = erp_count + wh_count + pay_count
print(f"\n[BRONZE] Complete. ERP={erp_count:,}  Warehouse={wh_count:,}  Payment={pay_count:,}  Total={total:,}")
spark.stop()
