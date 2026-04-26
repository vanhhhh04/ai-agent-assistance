"""
Bronze Ingestion Job — Trigger.AvailableNow streaming
======================================================
Reads new Kafka messages since the last run and APPENDS to HDFS Bronze layer.

Why Trigger.AvailableNow?
  - Same exactly-once + offset tracking as continuous streaming
  - But the job EXITS when no more data is available (batch-like lifecycle)
  - Airflow can schedule it hourly without leaving a long-running Spark process
  - Subsequent runs read only NEW offsets (checkpoint persisted on HDFS)

Topic sources:
  ERP (Debezium CDC) ── erp.public.customers
                        erp.public.orders          → hdfs:///datalake/bronze/erp_raw
                        erp.public.order_items
                        erp.public.coupons
  Warehouse (NiFi)   ── warehouse.events           → hdfs:///datalake/bronze/wh_raw
                        warehouse.events.dlq       → hdfs:///datalake/bronze/wh_dlq
  Payment (NiFi)    ── payment.events              → hdfs:///datalake/bronze/pay_raw
                        payment.events.dlq         → hdfs:///datalake/bronze/pay_dlq

Bronze rule: keep raw value as-is, only add Kafka lineage + ingest_date partition.
No business logic. No transformation.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP = "kafka:29092"
HDFS_BASE       = "hdfs://namenode:9000/datalake/bronze"
CHECKPOINT_BASE = "hdfs://namenode:9000/datalake/_checkpoints/bronze"

spark = (
    SparkSession.builder
    .appName("Bronze-Ingestion")
    .config("spark.sql.streaming.schemaInference",         "true")
    .config("spark.sql.streaming.stopActiveRunOnRestart",  "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def ingest_topics(topic_pattern: str, sink_name: str):
    """
    Read Kafka topics matching a regex pattern with Trigger.AvailableNow
    and append to HDFS.

    `subscribePattern` (vs `subscribe`) tolerates topics that don't exist yet —
    important for DLQ topics that are auto-created on first publish.

    Idempotency: Spark's checkpoint at CHECKPOINT_BASE/<sink_name> records
    consumed Kafka offsets. Re-running the job does not re-read old messages.
    """
    sink_path  = f"{HDFS_BASE}/{sink_name}"
    ckpt_path  = f"{CHECKPOINT_BASE}/{sink_name}"
    print(f"\n[BRONZE] sink={sink_name}")
    print(f"[BRONZE]   pattern = {topic_pattern}")
    print(f"[BRONZE]   sink    = {sink_path}")
    print(f"[BRONZE]   ckpt    = {ckpt_path}")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",  KAFKA_BOOTSTRAP)
        .option("subscribePattern",         topic_pattern)
        # Only used on the FIRST run (no checkpoint yet). After that
        # the checkpoint pins the next offset to read.
        .option("startingOffsets",          "earliest")
        .option("failOnDataLoss",           "false")
        .load()
    )

    bronze = df.select(
        F.col("value").cast("string").alias("raw_data"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("_source_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.current_timestamp().alias("_bronze_ingested_at"),
        # Date partition — Silver vacuums + retention rely on this
        F.to_date(F.col("timestamp")).alias("ingest_date"),
    )

    query = (
        bronze.writeStream
        .format("parquet")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", ckpt_path)
        .partitionBy("_source_topic", "ingest_date")
        .start(sink_path)
    )
    query.awaitTermination()
    print(f"[BRONZE]   ✓ {sink_name} done. lastProgress={query.lastProgress.get('numInputRows', 0) if query.lastProgress else 0} rows")


# ─────────────────────────────────────────────────────────────
#  ERP CDC — 4 Debezium topics → erp_raw
# ─────────────────────────────────────────────────────────────
ingest_topics(topic_pattern="erp\\.public\\..*",        sink_name="erp_raw")

# ─────────────────────────────────────────────────────────────
#  WAREHOUSE — CLEAN + DLQ split into separate Bronze tables
# ─────────────────────────────────────────────────────────────
ingest_topics(topic_pattern="warehouse\\.events",       sink_name="wh_raw")
ingest_topics(topic_pattern="warehouse\\.events\\.dlq", sink_name="wh_dlq")

# ─────────────────────────────────────────────────────────────
#  PAYMENT — CLEAN + DLQ split
# ─────────────────────────────────────────────────────────────
ingest_topics(topic_pattern="payment\\.events",         sink_name="pay_raw")
ingest_topics(topic_pattern="payment\\.events\\.dlq",   sink_name="pay_dlq")

print("\n[BRONZE] All sinks complete.")
spark.stop()
