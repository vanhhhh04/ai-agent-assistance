"""
Medallion Pipeline DAG
======================
Orchestrates Bronze → Silver → Gold Spark jobs on a schedule.

Each stage depends on the previous — Airflow enforces this ordering.
SparkSubmitOperator submits jobs to the Spark cluster (client mode);
the application file runs on the Airflow scheduler container, the executors
run on Spark workers.

Bronze uses Spark Structured Streaming with Trigger.AvailableNow:
  - On each run, drains all NEW Kafka offsets since the last checkpoint
  - Exits cleanly (does not run forever)
  - Subsequent runs are incremental — checkpoint at /datalake/_checkpoints/bronze/

Schedule: hourly. First run after sims have produced data; before that,
trigger manually from the Airflow UI or `python cli.py pipeline run`.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_MASTER = "spark://spark-master:7077"
SPARK_JOBS   = "/opt/airflow/spark_jobs"
HDFS_NN      = "hdfs://namenode:9000"
HIVE_MSTORE  = "thrift://hive-metastore:9083"
KAFKA_BROKER = "kafka:29092"

# Kafka structured streaming connector — required by Bronze, downloaded from
# Maven Central at submit time. Version must match Spark 3.5.x + Scala 2.12.
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# Spark config shared across all stages
SPARK_CONF = {
    "spark.hadoop.fs.defaultFS":               HDFS_NN,
    "spark.sql.catalogImplementation":         "hive",
    "spark.hadoop.hive.metastore.uris":        HIVE_MSTORE,
    "spark.sql.warehouse.dir":                 f"{HDFS_NN}/user/hive/warehouse",
    "spark.hadoop.dfs.replication":            "1",
    # Memory settings for the local Docker cluster
    "spark.driver.memory":                     "1g",
    "spark.executor.memory":                   "1g",
    # Streaming-friendly defaults (used by Bronze; harmless for batch)
    "spark.sql.streaming.metricsEnabled":      "true",
    "spark.sql.shuffle.partitions":            "8",
}

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="medallion_pipeline",
    description="Bronze (streaming, Trigger.AvailableNow) → Silver → Gold ETL",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # Near-realtime: trigger every 5 minutes. Each run drains new Kafka offsets
    # via Bronze (Trigger.AvailableNow) and rebuilds Silver/Gold. End-to-end
    # freshness ≈ 3-8 minutes. max_active_runs=1 prevents overlap.
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,   # never overlap — one run at a time
    tags=["data-engineering", "medallion", "etl"],
) as dag:

    # ── Stage 1: Bronze — incremental Kafka → HDFS ─────────────────────
    # Trigger.AvailableNow drains new offsets and exits. Checkpoint at
    # /datalake/_checkpoints/bronze/<sink> tracks consumed offsets.
    bronze = SparkSubmitOperator(
        task_id="bronze_ingestion",
        application=f"{SPARK_JOBS}/bronze_ingestion.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        packages=KAFKA_PKG,
        verbose=False,
        name="bronze-ingestion-{{ ds }}",
    )

    # ── Stage 2: Silver — clean, deduplicate, normalize, DLQ split ─────
    silver = SparkSubmitOperator(
        task_id="silver_transform",
        application=f"{SPARK_JOBS}/silver_transform.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        verbose=False,
        name="silver-transform-{{ ds }}",
    )

    # ── Stage 3: Gold — star schema + Hive registration ────────────────
    gold = SparkSubmitOperator(
        task_id="gold_transform",
        application=f"{SPARK_JOBS}/gold_transform.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        verbose=False,
        name="gold-transform-{{ ds }}",
    )

    bronze >> silver >> gold
