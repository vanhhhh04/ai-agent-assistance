"""
Medallion Pipeline DAG
======================
Orchestrates Bronze → Silver → Gold Spark jobs on a schedule.

Each stage depends on the previous — Airflow enforces this ordering.
SparkSubmitOperator submits jobs to the Spark cluster (client mode);
the job file runs on the Airflow container, executors run on Spark workers.

Schedule: every hour (adjust to match your data freshness requirement).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_MASTER = "spark://spark-master:7077"
SPARK_JOBS   = "/opt/airflow/spark_jobs"
HDFS_NN      = "hdfs://namenode:9000"
HIVE_MSTORE  = "thrift://hive-metastore:9083"
KAFKA_BROKER = "kafka:29092"

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
    description="Bronze → Silver → Gold ETL pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["data-engineering", "medallion", "etl"],
) as dag:

    # ── Stage 1: Bronze — raw ingestion from Kafka ───────────────────────
    bronze = SparkSubmitOperator(
        task_id="bronze_ingestion",
        application=f"{SPARK_JOBS}/bronze_ingestion.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        # Kafka connector downloaded from Maven at submit time
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        verbose=False,
        name="bronze-ingestion-{{ ds }}",
    )

    # ── Stage 2: Silver — clean, deduplicate, normalize ──────────────────
    silver = SparkSubmitOperator(
        task_id="silver_transform",
        application=f"{SPARK_JOBS}/silver_transform.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        verbose=False,
        name="silver-transform-{{ ds }}",
    )

    # ── Stage 3: Gold — star schema + Hive registration ──────────────────
    gold = SparkSubmitOperator(
        task_id="gold_transform",
        application=f"{SPARK_JOBS}/gold_transform.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        verbose=False,
        name="gold-transform-{{ ds }}",
    )

    # DAG dependency chain
    bronze >> silver >> gold
