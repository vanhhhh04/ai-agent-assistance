from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("DataCleaningPipeline") \
    .getOrCreate()

def clean_table(df, table_name, schema_config):
    """Pipeline lam sach tong hop cho mot bang"""
    print(f"\n{'='*50}")
    print(f"Cleaning: {table_name}")
    print(f"Original rows: {df.count()}")

    # Step 1: Remove corrupt records
    if "_corrupt_record" in df.columns:
        df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    # Step 2: Normalize placeholders to null
    placeholders = ["UNKNOWN","N/A","#N/A","None","null","EMPTY","--","???","TBD","not available"]
    for c in df.columns:
        df = df.withColumn(c,
            F.when(F.col(c).isin(placeholders, None))