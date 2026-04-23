"""
Silver Transform Job
====================
Reads Bronze Parquet and produces clean, typed Silver tables.

Each source has a DIFFERENT format — handled per source:

  ERP (Debezium + ExtractNewRecordState SMT):
    Flat JSON with all table columns at top level.
    __op    = "c"|"u"|"d"  (create/update/delete)
    __table = "orders"|"customers"|"order_items"|"coupons"
    _source_topic tells us which Debezium topic it came from.

  Warehouse (NiFi CSV→JSON, flat):
    Flat JSON — envelope fields (_source_system, _event_id, _quality_flag, etc.)
    AND payload fields (sku, name, price, ...) all at the same level.
    Produced by sim_warehouse.py writing flat CSV → NiFi ConvertRecord.

  Payment (NiFi HTTP passthrough, nested envelope):
    {"_source_system": "payment_gw", ..., "payload": { <data> }}
    Produced by sim_payment.py posting directly to NiFi ListenHTTP.

Silver outputs (HDFS Parquet):
  /datalake/silver/orders
  /datalake/silver/customers
  /datalake/silver/order_items
  /datalake/silver/products
  /datalake/silver/payments
  /datalake/silver/shipping
  /datalake/silver/dlq
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DecimalType, BooleanType,
)

HDFS_BRONZE = "hdfs://namenode:9000/datalake/bronze"
HDFS_SILVER = "hdfs://namenode:9000/datalake/silver"

PLACEHOLDERS = ["UNKNOWN", "N/A", "#N/A", "None", "null",
                 "EMPTY", "--", "???", "TBD", "not available", ""]

spark = SparkSession.builder \
    .appName("Silver-Transform") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ─────────────────────────────────────────────────────────────
#  SHARED HELPERS
# ─────────────────────────────────────────────────────────────
def null_placeholders(df: DataFrame, cols: list) -> DataFrame:
    for c in cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.when(F.trim(F.col(c).cast("string")).isin(PLACEHOLDERS), None)
                 .otherwise(F.col(c))
            )
    return df


def normalize_status(col_expr, valid: list) -> F.Column:
    cleaned = F.upper(F.trim(col_expr))
    return F.when(cleaned.isin(valid), cleaned).otherwise(None)


def drop_dirty(df: DataFrame, required: list):
    """Split into (clean, dirty) DataFrames."""
    null_cond = F.col(required[0]).isNull()
    for c in required[1:]:
        null_cond = null_cond | F.col(c).isNull()

    dirty_cond = F.col("_quality_flag").isin(["DIRTY", "QUARANTINE"]) if "_quality_flag" in df.columns else F.lit(False)

    df_dirty = df.filter(dirty_cond | null_cond)
    df_clean = df.filter(~dirty_cond & ~null_cond)
    return df_clean, df_dirty


def write_silver(df: DataFrame, name: str):
    path = f"{HDFS_SILVER}/{name}"
    df.write.mode("overwrite").parquet(path)
    print(f"[SILVER]   ✓ {name} ({df.count():,} rows) → {path}")


# ─────────────────────────────────────────────────────────────
#  SOURCE 1: ERP — Debezium flat format
#  _source_topic = "erp.public.{table}"
#  Columns: table fields + __op + __table + __source_ts_ms
# ─────────────────────────────────────────────────────────────
print("\n[SILVER] === ERP source (Debezium flat format) ===")
df_erp_raw = spark.read.parquet(f"{HDFS_BRONZE}/erp_raw")

# Schema for each ERP entity — all flat
ORDER_SCHEMA = StructType([
    StructField("id",                  IntegerType()),
    StructField("customer_id",         IntegerType()),
    StructField("shipping_address_id", IntegerType()),
    StructField("billing_address_id",  IntegerType()),
    StructField("coupon_id",           IntegerType()),
    StructField("status",              StringType()),
    StructField("subtotal",            StringType()),
    StructField("discount_amount",     StringType()),
    StructField("tax_amount",          StringType()),
    StructField("shipping_cost",       StringType()),
    StructField("total_amount",        StringType()),
    StructField("order_date",          StringType()),
    StructField("created_at",          StringType()),
    StructField("__op",                StringType()),
    StructField("__table",             StringType()),
    StructField("__source_ts_ms",      StringType()),
    StructField("__deleted",           StringType()),
])

CUSTOMER_SCHEMA = StructType([
    StructField("id",         IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name",  StringType()),
    StructField("email",      StringType()),
    StructField("phone",      StringType()),
    StructField("gender",     StringType()),
    StructField("date_of_birth", StringType()),
    StructField("created_at",    StringType()),
    StructField("__op",          StringType()),
    StructField("__table",       StringType()),
])

ORDER_ITEM_SCHEMA = StructType([
    StructField("id",          IntegerType()),
    StructField("order_id",    IntegerType()),
    StructField("product_id",  IntegerType()),
    StructField("quantity",    IntegerType()),
    StructField("unit_price",  StringType()),
    StructField("total_price", StringType()),
    StructField("__op",        StringType()),
    StructField("__table",     StringType()),
])


def parse_erp_topic(topic_suffix: str, schema: StructType) -> DataFrame:
    """Filter erp_raw by topic, parse JSON with given schema."""
    topic = f"erp.public.{topic_suffix}"
    return (
        df_erp_raw
        .filter(F.col("_source_topic") == topic)
        .select(
            F.from_json(F.col("raw_data"), schema).alias("d"),
            F.col("_bronze_ingested_at"),
        )
        .select("d.*", "_bronze_ingested_at")
        # Debezium dedup: keep only INSERT/UPDATE, skip DELETE for Silver
        .filter(F.col("__op") != "d")
    )


# --- orders ---
df_o = parse_erp_topic("orders", ORDER_SCHEMA)
df_o = null_placeholders(df_o, ["status", "total_amount"])
df_orders = (
    df_o
    .withColumn("subtotal",        F.col("subtotal").cast(DecimalType(12,2)))
    .withColumn("discount_amount", F.col("discount_amount").cast(DecimalType(12,2)))
    .withColumn("tax_amount",      F.col("tax_amount").cast(DecimalType(12,2)))
    .withColumn("shipping_cost",   F.col("shipping_cost").cast(DecimalType(12,2)))
    .withColumn("total_amount",    F.col("total_amount").cast(DecimalType(12,2)))
    .withColumn("order_date",      F.to_timestamp("order_date"))
    .withColumn("created_at",      F.to_timestamp("created_at"))
    .withColumn("status",
        normalize_status(F.col("status"),
            ["PENDING","PROCESSING","SHIPPED","DELIVERED","CANCELLED","RETURNED"])
    )
    .filter(F.col("total_amount") > 0)
    .dropDuplicates(["id"])
)
df_orders_clean, df_orders_dirty = drop_dirty(df_orders, ["id", "customer_id", "total_amount"])
print(f"[SILVER]   orders  clean={df_orders_clean.count():,}  dirty={df_orders_dirty.count():,}")

# --- customers ---
df_c = parse_erp_topic("customers", CUSTOMER_SCHEMA)
df_c = null_placeholders(df_c, ["email", "phone"])
df_customers = (
    df_c
    .withColumn("email",         F.lower(F.trim(F.col("email"))))
    .withColumn("first_name",   F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name",    F.initcap(F.trim(F.col("last_name"))))
    .withColumn("gender",       F.upper(F.trim(F.col("gender"))))
    .withColumn("date_of_birth",F.to_date("date_of_birth"))
    .withColumn("created_at",   F.to_timestamp("created_at"))
    .filter(F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))
    .dropDuplicates(["id"])
)
df_cust_clean, df_cust_dirty = drop_dirty(df_customers, ["id", "email"])
print(f"[SILVER]   customers  clean={df_cust_clean.count():,}  dirty={df_cust_dirty.count():,}")

# --- order_items ---
df_i = parse_erp_topic("order_items", ORDER_ITEM_SCHEMA)
df_items = (
    df_i
    .withColumn("unit_price",  F.col("unit_price").cast(DecimalType(12,2)))
    .withColumn("total_price", F.col("total_price").cast(DecimalType(12,2)))
    .filter(F.col("quantity") > 0)
    .dropDuplicates(["id"])
)
df_items_clean, df_items_dirty = drop_dirty(df_items, ["id", "order_id", "product_id"])
print(f"[SILVER]   order_items  clean={df_items_clean.count():,}  dirty={df_items_dirty.count():,}")


# ─────────────────────────────────────────────────────────────
#  SOURCE 2: WAREHOUSE — flat CSV-as-JSON
#  All fields at top level: _source_system, _event_id, _quality_flag,
#  category_id, name, sku, price, cost, stock_quantity, brand, ...
# ─────────────────────────────────────────────────────────────
print("\n[SILVER] === Warehouse source (flat CSV-as-JSON) ===")

WH_SCHEMA = StructType([
    # Envelope fields (flat)
    StructField("_source_system",  StringType()),
    StructField("_event_id",       StringType()),
    StructField("_event_type",     StringType()),
    StructField("_ingested_at",    StringType()),
    StructField("_quality_flag",   StringType()),
    StructField("_dirty_reason",   StringType()),
    # Product payload fields (flat — no nested object)
    StructField("category_id",     IntegerType()),
    StructField("name",            StringType()),
    StructField("description",     StringType()),
    StructField("sku",             StringType()),
    StructField("price",           StringType()),
    StructField("cost",            StringType()),
    StructField("stock_quantity",  IntegerType()),
    StructField("brand",           StringType()),
    StructField("weight",          StringType()),
    StructField("is_active",       StringType()),
    StructField("created_at",      StringType()),
    StructField("updated_at",      StringType()),
])

df_wh_raw = spark.read.parquet(f"{HDFS_BRONZE}/wh_raw")
df_wh = (
    df_wh_raw
    .select(F.from_json(F.col("raw_data"), WH_SCHEMA).alias("d"), F.col("_bronze_ingested_at"))
    .select("d.*", "_bronze_ingested_at")
)
df_wh = null_placeholders(df_wh, ["name", "sku", "brand"])

sku_canonical = F.regexp_replace(F.upper(F.trim(F.col("sku"))), r"[_\s]", "-")

df_products = (
    df_wh
    .withColumn("sku",        sku_canonical)
    .withColumn("name",       F.trim(F.col("name")))
    .withColumn("brand",      F.initcap(F.trim(F.col("brand"))))
    .withColumn("price",      F.col("price").cast(DecimalType(12,2)))
    .withColumn("cost",       F.col("cost").cast(DecimalType(12,2)))
    .withColumn("is_active",  F.col("is_active").cast(BooleanType()))
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .filter(F.col("price") > 0)
    .dropDuplicates(["sku"])
)
# Deduplicate by _event_id (warehouse envelope has this field)
df_products = df_products.dropDuplicates(["_event_id"])

df_prod_clean, df_prod_dirty = drop_dirty(df_products, ["sku", "name", "price"])
print(f"[SILVER]   products  clean={df_prod_clean.count():,}  dirty={df_prod_dirty.count():,}")


# ─────────────────────────────────────────────────────────────
#  SOURCE 3: PAYMENT — nested envelope format
#  {"_source_system":..., "payload": { <payment/shipping/review data> }}
# ─────────────────────────────────────────────────────────────
print("\n[SILVER] === Payment source (nested envelope) ===")

ENVELOPE_SCHEMA = StructType([
    StructField("_source_system",  StringType()),
    StructField("_event_id",       StringType()),
    StructField("_event_type",     StringType()),
    StructField("_op_type",        StringType()),
    StructField("_ingested_at",    StringType()),
    StructField("_quality_flag",   StringType()),
    StructField("payload",         StringType()),   # nested JSON string
])

PAYMENT_SCHEMA = StructType([
    StructField("id",             IntegerType()),
    StructField("order_id",       IntegerType()),
    StructField("payment_method", StringType()),
    StructField("amount",         StringType()),
    StructField("status",         StringType()),
    StructField("transaction_id", StringType()),
    StructField("paid_at",        StringType()),
])

SHIPPING_SCHEMA = StructType([
    StructField("id",              IntegerType()),
    StructField("order_id",        IntegerType()),
    StructField("carrier",         StringType()),
    StructField("tracking_number", StringType()),
    StructField("status",          StringType()),
    StructField("shipped_at",      StringType()),
    StructField("delivered_at",    StringType()),
])

df_pay_raw = spark.read.parquet(f"{HDFS_BRONZE}/pay_raw")
df_pay_env = (
    df_pay_raw
    .select(F.from_json(F.col("raw_data"), ENVELOPE_SCHEMA).alias("env"), F.col("_bronze_ingested_at"))
    .select("env.*", "_bronze_ingested_at")
    .dropDuplicates(["_event_id"])
)

# --- payments ---
df_pmt_raw = (
    df_pay_env
    .filter(F.col("_event_type") == "payments")
    .select(
        F.from_json(F.col("payload"), PAYMENT_SCHEMA).alias("p"),
        F.col("_quality_flag"), F.col("_bronze_ingested_at"),
    )
    .select("p.*", "_quality_flag", "_bronze_ingested_at")
)
df_pmt_raw = null_placeholders(df_pmt_raw, ["payment_method", "status", "transaction_id"])
df_payments = (
    df_pmt_raw
    .withColumn("amount",  F.col("amount").cast(DecimalType(12,2)))
    .withColumn("paid_at", F.to_timestamp("paid_at"))
    .withColumn("payment_method",
        normalize_status(F.col("payment_method"),
            ["CREDIT_CARD","DEBIT_CARD","PAYPAL","APPLE_PAY","GOOGLE_PAY"]))
    .withColumn("status",
        normalize_status(F.col("status"),
            ["PENDING","COMPLETED","FAILED","REFUNDED"]))
    .filter(F.col("amount") > 0)
    .dropDuplicates(["transaction_id"])
)
df_pmt_clean, df_pmt_dirty = drop_dirty(df_payments, ["id", "order_id", "transaction_id"])
print(f"[SILVER]   payments  clean={df_pmt_clean.count():,}  dirty={df_pmt_dirty.count():,}")

# --- shipping ---
df_ship_raw = (
    df_pay_env
    .filter(F.col("_event_type") == "shipping")
    .select(
        F.from_json(F.col("payload"), SHIPPING_SCHEMA).alias("p"),
        F.col("_quality_flag"), F.col("_bronze_ingested_at"),
    )
    .select("p.*", "_quality_flag", "_bronze_ingested_at")
)
df_shipping = (
    df_ship_raw
    .withColumn("status",       normalize_status(F.col("status"),
        ["PENDING","PICKED_UP","IN_TRANSIT","OUT_FOR_DELIVERY","DELIVERED","RETURNED"]))
    .withColumn("shipped_at",   F.to_timestamp("shipped_at"))
    .withColumn("delivered_at", F.to_timestamp("delivered_at"))
    .dropDuplicates(["tracking_number"])
)
df_ship_clean, df_ship_dirty = drop_dirty(df_shipping, ["id", "order_id"])
print(f"[SILVER]   shipping  clean={df_ship_clean.count():,}  dirty={df_ship_dirty.count():,}")


# ─────────────────────────────────────────────────────────────
#  WRITE SILVER TABLES
# ─────────────────────────────────────────────────────────────
print("\n[SILVER] Writing tables…")

write_silver(df_orders_clean,  "orders")
write_silver(df_cust_clean,    "customers")
write_silver(df_items_clean,   "order_items")
write_silver(df_prod_clean,    "products")
write_silver(df_pmt_clean,     "payments")
write_silver(df_ship_clean,    "shipping")

# Collect all dirty records into DLQ (union on common columns)
DLQ_COLS = ["id", "_quality_flag", "_bronze_ingested_at"]
all_dirty = df_orders_dirty.select(DLQ_COLS) \
    .union(df_cust_dirty.select(DLQ_COLS)) \
    .union(df_items_dirty.select(DLQ_COLS)) \
    .union(df_prod_dirty.select([c for c in DLQ_COLS if c in df_prod_dirty.columns])) \
    .union(df_pmt_dirty.select(DLQ_COLS)) \
    .union(df_ship_dirty.select([c for c in DLQ_COLS if c in df_ship_dirty.columns]))

all_dirty.write.mode("append").parquet(f"{HDFS_SILVER}/dlq")
print(f"[SILVER]   ✓ DLQ ({all_dirty.count():,} records) → {HDFS_SILVER}/dlq")

print("\n[SILVER] Done.")
spark.stop()
