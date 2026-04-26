"""
Silver Transform Job
====================
Reads Bronze Parquet (append-only from streaming Bronze) and rebuilds clean,
typed Silver tables in overwrite mode.

Why overwrite Silver?
  Bronze is append-only — Silver recomputed from the full history each run is
  the simplest correct strategy. For production with millions of rows you would
  switch to Delta/Iceberg MERGE; for this project the data volume is bounded.

Each source has a DIFFERENT payload format — handled per source:

  ERP (Debezium + ExtractNewRecordState SMT):
    Flat JSON, all table columns at top level.
    __op    = "c"|"u"|"d"        (create/update/delete)
    __table = "orders"|"customers"|"order_items"|"coupons"
    _source_topic = "erp.public.<table>"

  Warehouse (NiFi CSV→JSON, flat):
    Envelope fields and payload fields all flat at top level.
    Source topic: warehouse.events

  Payment (NiFi HTTP passthrough, NESTED envelope):
    {"_source_system": "...", "_event_type": "...", "payload": { <body> }}
    Source topic: payment.events
    `payload` is a nested JSON object — extracted via get_json_object,
    then re-parsed with the appropriate per-event-type schema.

Silver outputs (HDFS Parquet, overwrite each run):
  /datalake/silver/orders
  /datalake/silver/customers
  /datalake/silver/order_items
  /datalake/silver/products
  /datalake/silver/payments
  /datalake/silver/shipping
  /datalake/silver/dlq
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DecimalType, BooleanType,
)

HDFS_BRONZE = "hdfs://namenode:9000/datalake/bronze"
HDFS_SILVER = "hdfs://namenode:9000/datalake/silver"

PLACEHOLDERS = ["UNKNOWN", "N/A", "#N/A", "None", "null",
                "EMPTY", "--", "???", "TBD", "not available", ""]

spark = (
    SparkSession.builder
    .appName("Silver-Transform")
    .getOrCreate()
)
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
    """Split into (clean, dirty) DataFrames based on required-NULL + _quality_flag."""
    null_cond = F.col(required[0]).isNull()
    for c in required[1:]:
        null_cond = null_cond | F.col(c).isNull()

    if "_quality_flag" in df.columns:
        dirty_cond = F.col("_quality_flag").isin(["DIRTY", "QUARANTINE"])
    else:
        dirty_cond = F.lit(False)

    df_dirty = df.filter(dirty_cond | null_cond)
    df_clean = df.filter(~dirty_cond & ~null_cond)
    return df_clean, df_dirty


def write_silver(df: DataFrame, name: str):
    path = f"{HDFS_SILVER}/{name}"
    df.write.mode("overwrite").parquet(path)
    print(f"[SILVER]   ✓ {name} ({df.count():,} rows) → {path}")


def safe_read_parquet(path: str) -> DataFrame:
    """
    Read parquet, return empty DataFrame if path doesn't exist.
    Trigger.AvailableNow may produce no files when there are no new Kafka
    messages — Silver should not crash on first runs.
    """
    try:
        return spark.read.parquet(path)
    except Exception as e:
        msg = str(e)
        if "Path does not exist" in msg or "does not exist" in msg.lower():
            print(f"[SILVER]   (skip — {path} not found yet, returning empty)")
            return spark.createDataFrame([], "raw_data string, _source_topic string, _bronze_ingested_at timestamp")
        raise


# ─────────────────────────────────────────────────────────────
#  SOURCE 1: ERP — Debezium flat format
# ─────────────────────────────────────────────────────────────
print("\n[SILVER] === ERP source (Debezium flat format) ===")
df_erp_raw = safe_read_parquet(f"{HDFS_BRONZE}/erp_raw")

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
    StructField("id",            IntegerType()),
    StructField("first_name",    StringType()),
    StructField("last_name",     StringType()),
    StructField("email",         StringType()),
    StructField("phone",         StringType()),
    StructField("gender",        StringType()),
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

ERP_PRODUCT_SCHEMA = StructType([
    StructField("id",             IntegerType()),
    StructField("category_id",    IntegerType()),
    StructField("name",           StringType()),
    StructField("description",    StringType()),
    StructField("sku",            StringType()),
    StructField("price",          StringType()),
    StructField("cost",           StringType()),
    StructField("stock_quantity", IntegerType()),
    StructField("brand",          StringType()),
    StructField("weight",         StringType()),
    StructField("is_active",      StringType()),
    StructField("created_at",     StringType()),
    StructField("updated_at",     StringType()),
    StructField("__op",           StringType()),
    StructField("__table",        StringType()),
    StructField("__source_ts_ms", StringType()),
])


def parse_erp_topic(topic_suffix: str, schema: StructType) -> DataFrame:
    """Filter erp_raw by topic, parse JSON with the given schema, drop deletes."""
    topic = f"erp.public.{topic_suffix}"
    return (
        df_erp_raw
        .filter(F.col("_source_topic") == topic)
        .select(
            F.from_json(F.col("raw_data"), schema).alias("d"),
            F.col("_bronze_ingested_at"),
        )
        .select("d.*", "_bronze_ingested_at")
        .filter(F.col("__op") != "d")  # skip deletes for Silver
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
    # CDC dedup: take the LATEST event per id (highest __source_ts_ms)
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("id").orderBy(F.col("__source_ts_ms").desc_nulls_last())
    ))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
df_orders_clean, df_orders_dirty = drop_dirty(df_orders, ["id", "customer_id", "total_amount"])
print(f"[SILVER]   orders  clean={df_orders_clean.count():,}  dirty={df_orders_dirty.count():,}")

# --- customers ---
df_c = parse_erp_topic("customers", CUSTOMER_SCHEMA)
df_c = null_placeholders(df_c, ["email", "phone"])
df_customers = (
    df_c
    .withColumn("email",        F.lower(F.trim(F.col("email"))))
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

# --- products (from ERP/Debezium — CANONICAL source, has integer id) ---
df_p = parse_erp_topic("products", ERP_PRODUCT_SCHEMA)
df_p = null_placeholders(df_p, ["name", "sku", "brand"])
sku_canonical_erp = F.regexp_replace(F.upper(F.trim(F.col("sku"))), r"[_\s]", "-")
df_products_erp = (
    df_p
    .withColumn("sku",        sku_canonical_erp)
    .withColumn("name",       F.trim(F.col("name")))
    .withColumn("brand",      F.initcap(F.trim(F.col("brand"))))
    .withColumn("price",      F.col("price").cast(DecimalType(12,2)))
    .withColumn("cost",       F.col("cost").cast(DecimalType(12,2)))
    .withColumn("is_active",  F.col("is_active").cast(BooleanType()))
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .filter(F.col("price") > 0)
    # CDC dedup: latest event per id
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("id").orderBy(F.col("__source_ts_ms").desc_nulls_last())))
    .filter(F.col("_rn") == 1).drop("_rn")
)
df_prod_erp_clean, df_prod_erp_dirty = drop_dirty(df_products_erp, ["id", "sku", "name", "price"])
print(f"[SILVER]   products(ERP)  clean={df_prod_erp_clean.count():,}  dirty={df_prod_erp_dirty.count():,}")


# ─────────────────────────────────────────────────────────────
#  SOURCE 2: WAREHOUSE — flat CSV-as-JSON (stock updates + dirty test data)
#  NOTE: ERP/Debezium is the canonical source for products (has id).
#  This warehouse path now produces a SECONDARY table (warehouse_events) and
#  is no longer used for the main Silver/products table.
# ─────────────────────────────────────────────────────────────
print("\n[SILVER] === Warehouse source (flat CSV-as-JSON) ===")

WH_SCHEMA = StructType([
    StructField("_source_system",  StringType()),
    StructField("_event_id",       StringType()),
    StructField("_event_type",     StringType()),
    StructField("_ingested_at",    StringType()),
    StructField("_quality_flag",   StringType()),
    StructField("_dirty_reason",   StringType()),
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

df_wh_raw = safe_read_parquet(f"{HDFS_BRONZE}/wh_raw")
df_wh = (
    df_wh_raw
    .select(F.from_json(F.col("raw_data"), WH_SCHEMA).alias("d"), F.col("_bronze_ingested_at"))
    .select("d.*", "_bronze_ingested_at")
)
df_wh = null_placeholders(df_wh, ["name", "sku", "brand"])

sku_canonical = F.regexp_replace(F.upper(F.trim(F.col("sku"))), r"[_\s]", "-")

df_products = (
    df_wh
    # Only keep product lifecycle events (skip stock_update which has no SKU)
    .filter(F.col("_event_type").isin(["product.created", "product.updated"]))
    .withColumn("sku",        sku_canonical)
    .withColumn("name",       F.trim(F.col("name")))
    .withColumn("brand",      F.initcap(F.trim(F.col("brand"))))
    .withColumn("price",      F.col("price").cast(DecimalType(12,2)))
    .withColumn("cost",       F.col("cost").cast(DecimalType(12,2)))
    .withColumn("is_active",  F.col("is_active").cast(BooleanType()))
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .filter(F.col("price") > 0)
    .dropDuplicates(["_event_id"])
    .dropDuplicates(["sku"])
)
df_prod_clean, df_prod_dirty = drop_dirty(df_products, ["sku", "name", "price"])
print(f"[SILVER]   products  clean={df_prod_clean.count():,}  dirty={df_prod_dirty.count():,}")


# ─────────────────────────────────────────────────────────────
#  SOURCE 3: PAYMENT — NESTED envelope format
# ─────────────────────────────────────────────────────────────
#  FIX: payload is a JSON OBJECT, not a string. Previous code tried to cast
#       struct → string and got NULL. Use get_json_object to extract payload
#       as a serialized string, then from_json with the per-event-type schema.
# ─────────────────────────────────────────────────────────────
print("\n[SILVER] === Payment source (NESTED envelope, get_json_object) ===")

df_pay_raw = safe_read_parquet(f"{HDFS_BRONZE}/pay_raw")

# Flatten envelope using get_json_object (handles arbitrary nested payload)
df_pay_env = (
    df_pay_raw
    .select(
        F.get_json_object("raw_data", "$._source_system").alias("_source_system"),
        F.get_json_object("raw_data", "$._event_id"     ).alias("_event_id"),
        F.get_json_object("raw_data", "$._event_type"   ).alias("_event_type"),
        F.get_json_object("raw_data", "$._op_type"      ).alias("_op_type"),
        F.get_json_object("raw_data", "$._ingested_at"  ).alias("_ingested_at"),
        F.get_json_object("raw_data", "$._quality_flag" ).alias("_quality_flag"),
        F.get_json_object("raw_data", "$.payload"       ).alias("payload_json"),
        F.col("_bronze_ingested_at"),
    )
    .dropDuplicates(["_event_id"])
)

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

# --- payments ---
# sim_payment.py event_types: payment.created, payment.status_updated
df_pmt_raw = (
    df_pay_env
    .filter(F.col("_event_type").startswith("payment."))
    .select(
        F.from_json(F.col("payload_json"), PAYMENT_SCHEMA).alias("p"),
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
df_pmt_clean, df_pmt_dirty = drop_dirty(df_payments, ["order_id", "transaction_id"])
print(f"[SILVER]   payments  clean={df_pmt_clean.count():,}  dirty={df_pmt_dirty.count():,}")

# --- shipping ---
# sim_payment.py event_types: shipping.created, shipping.status_updated
df_ship_raw = (
    df_pay_env
    .filter(F.col("_event_type").startswith("shipping."))
    .select(
        F.from_json(F.col("payload_json"), SHIPPING_SCHEMA).alias("p"),
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
df_ship_clean, df_ship_dirty = drop_dirty(df_shipping, ["order_id", "tracking_number"])
print(f"[SILVER]   shipping  clean={df_ship_clean.count():,}  dirty={df_ship_dirty.count():,}")


# ─────────────────────────────────────────────────────────────
#  WRITE SILVER TABLES
# ─────────────────────────────────────────────────────────────
print("\n[SILVER] Writing tables…")
write_silver(df_orders_clean,    "orders")
write_silver(df_cust_clean,      "customers")
write_silver(df_items_clean,     "order_items")
write_silver(df_prod_erp_clean,  "products")           # canonical from ERP/CDC (has id)
write_silver(df_prod_clean,      "warehouse_events")   # secondary: stock updates etc.
write_silver(df_pmt_clean,       "payments")
write_silver(df_ship_clean,      "shipping")

# DLQ — union of all dirty records across sources.
# ERP CDC entities don't carry _quality_flag (Debezium → no envelope), so we
# synthesize one for them here based on which source they came from.
def _to_dlq(df: DataFrame, source: str) -> DataFrame:
    out = df
    if "_quality_flag" not in out.columns:
        out = out.withColumn("_quality_flag", F.lit("DIRTY"))
    if "_bronze_ingested_at" not in out.columns:
        out = out.withColumn("_bronze_ingested_at", F.current_timestamp())
    if "id" not in out.columns:
        out = out.withColumn("id", F.lit(None).cast("int"))
    return out.select(
        F.col("id").cast("string").alias("id"),
        F.col("_quality_flag"),
        F.col("_bronze_ingested_at"),
        F.lit(source).alias("_source"),
    )

all_dirty = (
    _to_dlq(df_orders_dirty,   "erp.orders")
    .unionByName(_to_dlq(df_cust_dirty,     "erp.customers"))
    .unionByName(_to_dlq(df_items_dirty,    "erp.order_items"))
    .unionByName(_to_dlq(df_prod_erp_dirty, "erp.products"))
    .unionByName(_to_dlq(df_prod_dirty,     "warehouse.products"))
    .unionByName(_to_dlq(df_pmt_dirty,      "payment.payments"))
    .unionByName(_to_dlq(df_ship_dirty,     "payment.shipping"))
)
all_dirty.write.mode("overwrite").parquet(f"{HDFS_SILVER}/dlq")
print(f"[SILVER]   ✓ DLQ ({all_dirty.count():,} records) → {HDFS_SILVER}/dlq")

print("\n[SILVER] Done.")
spark.stop()
