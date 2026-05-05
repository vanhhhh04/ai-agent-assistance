"""
Gold Transform Job
==================
Reads Silver Parquet, builds a star schema, and registers the tables
in Hive Metastore so the AI Agent can query them via SQL.

Star schema:
  fact_sales      — one row per order_item (grain = item sold)
  fact_reviews    — one row per product review (grain = review)
  fact_feedback   — one row per customer feedback (grain = feedback)
  dim_customers   — customer dimension
  dim_products    — product dimension (enriched with category_name)
  dim_categories  — category dimension (with parent_name self-lookup)
  dim_addresses   — address dimension
  dim_coupons     — coupon dimension
  dim_payments    — payment dimension
  dim_shipping    — shipping dimension

All Gold tables are EXTERNAL Hive tables pointing to HDFS Parquet.
The AI Agent queries HiveServer2 via JDBC using standard SQL.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

HDFS_SILVER = "hdfs://namenode:9000/datalake/silver"
HDFS_GOLD   = "hdfs://namenode:9000/datalake/gold"
HIVE_DB     = "gold"

spark = SparkSession.builder \
    .appName("Gold-Transform") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("spark.sql.parquet.outputTimestampType", "INT96") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DB}")
spark.sql(f"USE {HIVE_DB}")


def read_silver(entity: str) -> DataFrame:
    """Read a Silver table; tolerate missing path (Silver may not have run yet)."""
    path = f"{HDFS_SILVER}/{entity}"
    try:
        return spark.read.parquet(path)
    except Exception as e:
        if "does not exist" in str(e).lower():
            print(f"[GOLD]   (skip — {path} missing, returning empty)")
            return spark.createDataFrame([], "id int")
        raise


def write_gold(df: DataFrame, table_name: str, partition_cols: list = None):
    """
    Write a Gold table — Parquet on HDFS + register in Hive Metastore.
    Uses saveAsTable so Spark handles both the data write AND the catalog
    registration atomically (correct for partitioned tables).
    """
    path = f"{HDFS_GOLD}/{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {HIVE_DB}.{table_name}")

    writer = (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("path", path)        # external — data lives outside Hive warehouse
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{HIVE_DB}.{table_name}")

    count = df.count()
    print(f"[GOLD]   ✓ {table_name} ({count:,} rows) → {path}")


print("\n[GOLD] Reading Silver tables …")
df_orders      = read_silver("orders")
df_customers   = read_silver("customers")
df_order_items = read_silver("order_items")
df_products    = read_silver("products")
df_payments    = read_silver("payments")
df_shipping    = read_silver("shipping")
df_addresses   = read_silver("addresses")
df_categories  = read_silver("categories")
df_coupons     = read_silver("coupons")
df_reviews     = read_silver("reviews")
df_feedback    = read_silver("feedback")


# ─────────────────────────────────────────────────────────────
#  DIM CUSTOMERS
# ─────────────────────────────────────────────────────────────
print("\n[GOLD] Building dim_customers …")
dim_customers = df_customers.select(
    F.col("id").alias("customer_key"),
    F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("customer_name"),
    F.col("email"),
    F.col("gender"),
    F.col("date_of_birth"),
    F.col("created_at").alias("customer_since"),
)
write_gold(dim_customers, "dim_customers")


# ─────────────────────────────────────────────────────────────
#  DIM PRODUCTS
# ─────────────────────────────────────────────────────────────
print("\n[GOLD] Building dim_categories …")
# Self-join to fetch parent_name for hierarchical browsing
dim_categories = (
    df_categories.alias("c")
    .join(df_categories.alias("p"),
          F.col("c.parent_category_id") == F.col("p.id"), "left")
    .select(
        F.col("c.id").alias("category_key"),
        F.col("c.name").alias("category_name"),
        F.col("c.description").alias("category_description"),
        F.col("c.parent_category_id"),
        F.col("p.name").alias("parent_category_name"),
        F.col("c.created_at").alias("category_created_at"),
    )
)
write_gold(dim_categories, "dim_categories")


print("\n[GOLD] Building dim_addresses …")
dim_addresses = df_addresses.select(
    F.col("id").alias("address_key"),
    F.col("customer_id"),
    F.col("type").alias("address_type"),
    F.col("street"),
    F.col("city"),
    F.col("state"),
    F.col("zip_code"),
    F.col("country"),
    F.col("is_default"),
)
write_gold(dim_addresses, "dim_addresses")


print("\n[GOLD] Building dim_coupons …")
dim_coupons = df_coupons.select(
    F.col("id").alias("coupon_key"),
    F.col("code").alias("coupon_code"),
    F.col("discount_type"),
    F.col("discount_value"),
    F.col("min_order_amount"),
    F.col("max_uses"),
    F.col("times_used"),
    F.col("valid_from"),
    F.col("valid_until"),
    F.col("is_active").alias("coupon_active"),
)
write_gold(dim_coupons, "dim_coupons")


print("\n[GOLD] Building dim_products …")
# Denormalize category_name from dim_categories for fast AI queries
dim_products = (
    df_products.alias("p")
    .join(dim_categories.alias("c"),
          F.col("p.category_id") == F.col("c.category_key"), "left")
    .select(
        F.col("p.id").alias("product_key"),
        F.col("p.sku"),
        F.col("p.name").alias("product_name"),
        F.col("p.brand"),
        F.col("p.category_id"),
        F.col("c.category_name"),
        F.col("c.parent_category_name"),
        F.col("p.price").alias("list_price"),
        F.col("p.cost"),
        F.col("p.is_active"),
    )
)
write_gold(dim_products, "dim_products")


# ─────────────────────────────────────────────────────────────
#  DIM PAYMENTS
# ─────────────────────────────────────────────────────────────
print("\n[GOLD] Building dim_payments …")
dim_payments = df_payments.select(
    F.col("id").alias("payment_key"),
    F.col("order_id"),
    F.col("payment_method"),
    F.col("amount").alias("payment_amount"),
    F.col("status").alias("payment_status"),
    F.col("transaction_id"),
    F.col("paid_at"),
)
write_gold(dim_payments, "dim_payments")


# ─────────────────────────────────────────────────────────────
#  DIM SHIPPING
# ─────────────────────────────────────────────────────────────
print("\n[GOLD] Building dim_shipping …")
dim_shipping = df_shipping.select(
    F.col("id").alias("shipping_key"),
    F.col("order_id"),
    F.col("carrier"),
    F.col("tracking_number"),
    F.col("status").alias("shipping_status"),
    F.col("shipped_at"),
    F.col("delivered_at"),
    (
        (F.unix_timestamp("delivered_at") - F.unix_timestamp("shipped_at")) / 86400
    ).alias("delivery_days"),
)
write_gold(dim_shipping, "dim_shipping")


# ─────────────────────────────────────────────────────────────
#  FACT SALES  (grain: one row per order_item)
# ─────────────────────────────────────────────────────────────
print("\n[GOLD] Building fact_sales …")
fact_sales = (
    df_order_items.alias("i")
    .join(df_orders.alias("o"),    F.col("i.order_id")   == F.col("o.id"), "inner")
    .join(dim_customers.alias("c"),F.col("o.customer_id") == F.col("c.customer_key"), "left")
    .join(dim_products.alias("p"), F.col("i.product_id")  == F.col("p.product_key"), "left")
    .join(dim_payments.alias("pm"),F.col("o.id")          == F.col("pm.order_id"), "left")
    .join(dim_shipping.alias("s"), F.col("o.id")          == F.col("s.order_id"), "left")
    .join(dim_coupons.alias("cp"), F.col("o.coupon_id")   == F.col("cp.coupon_key"), "left")
    .join(dim_addresses.alias("sa"), F.col("o.shipping_address_id") == F.col("sa.address_key"), "left")
    .select(
        # Keys
        F.col("i.id").alias("order_item_key"),
        F.col("o.id").alias("order_key"),
        F.col("c.customer_key"),
        F.col("p.product_key"),
        F.col("pm.payment_key"),
        F.col("s.shipping_key"),
        F.col("cp.coupon_key"),
        F.col("sa.address_key").alias("shipping_address_key"),
        # Order measures
        F.col("o.status").alias("order_status"),
        F.col("o.order_date"),
        F.year("o.order_date").alias("order_year"),
        F.month("o.order_date").alias("order_month"),
        F.dayofmonth("o.order_date").alias("order_day"),
        F.col("o.subtotal"),
        F.col("o.discount_amount"),
        F.col("o.tax_amount"),
        F.col("o.shipping_cost"),
        F.col("o.total_amount").alias("order_total"),
        # Item measures
        F.col("i.quantity"),
        F.col("i.unit_price"),
        F.col("i.total_price").alias("item_total"),
        # Enriched dims (denormalized for fast AI queries)
        F.col("c.customer_name"),
        F.col("c.gender"),
        F.col("p.sku"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("p.list_price"),
        F.col("pm.payment_method"),
        F.col("pm.payment_status"),
        F.col("s.carrier"),
        F.col("s.shipping_status"),
        F.col("s.delivery_days"),
        F.col("cp.coupon_code"),
        F.col("cp.discount_type"),
        F.col("sa.city").alias("shipping_city"),
        F.col("sa.state").alias("shipping_state"),
        F.col("sa.country").alias("shipping_country"),
    )
)

# Drop rows with NULL partition keys — Hive 2.3 chokes on
# __HIVE_DEFAULT_PARTITION__ when the partition column had NULLs.
# Rows with NULL order_date come from order_items whose order isn't yet in
# Silver (race condition or Bronze-to-Silver lag) and can't be queried by date
# anyway, so they're not useful in fact_sales.
fact_sales = fact_sales.filter(
    F.col("order_year").isNotNull() & F.col("order_month").isNotNull()
)
write_gold(fact_sales, "fact_sales", partition_cols=["order_year", "order_month"])


# ─────────────────────────────────────────────
#  FACT REVIEWS  (grain: one row per review)
# ─────────────────────────────────────────────
print("\n[GOLD] Building fact_reviews …")
fact_reviews = (
    df_reviews.alias("r")
    .join(dim_customers.alias("c"), F.col("r.customer_id") == F.col("c.customer_key"), "left")
    .join(dim_products.alias("p"),  F.col("r.product_id")  == F.col("p.product_key"),  "left")
    .select(
        F.col("r.id").alias("review_key"),
        F.col("c.customer_key"),
        F.col("p.product_key"),
        F.col("r.order_id").alias("order_key"),
        F.col("r.rating"),
        F.col("r.title").alias("review_title"),
        F.col("r.comment").alias("review_comment"),
        F.col("r.is_verified"),
        F.col("r.created_at").alias("review_date"),
        F.year("r.created_at").alias("review_year"),
        F.month("r.created_at").alias("review_month"),
        # Denormalized for fast AI queries
        F.col("c.customer_name"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("p.category_name"),
    )
).filter(F.col("review_year").isNotNull() & F.col("review_month").isNotNull())
write_gold(fact_reviews, "fact_reviews", partition_cols=["review_year", "review_month"])


# ─────────────────────────────────────────────
#  FACT FEEDBACK  (grain: one row per feedback)
# ─────────────────────────────────────────────
print("\n[GOLD] Building fact_feedback …")
fact_feedback = (
    df_feedback.alias("f")
    .join(dim_customers.alias("c"), F.col("f.customer_id") == F.col("c.customer_key"), "left")
    .select(
        F.col("f.id").alias("feedback_key"),
        F.col("c.customer_key"),
        F.col("f.order_id").alias("order_key"),
        F.col("f.type").alias("feedback_type"),
        F.col("f.subject"),
        F.col("f.message"),
        F.col("f.status").alias("feedback_status"),
        F.col("f.priority"),
        F.col("f.created_at").alias("feedback_date"),
        F.col("f.resolved_at"),
        (
            (F.unix_timestamp("f.resolved_at") - F.unix_timestamp("f.created_at")) / 86400
        ).alias("resolution_days"),
        F.year("f.created_at").alias("feedback_year"),
        F.month("f.created_at").alias("feedback_month"),
        F.col("c.customer_name"),
    )
).filter(F.col("feedback_year").isNotNull() & F.col("feedback_month").isNotNull())
write_gold(fact_feedback, "fact_feedback", partition_cols=["feedback_year", "feedback_month"])


# ─────────────────────────────────────────────────────────────
#  VERIFY — quick sanity check
# ─────────────────────────────────────────────────────────────
print("\n[GOLD] Verification …")
spark.sql(f"SHOW TABLES IN {HIVE_DB}").show()
spark.sql(f"""
    SELECT order_year, order_month,
           COUNT(*) AS total_items,
           SUM(item_total) AS total_revenue
    FROM {HIVE_DB}.fact_sales
    GROUP BY order_year, order_month
    ORDER BY order_year, order_month
""").show(20)

print("\n[GOLD] Done.")
spark.stop()
