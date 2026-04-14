# 📋 TÀI LIỆU HƯỚNG DẪN LÀM SẠCH DỮ LIỆU VỚI APACHE SPARK

## 📊 Tổng quan bộ dữ liệu bẩn

Bộ dữ liệu này được tạo với **6 loại lỗi chất lượng dữ liệu** phân bố trên **11 bảng** của hệ thống e-commerce.

### Thống kê chi tiết theo bảng

| Bảng | Dòng gốc | Dòng bẩn | Null | Empty | Placeholder | Date lỗi | Số lỗi | Typo | Ngoài phạm vi | Mixed type | Trùng lặp | Gần trùng | Orphan | Casing |
|------|-----------|-----------|------|-------|-------------|-----------|---------|------|---------------|------------|-----------|------------|--------|--------|
| addresses | - | 8189 | 2360 | 1899 | 1781 | 311 | 0 | 2421 | 0 | 0 | 375 | 150 | 150 | 2726 |
| categories | - | 21 | 3 | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 0 |
| coupons | - | 53 | 24 | 17 | 17 | 4 | 11 | 7 | 9 | 5 | 2 | 1 | 0 | 4 |
| customers | - | 5350 | 1603 | 1219 | 1272 | 593 | 0 | 567 | 0 | 0 | 250 | 100 | 0 | 578 |
| feedback | - | 1635 | 418 | 317 | 315 | 85 | 0 | 480 | 0 | 0 | 75 | 30 | 30 | 555 |
| order_items | - | 25533 | 3775 | 2803 | 2842 | 956 | 5644 | 0 | 4296 | 2181 | 1171 | 468 | 468 | 0 |
| orders | - | 10900 | 3634 | 2726 | 2642 | 1213 | 4020 | 1109 | 3038 | 1564 | 500 | 200 | 200 | 1206 |
| payments | - | 10900 | 2384 | 1754 | 1767 | 705 | 842 | 2177 | 590 | 287 | 500 | 200 | 200 | 2443 |
| products | - | 545 | 228 | 173 | 178 | 45 | 168 | 45 | 119 | 54 | 25 | 10 | 10 | 57 |
| reviews | - | 4355 | 836 | 615 | 546 | 161 | 329 | 446 | 236 | 129 | 199 | 79 | 79 | 487 |
| shipping | - | 10371 | 3034 | 2341 | 2268 | 1714 | 0 | 2103 | 0 | 0 | 475 | 190 | 190 | 2276 |

---

## 🔧 HƯỚNG DẪN LÀM SẠCH TỪNG LOẠI LỖI VỚI PYSPARK

### 1️⃣ Dữ liệu khuyết thiếu (Missing & Null Data)

#### Mô tả lỗi đã inject:
- **Null values**: Các trường ngẫu nhiên được set thành `null`
- **Empty strings**: Giá trị `""` thay vì null
- **Placeholder values**: `"UNKNOWN"`, `"N/A"`, `"#N/A"`, `"None"`, `"null"`, `"EMPTY"`, `"--"`, `"???"`, `"TBD"`, `"not available"`

#### Code PySpark xử lý:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Đọc dữ liệu
df = spark.read.csv("dirty_csv/customers.csv", header=True, inferSchema=True)

# 1.1 Phát hiện null và empty
null_counts = df.select([
    F.sum(F.when(F.col(c).isNull() | (F.col(c) == ""), 1).otherwise(0)).alias(c)
    for c in df.columns
])
null_counts.show()

# 1.2 Thay thế placeholder values bằng null
placeholders = ["UNKNOWN", "N/A", "#N/A", "None", "null", "EMPTY", "--", "???", "TBD", "not available"]
for col_name in df.columns:
    df = df.withColumn(col_name,
        F.when(F.col(col_name).isin(placeholders), None)
         .otherwise(F.col(col_name))
    )

# 1.3 Thay thế empty strings bằng null
for col_name in df.columns:
    df = df.withColumn(col_name,
        F.when(F.trim(F.col(col_name)) == "", None)
         .otherwise(F.col(col_name))
    )

# 1.4 Xử lý null - điền giá trị (tuỳ chiến lược)
# Với cột số: dùng mean/median
from pyspark.ml.feature import Imputer
imputer = Imputer(
    inputCols=["price", "quantity"],
    outputCols=["price_imputed", "quantity_imputed"]
).setStrategy("median")

# Với cột text: dùng mode hoặc "Unknown"
df = df.na.fill({"gender": "Unknown", "status": "pending"})

# 1.5 Loại bỏ bản ghi không đầy đủ (thiếu quá nhiều trường)
threshold = len(df.columns) * 0.5  # Giữ lại nếu có ít nhất 50% giá trị
df_clean = df.dropna(thresh=int(threshold))
```

---

### 2️⃣ Dữ liệu sai định dạng (Malformed & Incorrectly Formatted)

#### Mô tả lỗi đã inject:
- **Date formats**: Trộn lẫn `YYYY-MM-DD`, `DD/MM/YYYY`, `MM-DD-YYYY`, `DD.MM.YYYY`, `"Jan 5, 2024"`, unix timestamp, `"not a date"`, `"00/00/0000"`, `"2024-13-45"`
- **Number formats**: `"$100.50"`, `"1,234.56"`, `"100.00USD"`, precision issues
- **Malformed CSV rows**: Dòng thừa/thiếu cột

#### Code PySpark xử lý:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

# 2.1 Xử lý malformed CSV records
df = spark.read.csv("dirty_csv/orders.csv",
    header=True,
    mode="PERMISSIVE",           # Không bỏ qua dòng lỗi
    columnNameOfCorruptRecord="_corrupt_record"  # Lưu dòng lỗi vào cột riêng
)

# Tách dòng lỗi ra để kiểm tra
corrupt_df = df.filter(F.col("_corrupt_record").isNotNull())
clean_df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

# 2.2 Chuẩn hoá định dạng ngày tháng
def parse_multi_format_date(col_name):
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "MM-dd-yyyy"),
        F.to_date(F.col(col_name), "dd.MM.yyyy"),
        F.to_date(F.col(col_name), "MMM d, yyyy"),
        # Unix timestamp
        F.to_date(F.from_unixtime(F.col(col_name).cast("long"))),
    )

df = df.withColumn("order_date_clean", parse_multi_format_date("order_date"))

# Kiểm tra các giá trị không parse được
df.filter(F.col("order_date_clean").isNull() & F.col("order_date").isNotNull()).show()

# 2.3 Chuẩn hoá số
def clean_numeric(col_name):
    return (
        F.regexp_replace(F.col(col_name), r"[\$,USD]", "")  # Bỏ ký hiệu tiền tệ
         .cast("double")
    )

df = df.withColumn("total_amount_clean", clean_numeric("total_amount"))

# 2.4 Xử lý precision issues
df = df.withColumn("amount_rounded", F.round(F.col("total_amount_clean"), 2))
```

---

### 3️⃣ Giá trị không hợp lệ (Invalid Values)

#### Mô tả lỗi đã inject:
- **Giá trị âm**: Giá sản phẩm, số lượng, tổng tiền < 0
- **Ngoài phạm vi**: Rating -1, 0, 6, 10, 99 (hợp lệ: 1-5)
- **Typos**: `"pendng"`, `"shiped"`, `"deliverd"`, `"procesing"`, `"cancled"`
- **Outliers**: Giá trị nhân 1000x so với bình thường

#### Code PySpark xử lý:

```python
# 3.1 Phát hiện và xử lý giá trị ngoài phạm vi
df = df.withColumn("rating_valid",
    F.when((F.col("rating") >= 1) & (F.col("rating") <= 5), F.col("rating"))
     .otherwise(None)
)

# 3.2 Phát hiện giá trị âm
df = df.withColumn("price_valid",
    F.when(F.col("price") > 0, F.col("price"))
     .otherwise(F.abs(F.col("price")))  # Hoặc set null tuỳ business rule
)

# 3.3 Sửa lỗi chính tả categorical columns bằng fuzzy matching
from pyspark.sql.functions import levenshtein, lower, trim

valid_statuses = ["pending", "processing", "confirmed", "shipped", "delivered", "cancelled", "refunded"]

# Tạo mapping từ giá trị sai -> giá trị đúng
from pyspark.sql import Row
mapping_df = spark.createDataFrame([
    Row(dirty="pendng", clean="pending"),
    Row(dirty="shiped", clean="shipped"),
    Row(dirty="deliverd", clean="delivered"),
    Row(dirty="procesing", clean="processing"),
    Row(dirty="cancled", clean="cancelled"),
    Row(dirty="completd", clean="completed"),
    Row(dirty="confirmd", clean="confirmed"),
    Row(dirty="refundd", clean="refunded"),
    Row(dirty="faild", clean="failed"),
])

# Chuẩn hoá: lowercase + trim trước, rồi map
df = df.withColumn("status_norm", F.lower(F.trim(F.col("status"))))
df = df.join(mapping_df, df.status_norm == mapping_df.dirty, "left")
df = df.withColumn("status_clean",
    F.coalesce(F.col("clean"), F.col("status_norm"))
).drop("dirty", "clean", "status_norm")

# 3.4 Phát hiện outliers bằng IQR
from pyspark.sql.window import Window

quantiles = df.approxQuantile("total_amount", [0.25, 0.75], 0.05)
Q1, Q3 = quantiles[0], quantiles[1]
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

df = df.withColumn("is_outlier",
    F.when((F.col("total_amount") < lower_bound) | (F.col("total_amount") > upper_bound), True)
     .otherwise(False)
)
```

---

### 4️⃣ Dữ liệu trùng lặp (Duplicate Data)

#### Mô tả lỗi đã inject:
- **Exact duplicates**: ~5% bản ghi trùng lặp hoàn toàn
- **Near duplicates**: ~2% bản ghi gần trùng (khác nhau rất nhỏ: thêm space, +0.01)

#### Code PySpark xử lý:

```python
# 4.1 Phát hiện exact duplicates
print(f"Total rows: {df.count()}")
print(f"Distinct rows: {df.distinct().count()}")
print(f"Duplicates: {df.count() - df.distinct().count()}")

# 4.2 Loại bỏ exact duplicates
df_dedup = df.dropDuplicates()

# 4.3 Phát hiện near-duplicates bằng subset columns
# Dùng các cột business key (không phải auto-increment ID)
df_near_dedup = df.dropDuplicates(["customer_id", "order_date", "total_amount"])

# 4.4 Near-duplicate detection nâng cao với Window functions
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "order_date").orderBy("id")
df = df.withColumn("row_num", F.row_number().over(w))
df = df.withColumn("is_potential_dup", F.when(F.col("row_num") > 1, True).otherwise(False))

# 4.5 Kiểm tra PK uniqueness
pk_check = df.groupBy("id").count().filter(F.col("count") > 1)
print(f"Duplicate PKs: {pk_check.count()}")
```

---

### 5️⃣ Schema Inconsistency (Mixed Types)

#### Mô tả lỗi đã inject:
- **Mixed types**: Cột số chứa `"abc"`, `"NaN"`, `"Inf"`, `"#REF!"`, `true`
- **Extra whitespace**: `"  John  \t"`, trailing newlines
- **Malformed CSV**: Dòng thừa/thiếu cột

#### Code PySpark xử lý:

```python
# 5.1 Đọc với schema tường minh (không dùng inferSchema)
from pyspark.sql.types import *

orders_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("order_date", DateType(), True),
    # ... các cột khác
])

df = spark.read.csv("dirty_csv/orders.csv",
    header=True,
    schema=orders_schema,
    mode="PERMISSIVE",
    columnNameOfCorruptRecord="_corrupt"
)

# 5.2 Phát hiện giá trị không cast được
# Khi dùng schema tường minh, giá trị không hợp lệ sẽ thành null
df_raw = spark.read.csv("dirty_csv/orders.csv", header=True)  # Tất cả string
df_typed = spark.read.csv("dirty_csv/orders.csv", header=True, schema=orders_schema)

# So sánh null count giữa raw và typed để tìm conversion failures
for col_name in ["total_amount", "order_date"]:
    raw_nulls = df_raw.filter(F.col(col_name).isNull()).count()
    typed_nulls = df_typed.filter(F.col(col_name).isNull()).count()
    print(f"{col_name}: {typed_nulls - raw_nulls} conversion failures")

# 5.3 Trim whitespace trên tất cả string columns
string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
for c in string_cols:
    df = df.withColumn(c, F.trim(F.regexp_replace(F.col(c), r"[\t\n\r]", "")))

# 5.4 Xử lý mixed-type columns
df = df.withColumn("price_clean",
    F.when(F.col("price").rlike(r"^-?\d+\.?\d*$"), F.col("price").cast("double"))
     .otherwise(None)
)
```

---

### 6️⃣ Lỗi ngữ nghĩa và ngữ cảnh (Semantic & Contextual Errors)

#### Mô tả lỗi đã inject:
- **Orphaned records**: FK tham chiếu đến ID 999990+ (không tồn tại)
- **Inconsistent casing**: `"DELIVERED"` vs `"delivered"` vs `"Delivered"`
- **Inconsistent naming**: `"credit_card"` vs `"Credit Card"` vs `"CREDIT_CARD"`

#### Code PySpark xử lý:

```python
# 6.1 Phát hiện orphaned records
orders_df = spark.read.json("dirty_json/orders.json")
customers_df = spark.read.json("dirty_json/customers.json")

# Tìm orders có customer_id không tồn tại
orphaned = orders_df.join(
    customers_df.select("id").withColumnRenamed("id", "cid"),
    orders_df.customer_id == F.col("cid"),
    "left_anti"  # Giữ lại những dòng KHÔNG match
)
print(f"Orphaned orders: {orphaned.count()}")

# Xử lý: loại bỏ hoặc gán customer_id mặc định
orders_clean = orders_df.join(
    customers_df.select("id").withColumnRenamed("id", "cid"),
    orders_df.customer_id == F.col("cid"),
    "left_semi"  # Chỉ giữ những dòng CÓ match
)

# 6.2 Chuẩn hoá casing
df = df.withColumn("status", F.lower(F.trim(F.col("status"))))
df = df.withColumn("carrier", F.lower(F.trim(F.col("carrier"))))
df = df.withColumn("payment_method",
    F.lower(F.trim(F.regexp_replace(F.col("payment_method"), r"[\s_]+", "_")))
)

# 6.3 Referential integrity check toàn diện
tables_fk = {
    "orders": [("customer_id", "customers"), ("shipping_address_id", "addresses")],
    "order_items": [("order_id", "orders"), ("product_id", "products")],
    "payments": [("order_id", "orders")],
    "reviews": [("product_id", "products"), ("customer_id", "customers")],
    "shipping": [("order_id", "orders")],
    "feedback": [("customer_id", "customers"), ("order_id", "orders")],
}

for table, fks in tables_fk.items():
    df_child = spark.read.json(f"dirty_json/{table}.json")
    for fk_col, parent_table in fks:
        df_parent = spark.read.json(f"dirty_json/{parent_table}.json")
        orphan_count = df_child.join(
            df_parent.select(F.col("id").alias("_pid")),
            df_child[fk_col] == F.col("_pid"),
            "left_anti"
        ).count()
        print(f"{table}.{fk_col} -> {parent_table}: {orphan_count} orphans")
```

---

## 🔄 PIPELINE LÀM SẠCH TỔNG HỢP

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("DataCleaningPipeline") \
    .getOrCreate()

def clean_table(df, table_name, schema_config):
    """Pipeline làm sạch tổng hợp cho một bảng"""

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
            F.when(F.col(c).isin(placeholders), None)
             .when(F.trim(F.col(c)) == "", None)
             .otherwise(F.col(c)))

    # Step 3: Trim whitespace
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(c, F.trim(F.regexp_replace(F.col(c), r"[\t\n\r]", "")))

    # Step 4: Normalize casing for categorical columns
    for c in schema_config.get("categorical_cols", []):
        df = df.withColumn(c, F.lower(F.trim(
            F.regexp_replace(F.col(c), r"[\s]+", "_"))))

    # Step 5: Fix date columns
    for c in schema_config.get("date_cols", []):
        df = df.withColumn(c, F.coalesce(
            F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col(c), "yyyy-MM-dd"),
            F.to_timestamp(F.col(c), "dd/MM/yyyy"),
            F.to_timestamp(F.col(c), "MM-dd-yyyy"),
            F.to_timestamp(F.col(c), "dd.MM.yyyy"),
            F.to_timestamp(F.col(c), "MMM d, yyyy"),
        ))

    # Step 6: Clean numeric columns
    for c in schema_config.get("numeric_cols", []):
        df = df.withColumn(c,
            F.regexp_replace(F.col(c).cast("string"), r"[\$,USD]", "").cast("double"))
        # Remove negative values where not allowed
        if c in schema_config.get("positive_only_cols", []):
            df = df.withColumn(c, F.when(F.col(c) > 0, F.col(c)).otherwise(None))

    # Step 7: Remove duplicates
    dedup_cols = schema_config.get("dedup_cols", df.columns)
    df = df.dropDuplicates(dedup_cols)

    # Step 8: Remove outliers (IQR method)
    for c in schema_config.get("outlier_cols", []):
        q = df.approxQuantile(c, [0.25, 0.75], 0.05)
        if len(q) == 2:
            iqr = q[1] - q[0]
            df = df.filter(
                (F.col(c).isNull()) |
                ((F.col(c) >= q[0] - 1.5*iqr) & (F.col(c) <= q[1] + 1.5*iqr)))

    print(f"Clean rows: {df.count()}")
    return df

# Ví dụ sử dụng:
orders_config = {
    "categorical_cols": ["status"],
    "date_cols": ["order_date", "created_at", "updated_at"],
    "numeric_cols": ["subtotal", "discount_amount", "tax_amount", "shipping_cost", "total_amount"],
    "positive_only_cols": ["subtotal", "tax_amount", "shipping_cost", "total_amount"],
    "dedup_cols": ["customer_id", "order_date", "total_amount"],
    "outlier_cols": ["total_amount"],
}

df_orders = spark.read.csv("dirty_csv/orders.csv", header=True, inferSchema=True)
df_orders_clean = clean_table(df_orders, "orders", orders_config)
```

---

## 📁 Cấu trúc file

```
dirty_csv/          # File CSV (có malformed rows)
├── addresses.csv
├── categories.csv
├── coupons.csv
├── customers.csv
├── feedback.csv
├── order_items.csv
├── orders.csv
├── payments.csv
├── products.csv
├── reviews.csv
└── shipping.csv

dirty_json/         # File JSON (mixed types rõ ràng hơn)
├── addresses.json
├── categories.json
├── coupons.json
├── customers.json
├── feedback.json
├── order_items.json
├── orders.json
├── payments.json
├── products.json
├── reviews.json
└── shipping.json
```

## 💡 Lưu ý quan trọng

1. **Dùng JSON khi**: Cần test mixed-type handling, schema enforcement
2. **Dùng CSV khi**: Cần test malformed record parsing, delimiter issues
3. **Luôn đọc dữ liệu với `mode="PERMISSIVE"`** để bắt được malformed records
4. **Nên define schema tường minh** thay vì dùng `inferSchema=True` trong production
5. **Kiểm tra referential integrity** sau khi clean từng bảng con
6. **Thứ tự clean**: Reference tables → Parent tables → Child tables
