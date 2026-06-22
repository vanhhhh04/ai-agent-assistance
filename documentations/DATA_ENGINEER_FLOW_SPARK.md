# Luồng biến đổi Spark: Bronze → Silver → Gold — giải thích từng bước

> Tài liệu này mổ xẻ **chặng biến đổi dữ liệu trong HDFS** do **Apache Spark** thực thi:
> từ tầng **Bronze** (JSON thô) → **Silver** (sạch, có kiểu, đã khử trùng) → **Gold**
> (star schema, đăng ký vào Hive để AI Agent truy vấn). Mọi đường dẫn, schema, tham số
> đều đối chiếu trực tiếp với mã nguồn thật: `spark/jobs/silver_transform.py`,
> `spark/jobs/gold_transform.py`, `airflow/dags/medallion_pipeline.py`.
>
> Đây là **mắt xích còn thiếu** giữa hai chuỗi tài liệu đã có:
> - Tài liệu trước (`DATA_ENGINEER_FLOW_HDFS.md`) dừng ở **Bronze** ("Kafka → HDFS").
> - Tài liệu sau (`AI_AGENT_FLOW.md`) bắt đầu từ **AI Agent truy vấn Gold qua Hive**.
> - Tài liệu này nối hai đầu: **Bronze → Silver → Gold + đăng ký Hive**.

---

## Mục lục

1. [Spark đóng vai gì — đặt đúng vị trí trong chuỗi](#1-spark-đóng-vai-gì--đặt-đúng-vị-trí-trong-chuỗi)
2. [Sơ đồ tổng thể Bronze → Silver → Gold](#2-sơ-đồ-tổng-thể-bronze--silver--gold)
3. [Ai kích hoạt? — Airflow điều phối 3 job theo thứ tự](#3-ai-kích-hoạt--airflow-điều-phối-3-job-theo-thứ-tự)
4. [PHẦN A — Silver: triết lý "overwrite, tính lại từ đầu"](#4-phần-a--silver-triết-lý-overwrite-tính-lại-từ-đầu)
5. [Silver Bước 1 — Đọc Bronze an toàn (safe_read_parquet)](#5-silver-bước-1--đọc-bronze-an-toàn-safe_read_parquet)
6. [Silver Bước 2 — Ba định dạng nguồn khác nhau](#6-silver-bước-2--ba-định-dạng-nguồn-khác-nhau)
7. [Silver Bước 3 — Parse JSON theo schema rõ ràng](#7-silver-bước-3--parse-json-theo-schema-rõ-ràng)
8. [Silver Bước 4 — Bộ 4 phép làm sạch dùng chung](#8-silver-bước-4--bộ-4-phép-làm-sạch-dùng-chung)
9. [Silver Bước 5 — Khử trùng CDC (latest_per_id)](#9-silver-bước-5--khử-trùng-cdc-latest_per_id)
10. [Silver Bước 6 — Tách clean/dirty và gom DLQ](#10-silver-bước-6--tách-cleandirty-và-gom-dlq)
11. [Silver Bước 7 — Ghi 12 bảng + DLQ](#11-silver-bước-7--ghi-12-bảng--dlq)
12. [PHẦN B — Gold: từ Silver sang star schema](#12-phần-b--gold-từ-silver-sang-star-schema)
13. [Gold Bước 1 — Kết nối Hive Metastore](#13-gold-bước-1--kết-nối-hive-metastore)
14. [Gold Bước 2 — Dựng dimension (denormalize)](#14-gold-bước-2--dựng-dimension-denormalize)
15. [Gold Bước 3 — Dựng fact_sales (grain = order_item)](#15-gold-bước-3--dựng-fact_sales-grain--order_item)
16. [Gold Bước 4 — Đăng ký bảng EXTERNAL vào Hive (saveAsTable)](#16-gold-bước-4--đăng-ký-bảng-external-vào-hive-saveastable)
17. [Cây thư mục HDFS + catalog Hive sau khi chạy](#17-cây-thư-mục-hdfs--catalog-hive-sau-khi-chạy)
18. [Tại sao thiết kế như vậy — biện luận cho đồ án](#18-tại-sao-thiết-kế-như-vậy--biện-luận-cho-đồ-án)
19. [Vận hành & sự cố thường gặp](#19-vận-hành--sự-cố-thường-gặp)
20. [Tóm tắt một câu](#20-tóm-tắt-một-câu)

---

## 1. Spark đóng vai gì — đặt đúng vị trí trong chuỗi

**Apache Spark** là **engine tính toán** (compute engine) duy nhất chịu trách nhiệm biến
đổi dữ liệu giữa các tầng Medallion. Nó chạy ở chế độ **Standalone**
(`spark://spark-master:7077`), **không qua YARN**.

> Phân biệt với tài liệu Hadoop: MapReduce-local **chỉ** phục vụ Hive lúc AI Agent
> truy vấn. Còn **toàn bộ ETL Bronze→Silver→Gold là Spark** đảm nhiệm.

Có **3 job Spark**, mỗi job là một file `.py` độc lập:

| Job | File | Đầu vào → đầu ra | Bản chất |
|---|---|---|---|
| Bronze | `bronze_ingestion.py` | Kafka → `/datalake/bronze` | Streaming (`Trigger.AvailableNow`) — *đã tả ở tài liệu HDFS* |
| **Silver** | **`silver_transform.py`** | `/datalake/bronze` → `/datalake/silver` | **Batch, `overwrite`** |
| **Gold** | **`gold_transform.py`** | `/datalake/silver` → `/datalake/gold` + Hive | **Batch, `overwrite` + đăng ký catalog** |

Tài liệu này tập trung vào **hai job sau** (Silver và Gold).

---

## 2. Sơ đồ tổng thể Bronze → Silver → Gold

```
   HDFS /datalake/bronze                HDFS /datalake/silver              HDFS /datalake/gold
   (JSON thô + lineage)                 (sạch, có kiểu, khử trùng)         (star schema, denormalized)
   ┌──────────────────────┐             ┌────────────────────────┐        ┌──────────────────────────┐
   │ erp_raw   (9 topic)   │             │ orders, customers,      │        │ FACT:                     │
   │ wh_raw                │  silver_    │ order_items, products,  │ gold_  │  fact_sales               │
   │ wh_dlq                │  transform  │ addresses, categories,  │ trans  │  fact_reviews             │
   │ pay_raw               │ ─────────▶ │ coupons, reviews,       │ ─────▶│  fact_feedback            │
   │ pay_dlq               │  (Spark)    │ feedback, payments,     │ (Spark)│ DIM:                      │
   │                       │             │ shipping (+warehouse_   │        │  dim_customers/products/  │
   │ raw_data: "{...}"     │             │ events, dlq)            │        │  categories/addresses/    │
   └──────────────────────┘             └────────────────────────┘        │  coupons/payments/shipping│
            │                                      │                       └────────────┬──────────────┘
            │                                      │                                    │ saveAsTable
            ▼                                      ▼                                    ▼
   parse JSON, cast kiểu,                 join nhiều bảng Silver,            ĐĂNG KÝ vào Hive Metastore
   chuẩn hóa, khử trùng,                  denormalize thành 1 bảng phẳng,   (thrift://hive-metastore:9083)
   tách dữ liệu bẩn → DLQ                 thêm key + measure                → AI Agent query qua HiveServer2
```

**Tư tưởng cốt lõi:**
- **Silver** = *làm sạch* (mỗi bảng tương ứng 1 thực thể nghiệp vụ, đã chuẩn hóa).
- **Gold** = *làm nhanh để hỏi* (gộp sẵn — denormalize — để AI Agent join ít, query đơn giản).

---

## 3. Ai kích hoạt? — Airflow điều phối 3 job theo thứ tự

Cả 3 job **không tự chạy**. **Airflow** (`airflow/dags/medallion_pipeline.py`) submit
chúng qua `SparkSubmitOperator`, mỗi 15 phút, đúng thứ tự phụ thuộc:

```python
# airflow/dags/medallion_pipeline.py
SPARK_MASTER = "spark://spark-master:7077"

silver = SparkSubmitOperator(
    task_id="silver_transform",
    application=f"{SPARK_JOBS}/silver_transform.py",
    conn_id="spark_default",
    conf=SPARK_CONF,            # fs.defaultFS, hive.metastore.uris, dfs.replication=1...
)
gold = SparkSubmitOperator(
    task_id="gold_transform",
    application=f"{SPARK_JOBS}/gold_transform.py",
    conn_id="spark_default",
    conf=SPARK_CONF,
)

bronze >> silver >> gold   # ← thứ tự bắt buộc: Silver chỉ chạy khi Bronze done
```

- **`bronze >> silver >> gold`**: Airflow đảm bảo Silver chỉ bắt đầu khi Bronze thành
  công, Gold chỉ bắt đầu khi Silver thành công. Đây là **lý do dữ liệu luôn nhất quán
  theo tầng**.
- **`SPARK_CONF`** truyền sẵn `spark.sql.catalogImplementation=hive` và
  `hive.metastore.uris=thrift://hive-metastore:9083` — nhờ đó Gold mới `saveAsTable` vào
  Hive được.
- Khác Bronze (cần `packages=spark-sql-kafka...`), Silver/Gold **không cần** connector
  Kafka vì chúng chỉ đọc/ghi Parquet trên HDFS.

---

## 4. PHẦN A — Silver: triết lý "overwrite, tính lại từ đầu"

Mỗi lần chạy, Silver **đọc toàn bộ Bronze** và **ghi đè (`overwrite`) toàn bộ Silver**.
Đây là quyết định thiết kế ghi rõ ngay trong docstring của job:

```python
# silver_transform.py — docstring
# Why overwrite Silver?
#   Bronze is append-only — Silver recomputed from the full history each run is
#   the simplest correct strategy. For production with millions of rows you would
#   switch to Delta/Iceberg MERGE; for this project the data volume is bounded.
```

**Diễn giải cho đồ án:**
- Bronze chỉ thêm (append-only). Cách **đúng và đơn giản nhất** là mỗi lần tính lại Silver
  từ đầu lịch sử Bronze → không cần logic MERGE phức tạp.
- **Trade-off (phần "hạn chế" nên viết vào đồ án):** với hàng triệu dòng, `overwrite`
  toàn bộ sẽ tốn kém → production sẽ chuyển sang **Delta Lake / Apache Iceberg** với phép
  `MERGE` incremental. Dự án có khối lượng giới hạn nên `overwrite` là đủ.

---

## 5. Silver Bước 1 — Đọc Bronze an toàn (safe_read_parquet)

```python
def safe_read_parquet(path: str) -> DataFrame:
    """Read parquet, return empty DataFrame if path doesn't exist."""
    try:
        return spark.read.parquet(path)
    except Exception as e:
        if "Path does not exist" in str(e) or "does not exist" in str(e).lower():
            print(f"[SILVER]   (skip — {path} not found yet, returning empty)")
            return spark.createDataFrame([], "raw_data string, _source_topic string, _bronze_ingested_at timestamp")
        raise
```

**Vì sao quan trọng:** Bronze dùng `Trigger.AvailableNow` — nếu chưa có message mới thì
**không sinh file nào**. Ở các lần chạy đầu, thư mục `bronze/wh_dlq` hay `bronze/pay_raw`
có thể **chưa tồn tại**. `safe_read_parquet` trả về DataFrame rỗng thay vì làm crash cả
job → pipeline **bền vững** ngay từ lần chạy đầu.

---

## 6. Silver Bước 2 — Ba định dạng nguồn khác nhau

Đây là phần khó nhất của Silver: **3 nguồn có 3 định dạng payload khác nhau**, mỗi nguồn
xử lý riêng.

| Nguồn | Topic Bronze | Định dạng `raw_data` | Cách bóc tách |
|---|---|---|---|
| **ERP** (Debezium + SMT unwrap) | `erp_raw` (9 topic `erp.public.*`) | JSON **phẳng**, cột bảng nằm top-level, kèm `__op/__table/__source_ts_ms` | `from_json` + schema |
| **Warehouse** (NiFi CSV→JSON) | `wh_raw` | JSON **phẳng**, envelope + payload chung 1 cấp | `from_json` + schema |
| **Payment** (NiFi HTTP passthrough) | `pay_raw` | JSON **lồng nhau** — `{"_event_type":..., "payload":{...}}` | `get_json_object` tách `payload` rồi `from_json` lần 2 |

Ví dụ sự khác biệt (trích docstring):

```
ERP (Debezium):  {"id":1, "customer_id":5, "total_amount":"120.00", "__op":"c", "__table":"orders"}
Payment (nested):{"_source_system":"...", "_event_type":"payment.created", "payload": {"id":1,"order_id":9,...}}
```

→ Payment phải bóc **hai lớp**: lấy chuỗi `payload` bằng `get_json_object`, sau đó
`from_json` lại theo schema từng loại event (`PAYMENT_SCHEMA` / `SHIPPING_SCHEMA`).

> **Lưu ý canonical:** `products` được lấy từ **ERP/Debezium** (có `id` integer — nguồn
> chuẩn), bảng `products` từ warehouse chỉ là **bảng phụ** `warehouse_events` (stock
> update). Ghi chú này nằm rõ trong code (dòng 446-448).

---

## 7. Silver Bước 3 — Parse JSON theo schema rõ ràng

Với nguồn ERP, mỗi bảng có một `StructType` **khai báo tường minh** kiểu từng cột:

```python
ORDER_SCHEMA = StructType([
    StructField("id",            IntegerType()),
    StructField("customer_id",   IntegerType()),
    StructField("total_amount",  StringType()),    # Debezium decimal → string
    StructField("order_date",    LongType()),       # Debezium TIMESTAMP → int64 ms
    StructField("__op",          StringType()),
    StructField("__source_ts_ms",LongType()),
    ...
])

def parse_erp_topic(topic_suffix: str, schema: StructType) -> DataFrame:
    topic = f"erp.public.{topic_suffix}"
    return (
        df_erp_raw
        .filter(F.col("_source_topic") == topic)           # lọc đúng bảng
        .select(F.from_json(F.col("raw_data"), schema).alias("d"), F.col("_bronze_ingested_at"))
        .select("d.*", "_bronze_ingested_at")
        .filter(F.col("__op") != "d")                       # bỏ bản ghi DELETE ở Silver
    )
```

Hai chi tiết kỹ thuật phải biết (do cấu hình Debezium ở tầng trước):
- **`decimal.handling.mode=string`** → tiền tệ về dưới dạng **chuỗi** → Silver phải
  `cast(DecimalType(12,2))`.
- **`time.precision.mode=connect`** → `TIMESTAMP` về dưới dạng **int64 milliseconds** →
  Silver dùng helper `ms_to_ts()` để đổi về `timestamp`:

```python
def ms_to_ts(col):
    return (F.col(col).cast("long") / 1000).cast("timestamp")
```

- **`.filter(__op != "d")`**: bản ghi xóa (delete) trong CDC bị loại khỏi Silver — Silver
  chỉ giữ trạng thái hiện hữu.

---

## 8. Silver Bước 4 — Bộ 4 phép làm sạch dùng chung

Silver có **4 helper** áp dụng nhất quán cho mọi bảng — đây là "bộ công cụ data quality"
của dự án:

### 8.1 `null_placeholders` — chuẩn hóa giá trị rác thành NULL

```python
PLACEHOLDERS = ["UNKNOWN","N/A","#N/A","None","null","EMPTY","--","???","TBD","not available",""]

def null_placeholders(df, cols):
    for c in cols:
        df = df.withColumn(c,
            F.when(F.trim(F.col(c).cast("string")).isin(PLACEHOLDERS), None).otherwise(F.col(c)))
    return df
```
→ Biến mọi "giá trị giả null" (do dữ liệu bẩn sim ra) thành `NULL` thật, để bước
`drop_dirty` bắt được.

### 8.2 `normalize_status` — chuẩn hóa enum

```python
def normalize_status(col_expr, valid):
    cleaned = F.upper(F.trim(col_expr))
    return F.when(cleaned.isin(valid), cleaned).otherwise(None)   # ngoài tập hợp lệ → NULL
```
→ Ví dụ `order.status` chỉ chấp nhận `PENDING/PROCESSING/SHIPPED/DELIVERED/CANCELLED/RETURNED`;
giá trị lạ bị NULL hóa.

### 8.3 Cast kiểu + chuẩn hóa miền (per-table)

Ví dụ với `orders`:
```python
.withColumn("total_amount", F.col("total_amount").cast(DecimalType(12,2)))
.withColumn("order_date",   ms_to_ts("order_date"))
.withColumn("status", normalize_status(F.col("status"), [...6 trạng thái...]))
.filter(F.col("total_amount") > 0)         # quy tắc nghiệp vụ: tổng tiền phải > 0
```
Với `customers` còn có validate email bằng regex:
```python
.withColumn("email", F.lower(F.trim(F.col("email"))))
.filter(F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))
```

### 8.4 `drop_dirty` — tách hợp lệ / không hợp lệ (xem Bước 6)

---

## 9. Silver Bước 5 — Khử trùng CDC (latest_per_id)

Vì CDC ghi **nhiều sự kiện** cho cùng một bản ghi (tạo, rồi nhiều lần update), Silver phải
**giữ phiên bản mới nhất**:

```python
def latest_per_id(df, id_col="id", ts_col="__source_ts_ms"):
    w = Window.partitionBy(id_col).orderBy(F.col(ts_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
```

**Cơ chế:** phân nhóm theo `id`, sắp xếp theo `__source_ts_ms` giảm dần, lấy dòng đầu
(`_rn == 1`) → mỗi `id` chỉ còn **1 dòng = trạng thái mới nhất**.

> Đây chính là nửa sau của lập luận **"near exactly-once"** đã nói ở tài liệu Kafka:
> Kafka đảm bảo *at-least-once* (không mất), còn `latest_per_id` / `dropDuplicates` ở
> Silver đảm bảo *không trùng* → kết quả gần như *exactly-once*.

Các bảng dùng `latest_per_id`: orders, products(ERP), addresses, categories, coupons,
reviews, feedback. Các bảng có khóa tự nhiên dùng `dropDuplicates`: customers (`id`),
order_items (`id`), payments (`transaction_id`), shipping (`tracking_number`).

---

## 10. Silver Bước 6 — Tách clean/dirty và gom DLQ

```python
def drop_dirty(df, required: list):
    """Split into (clean, dirty) based on required-NULL + _quality_flag."""
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
```

Một dòng bị coi là **dirty** nếu **một trong hai**:
1. Thiếu cột bắt buộc (`required` bị NULL) — ví dụ `orders` cần `[id, customer_id, total_amount]`.
2. `_quality_flag` = `DIRTY`/`QUARANTINE` (cờ do NiFi gắn cho dữ liệu kho/thanh toán bẩn).

Mọi dòng dirty từ **tất cả các nguồn** được gom vào một **DLQ Silver** thống nhất:

```python
def _to_dlq(df, source):
    # ERP CDC không có _quality_flag (Debezium không có envelope) → tổng hợp = "DIRTY"
    ...
    return out.select(F.col("id").cast("string"), "_quality_flag", "_bronze_ingested_at",
                      F.lit(source).alias("_source"))

all_dirty = (_to_dlq(df_orders_dirty, "erp.orders")
    .unionByName(_to_dlq(df_cust_dirty, "erp.customers"))
    ... 12 nguồn ...)
all_dirty.write.mode("overwrite").parquet(f"{HDFS_SILVER}/dlq")
```

→ DLQ là nơi audit dữ liệu bị loại, có cột `_source` để biết dòng bẩn đến từ thực thể nào.

---

## 11. Silver Bước 7 — Ghi 12 bảng + DLQ

```python
def write_silver(df, name):
    df.write.mode("overwrite").parquet(f"{HDFS_SILVER}/{name}")

write_silver(df_orders_clean,    "orders")
write_silver(df_cust_clean,      "customers")
write_silver(df_items_clean,     "order_items")
write_silver(df_prod_erp_clean,  "products")           # canonical từ ERP/CDC (có id)
write_silver(df_prod_clean,      "warehouse_events")   # phụ: stock update
write_silver(df_addr_clean,      "addresses")
write_silver(df_cat_clean,       "categories")
write_silver(df_coupon_clean,    "coupons")
write_silver(df_rev_clean,       "reviews")
write_silver(df_fb_clean,        "feedback")
write_silver(df_pmt_clean,       "payments")
write_silver(df_ship_clean,      "shipping")
# + dlq
```

**Đặc điểm Silver:** ghi Parquet thuần (`mode("overwrite")`), **KHÔNG** phân vùng,
**KHÔNG** đăng ký Hive. Silver là tầng "kho sạch trung gian" — chỉ Gold mới vào catalog.

### 11.1. `write_silver()` thật — 3 quyết định trong 1 dòng ghi

Code thật (`spark/jobs/silver_transform.py:114-117`):

```python
def write_silver(df: DataFrame, name: str):
    path = f"{HDFS_SILVER}/{name}"          # hdfs://namenode:9000/datalake/silver/<name>
    df.write.mode("overwrite").parquet(path)
    print(f"[SILVER]   ✓ {name} ({df.count():,} rows) → {path}")
```

| Thành phần | Ý nghĩa | Vì sao chọn vậy |
|---|---|---|
| `.mode("overwrite")` | Mỗi lần chạy **xóa sạch** thư mục cũ rồi ghi lại toàn bộ | Bronze là *append-only*. Tính lại Silver từ **toàn bộ lịch sử** mỗi lần là cách đơn-giản-mà-đúng nhất — không cần merge tăng dần |
| `.parquet(path)` | Định dạng cột, nén Snappy mặc định | Đọc nhanh cho Gold; giữ kiểu Decimal/timestamp |
| (không `.partitionBy`) | Ghi phẳng | Silver là kho trung gian, dữ liệu bounded — phân vùng không đáng |

> ⚠️ Chiến lược "recompute toàn bộ" chỉ đúng vì dữ liệu dự án có giới hạn. Comment đầu file
> (`silver_transform.py:8-10`) nói rõ: production hàng triệu dòng phải chuyển sang
> **Delta/Iceberg `MERGE`**. Đây là đánh đổi có chủ đích.

### 11.2. Vì sao đúng 12 bảng — và mánh `products` vs `warehouse_events`

12 lệnh `write_silver` đến từ **3 nguồn**:

```
ERP (Debezium CDC):  orders · customers · order_items · products · addresses
                     · categories · coupons · reviews · feedback        → 9
Warehouse (NiFi CSV→JSON):  warehouse_events                            → 1
Payment (NiFi HTTP, envelope lồng):  payments · shipping               → 2
                                                                        ── = 12
```

Điểm dễ nhầm nhất (`silver_transform.py:608-609`):

```python
write_silver(df_prod_erp_clean,  "products")           # canonical từ ERP/CDC (có id)
write_silver(df_prod_clean,      "warehouse_events")   # phụ: stock update
```

Cả hai đều chứa sản phẩm, nhưng:
- `df_prod_erp_clean` (từ topic `erp.public.products`) là **nguồn chuẩn (canonical)** vì có khóa
  `id` integer từ DB gốc → ghi vào bảng `products`.
- `df_prod_clean` (từ `wh_raw`) **không có `id` integer**, chỉ có `sku`. Trước đây từng là nguồn
  products chính, **nay bị giáng** xuống bảng phụ `warehouse_events`.

→ Tên biến (`df_prod_clean`) **cố tình lệch** tên bảng đầu ra (`warehouse_events`). Đọc tên biến
sẽ tưởng là products — không phải. Xem comment `silver_transform.py:446-448`.

### 11.3. DLQ — phần `# + dlq` mà code rút gọn lược đi

Code thật ở `silver_transform.py:618-651`. **Vấn đề:** mỗi nguồn đánh dấu dữ liệu bẩn khác nhau:
- Warehouse/Payment qua NiFi → **có** cột `_quality_flag` (`DIRTY`/`QUARANTINE`).
- ERP qua Debezium → **không có** envelope, nên không có `_quality_flag`; bản ghi bẩn chỉ là
  "thiếu cột bắt buộc" do `drop_dirty` bắt được.

→ Không thể `union` thẳng 12 DataFrame dirty vì khác schema hoàn toàn. Lời giải là hàm
`_to_dlq()` chuẩn hóa mọi df dirty về **cùng 4 cột**:

```python
def _to_dlq(df, source):
    out = df
    if "_quality_flag" not in out.columns:          # ERP không có → tự gắn "DIRTY"
        out = out.withColumn("_quality_flag", F.lit("DIRTY"))
    if "_bronze_ingested_at" not in out.columns:
        out = out.withColumn("_bronze_ingested_at", F.current_timestamp())
    if "id" not in out.columns:
        out = out.withColumn("id", F.lit(None).cast("int"))
    return out.select(
        F.col("id").cast("string").alias("id"),
        F.col("_quality_flag"),
        F.col("_bronze_ingested_at"),
        F.lit(source).alias("_source"),            # nhãn "erp.orders", "payment.shipping"...
    )

all_dirty = (_to_dlq(df_orders_dirty, "erp.orders")
    .unionByName(_to_dlq(df_cust_dirty, "erp.customers"))
    ... 12 nguồn ...)
all_dirty.write.mode("overwrite").parquet(f"{HDFS_SILVER}/dlq")   # 1 thư mục DLQ duy nhất
```

Cột `_source` chính là thứ giúp truy ngược bản ghi bẩn đến từ đâu (`erp.orders` vs
`payment.payments`...). Đây là **Dead Letter Queue pattern** ở tầng batch: không vứt dữ liệu
bẩn đi, mà gom một chỗ để điều tra sau.

### 11.4. Vì sao Silver KHÔNG vào Hive (so với Gold)

- **Không phân vùng**: `write_silver` không gọi `.partitionBy()`. (Gold thì có —
  `fact_sales` dùng `partitionBy(order_year, order_month)`.)
- **Không đăng ký Hive**: cả file Silver **không** gọi `saveAsTable`/`CREATE TABLE`, chỉ
  `.parquet(path)` (ghi file trần xuống HDFS). Ngược lại `gold_transform.py` dùng `saveAsTable`
  tạo EXTERNAL table → mới xuất hiện trong Hive Metastore → mới được AI agent/HiveServer2 truy vấn.
- **Hệ quả**: người dùng cuối và AI agent **không bao giờ** chạm Silver — họ chỉ thấy Gold
  (`gold.fact_sales`, `gold.dim_*`). Tách như vậy giúp đổi logic Gold mà không phải đụng lại
  pipeline làm sạch.

---

## 12. PHẦN B — Gold: từ Silver sang star schema

Gold đọc các bảng Silver và dựng **mô hình ngôi sao (star schema)** — chuẩn cho phân tích:

```
            dim_customers      dim_products      dim_payments
                  \                 |                /
                   \                |               /
   dim_addresses ─── ┌─────────────────────────────┐ ─── dim_shipping
                     │        FACT_SALES            │
   dim_coupons  ──── │  (grain = 1 dòng / order_item)│ ──── dim_categories
                     └─────────────────────────────┘
        fact_reviews   (review)        fact_feedback  (feedback)
```

| Loại | Bảng | Grain (hạt) |
|---|---|---|
| **Fact** | `fact_sales` | 1 dòng / order_item (món hàng bán ra) |
| | `fact_reviews` | 1 dòng / review |
| | `fact_feedback` | 1 dòng / feedback |
| **Dimension** | `dim_customers`, `dim_products`, `dim_categories`, `dim_addresses`, `dim_coupons`, `dim_payments`, `dim_shipping` | 1 dòng / thực thể |

---

## 13. Gold Bước 1 — Kết nối Hive Metastore

Điểm khác biệt lớn nhất giữa Gold và Silver: Gold **bật Hive support**.

```python
spark = SparkSession.builder \
    .appName("Gold-Transform") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("spark.sql.parquet.writeLegacyFormat", "true") \
    .config("spark.sql.parquet.outputTimestampType", "INT96") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("USE gold")
```

- **`enableHiveSupport()` + `hive.metastore.uris`** → Spark trở thành **client của Hive
  Metastore**, có thể tạo database/table trong catalog.
- **`writeLegacyFormat=true` + `outputTimestampType=INT96`** → bắt buộc để **HiveServer2
  (chạy MapReduce) đọc lại được** file Parquet do Spark ghi (Hive 2.3 cũ kén định dạng
  timestamp). Đây là chi tiết tương thích Spark↔Hive đáng nhắc trong đồ án.

---

## 14. Gold Bước 2 — Dựng dimension (denormalize)

Dimension thường chỉ là `select` đổi tên cột thành khóa `*_key`. Nhưng vài dim được
**làm giàu (denormalize)** sẵn để AI Agent khỏi join:

**`dim_categories`** — self-join lấy tên cha:
```python
dim_categories = (
    df_categories.alias("c")
    .join(df_categories.alias("p"), F.col("c.parent_category_id") == F.col("p.id"), "left")
    .select(F.col("c.id").alias("category_key"),
            F.col("c.name").alias("category_name"),
            F.col("p.name").alias("parent_category_name"), ...)   # ← tên danh mục cha gắn sẵn
)
```

**`dim_products`** — gắn sẵn `category_name`:
```python
dim_products = (
    df_products.alias("p")
    .join(dim_categories.alias("c"), F.col("p.category_id") == F.col("c.category_key"), "left")
    .select(F.col("p.id").alias("product_key"), F.col("p.sku"),
            F.col("p.name").alias("product_name"),
            F.col("c.category_name"), ...)   # ← AI Agent hỏi "sản phẩm theo danh mục" không cần join
)
```

**`dim_shipping`** — tính sẵn `delivery_days` (số ngày giao):
```python
((F.unix_timestamp("delivered_at") - F.unix_timestamp("shipped_at")) / 86400).alias("delivery_days")
```

> **Triết lý Gold:** *tính sẵn để hỏi nhanh*. Mỗi phép denormalize/derived-column ở đây
> là một phép join hoặc tính toán mà AI Agent **không phải làm lúc query** → SQL do LLM
> sinh đơn giản hơn, ít lỗi hơn, chạy nhanh hơn.

---

## 15. Gold Bước 3 — Dựng fact_sales (grain = order_item)

`fact_sales` là bảng trung tâm — gộp 7 bảng thành **một bảng phẳng**, mỗi dòng là một món
hàng trong đơn:

```python
fact_sales = (
    df_order_items.alias("i")
    .join(df_orders.alias("o"),     F.col("i.order_id")    == F.col("o.id"), "inner")
    .join(dim_customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_key"), "left")
    .join(dim_products.alias("p"),  F.col("i.product_id")  == F.col("p.product_key"),  "left")
    .join(dim_payments.alias("pm"), F.col("o.id")          == F.col("pm.order_id"),    "left")
    .join(dim_shipping.alias("s"),  F.col("o.id")          == F.col("s.order_id"),     "left")
    .join(dim_coupons.alias("cp"),  F.col("o.coupon_id")   == F.col("cp.coupon_key"),  "left")
    .join(dim_addresses.alias("sa"),F.col("o.shipping_address_id") == F.col("sa.address_key"), "left")
    .select(
        # Keys
        F.col("i.id").alias("order_item_key"), F.col("o.id").alias("order_key"),
        F.col("c.customer_key"), F.col("p.product_key"), ...
        # Measures (số liệu để SUM/AVG)
        F.col("o.total_amount").alias("order_total"), F.col("i.quantity"),
        F.col("i.total_price").alias("item_total"), ...
        # Denormalized dims (gắn sẵn để khỏi join)
        F.col("c.customer_name"), F.col("p.product_name"), F.col("p.brand"),
        F.col("s.delivery_days"), F.col("sa.city").alias("shipping_city"), ...
        # Time breakdown để phân vùng + lọc nhanh
        F.year("o.order_date").alias("order_year"),
        F.month("o.order_date").alias("order_month"), ...
    )
)
fact_sales = fact_sales.filter(F.col("order_year").isNotNull() & F.col("order_month").isNotNull())
write_gold(fact_sales, "fact_sales", partition_cols=["order_year", "order_month"])
```

Ba điểm thiết kế:
- **`inner` join với `orders`, `left` join với dim**: phải có đơn hàng tương ứng (inner),
  nhưng thiếu dimension (vd coupon NULL) vẫn giữ dòng (left) → không mất doanh thu.
- **Lọc `order_year/month` NotNull trước khi phân vùng**: code chú thích rõ Hive 2.3 lỗi
  với `__HIVE_DEFAULT_PARTITION__` khi cột phân vùng NULL → loại các dòng order_date NULL
  (do order_item tới trước order — race condition Bronze→Silver).
- **`partitionBy(order_year, order_month)`**: phân vùng theo thời gian → câu hỏi "doanh
  thu tháng X" chỉ quét đúng phân vùng (partition pruning).

---

## 16. Gold Bước 4 — Đăng ký bảng EXTERNAL vào Hive (saveAsTable)

Đây là bước biến file Parquet thành **bảng SQL truy vấn được**:

```python
def write_gold(df, table_name, partition_cols=None):
    path = f"{HDFS_GOLD}/{table_name}"           # vd hdfs://.../datalake/gold/fact_sales
    spark.sql(f"DROP TABLE IF EXISTS gold.{table_name}")
    writer = (df.write.mode("overwrite").format("parquet")
              .option("path", path))             # ← path ngoài warehouse ⇒ bảng EXTERNAL
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"gold.{table_name}")     # ← ghi data + đăng ký catalog 1 lệnh
```

Vì sao `saveAsTable` + `.option("path", ...)`:
- **`saveAsTable`** làm **hai việc cùng lúc**: (1) ghi Parquet xuống HDFS, (2) **đăng ký
  metadata** (tên bảng, schema, vị trí, phân vùng) vào **Hive Metastore**. Với bảng phân
  vùng, đây là cách đúng để cả data lẫn partition đều vào catalog.
- **`.option("path", path)`** → bảng là **EXTERNAL**: dữ liệu nằm tại `/datalake/gold`
  (do ta quản lý), không nằm trong warehouse mặc định của Hive. `DROP TABLE` chỉ xóa
  metadata, **không xóa file** — an toàn cho data lake.

**Mấu chốt nối sang AI Agent:** Sau bước này, **Hive Metastore biết** có database `gold`
với các bảng `fact_sales`, `dim_customers`… và vị trí HDFS của chúng. Khi AI Agent gửi
SQL qua **HiveServer2** (`:10000`), HiveServer2 hỏi Metastore "bảng này ở đâu, schema gì",
rồi chạy **MapReduce-local** đọc Parquet trả kết quả. → Đây chính là điểm khớp với
`AI_AGENT_FLOW.md` (Stage 7 — Executor → `hive_client.py`).

Cuối job có bước **verify** chạy thử một truy vấn để chắc bảng dùng được:
```python
spark.sql("SHOW TABLES IN gold").show()
spark.sql("SELECT order_year, order_month, COUNT(*), SUM(item_total) FROM gold.fact_sales GROUP BY ...").show()
```

---

## 17. Cây thư mục HDFS + catalog Hive sau khi chạy

```
hdfs://namenode:9000/datalake/
├── bronze/   (đã tả ở DATA_ENGINEER_FLOW_HDFS.md)
│
├── silver/                                  ← job silver_transform.py tạo
│   ├── orders/        part-*.parquet         (overwrite, không phân vùng)
│   ├── customers/     part-*.parquet
│   ├── order_items/   ...
│   ├── products/        (canonical từ ERP)
│   ├── warehouse_events/(phụ — stock update)
│   ├── addresses/  categories/  coupons/  reviews/  feedback/  payments/  shipping/
│   └── dlq/             (gom dữ liệu bẩn mọi nguồn)
│
└── gold/                                    ← job gold_transform.py tạo
    ├── fact_sales/
    │   ├── order_year=2026/order_month=5/part-*.parquet   (phân vùng theo thời gian)
    │   └── ...
    ├── fact_reviews/    (phân vùng review_year/review_month)
    ├── fact_feedback/   (phân vùng feedback_year/feedback_month)
    ├── dim_customers/  dim_products/  dim_categories/  dim_addresses/
    └── dim_coupons/  dim_payments/  dim_shipping/

Hive Metastore (thrift://hive-metastore:9083), backed by postgres:13:
   database "gold"
     ├── fact_sales      → location hdfs://.../datalake/gold/fact_sales      (EXTERNAL)
     ├── dim_customers   → location hdfs://.../datalake/gold/dim_customers   (EXTERNAL)
     └── ... (10 bảng) — chỉ lưu METADATA, dữ liệu thật nằm trên HDFS
```

Quan sát: HDFS UI `http://localhost:9870` → Browse `/datalake/gold`. Catalog:
`docker exec hiveserver2 beeline -e "SHOW TABLES IN gold;"`.

---

## 18. Tại sao thiết kế như vậy — biện luận cho đồ án

| Quyết định | Lý do | Trade-off / hạn chế |
|---|---|---|
| Silver `overwrite` tính lại từ đầu | Đơn giản, luôn đúng với Bronze append-only | Tốn kém khi data lớn → production dùng Delta/Iceberg MERGE |
| Tách 3 nguồn xử lý riêng | Mỗi nguồn (ERP/WH/Payment) định dạng khác nhau | Code dài, phải bảo trì 3 nhánh parse |
| `latest_per_id` + `dropDuplicates` | Khử trùng CDC → đạt "near exactly-once" cùng at-least-once của Kafka | Phải có cột timestamp nguồn (`__source_ts_ms`) |
| Gold denormalize (star schema) | AI Agent join ít → LLM sinh SQL đơn giản, ít hallucinate, query nhanh | Trùng lặp dữ liệu; phải rebuild khi dim đổi |
| Bảng EXTERNAL (`.option(path)`) | `DROP TABLE` không xóa data; tách lưu trữ khỏi catalog | Phải tự quản lý vòng đời file HDFS |
| `INT96` + `writeLegacyFormat` | HiveServer2 (Hive 2.3) đọc được Parquet của Spark | Định dạng timestamp cũ, kém tối ưu hơn |
| Phân vùng fact theo year/month | Partition pruning cho câu hỏi theo thời gian | Loại dòng có ngày NULL khỏi fact |

---

## 19. Vận hành & sự cố thường gặp

### 19.1 Lệnh kiểm tra

| Việc | Lệnh / địa chỉ |
|---|---|
| Xem bảng Gold đã đăng ký | `docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -e "SHOW TABLES IN gold;"` |
| Duyệt file Silver/Gold | HDFS UI `http://localhost:9870` → `/datalake/silver`, `/datalake/gold` |
| Theo dõi job Spark | Spark UI `http://localhost:8090` |
| Chạy thủ công | Airflow UI `http://localhost:8080` → `medallion_pipeline` → Trigger |
| Xem log Silver/Gold | `docker logs <airflow-scheduler>` hoặc Spark UI stdout |

### 19.2 Bảng triệu chứng → nguyên nhân

| Triệu chứng | Nguyên nhân khả dĩ | Cách xử lý |
|---|---|---|
| Silver báo `Path does not exist` | Bronze chưa từng chạy với sink đó | `safe_read_parquet` đã chịu lỗi → trả rỗng; chạy Bronze trước |
| Gold `Table not found` khi AI Agent query | Gold chưa chạy hoặc fail | Chạy lại DAG; kiểm `SHOW TABLES IN gold` |
| `fact_sales` thiếu dòng | order_item tới trước order (race) → order_date NULL → bị lọc | Chờ Silver có đủ orders rồi rebuild Gold |
| AI Agent đọc Parquet lỗi kiểu timestamp | Quên `INT96`/`writeLegacyFormat` | Đã set sẵn trong `gold_transform.py` |
| Bảng `payments`/`shipping` rỗng | Payment là nested envelope — sai `get_json_object` | Kiểm `pay_raw` có `payload` không |
| Tiền tệ ra NULL | Quên cast `DecimalType` (Debezium gửi string) | Đã cast trong Silver; kiểm `decimal.handling.mode=string` ở Debezium |

---

## 20. Tóm tắt một câu

> Chặng **Bronze → Silver → Gold** trong DataFinch do **Apache Spark** (Standalone) thực
> thi qua hai job batch được **Airflow** điều phối theo thứ tự `silver >> gold` mỗi 15
> phút: **`silver_transform.py`** đọc JSON thô từ `/datalake/bronze`, parse theo schema
> tường minh cho **3 định dạng nguồn khác nhau** (ERP-Debezium phẳng, warehouse phẳng,
> payment lồng nhau), **làm sạch** (null placeholder, chuẩn hóa enum, cast Decimal/
> timestamp, validate nghiệp vụ), **khử trùng CDC** (`latest_per_id`/`dropDuplicates`),
> **tách dữ liệu bẩn vào DLQ**, rồi ghi `overwrite` thành **12 bảng Silver sạch**; tiếp đó
> **`gold_transform.py`** join các bảng Silver thành **star schema denormalized** (3 fact
> + 7 dimension), tính sẵn các cột phái sinh để AI Agent khỏi join, và dùng `saveAsTable`
> với `.option("path")` để ghi Parquet **EXTERNAL** xuống `/datalake/gold` đồng thời
> **đăng ký vào Hive Metastore** — nhờ đó **AI Agent truy vấn được qua HiveServer2 bằng
> SQL chuẩn**, khép kín luồng dữ liệu từ Kafka tới câu trả lời.
