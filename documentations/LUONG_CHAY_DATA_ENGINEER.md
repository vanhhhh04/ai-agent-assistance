# LUỒNG CHẠY DỰ ÁN — PHẦN DATA ENGINEER

> **Đối tượng**: Data Engineer onboard dự án, người review kiến trúc, sinh viên đồ án.
> **Nguồn tham chiếu**: `data-source/sim_*.py`, `nifi/`, `scripts/register_debezium.sh`, `spark/jobs/{bronze,silver,gold}_*.py`, `airflow/dags/medallion_pipeline.py`, `data/initial_table.sql`.
> **Phạm vi**: chỉ mô tả khâu **sinh dữ liệu → ingest → lưu trữ → xử lý ETL → catalog**. Phần AI Agent / FastAPI / CLI không thuộc tài liệu này.

---

## 0. Tổng quan kiến trúc Data Pipeline

Hệ thống áp dụng kiến trúc **Lakehouse Medallion** (Bronze → Silver → Gold) trên HDFS, với **hai đường ingest song song**:

```
                                 ┌─────────────────────────────────────────────┐
                                 │            HẠ TẦNG LƯU TRỮ                  │
                                 │                                             │
 ┌──────────────┐  WAL  ┌──────┐ │   ┌────────┐   ┌────────┐   ┌────────┐     │
 │ sim_erp.py   │──────▶│ PG   │─┼──▶│ Bronze │──▶│ Silver │──▶│  Gold  │──┐  │
 │ (OLTP)       │ CDC   └──────┘ │   │ (HDFS) │   │ (HDFS) │   │ (HDFS) │  │  │
 └──────────────┘                │   └────────┘   └────────┘   └────────┘  │  │
                                 │        ▲                                 │  │
 ┌──────────────┐  CSV   ┌─────┐ │        │       ┌────────────────┐       │  │
 │ sim_warehouse│───────▶│NiFi │─┼────────┤       │ Hive Metastore │◀──────┘  │
 └──────────────┘        │     │ │        │       │ (catalog gold) │          │
                         │     │ │   ┌────┴──┐    └────────────────┘          │
 ┌──────────────┐  HTTP  │     │ │   │ Kafka │                                │
 │ sim_payment  │───────▶│     │─┼──▶│       │                                │
 └──────────────┘        └─────┘ │   └───────┘                                │
                                 └─────────────────────────────────────────────┘
                                              ▲
                                              │  điều phối hourly
                                       ┌──────┴───────┐
                                       │   Airflow    │
                                       │ medallion DAG│
                                       └──────────────┘
```

**Phong cách kiến trúc**: hybrid — `event-driven streaming` (Kafka + NiFi + Debezium) kết hợp `batch ETL` (Airflow điều phối Spark mỗi 15 phút).

---

## 1. Lớp sinh dữ liệu (Data Sources)

Dự án mô phỏng **3 nguồn dữ liệu khác biệt** để phản ánh môi trường thực tế không đồng nhất:

| Mô phỏng | Domain | Cơ chế truyền | Có dùng NiFi? | Có CDC? |
|---|---|---|---|---|
| `sim_erp.py` | customers, orders, order_items, coupons, addresses, reviews, feedback | PG → WAL → Debezium → Kafka | Không | **Có** (logical decoding) |
| `sim_warehouse.py` | categories, products | Ghi file CSV → NiFi `GetFile` → Kafka | Có | Không |
| `sim_payment.py` | payments, shipping | HTTP POST → NiFi `ListenHTTP` → Kafka | Có | Không |

### 1.1 ERP (`data-source/sim_erp.py`)

- **Bootstrap (chạy một lần)**: insert 100k customers, 200k addresses, 100k coupons, 500k orders, ~1.2M order_items vào PostgreSQL `ecommerce`.
- **Vòng lặp realtime**:
  - khách hàng mới mỗi 30s
  - đơn hàng mới mỗi 2–5s
  - update trạng thái đơn hàng mỗi 10s
  - coupon mới mỗi 5 phút
  - review mỗi 60s, feedback mỗi 120s
- **Tiêm dữ liệu bẩn 5%** (`DIRTY_RATE = 0.05`): `null_email`, `invalid_email`, `phone_too_long`, `duplicate_email`, `status_typo`, `future_order_date`, `negative_amount`, `late_cdc_delay`, `missing_items`.

### 1.2 Warehouse (`data-source/sim_warehouse.py`)

- Ghi file CSV vào volume `csv_shared` mount tại `/app/csv_output` (phía simulator) và `/opt/nifi/csv_input` (phía NiFi).
- **Envelope phẳng** (mọi field nằm cùng cấp top-level):
  ```json
  {"_source_system":"warehouse","_event_type":"products",
   "_event_id":"<uuid>","_quality_flag":"CLEAN|DIRTY",
   "category_id":12,"name":"...","sku":"...","price":"..."}
  ```

### 1.3 Payment Gateway (`data-source/sim_payment.py`)

- INSERT/UPDATE PostgreSQL trước, sau đó `requests.post('http://nifi:8181/payment-events', ...)`.
- **Envelope lồng nhau** (`payload` bọc body — khác hẳn warehouse, **cố ý** để Silver phải xử lý cả hai dạng):
  ```json
  {"_source_system":"payment_gw","_event_type":"payments|shipping|reviews|feedback",
   "_op_type":"c|u","payload":{"id":...,"order_id":...,"amount":...}}
  ```
- Mô phỏng race-condition: post sang NiFi **trước khi** commit PG, hoặc inject `mismatched_order_ref` (5%) tham chiếu order_id giả ≥9_000_000.

---

## 2. Lớp Ingestion

### 2.1 Đường ERP — Debezium CDC

```
psycopg2.commit()
     ↓
PostgreSQL WAL (wal_level=logical, max_replication_slots=10)
     ↓
slot 'erp_debezium_slot' + publication 'erp_publication'
     ↓
Debezium PostgresConnector (plugin=pgoutput)
     ↓
SMT: ExtractNewRecordState → thêm __op, __table, __source_ts_ms
     ↓
Kafka topics:
   erp.public.customers       (1 record / 1 row mutation)
   erp.public.orders
   erp.public.order_items
   erp.public.coupons
```

Cấu hình connector quan trọng (`scripts/register_debezium.sh`):

```json
"snapshot.mode": "initial",
"transforms": "unwrap",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.unwrap.add.fields": "op,table,source.ts_ms",
"transforms.unwrap.delete.handling.mode": "rewrite"
```

**Hệ quả**: payload Kafka là JSON phẳng — Spark Silver giả định có `__op`, `__table` ở top-level.

### 2.2 Đường Warehouse — NiFi GetFile

```
sim_warehouse.py
     ↓ ghi /app/csv_output/products_delta_{ts}.csv
     ↓ (Docker volume mount)
/opt/nifi/csv_input/
     ↓ NiFi GetFile (poll 1s)
     ↓ ConvertRecord (CSVReader → JsonRecordSetWriter)
     ↓ PublishKafkaRecord
     ↓
Kafka: warehouse.events  (CLEAN)
       warehouse.events.dlq  (DIRTY — auto-route theo `_quality_flag`)
```

### 2.3 Đường Payment — NiFi ListenHTTP

```
sim_payment.py
     ↓ INSERT/UPDATE PG
     ↓ requests.post('http://nifi:8181/payment-events', envelope_nested)
     ↓
NiFi ListenHTTP (port 8181, basePath 'payment-events')
     ↓ PublishKafkaRecord
     ↓
Kafka: payment.events / payment.events.dlq
```

### 2.4 Bảng tổng hợp Kafka topic

| Topic | Partitions | Producer | Consumer | Schema |
|---|---:|---|---|---|
| `erp.public.customers` | 1 | Debezium | Spark Bronze | flat JSON + `__op,__table,__source_ts_ms` |
| `erp.public.orders` | 1 | Debezium | Spark Bronze | flat JSON |
| `erp.public.order_items` | 1 | Debezium | Spark Bronze | flat JSON |
| `erp.public.coupons` | 1 | Debezium | Spark Bronze | flat JSON |
| `warehouse.events` (+`.dlq`) | 1 | NiFi | Spark Bronze | envelope phẳng |
| `payment.events` (+`.dlq`) | 1 | NiFi | Spark Bronze | envelope lồng (`payload:{...}`) |

> **Hạn chế cần nâng cấp production**: 1 partition/topic = không có parallelism cho consumer Spark — nên tăng ≥3 partitions trước khi đưa lên production.

---

## 3. Lớp Bronze — `spark/jobs/bronze_ingestion.py`

**Nguyên tắc Bronze**: giữ nguyên `value` từ Kafka (cột `raw_data`), chỉ thêm lineage Kafka và partition `ingest_date`. **Không có business logic.**

### 3.1 Cơ chế đọc Kafka

Dùng **Spark Structured Streaming + `Trigger.AvailableNow`**:

```python
spark.readStream.format("kafka")
    .option("subscribePattern", "erp\\.public\\..*")
    .option("startingOffsets", "earliest")  # chỉ dùng lần đầu, sau đó dựa vào checkpoint
    .option("failOnDataLoss", "false")
    .load()
```

- `Trigger.AvailableNow`: drain mọi offset Kafka mới kể từ lần checkpoint trước rồi **thoát** (vòng đời giống batch nhưng có offset tracking như streaming) → Airflow lên lịch được mà không phải nuôi process Spark long-running.
- Checkpoint tại `hdfs://namenode:9000/datalake/_checkpoints/bronze/<sink_name>` đảm bảo **exactly-once + incremental**.
- `subscribePattern` (thay vì `subscribe`) tolerate topic DLQ chưa tồn tại.

### 3.2 Cấu trúc Bronze

```python
F.col("value").cast("string").alias("raw_data"),
F.col("key").cast("string").alias("kafka_key"),
F.col("topic").alias("_source_topic"),
F.col("partition").alias("_kafka_partition"),
F.col("offset").alias("_kafka_offset"),
F.col("timestamp").alias("_kafka_timestamp"),
F.current_timestamp().alias("_bronze_ingested_at"),
F.to_date(F.col("timestamp")).alias("ingest_date"),
```

### 3.3 Sink HDFS

```
hdfs://namenode:9000/datalake/bronze/
    erp_raw/          ← pattern erp\.public\..*
    wh_raw/           ← pattern warehouse\.events
    wh_dlq/           ← pattern warehouse\.events\.dlq
    pay_raw/          ← pattern payment\.events
    pay_dlq/          ← pattern payment\.events\.dlq
```

- Format: **Parquet append**, partitioned by `(_source_topic, ingest_date)`.

---

## 4. Lớp Silver — `spark/jobs/silver_transform.py`

**Vai trò**: làm sạch, chuẩn hoá kiểu dữ liệu, deduplicate, tách DLQ. Mỗi nguồn được xử lý theo nhánh riêng vì payload **khác nhau hoàn toàn**.

### 4.1 Vì sao Silver dùng `overwrite`?

Bronze là append-only. Silver được **tính lại toàn bộ** mỗi lần chạy — đơn giản và đảm bảo idempotent. Với production >10M dòng nên chuyển sang Delta/Iceberg `MERGE`.

### 4.2 Nhánh ERP (CDC Debezium)

- Filter theo `_source_topic = erp.public.<table>`.
- Parse với schema strict: `ORDER_SCHEMA`, `CUSTOMER_SCHEMA`, `ORDER_ITEM_SCHEMA`, `COUPON_SCHEMA`.
- **Loại bỏ event delete** (`__op != 'd'`) — Silver chỉ giữ snapshot mới nhất.
- Clean rules:
  - `null_placeholders()` thay `"UNKNOWN"`, `"N/A"`, `"#N/A"`, `"None"`, `"null"`, `"EMPTY"`, `"--"`, `"???"`, `"TBD"`, `"not available"`, `""` → `NULL`.
  - `normalize_status()` whitelist enum, typo trả về `NULL`.
  - Validate email bằng regex `^[^@\s]+@[^@\s]+\.[^@\s]+$`.
  - Cast tiền tệ `DecimalType(12,2)`, thời gian `Timestamp`.
  - Dedup theo primary key (giữ bản ghi `__source_ts_ms` mới nhất).

### 4.3 Nhánh Warehouse

- Parse `WH_SCHEMA` **phẳng**.
- Chuẩn hoá SKU: `regexp_replace(upper(trim(sku)), r"[_\s]", "-")`.
- Dedup theo `_event_id` (idempotent với retry).
- Lọc `price > 0`.

### 4.4 Nhánh Payment

- Parse **2 lớp**: `ENVELOPE_SCHEMA` rồi `from_json(payload, PAYMENT_SCHEMA | SHIPPING_SCHEMA | REVIEW_SCHEMA | FEEDBACK_SCHEMA)` tuỳ `_event_type`.
- Dedup theo `_event_id`, sau đó theo `transaction_id` / `tracking_number`.
- Whitelist `payment_method`, status.

### 4.5 Pattern DLQ

Mỗi entity dùng `drop_dirty()` → `(clean_df, dirty_df)`. Dòng dirty ghi vào:

```
hdfs://namenode:9000/datalake/silver/dlq/  (mode=append)
```

với schema chung `id, _quality_flag, _bronze_ingested_at`. Reviewer cần trace ngược về Bronze để xem full context.

### 4.6 Sink Silver

```
/datalake/silver/orders
/datalake/silver/customers
/datalake/silver/order_items
/datalake/silver/products
/datalake/silver/addresses
/datalake/silver/categories
/datalake/silver/coupons
/datalake/silver/reviews
/datalake/silver/feedback
/datalake/silver/payments
/datalake/silver/shipping
/datalake/silver/dlq
```

---

## 5. Lớp Gold — `spark/jobs/gold_transform.py`

**Vai trò**: dựng **star schema** denormalized phục vụ truy vấn analytics + AI Agent NL→SQL.

### 5.1 Schema sao

```
fact_sales      (grain = 1 order_item)   ← partition by (order_year, order_month)
fact_reviews    (grain = 1 review)        ← partition by (review_year, review_month)
fact_feedback   (grain = 1 feedback)      ← partition by (feedback_year, feedback_month)

dim_customers   (customer_key)
dim_products    (product_key, đã denormalize category_name)
dim_categories  (category_key, self-join để có parent_category_name)
dim_addresses   (address_key)
dim_coupons     (coupon_key)
dim_payments    (payment_key)
dim_shipping    (shipping_key, có cột delivery_days)
```

### 5.2 Quy tắc join — `fact_sales`

```python
order_items i
    INNER JOIN orders o       ON i.order_id   = o.id
    LEFT  JOIN dim_customers  ON o.customer_id = customer_key
    LEFT  JOIN dim_products   ON i.product_id  = product_key
    LEFT  JOIN dim_payments   ON o.id          = pm.order_id
    LEFT  JOIN dim_shipping   ON o.id          = s.order_id
    LEFT  JOIN dim_coupons    ON o.coupon_id   = coupon_key
    LEFT  JOIN dim_addresses  ON o.shipping_address_id = address_key
```

**Chú ý quan trọng**: `LEFT JOIN` để giữ lại fact dù dim chưa có (race condition giữa các nguồn). `NULL customer_key` trong `fact_sales` là **kỳ vọng**, không phải bug.

Drop dòng `order_year IS NULL OR order_month IS NULL` vì Hive 2.3 không xử lý được `__HIVE_DEFAULT_PARTITION__`.

### 5.3 Đăng ký Hive

Mọi bảng Gold là `EXTERNAL` Hive table — data và schema tách rời:

```python
df.write.mode("overwrite").format("parquet")
    .option("path", "hdfs://.../datalake/gold/fact_sales")
    .partitionBy("order_year", "order_month")
    .saveAsTable("gold.fact_sales")
```

Hive Metastore (`thrift://hive-metastore:9083`) lưu catalog. HiveServer2 (`jdbc:hive2://hiveserver2:10000`) là cổng JDBC cho AI Agent / BI tools.

---

## 6. Lớp Orchestration — `airflow/dags/medallion_pipeline.py`

### 6.1 DAG

```python
schedule_interval = "*/15 * * * *"   # mỗi 15 phút
max_active_runs   = 1                # không chạy chồng lấn
catchup           = False

bronze >> silver >> gold
```

3 task đều dùng `SparkSubmitOperator` submit vào cluster `spark://spark-master:7077` (client mode).

### 6.2 Cấu hình Spark dùng chung

```python
"spark.hadoop.fs.defaultFS":         "hdfs://namenode:9000",
"spark.sql.catalogImplementation":   "hive",
"spark.hadoop.hive.metastore.uris":  "thrift://hive-metastore:9083",
"spark.sql.warehouse.dir":           "hdfs://namenode:9000/user/hive/warehouse",
"spark.driver.memory":               "1g",
"spark.executor.memory":             "1g",
"spark.sql.shuffle.partitions":      "8",
```

Bronze cần thêm package `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` (Spark 3.5.x + Scala 2.12).

### 6.3 Freshness end-to-end

| Hop | Độ trễ kỳ vọng |
|---|---|
| Sim → PG | <100ms |
| PG WAL → Kafka (Debezium) | <1s |
| Sim → NiFi → Kafka | <500ms |
| Kafka → Bronze | drain mỗi 15 phút |
| Bronze → Silver | tuần tự (~vài phút) |
| Silver → Gold | tuần tự (~vài phút) |

→ **End-to-end Gold freshness ≈ 15–25 phút** (giới hạn bởi cadence Airflow).

---

## 7. Race-condition & dữ liệu bẩn — điều DE phải biết

| Loại | Cách inject | Cách xử lý |
|---|---|---|
| `late_cdc_delay` (sim_erp) | sleep 1–3s trước commit | Bronze giữ nguyên; Silver dedup theo PK + `__source_ts_ms` |
| `mismatched_order_ref` (sim_payment) | order_id giả ≥9_000_000 | Gold `LEFT JOIN` → `order_key` NULL |
| Payment POST trước commit PG | bất đồng bộ giữa 2 đường | fact_sales `LEFT JOIN` → NULL dim_payments |
| Dirty 5% (mọi loại) | xem `DIRTY_CONFIG` trong sim_erp | Silver `drop_dirty()` → DLQ |
| Duplicate event Kafka retry | producer at-least-once | Silver dedup theo `_event_id` |

---

## 8. Cách vận hành thường ngày

### 8.1 Khởi động hệ thống

```powershell
docker compose up -d
# Đợi PG, Kafka, NiFi, HDFS, Hive sẵn sàng (~2-3 phút)

python cli.py debezium setup        # đăng ký Debezium connector
python cli.py nifi setup            # nạp NiFi templates
python cli.py simulator start       # chạy 3 simulators
```

### 8.2 Trigger pipeline thủ công

```powershell
python cli.py pipeline run          # hoặc bật DAG trên Airflow UI :8080
```

### 8.3 Kiểm tra dữ liệu

| Lớp | Cách kiểm tra |
|---|---|
| Kafka | Mở Kafka UI tại http://localhost:8888 |
| Bronze (HDFS) | `docker exec namenode hdfs dfs -ls /datalake/bronze` |
| Silver | `docker exec namenode hdfs dfs -ls /datalake/silver` |
| Gold (Hive) | `beeline -u jdbc:hive2://localhost:10000 -e "USE gold; SHOW TABLES; SELECT COUNT(*) FROM fact_sales;"` |
| DLQ | Bảng `silver/dlq` + topic `*.events.dlq` |
| Airflow | http://localhost:8080 — DAG `medallion_pipeline` |

### 8.4 Reset toàn bộ datalake

```powershell
docker exec namenode hdfs dfs -rm -r /datalake
# Xoá luôn checkpoint Bronze, lần chạy kế tiếp sẽ đọc lại từ earliest
```

---

## 9. Quy mô dữ liệu mong đợi

Theo cấu hình mặc định của simulator:

- 100k customers (bootstrap)
- 500k orders × ~2.4 items/order ≈ 1.2M order_items
- Steady-state: ~30 orders/phút

Theo bộ luyện làm sạch CSV (`documentations/CHI_TIET_LOI_DU_LIEU.md`):

- 11 bảng, ~71k dòng clean → ~77k dòng dirty
- ~3.6k duplicate chính xác, ~1.4k near-duplicate, ~1.3k orphaned FK

→ Đây là **quy mô đồ án**, không phải production traffic. Trước khi production cần: tăng partition Kafka, chuyển Silver/Gold sang Delta/Iceberg MERGE, tách Spark cluster, bật monitoring (Grafana + Prometheus).

---

## 10. Các điểm cần lưu ý khi mở rộng

1. **Kafka partitions = 1** → không scale consumer. Tăng lên ≥3.
2. **Silver overwrite** → tốn I/O. Chuyển sang Delta/Iceberg `MERGE INTO` khi dữ liệu lớn.
3. **Gold không có SCD** — dim hiện chỉ giữ snapshot mới nhất. Nếu cần lịch sử thay đổi, áp dụng SCD Type-2.
4. **Hive 2.3 cũ** — nên thay bằng Iceberg + Trino để query Gold trực tiếp, bỏ HiveServer2.
5. **Checkpoint Bronze** trên HDFS đơn replica — production cần ≥3 replicas hoặc lưu trên S3/MinIO.
6. **DLQ thiếu schema gốc** — chỉ giữ `id, _quality_flag, _bronze_ingested_at`. Mở rộng để giữ snapshot toàn dòng giúp recovery dễ hơn.

---

## 11. Mapping file ↔ trách nhiệm

| File | Vai trò DE |
|---|---|
| `data/initial_table.sql` | Schema OLTP nguồn (11 bảng PostgreSQL) |
| `data-source/sim_erp.py` | Sinh OLTP traffic + tiêm dirty data |
| `data-source/sim_warehouse.py` | Sinh CSV cho NiFi GetFile |
| `data-source/sim_payment.py` | Sinh HTTP webhook cho NiFi ListenHTTP |
| `scripts/register_debezium.sh` | Cấu hình Debezium PostgresConnector |
| `nifi/setup_flows.py` | Bootstrap flow NiFi (GetFile, ListenHTTP, PublishKafka) |
| `spark/jobs/bronze_ingestion.py` | Drain Kafka → Bronze Parquet (streaming AvailableNow) |
| `spark/jobs/silver_transform.py` | Clean + normalize + dedup + DLQ |
| `spark/jobs/gold_transform.py` | Star schema + đăng ký Hive |
| `airflow/dags/medallion_pipeline.py` | Điều phối 3 stage Spark mỗi 15 phút |
| `hive/init.sh` | Init Hive Metastore schema |
| `cli.py` | Entry point quản lý simulator, debezium, nifi, pipeline |

---

**Tóm tắt 1 dòng cho DE**:
> Mỗi 15 phút, Airflow gọi Spark drain incremental Kafka về Bronze (Parquet+checkpoint) → rebuild Silver (clean+dedup+DLQ) → dựng Gold star schema (đăng ký Hive EXTERNAL) phục vụ truy vấn analytics.
