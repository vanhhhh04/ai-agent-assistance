# DATA ENGINEER FLOW — Data Source → NiFi → Kafka

Tài liệu này mô tả **chi tiết và đầy đủ** luồng dữ liệu từ 3 nguồn (Data Source) đi qua tầng ingest (NiFi + Debezium) và đổ vào Kafka. Đây là **nửa đầu** của data pipeline DataFinch (nửa sau là Kafka → Spark → HDFS/Hive Gold).

> Phạm vi: từ `data-source/sim_*.py` → NiFi/Debezium → Kafka topics.
> Không bao gồm: Spark Bronze/Silver/Gold, Airflow, Hive (sẽ ở tài liệu khác).

---

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Triết lý: 3 nguồn, 3 kiểu ingest](#2-triết-lý-3-nguồn-3-kiểu-ingest)
3. [Source Envelope — hợp đồng dữ liệu chung](#3-source-envelope--hợp-đồng-dữ-liệu-chung)
4. [Nguồn 1: Warehouse (CSV → NiFi GetFile)](#4-nguồn-1-warehouse-csv--nifi-getfile)
5. [Nguồn 2: Payment Gateway (HTTP → NiFi ListenHTTP)](#5-nguồn-2-payment-gateway-http--nifi-listenhttp)
6. [Nguồn 3: ERP (Postgres WAL → Debezium CDC)](#6-nguồn-3-erp-postgres-wal--debezium-cdc)
7. [NiFi chi tiết — processors & controller services](#7-nifi-chi-tiết--processors--controller-services)
8. [Debezium CDC chi tiết](#8-debezium-cdc-chi-tiết)
9. [Kafka — topics & cấu hình](#9-kafka--topics--cấu-hình)
10. [Dirty data & Dead Letter Queue](#10-dirty-data--dead-letter-queue)
11. [Các pattern DE quan trọng](#11-các-pattern-de-quan-trọng)
12. [Cấu hình docker-compose liên quan](#12-cấu-hình-docker-compose-liên-quan)
13. [Vận hành & Debug](#13-vận-hành--debug)
14. [Tóm tắt](#14-tóm-tắt)

---

## 1. Tổng quan kiến trúc

```
┌──────────────────────── DATA SOURCE (container: data-source) ────────────────────────┐
│                                                                                        │
│   sim_warehouse.py          sim_erp.py                   sim_payment.py                │
│   (warehouse)               (erp)                        (payment_gw)                  │
│   categories, products      customers, orders,           payments, shipping,          │
│                             addresses, coupons,          reviews, feedback             │
│                             order_items, reviews,                                      │
│                             feedback                                                   │
│        │                        │                              │                       │
│   ① ghi CSV               ② chỉ ghi Postgres           ③ ghi Postgres + POST HTTP      │
│   + ghi Postgres                                                                       │
└────────┼────────────────────────┼──────────────────────────────┼──────────────────────┘
         │                         │                              │
         │ CSV file                │ WAL (Write-Ahead Log)        │ HTTP POST (JSON)
         │ /opt/nifi/csv_input     │                              │ :8181/payment-events
         ▼                         ▼                              ▼
   ┌───────────┐          ┌─────────────────┐            ┌───────────────┐
   │   NiFi    │          │    Debezium     │            │     NiFi      │
   │  GetFile  │          │ Kafka Connect   │            │  ListenHTTP   │
   │ (warehouse│          │ (PostgresConn.) │            │  (payment     │
   │  pipeline)│          │  reads WAL      │            │   pipeline)   │
   └─────┬─────┘          └────────┬────────┘            └───────┬───────┘
         │                         │                             │
         │ ConvertRecord           │ transforms.unwrap           │ EvaluateJsonPath
         │ SplitJson               │ (flat JSON +__op,__table)   │ RouteOnAttribute
         │ EvaluateJsonPath        │                             │
         │ RouteOnAttribute        │                             │
         ▼                         ▼                             ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │                          KAFKA (broker :9092 / :29092)                 │
   │                                                                        │
   │  warehouse.events       erp.public.customers        payment.events     │
   │  warehouse.events.dlq   erp.public.orders           payment.events.dlq │
   │                         erp.public.order_items                         │
   │                         erp.public.products                            │
   │                         erp.public.categories                          │
   │                         erp.public.coupons                             │
   │                         erp.public.addresses                           │
   │                         erp.public.reviews                             │
   │                         erp.public.feedback                            │
   └──────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
                       (Spark Bronze ingestion — tài liệu khác)
```

**Tóm tắt 1 dòng:** 3 simulator sinh dữ liệu giả lập theo 3 kiểu nguồn khác nhau (file, DB, webhook), đi qua 2 cơ chế ingest (NiFi cho file+webhook, Debezium cho CDC), và hội tụ tại Kafka dưới dạng các topic phân theo nguồn + chất lượng.

---

## 2. Triết lý: 3 nguồn, 3 kiểu ingest

Đây là điểm thiết kế **có chủ đích về mặt sư phạm**. Trong thực tế, một data platform phải nuốt dữ liệu từ nhiều loại nguồn khác nhau. Dự án này tái hiện **3 pattern ingest kinh điển**:

| # | Nguồn | Loại nguồn thực tế | Cơ chế ingest | Đường đi |
|---|---|---|---|---|
| 1 | **Warehouse** | Hệ thống kho/ERP legacy export file định kỳ | **Batch file** | CSV → NiFi `GetFile` |
| 2 | **Payment GW** | Cổng thanh toán bắn webhook | **Event/Webhook push** | HTTP → NiFi `ListenHTTP` |
| 3 | **ERP** | Database giao dịch OLTP | **CDC log-based** | Postgres WAL → Debezium |

### Tại sao mỗi nguồn dùng cách khác nhau?

- **Warehouse → CSV**: Mô phỏng hệ thống cũ chỉ biết "xuất file CSV mỗi giờ". Đây là kiểu ingest phổ biến nhất với dữ liệu legacy. NiFi `GetFile` poll thư mục, parse, đẩy đi.
- **Payment → HTTP**: Mô phỏng cổng thanh toán (Stripe, PayPal) gửi **webhook** mỗi khi có giao dịch. Đây là **push model** — nguồn chủ động đẩy, NiFi `ListenHTTP` lắng nghe.
- **ERP → CDC**: Database giao dịch chạy liên tục. Không thể export file hay gửi webhook cho mọi thay đổi. **Change Data Capture** đọc trực tiếp WAL (transaction log) của Postgres → bắt được **mọi** INSERT/UPDATE/DELETE mà không cần ứng dụng phải làm gì thêm.

→ **Bài học DE:** không có "một cách ingest đúng cho mọi nguồn". Phải chọn cơ chế phù hợp với khả năng và đặc tính của từng nguồn.

---

## 3. Source Envelope — hợp đồng dữ liệu chung

Cả 3 nguồn đều **bọc payload trong một "phong bì" (envelope)** chung trước khi gửi đi. Đây là khái niệm cốt lõi nhất của tầng này.

### Cấu trúc envelope

```json
{
  "_source_system":  "warehouse | erp | payment_gw",
  "_schema_version": "1.0",
  "_event_id":       "uuid4 — idempotency key (dùng để dedup)",
  "_event_type":     "product.created | order.created | payment.created ...",
  "_op_type":        "c | u | d  (create/update/delete — chuẩn Debezium)",
  "_ingested_at":    "2026-05-29T10:30:00 (ISO timestamp)",
  "_quality_flag":   "CLEAN | DIRTY | QUARANTINE",
  "_surrogate_hint": "md5(source_system:source_id)  (chỉ ERP có)",
  "payload": {
    "...dữ liệu thật của bản ghi..."
  }
}
```

### Vai trò từng field

| Field | Mục đích | Ai dùng nó |
|---|---|---|
| `_source_system` | Biết event đến từ hệ thống nào | Spark routing, debug |
| `_schema_version` | Xử lý schema evolution (v1.0 vs v2.0) | Spark deserialize |
| `_event_id` | **Idempotency** — dedup khi event bị gửi lặp | Spark dedup |
| `_event_type` | Loại nghiệp vụ | NiFi routing, Spark dispatch |
| `_op_type` | INSERT/UPDATE/DELETE — Spark biết phải merge thế nào | Spark Silver upsert |
| `_ingested_at` | Tính latency, watermark cho late-arrival | Spark watermark |
| `_quality_flag` | **CLEAN/DIRTY** — quyết định đi topic chính hay DLQ | NiFi RouteOnAttribute |
| `_surrogate_hint` | Khóa thay thế thống nhất giữa các nguồn | Spark Silver join |
| `payload` | Dữ liệu nghiệp vụ thật | Spark transform |

### Tại sao cần envelope thay vì gửi payload trần?

Code: `make_envelope()` trong cả 3 file `sim_*.py`.

```python
# data-source/sim_warehouse.py
def make_envelope(event_type, payload, quality_flag="CLEAN"):
    return {
        "_source_system":  SOURCE_SYSTEM,
        "_schema_version": SCHEMA_VERSION,
        "_event_id":       str(uuid.uuid4()),   # idempotency key
        "_event_type":     event_type,
        "_ingested_at":    datetime.now().isoformat(),
        "_quality_flag":   quality_flag,
        "payload": payload,
    }
```

3 lý do:

1. **Routing không cần parse payload**: NiFi/Spark chỉ nhìn `_quality_flag`, `_event_type` ở tầng envelope → quyết định route, không cần hiểu nội dung payload. Nhanh + tách biệt.
2. **Dedup idempotent**: `_event_id` (UUID) cho phép downstream phát hiện "event này đã xử lý chưa" → xử lý lại an toàn.
3. **Observability**: `_source_system` + `_ingested_at` cho phép trace 1 record xuyên suốt pipeline và đo latency từng chặng.

### Surrogate key — chỉ ERP mới có

```python
# data-source/sim_erp.py
def make_surrogate_key(source_system, source_id):
    raw = f"{source_system}:{source_id}"
    return hashlib.md5(raw.encode()).hexdigest()
```

**Vấn đề:** ERP dùng BIGINT serial, Payment GW dùng string ID, Warehouse dùng INT. Khi merge vào fact table cần **1 khóa thống nhất**.

`MD5(source_system:source_id)` đảm bảo:
- **Deterministic**: cùng input → cùng output (idempotent)
- **Unique cross-system**: `"erp:123"` ≠ `"payment_gw:123"`
- **Stable**: không đổi khi source DB migrate

Payment GW **không** có surrogate_hint vì nó không biết internal ID scheme của ERP → Spark phải tự resolve qua JOIN `payload.order_id → dim_orders`.

---

## 4. Nguồn 1: Warehouse (CSV → NiFi GetFile)

### File: `data-source/sim_warehouse.py`

**Nguồn:** mô phỏng hệ thống kho xuất file CSV.
**Bảng:** `categories`, `products`.
**Seed:** `Faker.seed(2001)` (deterministic).

### Sinh dữ liệu

```
Bootstrap (chạy 1 lần):
  - 1,000 categories (10 parent + children)
  - 10,000 products
  → ghi vào PostgreSQL  ┐
  → ghi CSV delta       ┘ đồng thời

Realtime loop:
  - Mỗi 10s : UPDATE stock/price cho 50 products  → CSV delta
  - Mỗi 2p  : INSERT 3-10 products mới             → DB (+CSV)
  - Mỗi 10p : INSERT category mới                  → DB

Cap: MAX_PRODUCTS=20,000 (chạm cap thì ngừng INSERT, vẫn UPDATE stock)
```

### Đường đi CSV → NiFi

Điểm mấu chốt: warehouse **ghi CSV ra thư mục chia sẻ** mà NiFi đang theo dõi.

```python
# write_csv_delta() — sim_warehouse.py
ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
path = CSV_OUTPUT_DIR / f"{filename}_{ts}.csv"   # CSV_DIR=/app/csv_output
with open(path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
```

**Shared volume** (docker-compose):

```yaml
data-source:
  volumes:
    - csv_shared:/app/csv_output      # warehouse ghi vào đây
nifi:
  volumes:
    - csv_shared:/opt/nifi/csv_input  # NiFi đọc từ đây
```

→ Cùng một named volume `csv_shared` được mount vào **cả 2 container**. Warehouse ghi file → file xuất hiện ngay trong thư mục NiFi đang poll.

> **Lưu ý quyền:** `data-source` chạy lệnh `chmod 777 /app/csv_output` lúc khởi động vì NiFi (chạy dưới user `nifi`) cần quyền **xóa** file sau khi xử lý (`Keep Source File=false`).

### CSV có gì? — Flat format

CSV **không phải nested JSON** — nó flatten envelope thành các cột:

```csv
_source_system,_schema_version,_event_id,_event_type,_ingested_at,_quality_flag,_dirty_reason,category_id,name,description,sku,price,cost,stock_quantity,brand,weight,is_active,created_at,updated_at
warehouse,1.0,a3f...,product.created,2026-05-29T...,CLEAN,,42,Áo thun,...,SKU-AB12CD34EF,250000,150000,87,Uniqlo,0.3,true,...,...
```

→ Lý do flatten: NiFi `CSVReader` đọc CSV phẳng dễ hơn nested. Spark sau đó đọc các cột `_*` làm metadata, các cột còn lại làm payload.

### NiFi Warehouse Pipeline (5 processor + 2 publish)

```
GetFile → ConvertRecord → SplitJson → EvaluateJsonPath → RouteOnAttribute ─┬─ PublishKafka → warehouse.events
(CSV)     (CSV→JSON)      (1 rec/FF)  (đọc _quality_flag) (CLEAN vs DIRTY)  └─ PublishKafka → warehouse.events.dlq
```

Chi tiết từng processor (từ `nifi/setup_flows.py`):

| Processor | Type | Vai trò | Config chính |
|---|---|---|---|
| **GetFile-Warehouse** | `GetFile` | Poll thư mục CSV mỗi 5s | `Input Directory=/opt/nifi/csv_input`, `File Filter=.*\.csv$`, `Keep Source File=false` |
| **CSV-to-JSON** | `ConvertRecord` | Parse CSV → JSON array | `record-reader=CSVReader`, `record-writer=JsonRecordSetWriter` |
| **SplitJson-Records** | `SplitJson` | Tách array → 1 FlowFile/record | `JsonPath Expression=$.*` |
| **Extract-QualityFlag-WH** | `EvaluateJsonPath` | Đọc `$._quality_flag` → attribute | `Destination=flowfile-attribute` |
| **Route-CLEAN-or-DLQ-WH** | `RouteOnAttribute` | Phân nhánh CLEAN/DIRTY | `clean=${quality_flag:equals('CLEAN')}` |
| **PublishKafka-warehouse.events** | `PublishKafka_2_6` | Đẩy CLEAN → topic chính | `topic=warehouse.events`, `acks=1` |
| **PublishKafka-warehouse.dlq** | `PublishKafka_2_6` | Đẩy DIRTY → DLQ | `topic=warehouse.events.dlq` |

---

## 5. Nguồn 2: Payment Gateway (HTTP → NiFi ListenHTTP)

### File: `data-source/sim_payment.py`

**Nguồn:** mô phỏng cổng thanh toán bắn webhook.
**Bảng:** `payments`, `shipping`, `reviews`, `feedback`.
**Seed:** `Faker.seed(3001)`.

### Sinh dữ liệu (không bootstrap, chỉ realtime)

```
Realtime loop:
  - Mỗi 3s : payment mới cho order chưa thanh toán   → INSERT payments
  - Mỗi 5s : cập nhật status payment                  → UPDATE (pending→completed→refunded)
  - Mỗi 15s: shipping mới + cập nhật trạng thái        → INSERT/UPDATE shipping
  - Mỗi 30s: review sau giao hàng                      → INSERT reviews
  - Mỗi 45s: feedback / khiếu nại                      → INSERT feedback

Cap: MAX_PAYMENTS=250,000
```

### Đường đi HTTP → NiFi

Khác warehouse, payment **POST trực tiếp lên NiFi qua HTTP**:

```python
# post_to_nifi() — sim_payment.py
def post_to_nifi(envelope):
    requests.post(
        NIFI_ENDPOINT,                          # http://nifi:8181/payment-events
        data=json.dumps(envelope, default=str),
        headers={
            "Content-Type":   "application/json",
            "X-Event-Type":   envelope.get("_event_type"),
            "X-Quality-Flag": envelope.get("_quality_flag"),
            "X-Event-ID":     envelope.get("_event_id"),
        },
        timeout=3,
    )
```

Mỗi event được xử lý **2 nơi**:
1. INSERT/UPDATE vào PostgreSQL (để giữ trạng thái OLTP)
2. POST envelope (JSON đầy đủ, nested) lên NiFi `ListenHTTP`

> **Fail-soft:** nếu NiFi chưa sẵn sàng → `ConnectionError` được nuốt, event vẫn nằm trong DB. Sim không crash.

### NiFi Payment Pipeline (3 processor + 2 publish)

```
ListenHTTP → EvaluateJsonPath → RouteOnAttribute ─┬─ PublishKafka → payment.events
(:8181)     (đọc _quality_flag, └─ PublishKafka → payment.events.dlq
            _event_type)         (CLEAN vs DIRTY)
```

| Processor | Type | Vai trò | Config chính |
|---|---|---|---|
| **ListenHTTP-Payment** | `ListenHTTP` | Nhận POST webhook | `Listening Port=8181`, `Base Path=payment-events` |
| **Extract-QualityFlag-PAY** | `EvaluateJsonPath` | Đọc `_quality_flag` + `_event_type` → attr | `Destination=flowfile-attribute` |
| **Route-CLEAN-or-DLQ-PAY** | `RouteOnAttribute` | Phân nhánh CLEAN/DIRTY | giống warehouse |
| **PublishKafka-payment.events** | `PublishKafka_2_6` | CLEAN → topic chính | `topic=payment.events` |
| **PublishKafka-payment.dlq** | `PublishKafka_2_6` | DIRTY → DLQ | `topic=payment.events.dlq` |

→ Pipeline payment **ngắn hơn** warehouse vì input đã là JSON (không cần `ConvertRecord` CSV→JSON, không cần `SplitJson` vì mỗi POST đã là 1 record).

---

## 6. Nguồn 3: ERP (Postgres WAL → Debezium CDC)

### File: `data-source/sim_erp.py` + `nifi/init_debezium.sh`

**Nguồn:** database giao dịch OLTP.
**Bảng:** `customers`, `addresses`, `coupons`, `orders`, `order_items`, `reviews`, `feedback`.
**Seed:** `Faker.seed(1001)`.

### Điểm khác biệt then chốt

ERP **KHÔNG đi qua NiFi và KHÔNG ghi CSV/POST HTTP.** Nó chỉ làm 1 việc: **ghi vào PostgreSQL**.

```python
# sim_erp.py — chỉ INSERT/UPDATE vào DB, không có post_to_nifi() hay write_csv()
cur.execute("INSERT INTO orders (...) VALUES (...)")
conn.commit()
# Xong. Debezium tự bắt thay đổi này từ WAL.
```

→ Việc bắt thay đổi được **Debezium** làm hoàn toàn tự động bằng cách đọc **WAL (Write-Ahead Log)** của Postgres. Ứng dụng (sim_erp) không cần biết gì về Kafka.

### CDC hoạt động thế nào

```
sim_erp.py: INSERT INTO orders → Postgres ghi vào WAL (transaction log)
                                       │
                                       │ logical replication (pgoutput)
                                       ▼
                          Debezium PostgresConnector
                          (đọc replication slot "erp_debezium_slot")
                                       │
                                       │ transforms.unwrap → flat JSON
                                       ▼
                          Kafka topic: erp.public.orders
```

### Điều kiện tiên quyết: wal_level=logical

```yaml
# docker-compose.yml — postgres command
# FIX: bật wal_level=logical để NiFi/Debezium CDC hoạt động
```

Postgres mặc định `wal_level=replica` (chỉ đủ cho physical replication). CDC logic cần `wal_level=logical` để Debezium đọc được nội dung từng row thay đổi.

### Cấu hình Debezium connector

File `nifi/init_debezium.sh` đăng ký connector qua Kafka Connect REST API (`:8083`):

```json
{
  "name": "erp-postgres-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.dbname": "ecommerce",
    "topic.prefix": "erp",
    "table.include.list": "public.customers,public.addresses,public.categories,public.orders,public.order_items,public.coupons,public.products,public.reviews,public.feedback",
    "plugin.name": "pgoutput",
    "slot.name": "erp_debezium_slot",
    "publication.name": "erp_publication",
    "snapshot.mode": "initial",
    "decimal.handling.mode": "string",
    "time.precision.mode": "connect",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.add.fields": "op,table,source.ts_ms",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
```

### Giải thích từng config quan trọng

| Config | Ý nghĩa |
|---|---|
| `plugin.name=pgoutput` | Dùng logical replication **native** của Postgres (không cần cài plugin wal2json) |
| `slot.name=erp_debezium_slot` | Replication slot — Postgres giữ WAL chưa đọc cho slot này. **Tồn tại qua restart** → không mất event |
| `publication.name=erp_publication` | Tập các bảng được publish thay đổi |
| `topic.prefix=erp` | Topic được đặt tên `erp.public.{table}` |
| `snapshot.mode=initial` | Lần đầu start: **snapshot toàn bộ** rows hiện có → rồi stream WAL tiếp |
| `decimal.handling.mode=string` | Decimal → string (tránh mất độ chính xác khi qua JSON) |
| `transforms.unwrap` | **Quan trọng nhất** — xem dưới |

### transforms.unwrap — bóc phong bì Debezium

Debezium raw output rất "nặng" (before/after/source/op nested). `ExtractNewRecordState` (unwrap) **làm phẳng** nó:

**Trước unwrap (raw Debezium):**
```json
{
  "before": null,
  "after": {"id": 123, "status": "pending", "total_amount": "250000"},
  "source": {"ts_ms": 1234567890, "table": "orders", ...},
  "op": "c"
}
```

**Sau unwrap (`add.fields=op,table,source.ts_ms`):**
```json
{
  "id": 123,
  "status": "pending",
  "total_amount": "250000",
  "__op": "c",
  "__table": "orders",
  "__source_ts_ms": 1234567890
}
```

→ Spark đọc flat row + biết `__op` (c/u/d) để routing INSERT/UPDATE/DELETE, `__table` để biết bảng nguồn.

> `delete.handling.mode=rewrite` → khi DELETE, thay vì tombstone null, Debezium ghi row với `__deleted=true` để Spark xử lý soft-delete.

### Topics sinh ra từ ERP

```
erp.public.customers      erp.public.orders        erp.public.order_items
erp.public.products       erp.public.categories    erp.public.coupons
erp.public.addresses      erp.public.reviews       erp.public.feedback
```

---

## 7. NiFi chi tiết — processors & controller services

### File: `nifi/setup_flows.py`

Script này dùng **NiFi REST API** (`https://localhost:8443/nifi-api`) để **tự động dựng 2 pipeline** thay vì kéo thả thủ công trong UI.

### Controller Services (dùng chung)

Tạo trên root Process Group, phải `ENABLED` trước khi processor dùng:

| Service | Type | Vai trò |
|---|---|---|
| **CSVReader-WithHeader** | `CSVReader` | Đọc CSV, suy schema từ header row |
| **JsonRecordSetWriter** | `JsonRecordSetWriter` | Ghi ra JSON array, `output-grouping=output-array` |

```python
# Quan trọng: property name là INTERNAL (kebab-case), không phải label UI
json_writer = create_controller_service(
    pg, "org.apache.nifi.json.JsonRecordSetWriter", "JsonRecordSetWriter",
    props={
        "Schema Write Strategy":  "no-schema",
        "schema-access-strategy": "inherit-record-schema",
        "output-grouping":        "output-array",
    },
)
```

### Idempotency của script

`setup_flows.py` **tự wipe** flow cũ trước khi tạo mới:

```python
existing = list_processors(pg)
if existing:
    reset_root_pg(pg)   # stop → empty queues → delete connections → delete processors → disable+delete services
```

→ Chạy lại script nhiều lần an toàn (sau khi sửa property bug). Thứ tự wipe quan trọng: connection phải rỗng mới xóa được.

### PublishKafka config — at-least-once

```python
props={
    "bootstrap.servers": "kafka:29092",
    "topic":             "warehouse.events",
    "use-transactions":  "false",   # tắt EOS — cần transactional-id-prefix
    "acks":              "1",        # at-least-once (1 replica ACK)
}
```

- `use-transactions=false`: tắt exactly-once semantics (vì single-broker dev không cần). `acks=1`: chỉ cần broker leader ACK → nhanh, đánh đổi: có thể duplicate khi retry → **đây là lý do cần `_event_id` để Spark dedup.**

### RouteOnAttribute — luật phân nhánh

```python
props={
    "Routing Strategy": "Route to Property name",
    "clean": "${quality_flag:equals('CLEAN')}",
    "dirty": "${quality_flag:equals('DIRTY'):or(${quality_flag:equals('QUARANTINE')})}",
}
```

→ FlowFile có attribute `quality_flag=CLEAN` đi relationship `clean` → topic `.events`.
→ `DIRTY` hoặc `QUARANTINE` đi relationship `dirty` → topic `.events.dlq`.

---

## 8. Debezium CDC chi tiết

### Vòng đời connector

```
init_debezium.sh chạy:
  1. Đợi Kafka Connect REST sẵn sàng (:8083, tối đa 150s)
  2. DELETE connector cũ nếu tồn tại (idempotent)
  3. POST config connector mới
  4. Đợi connector vào trạng thái RUNNING (tối đa 60s)
  5. snapshot.mode=initial → snapshot toàn bộ rows (30-90s tùy data)
  6. Sau snapshot → stream WAL liên tục
```

### Snapshot vs Streaming

```
Phase 1 — SNAPSHOT (1 lần, lúc connector start lần đầu):
  Debezium đọc TOÀN BỘ rows hiện có trong 9 bảng
  → đẩy vào erp.public.{table} với __op="r" (read/snapshot)

Phase 2 — STREAMING (liên tục, sau snapshot):
  Debezium đọc WAL, mỗi thay đổi mới
  → đẩy với __op="c"/"u"/"d"
```

→ `snapshot.mode=initial` đảm bảo dữ liệu cũ (bootstrap của sim_erp) **không bị bỏ sót** — nó được snapshot trước, rồi mới stream các thay đổi realtime.

### Replication slot — tại sao không mất event

```
slot.name = erp_debezium_slot
```

Replication slot là cơ chế của Postgres: **giữ lại WAL chưa được consumer đọc**. Kể cả khi Debezium container restart, slot vẫn còn → khi Debezium quay lại, nó đọc tiếp từ vị trí cũ → **không mất event**.

> ⚠️ Mặt trái: nếu Debezium chết lâu mà sim_erp vẫn chạy → WAL tích tụ → đầy đĩa Postgres. Slot phải được dọn nếu bỏ connector vĩnh viễn.

---

## 9. Kafka — topics & cấu hình

### Cấu hình broker (docker-compose)

```yaml
kafka:
  image: confluentinc/cp-kafka:7.5.0
  ports:
    - "9092:9092"
  environment:
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

| Listener | Dùng bởi | Địa chỉ |
|---|---|---|
| `PLAINTEXT` (internal) | NiFi, Debezium, Spark (trong Docker network) | `kafka:29092` |
| `PLAINTEXT_HOST` (external) | Client trên host (debug từ máy thật) | `localhost:9092` |

→ Đây là lý do tất cả config trong container dùng `kafka:29092`, còn bạn debug từ máy host thì dùng `localhost:9092`.

### Toàn bộ topics

| Topic | Nguồn | Cơ chế | Nội dung |
|---|---|---|---|
| `warehouse.events` | warehouse | NiFi CSV | products/categories CLEAN |
| `warehouse.events.dlq` | warehouse | NiFi CSV | products/categories DIRTY |
| `payment.events` | payment_gw | NiFi HTTP | payments/shipping/reviews CLEAN |
| `payment.events.dlq` | payment_gw | NiFi HTTP | DIRTY |
| `erp.public.customers` | erp | Debezium | CDC customers |
| `erp.public.orders` | erp | Debezium | CDC orders |
| `erp.public.order_items` | erp | Debezium | CDC order_items |
| `erp.public.products` | erp | Debezium | CDC products |
| `erp.public.categories` | erp | Debezium | CDC categories |
| `erp.public.coupons` | erp | Debezium | CDC coupons |
| `erp.public.addresses` | erp | Debezium | CDC addresses |
| `erp.public.reviews` | erp | Debezium | CDC reviews |
| `erp.public.feedback` | erp | Debezium | CDC feedback |

> **Lưu ý dữ liệu trùng:** `products`, `reviews`, `feedback` xuất hiện ở **cả** Debezium CDC (từ Postgres) **và** NiFi (warehouse/payment). Đây là chủ ý: cùng 1 thực thể được nhìn qua 2 lăng kính (DB state vs event stream). Spark Silver là nơi reconcile.

### Kafka UI

```
http://localhost:8888   (đổi từ 8080 → 8888 để tránh conflict với NiFi ListenHTTP)
```

Dùng để xem topics, messages, consumer groups, lag.

---

## 10. Dirty data & Dead Letter Queue

### Triết lý: dirty data đến từ NGUỒN, không phải từ pipeline

```python
# Comment trong sim_warehouse.py
# Tại sao inject ở đây thay vì random trong Spark?
# → Vì dirty data thực tế xuất phát từ SOURCE, không phải từ pipeline.
# → Source system có lỗi, pipeline phải detect và xử lý.
```

Mỗi simulator inject `DIRTY_RATE=5%` bản ghi lỗi **có kiểm soát** để luyện kỹ năng xử lý data bẩn.

### Các loại dirty data theo nguồn

**Warehouse (`maybe_dirty_product`):**
| Loại | Mô phỏng lỗi thực tế |
|---|---|
| `null_price` | Import CSV legacy mất cột price |
| `negative_stock` | Oversell hoặc return chưa reconcile |
| `zero_price` | Mơ hồ: flash sale hợp lệ HAY lỗi? |
| `sku_format_variant` | Mỗi region có convention SKU khác |
| `duplicate_sku` | Import batch bị lặp |
| `future_created_at` | Clock skew (NTP drift) |
| `truncated_name` | Encoding UTF-8 lỗi cắt giữa chừng |

**ERP (`maybe_dirty_customer` / `maybe_dirty_order`):**
| Loại | Mô phỏng |
|---|---|
| `null_email` / `invalid_email` | Form validation yếu |
| `phone_too_long` | Import từ CRM legacy |
| `status_typo` | "SHIPED", "DELIVERD" |
| `future_order_date` | Timezone mismatch |
| `negative_amount` | Refund xử lý nhầm thành order |
| `missing_items` | Race condition checkout |

**Payment (`maybe_dirty_payment`):**
| Loại | Mô phỏng |
|---|---|
| `duplicate_txn_id` | Network retry gửi 2 lần |
| `mismatched_order_ref` | order_id trỏ đến order không tồn tại |
| `wrong_amount` | Currency rounding / double promotion |
| `invalid_payment_method` | "bitcoin", "barter" |
| `late_arrival` | POST trước commit (race condition) |

### Dead Letter Queue pattern

```
              EvaluateJsonPath đọc _quality_flag
                          │
              RouteOnAttribute
                    ┌─────┴─────┐
              CLEAN │           │ DIRTY/QUARANTINE
                    ▼           ▼
            warehouse.events  warehouse.events.dlq
            (Spark Silver)    (chờ review thủ công)
```

→ Dữ liệu bẩn **không bị vứt bỏ** — nó đi vào topic `.dlq` riêng. Sau này có thể review, sửa, replay. Dữ liệu sạch chảy thẳng vào Silver mà không bị nhiễm.

---

## 11. Các pattern DE quan trọng

### 11.1. Race condition / Late arrival (Payment)

Đây là pattern phức tạp nhất, mô phỏng lỗi phổ biến nhất trong payment microservice:

```python
# sim_payment.py — handle_new_payment()
# Timeline DIRTY (POST trước commit):
#   t=0: Payment POST /payment → NiFi nhận event
#   t=0: Spark nhận payment.created từ Kafka
#   t=1: ERP CDC chưa commit order → Spark JOIN miss → NULL ERP fields
#   t=2: Payment INSERT vào DB → commit
#
# Timeline CLEAN (commit trước POST):
#   t=0: DB commit thành công
#   t=1: Payment POST lên NiFi
#   t=2: Spark nhận event → order đã có → JOIN OK
if DIRTY_CONFIG.get("late_arrival") and quality_flag == "DIRTY":
    post_to_nifi(envelope)   # POST TRƯỚC
    # ... rồi mới INSERT DB
```

→ Spark xử lý bằng `withWatermark("_ingested_at", "10 minutes")` — chờ tối đa 10 phút cho event đến muộn trước khi kết luận JOIN miss.

### 11.2. Domain boundary respect (Payment không ghi vào ERP)

```python
# sim_payment.py — handle_shipping()
# FIX ANTI-PATTERN: Payment GW không được UPDATE orders.status trực tiếp
# Version cũ: UPDATE orders SET status='delivered' → vi phạm domain boundary
#
# DE-grade: emit event, để ERP tự xử lý
if to_s == "delivered":
    delivered_envelope = make_envelope("order.delivered_notification", {...})
    post_to_nifi(delivered_envelope)   # ERP consumer sẽ tự UPDATE
```

→ Mỗi domain (Payment, ERP) chỉ ghi vào DB của mình. Giao tiếp cross-domain qua **event**, không phải ghi trực tiếp. Đây là nguyên tắc microservice quan trọng.

### 11.3. Idempotency — chống xử lý lặp

3 lớp idempotency:
1. **`_event_id` (UUID)**: Spark dedup events trùng (do `acks=1` có thể duplicate)
2. **`ON CONFLICT DO NOTHING`**: DB insert idempotent (txn_id, email, sku unique)
3. **`_surrogate_hint` (MD5)**: deterministic key, replay an toàn

### 11.4. Thứ tự khởi động (dependency)

```
sim_warehouse.py  (chạy ĐẦU TIÊN)
    │ tạo products
    ▼
sim_erp.py  (đợi products > 0, poll mỗi 15s)
    │ tạo orders (cần product_id)
    ▼
sim_payment.py  (đợi orders > 0, poll mỗi 15s)
    │ tạo payments (cần order_id)
```

```bash
# cli/sim-start.sh
start_sim warehouse
sleep 2       # warehouse phải bootstrap products trước
start_sim erp
sleep 1
start_sim payment
```

→ Có FK dependency: order cần product, payment cần order. Mỗi sim **poll chờ** nguồn trước sẵn sàng thay vì crash.

---

## 12. Cấu hình docker-compose liên quan

### Các service trong luồng này

| Service | Image | Port | Vai trò |
|---|---|---|---|
| `postgres` | postgres (wal_level=logical) | 5432 | ERP OLTP source |
| `zookeeper` | cp-zookeeper:7.5.0 | 2181 | Kafka metadata |
| `kafka` | cp-kafka:7.5.0 | 9092/29092 | Message broker |
| `kafka-ui` | provectuslabs/kafka-ui | 8888 | Xem topics/messages |
| `nifi` | apache/nifi:1.23.2 | 8443/8181 | Ingest CSV + HTTP |
| `kafka-connect` | debezium/connect:2.5 | 8083 | CDC connector |
| `data-source` | (build local) | — | 3 simulators |

### Volume chia sẻ then chốt

```yaml
volumes:
  csv_shared:   # data-source ghi CSV, nifi đọc CSV — CÙNG volume

data-source:
  volumes:
    - csv_shared:/app/csv_output
nifi:
  volumes:
    - csv_shared:/opt/nifi/csv_input
    - ./nifi/provenance_repository:...   # NiFi lineage tracking
    - ./nifi/flowfile_repository:...      # FlowFile state
```

### Biến môi trường data-source

```yaml
data-source:
  environment:
    DB_HOST: postgres
    NIFI_ENDPOINT: http://nifi:8181/payment-events   # payment POST đến đây
    CSV_DIR: /app/csv_output                          # warehouse ghi CSV vào đây
```

---

## 13. Vận hành & Debug

### Khởi động toàn bộ luồng (thứ tự)

```bash
# 1. Postgres, Kafka, NiFi, Connect đã up qua docker compose
docker compose up -d postgres zookeeper kafka nifi kafka-connect

# 2. Dựng NiFi flows (2 pipeline)
python3 nifi/setup_flows.py

# 3. Đăng ký Debezium CDC connector
bash nifi/init_debezium.sh

# 4. Chạy 3 simulators
bash cli/sim-start.sh
```

### Theo dõi

```bash
# Log của từng simulator
bash cli/sim-logs.sh warehouse
bash cli/sim-logs.sh erp
bash cli/sim-logs.sh payment

# Sức khỏe pipeline tổng thể
bash cli/pipeline-status.sh

# UI
#   NiFi:     https://localhost:8443/nifi  (admin / adminadminadmin)
#   Kafka UI: http://localhost:8888
#   Connect:  http://localhost:8083/connectors/erp-postgres-cdc/status
```

### Kiểm tra từng chặng

```bash
# 1. Simulator có ghi DB không?
docker exec postgres psql -U postgres -d ecommerce -c "SELECT COUNT(*) FROM products"

# 2. Warehouse có ghi CSV không?
docker exec nifi ls -la /opt/nifi/csv_input/

# 3. Debezium connector RUNNING?
curl -s http://localhost:8083/connectors/erp-postgres-cdc/status | python3 -m json.tool

# 4. Topics có data không? (Kafka UI hoặc CLI)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic erp.public.orders --from-beginning --max-messages 5

# 5. DLQ có dirty data không?
docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic warehouse.events.dlq --from-beginning --max-messages 5
```

### Bảng triệu chứng → nguyên nhân

| Triệu chứng | Nguyên nhân | Fix |
|---|---|---|
| `warehouse.events` rỗng | NiFi GetFile không thấy CSV | Check volume `csv_shared` mount, quyền chmod 777 |
| `payment.events` rỗng | sim_payment không POST được | Check `NIFI_ENDPOINT`, NiFi ListenHTTP RUNNING? |
| `erp.public.*` rỗng | Debezium không RUNNING | Check `wal_level=logical`, replication slot, connector status |
| Connector FAILED | slot/publication lỗi | Xóa slot cũ, chạy lại `init_debezium.sh` |
| CSV không bị xóa sau xử lý | NiFi thiếu quyền | `chmod 777 /app/csv_output` |
| NiFi processor không start | Controller service chưa ENABLED | Check CSVReader/JsonRecordSetWriter state |
| Postgres đầy đĩa | Debezium chết, WAL tích tụ | Dọn replication slot hoặc restart connector |
| Topic toàn dirty | DIRTY_RATE quá cao | Giảm `DIRTY_RATE` trong sim_*.py |

### Reset & recover

```bash
bash cli/nifi-reset.sh      # wipe NiFi flow
bash cli/nifi-recover.sh    # dựng lại NiFi flow
bash cli/kafka-recover.sh   # recover Kafka
```

---

## 14. Tóm tắt

### Bảng so sánh 3 nguồn

| Khía cạnh | Warehouse | Payment GW | ERP |
|---|---|---|---|
| **source_system** | `warehouse` | `payment_gw` | `erp` |
| **Bảng** | categories, products | payments, shipping, reviews, feedback | customers, orders, addresses, coupons, order_items, reviews, feedback |
| **Cơ chế ingest** | Batch file (CSV) | Webhook (HTTP) | CDC (WAL) |
| **Đường vào** | NiFi GetFile | NiFi ListenHTTP | Debezium |
| **Có qua NiFi?** | ✅ | ✅ | ❌ |
| **Định dạng** | CSV flat | JSON nested | JSON (Debezium unwrap) |
| **Bootstrap** | 1k cat + 10k products | Không | 20k cust + 100k orders |
| **Surrogate hint** | ❌ | ❌ | ✅ MD5 |
| **Topic** | warehouse.events(.dlq) | payment.events(.dlq) | erp.public.{table} |
| **Pattern đặc biệt** | dirty data | race condition, domain boundary | snapshot+stream |

### 3 nguyên tắc thiết kế

1. **Right ingest for right source**: file→GetFile, webhook→ListenHTTP, DB→CDC. Không ép 1 cơ chế cho mọi nguồn.

2. **Envelope chuẩn hóa + DLQ**: mọi event bọc envelope thống nhất (`_event_id`, `_quality_flag`...), dirty data tách ra DLQ thay vì vứt bỏ → pipeline vừa sạch vừa không mất dữ liệu.

3. **Idempotency xuyên suốt**: `_event_id` (UUID) + `ON CONFLICT DO NOTHING` + surrogate key (MD5) → an toàn với duplicate do `acks=1` và replay.

### Luồng 1 câu

> 3 simulator sinh dữ liệu e-commerce giả lập (có 5% dirty data chủ đích) theo 3 kiểu nguồn thực tế; warehouse ghi CSV cho NiFi `GetFile` đọc, payment POST webhook vào NiFi `ListenHTTP`, ERP chỉ ghi Postgres để Debezium bắt WAL — cả ba hội tụ tại Kafka dưới dạng các topic phân theo nguồn và chất lượng (CLEAN→`.events`, DIRTY→`.dlq`), sẵn sàng cho Spark Bronze consume.
