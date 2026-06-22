# Luồng Kafka trong DataFinch — giải thích từng bước

> Tài liệu này mô tả **vai trò và luồng hoạt động của Apache Kafka** trong hệ thống
> DataFinch, theo từng bước từ lúc dữ liệu được sinh ra cho đến khi Spark đọc ra để
> xây dựng data lake. Mọi tên topic, port, tham số cấu hình trong tài liệu đều được
> đối chiếu trực tiếp với mã nguồn thật:
> `docker-compose.yml`, `nifi/init_debezium.sh`, `nifi/setup_flows.py`,
> `spark/jobs/bronze_ingestion.py`, `cli/kafka-recover.sh`.

---

## Mục lục

1. [Kafka là gì trong dự án này — đặt đúng vai trò](#1-kafka-là-gì-trong-dự-án-này--đặt-đúng-vai-trò)
2. [Mô hình lõi: 6 khái niệm Kafka](#2-mô-hình-lõi-6-khái-niệm-kafka)
3. [Sơ đồ tổng thể luồng Kafka](#3-sơ-đồ-tổng-thể-luồng-kafka)
4. [Hạ tầng Kafka — giải mã docker-compose](#4-hạ-tầng-kafka--giải-mã-docker-compose)
5. [Danh mục topic (taxonomy)](#5-danh-mục-topic-taxonomy)
6. [Bước 1 — Producer A: Debezium CDC (nguồn ERP)](#6-bước-1--producer-a-debezium-cdc-nguồn-erp)
7. [Bước 2 — Producer B: NiFi PublishKafka (Warehouse & Payment)](#7-bước-2--producer-b-nifi-publishkafka-warehouse--payment)
8. [Bước 3 — Kafka broker lưu trữ message (commit log)](#8-bước-3--kafka-broker-lưu-trữ-message-commit-log)
9. [Bước 4 — Consumer: Spark Structured Streaming](#9-bước-4--consumer-spark-structured-streaming)
10. [Delivery semantics: at-least-once + idempotency](#10-delivery-semantics-at-least-once--idempotency)
11. [Vận hành & sự cố thường gặp](#11-vận-hành--sự-cố-thường-gặp)
12. [Tóm tắt một câu](#12-tóm-tắt-một-câu)

---

## 1. Kafka là gì trong dự án này — đặt đúng vai trò

Trong DataFinch, Kafka **không phải** là một message queue kiểu RabbitMQ. Nó là một
**distributed commit log** — một cuốn "sổ cái" chỉ ghi thêm vào cuối, bất biến, và
**không xóa message khi consumer đọc**.

Vai trò của Kafka là **trục backbone bất đồng bộ** nằm giữa hai thế giới chạy ở hai
nhịp độ hoàn toàn khác nhau:

```
   3 NGUỒN dữ liệu (chạy liên tục, realtime)        TẦNG PHÂN TÍCH (chạy theo lịch)
   ───────────────────────────────────────          ──────────────────────────────
   • ERP      (Debezium CDC)                          • Spark Bronze → Silver → Gold
   • Warehouse (NiFi, batch CSV)        ──KAFKA──▶     • Airflow trigger vài lần/ngày
   • Payment  (NiFi, webhook HTTP)      (vùng đệm)
```

Nếu **bỏ Kafka** và cho nguồn ghi thẳng vào Spark, hệ thống sẽ vỡ vì:

| Vấn đề nếu không có Kafka | Kafka giải quyết bằng |
|---|---|
| Spark ngủ (chờ Airflow), nguồn vẫn sinh data → **mất** | Log bền vững, giữ lại đến khi consumer đọc |
| Nguồn nhanh, consumer chậm → **nghẽn ngược** về Postgres | Buffer tách rời tốc độ producer ↔ consumer |
| Sửa bug logic Silver → **không có cách tính lại** | **Replay**: đọc lại từ offset đầu (`earliest`) |
| Muốn thêm consumer mới → **phải sửa producer** | Fan-out: thêm consumer group mới, không đụng producer |

---

## 2. Mô hình lõi: 6 khái niệm Kafka

| Khái niệm | Định nghĩa | Trong DataFinch |
|---|---|---|
| **Broker** | Một server Kafka, lưu log và phục vụ producer/consumer | 1 broker, container `kafka`, `KAFKA_BROKER_ID=1` |
| **Topic** | Tên logic của một luồng dữ liệu ("cuốn sổ") | `erp.public.orders`, `warehouse.events`, `payment.events`… |
| **Partition** | Một topic chia nhỏ thành N log song song để scale | Mặc định (auto-create) → mỗi topic 1 partition |
| **Offset** | Số thứ tự tăng dần của message trong một partition | Spark lưu offset đã đọc vào checkpoint trên HDFS |
| **Producer** | Tiến trình ghi message vào topic | Debezium (ERP) + NiFi (Warehouse, Payment) |
| **Consumer group** | Nhóm consumer cùng đọc một topic; Kafka nhớ offset theo group | Spark Structured Streaming |

> **Lưu ý thiết kế:** thứ tự message **chỉ được đảm bảo trong cùng một partition**.
> Dự án để 1 partition/topic ⇒ thứ tự CDC luôn đúng tuyệt đối (đơn giản, an toàn cho
> đồ án) nhưng **không scale ngang**. Đây là một trade-off đáng ghi vào phần "hạn chế":
> nếu nâng lên nhiều partition, phải route các event của cùng một thực thể về cùng
> partition bằng message key.

---

## 3. Sơ đồ tổng thể luồng Kafka

```
┌────────────┐   WAL    ┌──────────────┐  erp.public.*   ┌─────────────────────────┐
│  Postgres  │ ───────▶ │  Debezium    │ ──────────────▶ │                         │
│  (ERP)     │ logical  │ (kafka-      │  (9 topic)      │                         │
└────────────┘ replicat.│  connect)    │                 │                         │
                        └──────────────┘                 │                         │
                                                          │   KAFKA BROKER          │   subscribePattern
┌────────────┐  CSV file ┌──────────────┐ warehouse.events│   (commit log,          │ ──────────────────▶ ┌───────────┐
│ Warehouse  │ ────────▶ │   NiFi       │ ───────────────▶│   ./data/kafka          │   "earliest"+ckpt   │   SPARK   │
│ simulator  │ (volume)  │  GetFile→... │ warehouse.*.dlq │   persist on disk)      │                     │  Bronze   │
└────────────┘           │  →PublishKfk │                 │                         │                     │ ingestion │
                         └──────────────┘                 │   ~13 topic nghiệp vụ   │                     └─────┬─────┘
┌────────────┐ HTTP POST ┌──────────────┐ payment.events  │   + 3 topic nội bộ      │                           │
│  Payment   │ ────────▶ │   NiFi       │ ───────────────▶│   Debezium connect      │                           ▼
│ simulator  │ :8181     │  ListenHTTP→ │ payment.*.dlq   │                         │                    HDFS /datalake/bronze
└────────────┘           │  →PublishKfk │                 │                         │                    (+ _source_topic, offset)
                         └──────────────┘                 └─────────────────────────┘
```

**Đọc sơ đồ:** có **2 cơ chế producer** (Debezium cho ERP, NiFi cho 2 nguồn còn lại)
bơm vào **~13 topic nghiệp vụ**; Kafka giữ chúng trên đĩa; **1 consumer** (Spark) đọc
ra bằng `subscribePattern` rồi đổ thô xuống HDFS Bronze.

---

## 4. Hạ tầng Kafka — giải mã docker-compose

### 4.1 Broker và Zookeeper

```yaml
kafka:
  image: confluentinc/cp-kafka:7.5.0
  environment:
    KAFKA_BROKER_ID: 1
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
  volumes:
    - ./data/kafka:/var/lib/kafka/data   # persist message qua restart
```

- **Zookeeper** (`:2181`): quản lý metadata của Kafka 7.5 (danh sách broker, topic,
  leader partition). *Phần hướng phát triển:* Kafka đời mới đã bỏ Zookeeper, thay bằng
  KRaft.
- **Persist log:** volume `./data/kafka` ⇒ restart container **không mất message**
  (đúng triết lý commit log bền vững).
- **`AUTO_CREATE_TOPICS_ENABLE=true`:** topic được tạo tự động khi có message đầu tiên.
  Nhờ vậy các topic `.dlq` chỉ ra đời khi xuất hiện dữ liệu bẩn đầu tiên.

### 4.2 Hai listener — câu hỏi vấn đáp kinh điển

> **Tại sao một broker lại quảng bá hai địa chỉ?**

Vì có hai loại client ở hai "thế giới mạng" khác nhau:

| Listener | Địa chỉ | Ai dùng | Vì sao |
|---|---|---|---|
| `PLAINTEXT` (internal) | `kafka:29092` | Debezium, NiFi, Spark, Kafka-UI — **trong mạng Docker** | Trong network `dataplatform`, tên container `kafka` mới phân giải được |
| `PLAINTEXT_HOST` | `localhost:9092` | Công cụ CLI chạy **trên máy host** | Trên host chỉ có `localhost`, không phân giải được `kafka` |

`advertised.listeners` là địa chỉ broker **tự khai báo** để client "quay lại" kết nối.
Nếu chỉ có một listener `localhost`, mọi container khác sẽ cố nối `localhost:9092` của
**chính nó** → fail. Tách hai listener là cách xử lý đúng vấn đề này.

> **Quy tắc nhớ:** mọi service trong dự án (Debezium `BOOTSTRAP_SERVERS`, NiFi
> `bootstrap.servers`, Spark `KAFKA_BOOTSTRAP`) đều dùng **`kafka:29092`**.
> Chỉ debug từ host mới dùng `localhost:9092`.

### 4.3 Kafka UI

`kafka-ui` (host `:8888`) trỏ vào `kafka:29092` — dùng để quan sát topic, offset,
message ngay trên trình duyệt: `http://localhost:8888`.

---

## 5. Danh mục topic (taxonomy)

Quy ước đặt tên mang thông tin — nhìn tên topic là biết nguồn, cơ chế, và sạch/bẩn:

```
   <nguồn> . <loại> [. dlq]
      │        │       └── nhánh dữ liệu lỗi (Dead Letter Queue)
      │        └────────── events (NiFi)  |  public.<table> (Debezium = schema.bảng)
      └─────────────────── erp | warehouse | payment
```

| Topic | Producer | Nội dung | Bronze sink |
|---|---|---|---|
| `erp.public.customers` | Debezium | CDC bảng customers | `erp_raw` |
| `erp.public.orders` | Debezium | CDC bảng orders | `erp_raw` |
| `erp.public.order_items` | Debezium | CDC bảng order_items | `erp_raw` |
| `erp.public.coupons` | Debezium | CDC bảng coupons | `erp_raw` |
| `erp.public.products` | Debezium | CDC bảng products (canonical) | `erp_raw` |
| `erp.public.categories` | Debezium | CDC bảng categories | `erp_raw` |
| `erp.public.addresses` | Debezium | CDC bảng addresses | `erp_raw` |
| `erp.public.reviews` | Debezium | CDC bảng reviews | `erp_raw` |
| `erp.public.feedback` | Debezium | CDC bảng feedback | `erp_raw` |
| `warehouse.events` | NiFi | Sự kiện kho sạch | `wh_raw` |
| `warehouse.events.dlq` | NiFi | Sự kiện kho bẩn | `wh_dlq` |
| `payment.events` | NiFi | Sự kiện thanh toán/ship sạch | `pay_raw` |
| `payment.events.dlq` | NiFi | Sự kiện thanh toán/ship bẩn | `pay_dlq` |

Ngoài ra Debezium tự tạo 3 topic nội bộ (không phải dữ liệu nghiệp vụ):
`debezium.connect.configs`, `debezium.connect.offsets`, `debezium.connect.statuses`.

---

## 6. Bước 1 — Producer A: Debezium CDC (nguồn ERP)

ERP là nguồn **duy nhất không đi qua NiFi**. Simulator chỉ ghi vào Postgres; Debezium
bắt thay đổi từ **WAL (Write-Ahead Log)** rồi tự publish vào Kafka. Đây là **CDC
log-based** — không cần sửa ứng dụng nguồn, không cần query polling.

### 6.1 Tiền đề ở Postgres

```yaml
postgres:
  command:
    - "postgres" -c "wal_level=logical"      # bắt buộc cho logical replication
    - "-c" "max_replication_slots=10"
    - "-c" "shared_preload_libraries=pgoutput"
```

`wal_level=logical` cho phép đọc WAL ở mức logic (từng dòng thay đổi), thay vì chỉ mức
vật lý dùng cho replica.

### 6.2 Cấu hình connector (`nifi/init_debezium.sh`)

Connector `erp-postgres-cdc` đăng ký qua Kafka Connect REST (`http://localhost:8083`):

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.dbname":  "ecommerce",
  "topic.prefix":     "erp",
  "table.include.list": "public.customers,public.addresses,public.categories,
                         public.orders,public.order_items,public.coupons,
                         public.products,public.reviews,public.feedback",
  "plugin.name":      "pgoutput",
  "slot.name":        "erp_debezium_slot",
  "publication.name": "erp_publication",
  "snapshot.mode":    "initial",
  "decimal.handling.mode": "string",
  "time.precision.mode":   "connect",
  "transforms":            "unwrap",
  "transforms.unwrap.type":"io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.add.fields": "op,table,source.ts_ms",
  "transforms.unwrap.delete.handling.mode": "rewrite"
}
```

**Giải mã các tham số quan trọng:**

| Tham số | Ý nghĩa |
|---|---|
| `plugin.name=pgoutput` | Dùng plugin logical replication **native** của Postgres (không cần cài thêm) |
| `slot.name` | Replication slot — Postgres ghi nhớ "Debezium đã đọc WAL đến đâu", **bền vững qua restart** |
| `topic.prefix=erp` | Mỗi bảng → topic `erp.public.<table>` |
| `snapshot.mode=initial` | Lần đầu: **snapshot** toàn bộ dòng hiện có → sau đó **stream** các thay đổi mới từ WAL |
| `transforms.unwrap` | **Quan trọng nhất** — xem 6.3 |

### 6.3 `transforms.unwrap` — biến envelope Debezium thành dòng phẳng

Mặc định Debezium gửi message dạng envelope lồng nhau `{ "before": {...}, "after":
{...}, "op": "u", "source": {...} }`. SMT `ExtractNewRecordState` **bóc tách** chỉ lấy
phần `after`, làm phẳng, và thêm metadata:

```
Trước (raw Debezium):                 Sau unwrap (cái Kafka thực sự nhận):
{                                     {
  "before": {...},                      "id": 123,
  "after": {                            "customer_id": 45,
    "id": 123,              ──────▶      "total_amount": "250.00",
    "customer_id": 45, ...              "__op": "u",          ← create/update/delete
  },                                    "__table": "orders",  ← bảng nào
  "op": "u",                            "__source_ts_ms": 1717000000000  ← thời điểm WAL
  "source": {...}
}
```

Nhờ đó Spark Silver chỉ cần parse JSON phẳng, dùng `__op` để bỏ bản ghi xóa và dùng
`__source_ts_ms` để khử trùng CDC (giữ bản mới nhất mỗi id). `decimal.handling.mode=
string` để số tiền không bị mất chính xác (gửi dạng chuỗi `"250.00"`); `time.precision.
mode=connect` để timestamp về dạng int64 milliseconds.

### 6.4 Kết quả Bước 1

Debezium publish liên tục vào 9 topic `erp.public.*`. Mỗi INSERT/UPDATE/DELETE ở
Postgres ⇒ một message mới ở Kafka, **gần như tức thời** (streaming WAL).

---

## 7. Bước 2 — Producer B: NiFi PublishKafka (Warehouse & Payment)

Warehouse và Payment đi qua NiFi. Sau khi NiFi parse + gắn cờ chất lượng + route
CLEAN/DIRTY, processor cuối cùng **`PublishKafka_2_6`** đẩy vào Kafka.

### 7.1 Cấu hình PublishKafka (`nifi/setup_flows.py`)

```python
pub_clean = create_processor(
    pg, "...PublishKafka_2_6", "PublishKafka-warehouse.events", ...,
    props={
        "bootstrap.servers": KAFKA_BOOT,        # kafka:29092
        "topic":             "warehouse.events",
        "use-transactions":  "false",           # tắt exactly-once-semantics
        "acks":              "1",               # at-least-once (1 replica ACK)
    },
    auto_term=["success", "failure"],
)
```

Mỗi nguồn có **2 processor PublishKafka**: một cho topic sạch (`*.events`), một cho
topic bẩn (`*.events.dlq`). Nhánh route quyết định message đi đâu:

```
RouteOnAttribute:
   clean  = ${quality_flag:equals('CLEAN')}                          → PublishKafka(*.events)
   dirty  = ${quality_flag:equals('DIRTY'):or(equals('QUARANTINE'))} → PublishKafka(*.events.dlq)
```

**Giải mã `acks` và `use-transactions`:**

| Tham số | Giá trị | Ý nghĩa |
|---|---|---|
| `acks=1` | broker xác nhận khi **leader** đã ghi log (không chờ replica vì chỉ 1 broker) | đủ an toàn cho local, nhanh; đánh đổi: nếu broker chết ngay sau ack mà chưa flush đĩa thì *có thể* mất (xác suất rất thấp) |
| `use-transactions=false` | tắt cơ chế exactly-once của producer | EOS cần cấu hình transactional-id phức tạp, không cần cho đồ án → chọn at-least-once |

### 7.2 Phân biệt cơ chế của hai nguồn qua NiFi

| | Warehouse | Payment |
|---|---|---|
| Đầu vào NiFi | `GetFile` đọc CSV (shared volume) | `ListenHTTP` nhận webhook POST `:8181/payment-events` |
| Chuỗi processor | GetFile→ConvertRecord→SplitJson→EvaluateJsonPath→RouteOnAttribute→PublishKafka | ListenHTTP→EvaluateJsonPath→RouteOnAttribute→PublishKafka |
| Topic ra | `warehouse.events` / `.dlq` | `payment.events` / `.dlq` |

### 7.3 Kết quả Bước 2

NiFi publish vào 4 topic (`warehouse.events`, `warehouse.events.dlq`,
`payment.events`, `payment.events.dlq`). Topic `.dlq` chỉ được auto-create khi xuất
hiện message bẩn đầu tiên.

---

## 8. Bước 3 — Kafka broker lưu trữ message (commit log)

Mọi message từ Bước 1 và Bước 2 được **append vào cuối log** của topic tương ứng, gán
một **offset** tăng dần, và ghi xuống đĩa (`./data/kafka`).

```
Topic warehouse.events (1 partition):

   offset:   0      1      2      3      4      5    ← producer ghi thêm về bên phải
            [msg]  [msg]  [msg]  [msg]  [msg]  [msg] ...→
                          ▲
                          └── con trỏ đọc của Spark (đọc xong tới đâu lưu offset tới đó)
```

Đặc tính then chốt:
- **Bất biến:** message đã ghi không sửa, không xóa khi đọc.
- **Nhiều consumer độc lập** có thể đọc cùng topic, mỗi consumer giữ offset riêng.
- **Replay:** muốn tính lại từ đầu chỉ cần reset offset về `earliest` — dữ liệu vẫn
  còn nguyên trong log (giới hạn bởi retention; mặc định 7 ngày).

---

## 9. Bước 4 — Consumer: Spark Structured Streaming

Consumer duy nhất hiện tại là job **Bronze ingestion** (`spark/jobs/bronze_ingestion.py`).

### 9.1 Đọc topic theo regex pattern

```python
KAFKA_BOOTSTRAP = "kafka:29092"

df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
      .option("subscribePattern", "erp\\.public\\..*")   # đọc theo regex
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss",  "false")
      .load())
```

- **`subscribePattern` (regex) thay vì `subscribe` (tên cố định):** Spark tự bắt được
  cả topic `.dlq` **chưa tồn tại lúc khởi động** — vì DLQ chỉ auto-create khi có dữ
  liệu bẩn. Đây là quyết định chống lỗi quan trọng.
- **`startingOffsets=earliest`:** chỉ áp dụng cho **lần chạy đầu** (chưa có checkpoint).
  Các lần sau, checkpoint quyết định offset bắt đầu.
- **`failOnDataLoss=false`:** không crash nếu một số offset cũ đã bị xóa do retention.

### 9.2 Bronze chỉ thêm lineage, không biến đổi

```python
bronze = df.select(
    F.col("value").cast("string").alias("raw_data"),       # GIỮ NGUYÊN payload thô
    F.col("topic").alias("_source_topic"),                 # + từ topic nào
    F.col("partition").alias("_kafka_partition"),
    F.col("offset").alias("_kafka_offset"),                # + offset (truy vết)
    F.col("timestamp").alias("_kafka_timestamp"),
    F.to_date(F.col("timestamp")).alias("ingest_date"),    # + cột partition theo ngày
)
```

### 9.3 Ghi xuống HDFS với checkpoint

```python
query = (bronze.writeStream.format("parquet")
         .outputMode("append")
         .trigger(availableNow=True)                       # đọc HẾT data hiện có rồi DỪNG
         .option("checkpointLocation", ckpt_path)          # lưu offset đã đọc trên HDFS
         .partitionBy("_source_topic", "ingest_date")
         .start(sink_path))
query.awaitTermination()
```

**Ba điểm cần hiểu sâu:**

1. **`Trigger.AvailableNow`:** đây **không phải** streaming chạy 24/7. Mỗi lần Airflow
   trigger, Spark đọc *tất cả message hiện có* rồi **thoát**. Bản chất là batch dùng cơ
   chế offset của streaming → phù hợp pipeline chạy theo lịch, tiết kiệm tài nguyên.

2. **Checkpoint = bộ nhớ offset:** Spark ghi vào
   `hdfs://.../_checkpoints/bronze/<sink>` rằng "đã đọc đến offset nào của partition
   nào". Lần chạy sau chỉ đọc **offset mới** ⇒ không đọc lại data cũ ⇒ **idempotent**.

3. **5 sink riêng biệt:** job gọi `ingest_topics()` 5 lần:

   ```python
   ingest_topics("erp\\.public\\..*",        "erp_raw")   # 9 topic ERP CDC
   ingest_topics("warehouse\\.events",        "wh_raw")
   ingest_topics("warehouse\\.events\\.dlq",  "wh_dlq")
   ingest_topics("payment\\.events",          "pay_raw")
   ingest_topics("payment\\.events\\.dlq",    "pay_dlq")
   ```

### 9.4 Kết quả Bước 4

Message Kafka đổ xuống HDFS Bronze dưới dạng Parquet, partition theo
`_source_topic` + `ingest_date`, kèm offset/partition/timestamp để **truy vết lineage**
ngược về tận topic gốc. Từ đây Silver và Gold xử lý tiếp (ngoài phạm vi tài liệu này).

---

## 10. Delivery semantics: at-least-once + idempotency

DataFinch chọn **at-least-once** một cách có chủ đích — ráp từ nhiều mảnh:

| Mức đảm bảo | Nghĩa | Dự án? |
|---|---|---|
| at-most-once | Có thể mất, không trùng | ✗ |
| **at-least-once** | **Không mất, có thể trùng** | ✓ |
| exactly-once | Không mất, không trùng (đắt) | ✗ |

- **Phía producer:** NiFi `acks=1`, `use-transactions=false`; Debezium dùng replication
  slot ⇒ không bỏ sót thay đổi WAL.
- **Phía consumer:** checkpoint + offset ⇒ **không bao giờ mất** message (đọc lại
  được), nhưng khi job fail giữa chừng và retry, một số message **có thể bị xử lý lại**
  ⇒ sinh **trùng lặp**.

Vì at-least-once *chấp nhận trùng*, hệ thống **bắt buộc** khử trùng ở downstream:
Silver dùng **surrogate key = MD5(source_system:source_id)** + `latest_per_id` (giữ
bản mới nhất theo `__source_ts_ms`) ⇒ **idempotent**.

> **Cặp đôi cần nhấn mạnh trong đồ án:**
> Kafka đảm bảo "không mất" (at-least-once) **+** Surrogate key/dedup đảm bảo "không
> trùng" (idempotency) **= đạt hiệu quả gần exactly-once mà không trả giá đắt của
> exactly-once thật.**

---

## 11. Vận hành & sự cố thường gặp

### 11.1 Lệnh kiểm tra nhanh

| Việc | Lệnh / địa chỉ |
|---|---|
| Xem topic + message | Kafka UI: `http://localhost:8888` |
| Trạng thái Debezium connector | `curl http://localhost:8083/connectors/erp-postgres-cdc/status` |
| Đăng ký lại connector | `bash nifi/init_debezium.sh` |
| Liệt kê topic từ host | `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list` |

### 11.2 Sự cố kinh điển: `NodeExistsException: /brokers/ids/1`

**Triệu chứng:** sau khi reboot máy/Docker, Kafka khởi động rồi tự thoát.

**Nguyên nhân:** Zookeeper persist session ephemeral trong `./data/zookeeper/`. Khi
restart, ZK nạp lại snapshot cũ — node đăng ký broker của phiên trước vẫn còn. Kafka
cố đăng ký `broker.id=1`, thấy node đã tồn tại → exit.

**Cách xử lý** (`cli/kafka-recover.sh`, idempotent, không đụng phần còn lại của stack):
1. Stop `kafka-connect`, `kafka`, `zookeeper`.
2. Xóa state cache: `data/zookeeper/data/*`, `data/zookeeper/log/*`, `data/kafka/*`.
3. Khởi động lại lần lượt ZK → Kafka → Kafka Connect.
4. Verify connector Debezium còn `RUNNING` không; nếu mất thì đăng ký lại.

### 11.3 Bảng triệu chứng → nguyên nhân

| Triệu chứng | Nguyên nhân khả dĩ |
|---|---|
| Không có topic `erp.public.*` | Debezium connector chưa `RUNNING` → chạy `init_debezium.sh` |
| Không có topic `*.events` | NiFi chưa publish (flow chưa start / nguồn chưa sinh data) |
| Spark Bronze không có dòng mới | Checkpoint đã đọc hết offset; hoặc producer chưa ghi gì mới |
| Mất hết message sau reboot | `./data/kafka` bị xóa, hoặc retention hết hạn |
| Kafka tự thoát khi khởi động | `NodeExistsException` → `cli/kafka-recover.sh` |

---

## 12. Tóm tắt một câu

> Trong DataFinch, **Kafka là trục backbone bất đồng bộ dạng commit log bền vững**:
> **Debezium** (CDC log-based, bóc envelope WAL của Postgres thành dòng phẳng kèm
> `__op`/`__table`/`__source_ts_ms`) publish vào 9 topic `erp.public.*`, còn **NiFi**
> (`PublishKafka`, `acks=1`) publish Warehouse/Payment vào 4 topic `*.events[.dlq]`;
> broker (1 partition/topic, persist trên `./data/kafka`, hai listener
> `kafka:29092` nội bộ và `localhost:9092` cho host) giữ message bất biến; **Spark
> Structured Streaming** đọc bằng `subscribePattern` + `startingOffsets=earliest` +
> checkpoint trên HDFS theo cơ chế `Trigger.AvailableNow`, đổ thô xuống Bronze kèm
> lineage offset; toàn hệ vận hành ở ngữ nghĩa **at-least-once** (Kafka chống mất) kết
> hợp **surrogate key/dedup ở Silver** (chống trùng) để tách rời hoàn toàn 3 nguồn khỏi
> tốc độ xử lý của tầng phân tích và cho phép replay lịch sử bất cứ lúc nào.
