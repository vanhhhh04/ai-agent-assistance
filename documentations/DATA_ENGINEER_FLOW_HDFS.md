# Luồng dữ liệu Kafka → HDFS trong DataFinch — giải thích từng bước

> Tài liệu này mô tả **chặng từ Kafka vào HDFS** — tức job **Bronze ingestion** của
> Spark: đọc message từ Kafka và ghi xuống tầng Bronze của data lake trên HDFS. Mọi
> đường dẫn, port, tham số đều đối chiếu trực tiếp với mã nguồn thật:
> `spark/jobs/bronze_ingestion.py`, `airflow/dags/medallion_pipeline.py`,
> `docker-compose.yml`.
>
> Tài liệu trước (`DATA_ENGINEER_FLOW_KAFKA.md`) dừng ở chỗ "Spark đọc Kafka". Tài liệu
> này **phóng to đúng mũi tên đó**: Kafka → Spark Bronze → HDFS Parquet.

---

## Mục lục

1. [HDFS là gì — đặt đúng vai trò trong dự án](#1-hdfs-là-gì--đặt-đúng-vai-trò-trong-dự-án)
2. [Kiến trúc HDFS: NameNode, DataNode, block, replication](#2-kiến-trúc-hdfs-namenode-datanode-block-replication)
3. [Sơ đồ tổng thể chặng Kafka → HDFS](#3-sơ-đồ-tổng-thể-chặng-kafka--hdfs)
4. [Ai kích hoạt job? — Airflow điều phối](#4-ai-kích-hoạt-job--airflow-điều-phối)
5. [Bước 1 — Spark khởi tạo session và kết nối Kafka](#5-bước-1--spark-khởi-tạo-session-và-kết-nối-kafka)
6. [Bước 2 — Đọc topic Kafka (readStream)](#6-bước-2--đọc-topic-kafka-readstream)
7. [Bước 3 — Chọn cột & gắn metadata lineage](#7-bước-3--chọn-cột--gắn-metadata-lineage)
8. [Bước 4 — Ghi xuống HDFS dạng Parquet có phân vùng](#8-bước-4--ghi-xuống-hdfs-dạng-parquet-có-phân-vùng)
9. [Bước 5 — Checkpoint: cơ chế ghi nhớ offset](#9-bước-5--checkpoint-cơ-chế-ghi-nhớ-offset)
10. [Bước 6 — Trigger.AvailableNow: vòng đời "đọc hết rồi dừng"](#10-bước-6--triggeravailablenow-vòng-đời-đọc-hết-rồi-dừng)
11. [Cây thư mục HDFS thực tế sau khi chạy](#11-cây-thư-mục-hdfs-thực-tế-sau-khi-chạy)
12. [Tính idempotent & khả năng replay](#12-tính-idempotent--khả-năng-replay)
13. [Vận hành & sự cố thường gặp](#13-vận-hành--sự-cố-thường-gặp)
14. [Tóm tắt một câu](#14-tóm-tắt-một-câu)

---

## 1. HDFS là gì — đặt đúng vai trò trong dự án

**HDFS (Hadoop Distributed File System)** là một hệ thống file phân tán: nó chia file
lớn thành các **block**, phân tán trên nhiều máy (DataNode), và một máy điều phối
(NameNode) giữ metadata. Trong DataFinch, HDFS là **kho lưu trữ vật lý** cho toàn bộ
data lake.

> **Tránh nhầm lẫn khái niệm:** HDFS **không có** "3 layer". "3 tầng" Bronze/Silver/Gold
> là **kiến trúc Medallion** — chỉ là cách **tổ chức thư mục logic** nằm *trên* HDFS.
> Chặng trong tài liệu này (Kafka → HDFS) chính là bước tạo ra **tầng Bronze**.

Vai trò Bronze trên HDFS:

```
Kafka (dữ liệu tạm thời, retention ~7 ngày)
   │  Spark Bronze ingestion
   ▼
HDFS /datalake/bronze  ← "bản gốc bất khả xâm phạm", lưu vĩnh viễn, để replay/audit
```

Kafka chỉ giữ message trong thời gian retention; HDFS Bronze là nơi **lưu trữ lâu dài**
bản thô để mọi tầng sau (Silver/Gold) có thể tính lại bất cứ lúc nào.

---

## 2. Kiến trúc HDFS: NameNode, DataNode, block, replication

| Thành phần | Container | Vai trò | Config trong dự án |
|---|---|---|---|
| **NameNode** | `namenode` | Giữ metadata: cây thư mục, file gồm block nào, block ở DataNode nào | `fs.defaultFS = hdfs://namenode:9000`, Web UI `:9870` |
| **DataNode** | `datanode` | Lưu **block** dữ liệu thật trên đĩa | `dfs.replication=1` |

**Cách HDFS lưu một file Parquet:**

```
File part-00000.parquet (vd 200MB)
        │  HDFS chia thành block (mặc định 128MB)
        ▼
   ┌─ block 1 (128MB) ─┐   ┌─ block 2 (72MB) ─┐
   │  lưu ở DataNode    │   │  lưu ở DataNode   │
   └────────────────────┘   └───────────────────┘
        ▲
   NameNode ghi nhớ: "part-00000.parquet = [block1@datanode, block2@datanode]"
```

**`dfs.replication=1`** (đặt ở cả `docker-compose.yml` và `SPARK_CONF` trong DAG) là
chi tiết phải biện luận:
- Production thường để **3** — mỗi block 3 bản sao trên 3 máy khác nhau để chịu lỗi đĩa.
- Dự án để **1** vì chỉ có 1 DataNode trên 1 máy đồ án → nhân bản không có lợi ích, lại
  tốn đĩa.
- **Trade-off (phần "hạn chế"):** không chịu được lỗi đĩa, không phải HA.

Mọi đường dẫn `hdfs://namenode:9000/datalake/...` trong job Spark chính là đang ghi/đọc
lên HDFS này.

---

## 3. Sơ đồ tổng thể chặng Kafka → HDFS

```
┌──────────────────────────┐
│        KAFKA              │   subscribePattern (regex)
│  ~13 topic nghiệp vụ      │ ─────────────────────────────┐
│  (commit log, có offset)  │   startingOffsets=earliest    │
└──────────────────────────┘                               ▼
                                              ┌──────────────────────────────┐
                                              │   SPARK Bronze ingestion       │
                                              │   (spark-submit, client mode)  │
                                              │                                │
                                              │   1. readStream.format(kafka)  │
                                              │   2. select(raw + lineage)     │
                                              │   3. writeStream.parquet       │
                                              │      trigger(availableNow)     │
                                              └───────────┬────────────────────┘
                                       ┌──────────────────┼─────────────────────┐
                                       ▼                  ▼                     ▼
                          HDFS /datalake/bronze    HDFS /datalake/_checkpoints  (đọc hết → job EXIT)
                          (Parquet, partitioned)    /bronze/<sink> (lưu offset)
```

**Tư tưởng:** Spark là **cây cầu một chiều** kéo message từ Kafka (lưu tạm) sang HDFS
(lưu lâu dài), trong khi **checkpoint** ghi nhớ "đã kéo đến đâu" để lần sau không kéo
lại.

---

## 4. Ai kích hoạt job? — Airflow điều phối

Job Bronze **không tự chạy**. Nó được **Airflow** kích hoạt theo lịch
(`airflow/dags/medallion_pipeline.py`):

```python
schedule_interval="*/15 * * * *"   # mỗi 15 phút
max_active_runs=1                  # không bao giờ chạy chồng (one run at a time)

bronze = SparkSubmitOperator(
    task_id="bronze_ingestion",
    application=f"{SPARK_JOBS}/bronze_ingestion.py",
    conn_id="spark_default",                          # submit tới spark://spark-master:7077
    conf=SPARK_CONF,
    packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",  # connector Kafka
)

bronze >> silver >> gold   # Bronze xong mới tới Silver, rồi Gold
```

Các điểm cần biết:
- **`packages=spark-sql-kafka-0-10_2.12:3.5.1`:** connector Kafka cho Spark **không có
  sẵn**, được tải từ Maven Central lúc submit. Phiên bản phải khớp Spark 3.5.x + Scala
  2.12.
- **`SparkSubmitOperator` (client mode):** file ứng dụng chạy trên container Airflow
  scheduler, executor chạy trên Spark worker.
- **Tần suất 15 phút + `Trigger.AvailableNow`:** mỗi lần chạy hút hết offset mới rồi
  thoát → độ tươi dữ liệu end-to-end ≈ **15–25 phút**.
- **`max_active_runs=1`:** chống hai lần chạy đè nhau gây tranh chấp checkpoint.

---

## 5. Bước 1 — Spark khởi tạo session và kết nối Kafka

```python
KAFKA_BOOTSTRAP = "kafka:29092"
HDFS_BASE       = "hdfs://namenode:9000/datalake/bronze"
CHECKPOINT_BASE = "hdfs://namenode:9000/datalake/_checkpoints/bronze"

spark = (
    SparkSession.builder
    .appName("Bronze-Ingestion")
    .config("spark.sql.streaming.schemaInference",        "true")
    .config("spark.sql.streaming.stopActiveRunOnRestart", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

- **`KAFKA_BOOTSTRAP=kafka:29092`** — dùng listener **nội bộ** (vì Spark chạy trong
  mạng Docker `dataplatform`).
- **`HDFS_BASE`** trỏ tới NameNode `:9000`, thư mục gốc tầng Bronze.
- **`stopActiveRunOnRestart=true`** — nếu có một streaming query cũ còn sống, dừng nó
  khi job khởi động lại (tránh hai query tranh nhau cùng checkpoint).

---

## 6. Bước 2 — Đọc topic Kafka (readStream)

Job định nghĩa một hàm `ingest_topics(topic_pattern, sink_name)` và gọi nó **5 lần**
cho 5 sink. Phần đọc:

```python
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribePattern",        topic_pattern)   # vd "erp\\.public\\..*"
    .option("startingOffsets",         "earliest")
    .option("failOnDataLoss",          "false")
    .load()
)
```

**Giải mã từng option:**

| Option | Ý nghĩa | Vì sao quan trọng |
|---|---|---|
| `subscribePattern` (regex) | Đăng ký topic theo biểu thức chính quy, **không** liệt kê tên cứng | Tự bắt được topic `.dlq` **chưa tồn tại** lúc khởi động (DLQ chỉ auto-create khi có dữ liệu bẩn) |
| `startingOffsets=earliest` | Lần chạy **đầu tiên** (chưa có checkpoint) đọc từ offset 0 | Đảm bảo không bỏ sót message cũ ở lần khởi tạo |
| `failOnDataLoss=false` | Không crash nếu một số offset cũ đã bị xóa do retention | Bền vững khi Kafka đã dọn log cũ |

DataFrame `df` trả về có schema cố định của Kafka source: `key`, `value`, `topic`,
`partition`, `offset`, `timestamp`, `timestampType`. **`value`** là payload thật (dạng
nhị phân).

Năm lần gọi:

```python
ingest_topics("erp\\.public\\..*",        "erp_raw")   # 9 topic ERP CDC → 1 sink
ingest_topics("warehouse\\.events",        "wh_raw")
ingest_topics("warehouse\\.events\\.dlq",  "wh_dlq")
ingest_topics("payment\\.events",          "pay_raw")
ingest_topics("payment\\.events\\.dlq",    "pay_dlq")
```

---

## 7. Bước 3 — Chọn cột & gắn metadata lineage

**Nguyên tắc vàng của Bronze:** *giữ nguyên payload thô, chỉ thêm metadata truy vết —
KHÔNG parse, KHÔNG cast, KHÔNG lọc, KHÔNG biến đổi nghiệp vụ.*

```python
bronze = df.select(
    F.col("value").cast("string").alias("raw_data"),       # GIỮ NGUYÊN payload (JSON thô)
    F.col("key").cast("string").alias("kafka_key"),
    F.col("topic").alias("_source_topic"),                 # + từ topic nào
    F.col("partition").alias("_kafka_partition"),          # + partition nào
    F.col("offset").alias("_kafka_offset"),                # + offset nào (truy vết duy nhất)
    F.col("timestamp").alias("_kafka_timestamp"),          # + thời điểm Kafka nhận
    F.current_timestamp().alias("_bronze_ingested_at"),    # + thời điểm Spark ghi Bronze
    F.to_date(F.col("timestamp")).alias("ingest_date"),    # + cột để phân vùng theo ngày
)
```

Vì sao gắn lineage là quyết định thiết kế quan trọng:
- **`_source_topic`** — sau này một dòng ở Gold có thể truy ngược về tận topic Kafka
  gốc (full lineage).
- **`_kafka_offset` + `_kafka_partition`** — định danh duy nhất một message trong Kafka,
  phục vụ debug và đối soát.
- **`ingest_date`** — dùng để phân vùng (xem Bước 4) và phục vụ retention/vacuum ở
  Silver.

> `raw_data` vẫn là **chuỗi JSON nguyên bản** từ Debezium/NiFi. Việc parse JSON, cast
> kiểu, chuẩn hóa, khử trùng… tất cả để dành cho tầng **Silver** — không làm ở Bronze.

---

## 8. Bước 4 — Ghi xuống HDFS dạng Parquet có phân vùng

```python
query = (
    bronze.writeStream
    .format("parquet")                              # định dạng cột, nén tốt
    .outputMode("append")                           # chỉ THÊM, không sửa (bất biến)
    .trigger(availableNow=True)                     # đọc hết rồi dừng (Bước 6)
    .option("checkpointLocation", ckpt_path)        # offset (Bước 5)
    .partitionBy("_source_topic", "ingest_date")    # phân vùng 2 cấp
    .start(sink_path)                               # vd hdfs://.../datalake/bronze/erp_raw
)
query.awaitTermination()
```

**Tại sao Parquet?**
- Định dạng **cột (columnar)** — nén tốt, đọc nhanh khi chỉ cần vài cột.
- Có schema nhúng — Silver đọc lại không cần khai báo lại kiểu.

**Tại sao `append`?** Bronze là bản gốc bất biến — chỉ ghi thêm message mới, không bao
giờ sửa/xóa dữ liệu đã ghi.

**`partitionBy("_source_topic", "ingest_date")`** tạo cây thư mục 2 cấp trên HDFS:

```
/datalake/bronze/erp_raw/
   _source_topic=erp.public.orders/
       ingest_date=2026-05-30/
           part-00000-....parquet
           part-00001-....parquet
   _source_topic=erp.public.customers/
       ingest_date=2026-05-30/
           part-00000-....parquet
```

Lợi ích của phân vùng — **partition pruning**: khi Silver chỉ cần đọc orders của một
ngày, Spark **chỉ quét đúng thư mục đó**, không đọc toàn bộ Bronze → nhanh hơn nhiều.

> `spark.sql.shuffle.partitions=8` (trong `SPARK_CONF`) giới hạn số file output cho phù
> hợp cụm Docker nhỏ, tránh tạo quá nhiều file vụn.

---

## 9. Bước 5 — Checkpoint: cơ chế ghi nhớ offset

Đây là trái tim của tính **incremental** và **idempotent**.

```python
.option("checkpointLocation", "hdfs://.../datalake/_checkpoints/bronze/erp_raw")
```

Checkpoint là một thư mục trên HDFS, Spark Structured Streaming ghi vào đó:
- **offsets/** — "đã đọc đến offset nào của partition nào, của topic nào".
- **commits/** — micro-batch nào đã ghi xong thành công.
- **metadata** — id của streaming query.

```
Lần chạy 1:  checkpoint trống → đọc từ earliest (offset 0..120) → ghi Bronze
             → checkpoint lưu "next offset = 121"
Lần chạy 2:  đọc checkpoint → bắt đầu từ offset 121 → đọc 121..150
             → checkpoint lưu "next offset = 151"
```

**Hệ quả then chốt:**
- Mỗi lần chạy **chỉ đọc message mới** kể từ offset đã lưu ⇒ **incremental**.
- Chạy lại job **không đọc lại** data cũ ⇒ **idempotent** (không nhân đôi Bronze).
- `startingOffsets=earliest` chỉ có tác dụng ở **lần đầu** (khi checkpoint chưa tồn
  tại); sau đó checkpoint luôn thắng.

Mỗi sink có một checkpoint **riêng** (`/_checkpoints/bronze/erp_raw`,
`/_checkpoints/bronze/wh_raw`, …) ⇒ 5 luồng đọc độc lập, offset không lẫn nhau.

---

## 10. Bước 6 — Trigger.AvailableNow: vòng đời "đọc hết rồi dừng"

```python
.trigger(availableNow=True)
...
query.awaitTermination()
```

Đây **không phải** streaming chạy 24/7. `Trigger.AvailableNow`:
1. Khi job bắt đầu, Spark hỏi Kafka "hiện có bao nhiêu offset mới kể từ checkpoint?".
2. Đọc **toàn bộ** lượng đó dưới dạng một (hoặc vài) micro-batch.
3. Ghi hết xuống HDFS, cập nhật checkpoint.
4. **Thoát** (job kết thúc, trả control về Airflow).

```
   Streaming liên tục (KHÔNG dùng):        Trigger.AvailableNow (dự án dùng):
   ─────────────────────────────           ──────────────────────────────────
   chạy mãi, micro-batch mỗi X giây         Airflow gọi → đọc hết offset mới → DỪNG
   tốn 1 process Spark thường trực          không có process treo; điều phối được như batch
```

Vì sao chọn cách này:
- Phù hợp pipeline **theo lịch** (Airflow 15 phút/lần) — không cần Spark chạy thường
  trực.
- Vẫn hưởng cơ chế **offset + checkpoint** của streaming (chính xác, incremental).
- Bản chất: **"batch dùng engine streaming"** — đọc đúng phần mới, đảm bảo không bỏ
  sót, không trùng.

---

## 11. Cây thư mục HDFS thực tế sau khi chạy

```
hdfs://namenode:9000/datalake/
│
├── bronze/                                   ← TẦNG BRONZE (chặng này tạo ra)
│   ├── erp_raw/                              (9 topic erp.public.* gộp vào đây)
│   │   ├── _source_topic=erp.public.orders/
│   │   │   └── ingest_date=2026-05-30/part-*.parquet
│   │   ├── _source_topic=erp.public.customers/
│   │   │   └── ingest_date=2026-05-30/part-*.parquet
│   │   └── ... (order_items, products, categories, addresses, coupons, reviews, feedback)
│   ├── wh_raw/                               (warehouse.events)
│   │   └── _source_topic=warehouse.events/ingest_date=.../part-*.parquet
│   ├── wh_dlq/                               (warehouse.events.dlq — dữ liệu bẩn kho)
│   ├── pay_raw/                              (payment.events)
│   └── pay_dlq/                              (payment.events.dlq — dữ liệu bẩn thanh toán)
│
├── _checkpoints/
│   └── bronze/
│       ├── erp_raw/   {offsets, commits, metadata}
│       ├── wh_raw/    {offsets, commits, metadata}
│       ├── wh_dlq/    ...
│       ├── pay_raw/   ...
│       └── pay_dlq/   ...
│
├── silver/                                   ← tầng Silver (job khác, ngoài phạm vi)
└── gold/                                     ← tầng Gold  (job khác, ngoài phạm vi)
```

**Quan sát qua HDFS Web UI:** `http://localhost:9870` → Utilities → Browse the file
system → `/datalake/bronze`.

---

## 12. Tính idempotent & khả năng replay

Hai tính chất "vàng" mà chặng Kafka → HDFS đạt được:

**Idempotent (chạy lại không nhân đôi):**
- Checkpoint ghi nhớ offset ⇒ chạy job 10 lần liên tiếp khi không có message mới ⇒
  Bronze **không tăng thêm dòng nào**.
- `max_active_runs=1` ở Airflow chống hai lần chạy đè checkpoint.

**Replay (tính lại từ đầu):**
- Muốn nạp lại toàn bộ lịch sử: **xóa thư mục checkpoint** của sink tương ứng → lần
  chạy sau đọc lại từ `earliest`.
- Điều kiện: message vẫn còn trong Kafka (chưa hết retention). Nếu Kafka đã dọn, chỉ
  replay được phần còn trong log — đây là lý do Bronze trên HDFS đóng vai "bản lưu trữ
  lâu dài" bù cho retention ngắn của Kafka.

> **Lưu ý quan trọng khi replay:** nếu xóa checkpoint mà **không** xóa dữ liệu Bronze
> cũ, dữ liệu cũ + đọc lại sẽ tạo **trùng lặp** ở Bronze. Vì Silver chạy `overwrite` và
> khử trùng theo surrogate key/`latest_per_id`, trùng ở Bronze **không** lan xuống
> Silver/Gold — nhưng để sạch, nên xóa cả `bronze/<sink>` lẫn `_checkpoints/bronze/
> <sink>` khi muốn replay hoàn toàn.

---

## 13. Vận hành & sự cố thường gặp

### 13.1 Lệnh kiểm tra

| Việc | Lệnh / địa chỉ |
|---|---|
| Duyệt file Bronze | HDFS UI: `http://localhost:9870` → Browse `/datalake/bronze` |
| Liệt kê file qua CLI | `docker exec namenode hdfs dfs -ls -R /datalake/bronze` |
| Đếm dòng một sink | đọc bằng Spark/`parquet-tools`, hoặc kiểm qua Silver log |
| Theo dõi job Spark | Spark UI: `http://localhost:8090` |
| Trigger thủ công | Airflow UI `http://localhost:8080` → DAG `medallion_pipeline` → Trigger |

### 13.2 Bảng triệu chứng → nguyên nhân

| Triệu chứng | Nguyên nhân khả dĩ | Cách xử lý |
|---|---|---|
| Bronze trống sau khi chạy | Producer chưa ghi gì mới; hoặc checkpoint đã hút hết offset | Kiểm Kafka UI `:8888` xem topic có message không |
| `ClassNotFound` kafka source | Thiếu `packages=spark-sql-kafka-...` lúc submit | Kiểm `KAFKA_PKG` trong DAG khớp Spark 3.5.1 |
| Job báo lỗi `Path does not exist` ở Silver | Bronze chưa từng chạy (sink chưa tạo) | Chạy Bronze trước; Silver có `safe_read_parquet` chịu lỗi này |
| Bronze bị trùng dữ liệu | Đã xóa checkpoint nhưng không xóa data cũ rồi replay | Xóa cả `bronze/<sink>` lẫn `_checkpoints/bronze/<sink>` |
| `Connection refused` tới namenode:9000 | NameNode chưa sẵn sàng | Chờ healthcheck namenode; kiểm `docker logs namenode` |
| File Parquet vụn (quá nhiều) | shuffle partitions cao | đã giới hạn `spark.sql.shuffle.partitions=8` |

---

## 14. Tóm tắt một câu

> Chặng Kafka → HDFS trong DataFinch là job **Spark Bronze ingestion** do **Airflow**
> kích hoạt mỗi 15 phút: Spark `readStream` từ Kafka (`kafka:29092`) theo
> `subscribePattern` với `startingOffsets=earliest`, **chỉ thêm metadata lineage**
> (`_source_topic`, `_kafka_offset`, `ingest_date`) vào payload thô mà không biến đổi
> nghiệp vụ, rồi `writeStream` xuống **HDFS** (`hdfs://namenode:9000/datalake/bronze`,
> `dfs.replication=1`) dưới dạng **Parquet phân vùng theo `_source_topic` + ingest_date`**;
> cơ chế **`Trigger.AvailableNow`** khiến job "đọc hết offset mới rồi dừng" (batch dùng
> engine streaming), còn **checkpoint trên HDFS** (`/datalake/_checkpoints/bronze/<sink>`)
> ghi nhớ offset đã đọc để các lần chạy sau là **incremental + idempotent** và cho phép
> **replay** toàn bộ lịch sử khi cần — biến tầng Bronze thành bản gốc bất biến, lưu lâu
> dài, bù cho retention ngắn của Kafka.
