# Luồng dữ liệu Kafka → Hadoop trong DataFinch — giải thích từng bước

> Tài liệu này nhìn ở **góc độ nền tảng Hadoop**: dữ liệu từ Kafka đi *vào và sống
> trong* hệ sinh thái Hadoop như thế nào — không chỉ "ghi file xuống HDFS" mà còn cả
> **tầng tính toán** (Spark, MapReduce) chạy trên/cùng Hadoop. Mọi tham số đối chiếu
> trực tiếp với mã nguồn: `docker-compose.yml`, `spark/jobs/bronze_ingestion.py`,
> `ai-agent/core/hive_client.py`, `hive/init.sh`, `airflow/dags/medallion_pipeline.py`.

---

## ⚠️ Đọc trước: "Hadoop" KHÁC "HDFS" như thế nào?

Đây là điểm hội đồng dễ bắt lỗi nhất, nên phải làm rõ ngay:

```
                    HADOOP (cả một NỀN TẢNG / ecosystem)
   ┌───────────────────────────────────────────────────────────────────┐
   │                                                                     │
   │   HDFS              YARN              MapReduce        Hadoop Common │
   │   (lưu trữ)         (quản lý tài      (engine tính     (thư viện     │
   │                      nguyên)           toán cũ)         dùng chung)  │
   │                                                                     │
   └───────────────────────────────────────────────────────────────────┘
        ▲                   ▲                   ▲
        │                   │                   │
   ĐƯỢC triển khai     KHÔNG triển khai    CÓ — nhưng chạy LOCAL,
   (namenode +         (không có             chỉ dùng cho Hive query
    datanode)           resourcemanager)     ("return code 2 MapRedTask")
```

- **HDFS** = chỉ phần **lưu trữ**. File `DATA_ENGINEER_FLOW_HDFS.md` tập trung vào
  *logic của job Spark Bronze* khi ghi file xuống HDFS.
- **Hadoop** = **cả nền tảng** (lưu trữ + tính toán + quản lý tài nguyên). Tài liệu
  này tập trung vào: **bytes thực sự đi vào Hadoop bằng giao thức nào**, và **những
  engine tính toán nào chạy trên Hadoop** để xử lý dữ liệu đó.

> Một câu cho đồ án: *"HDFS là cái ổ cứng phân tán; Hadoop là cả cỗ máy gồm ổ cứng đó
> cộng với bộ máy tính toán chạy trên nó."*

---

## Mục lục

1. [Bốn trụ cột Hadoop — cái nào dùng, cái nào không trong DataFinch](#1-bốn-trụ-cột-hadoop--cái-nào-dùng-cái-nào-không-trong-datafinch)
2. [Sơ đồ tổng thể: Kafka đi vào nền tảng Hadoop](#2-sơ-đồ-tổng-thể-kafka-đi-vào-nền-tảng-hadoop)
3. [Bước 1 — Spark (compute) kéo dữ liệu từ Kafka](#3-bước-1--spark-compute-kéo-dữ-liệu-từ-kafka)
4. [Bước 2 — Spark trở thành HDFS client, gọi NameNode](#4-bước-2--spark-trở-thành-hdfs-client-gọi-namenode)
5. [Bước 3 — Ghi block xuống DataNode (write pipeline)](#5-bước-3--ghi-block-xuống-datanode-write-pipeline)
6. [Bước 4 — NameNode ghi nhận metadata (fsimage + edits)](#6-bước-4--namenode-ghi-nhận-metadata-fsimage--edits)
7. [Bước 5 — Dữ liệu sống trong Hadoop: block, replication](#7-bước-5--dữ-liệu-sống-trong-hadoop-block-replication)
8. [Bước 6 — Hadoop tiếp tục tính toán: Silver, Gold trên cùng cụm](#8-bước-6--hadoop-tiếp-tục-tính-toán-silver-gold-trên-cùng-cụm)
9. [Bước 7 — MapReduce (local) phục vụ truy vấn Hive](#9-bước-7--mapreduce-local-phục-vụ-truy-vấn-hive)
10. [Hai engine tính toán trên Hadoop: Spark vs MapReduce](#10-hai-engine-tính-toán-trên-hadoop-spark-vs-mapreduce)
11. [Vận hành & sự cố thường gặp](#11-vận-hành--sự-cố-thường-gặp)
12. [Tóm tắt một câu](#12-tóm-tắt-một-câu)

---

## 1. Bốn trụ cột Hadoop — cái nào dùng, cái nào không trong DataFinch

| Trụ cột | Vai trò | Trong DataFinch? | Bằng chứng |
|---|---|---|---|
| **HDFS** | Lưu trữ phân tán | ✅ **Có** — `namenode` + `datanode` | `docker-compose.yml` |
| **YARN** | Quản lý tài nguyên cụm, lập lịch container | ❌ **Không** — không có `resourcemanager`/`nodemanager` | Không có service trong compose |
| **MapReduce** | Engine tính toán (map → shuffle → reduce) | ⚠️ **Có nhưng chạy LOCAL** — chỉ cho Hive query | `hive_client.py:158`: *"Hive on MapReduce in local mode"* |
| **Hadoop Common** | Thư viện/giao thức dùng chung (HDFS client, RPC) | ✅ **Có** — Spark dùng để nói chuyện với HDFS | `spark.hadoop.fs.defaultFS` trong DAG |

**Hệ quả thiết kế quan trọng (đáng viết vào đồ án):**

- **Spark KHÔNG chạy trên YARN.** Dự án dùng **Spark Standalone cluster**
  (`spark-master:7077` + `spark-worker`). Spark tự quản lý tài nguyên, chỉ *mượn* HDFS
  của Hadoop làm nơi lưu trữ thông qua thư viện Hadoop Common.
- **Hive chạy MapReduce ở chế độ LOCAL** — không cần YARN. MapReduce job chạy ngay
  trong JVM của HiveServer2. Đây là lý do khi nhiều truy vấn AI Agent chạy đồng thời,
  một query có thể fail với *"return code 2 from MapRedTask"* (xem Bước 7).

→ Trong dự án, **"Hadoop" thực chất = HDFS (lưu trữ) + MapReduce-local (engine cho
Hive)**. YARN bị lược bỏ để gọn nhẹ cho môi trường đồ án trên một máy.

---

## 2. Sơ đồ tổng thể: Kafka đi vào nền tảng Hadoop

```
┌──────────┐                  ┌───────────────────────────────────────────────────────┐
│  KAFKA   │                  │                  NỀN TẢNG HADOOP                        │
│ (ngoài   │                  │                                                         │
│  Hadoop) │   Spark đọc      │   ┌─────────────┐  RPC :9000   ┌──────────────────┐    │
│          │ ───────────────▶ │   │  Spark       │ ───────────▶ │  NameNode        │    │
└──────────┘   (compute       │   │  (standalone │  "xin ghi    │  (metadata:      │    │
               standalone)    │   │   cluster)   │   file X"    │   file→block→DN) │    │
                              │   └──────┬───────┘              └────────┬─────────┘    │
                              │          │ ghi block trực tiếp           │ chỉ định DN  │
                              │          ▼                               ▼              │
                              │   ┌──────────────────────────────────────────────┐     │
                              │   │  DataNode  (lưu block Parquet trên đĩa)        │     │
                              │   │  /datalake/bronze/.../part-*.parquet           │     │
                              │   └──────────────────────────────────────────────┘     │
                              │          │ (Silver/Gold đọc lại → tính tiếp)            │
                              │          ▼                                              │
                              │   ┌──────────────┐  MapReduce-local  ┌─────────────┐    │
                              │   │ HiveServer2  │ ────────────────▶ │ kết quả SQL │    │
                              │   │ (đọc Gold)   │  (map→reduce       │ → AI Agent  │    │
                              │   └──────────────┘   trong JVM)       └─────────────┘    │
                              └───────────────────────────────────────────────────────┘
```

**Tư tưởng:** Kafka nằm **ngoài** Hadoop. Spark đóng vai **cây cầu**: nó là một
**compute engine** đọc Kafka rồi trở thành **HDFS client** ghi vào Hadoop. Sau khi dữ
liệu nằm trong HDFS, Hadoop tiếp tục là **nơi lưu + nơi tính** cho cả Silver, Gold và
truy vấn Hive.

---

## 3. Bước 1 — Spark (compute) kéo dữ liệu từ Kafka

Spark là tiến trình tính toán đầu tiên chạm vào dữ liệu. Job Bronze submit lên Spark
Standalone master (`airflow/dags/medallion_pipeline.py`):

```python
SPARK_MASTER = "spark://spark-master:7077"     # KHÔNG phải yarn://
SPARK_CONF = {
    "spark.hadoop.fs.defaultFS":        "hdfs://namenode:9000",  # ← cấu hình HDFS client
    "spark.hadoop.dfs.replication":     "1",
    "spark.driver.memory":              "1g",
    "spark.executor.memory":            "1g",
}
```

- `spark://spark-master:7077` xác nhận Spark dùng **standalone resource manager** của
  riêng nó, **không** qua YARN.
- `spark.hadoop.fs.defaultFS` chính là cấu hình biến Spark thành một **HDFS client** —
  nó biết NameNode ở `hdfs://namenode:9000`.

Spark đọc Kafka (`kafka:29092`) bằng `readStream` (chi tiết logic đã ở
`DATA_ENGINEER_FLOW_HDFS.md`). Kết quả: một DataFrame chứa payload thô + metadata
lineage, **đang nằm trong bộ nhớ của Spark executor**, chưa vào Hadoop.

---

## 4. Bước 2 — Spark trở thành HDFS client, gọi NameNode

Khi Spark thực thi `writeStream...start("hdfs://namenode:9000/datalake/bronze/erp_raw")`,
thư viện **Hadoop Common** trong Spark mở một kết nối **RPC tới NameNode** (port 9000):

```
Spark executor                              NameNode (:9000)
─────────────                              ────────────────
"Tôi muốn tạo file
 /datalake/bronze/erp_raw/
 .../part-00000.parquet"      ─────RPC────▶ kiểm tra quyền, namespace
                                            cấp phát block ID mới
                              ◀────────────  "ghi block blk_123 vào DataNode D"
```

**Vai trò NameNode:** nó **không nhận dữ liệu**, chỉ quản lý **metadata** —
quyết định file gồm block nào, mỗi block đặt ở DataNode nào. Đây là kiến trúc
**master/worker** kinh điển của HDFS: một master nhẹ (metadata) + nhiều worker nặng
(dữ liệu).

---

## 5. Bước 3 — Ghi block xuống DataNode (write pipeline)

Sau khi NameNode chỉ định DataNode, Spark client **ghi dữ liệu trực tiếp** tới DataNode
(không qua NameNode):

```
Spark client ──[block data]──▶ DataNode D ──(nếu replication>1: forward sang DN khác)──▶ ...
                                   │
                                   ▼
                          ghi xuống đĩa thật:
                          ./hdfs/datanode/.../blk_123
```

- Dữ liệu được chia thành **block** (mặc định 128MB). File Parquet nhỏ hơn 128MB → 1
  block.
- Với `dfs.replication=1`, **không có pipeline nhân bản** — block chỉ ghi vào 1
  DataNode (dự án chỉ có 1 DataNode). Nếu replication=3, DataNode đầu sẽ forward block
  sang 2 DataNode khác theo dây chuyền (replication pipeline).
- Trên host, block thật nằm ở bind-mount `./hdfs/datanode` (khai báo trong
  `docker-compose.yml`).

---

## 6. Bước 4 — NameNode ghi nhận metadata (fsimage + edits)

Sau khi block ghi xong, DataNode báo cáo lại NameNode, và NameNode cập nhật metadata:

```
NameNode lưu metadata bằng 2 cơ chế:
  • fsimage  — ảnh chụp toàn bộ cây thư mục tại một thời điểm (trên đĩa ./hdfs/namenode)
  • edits    — nhật ký các thay đổi kể từ fsimage gần nhất (append liên tục)

Khi khởi động: NameNode nạp fsimage + replay edits → dựng lại toàn bộ namespace trong RAM.
```

Đây là lý do NameNode là **single point of failure** trong HDFS cơ bản: nếu mất
metadata thì dù block còn trên DataNode cũng không biết file nào gồm block nào.
*(Phần "hạn chế": dự án 1 NameNode, không có Standby NameNode/HA.)*

Đến đây, dữ liệu từ Kafka đã **chính thức nằm trong Hadoop**: bytes ở DataNode, bản đồ
ở NameNode.

---

## 7. Bước 5 — Dữ liệu sống trong Hadoop: block, replication

Sau khi vào Hadoop, một file Bronze tồn tại như sau:

```
File logic:   /datalake/bronze/erp_raw/_source_topic=erp.public.orders/ingest_date=2026-05-30/part-00000.parquet
                    │
   NameNode ánh xạ │
                    ▼
Block vật lý: blk_1073741825  ──lưu ở──▶ DataNode (1 bản, vì replication=1)
```

| Khái niệm | Trong dự án |
|---|---|
| Block size | mặc định 128MB |
| Replication | **1** (đồ án) — production thường 3 |
| Vị trí vật lý | `./hdfs/datanode` trên host (qua volume) |
| Web UI quan sát | `http://localhost:9870` → Browse the file system |

Vì replication=1, **không chịu được lỗi đĩa**: mất DataNode = mất dữ liệu. Đây là đánh
đổi có chủ ý cho môi trường 1 máy.

---

## 8. Bước 6 — Hadoop tiếp tục tính toán: Silver, Gold trên cùng cụm

Điểm mấu chốt của góc nhìn "Hadoop" (mà góc nhìn "HDFS" không nêu): sau khi dữ liệu
nằm trong HDFS, **Hadoop tiếp tục vừa là kho vừa là sân chơi tính toán** cho các tầng
sau:

```
HDFS /datalake/bronze   ──Spark đọc──▶  xử lý (clean/dedup)  ──Spark ghi──▶  HDFS /datalake/silver
HDFS /datalake/silver   ──Spark đọc──▶  dựng star schema     ──Spark ghi──▶  HDFS /datalake/gold
```

- Cùng một cụm Spark Standalone (`spark-master`/`spark-worker`) thực thi cả Bronze,
  Silver, Gold — điều phối tuần tự bởi Airflow (`bronze >> silver >> gold`).
- Tất cả input/output đều là HDFS (`hdfs://namenode:9000/datalake/...`).
- Spark đọc/ghi HDFS qua **data locality** lý tưởng: executor nên chạy gần block (cùng
  máy DataNode). Trên cụm 1 máy đồ án, mọi thứ cùng host nên locality luôn đạt.

→ Dữ liệu Kafka, sau khi "vào Hadoop", **không rời Hadoop** cho đến tận khi AI Agent
query — nó di chuyển Bronze → Silver → Gold **bên trong HDFS**, được tính toán bởi
Spark.

---

## 9. Bước 7 — MapReduce (local) phục vụ truy vấn Hive

Đây là chỗ trụ cột **MapReduce** của Hadoop xuất hiện. Khi AI Agent gửi SQL tới
HiveServer2 (`:10000`) để đọc bảng Gold:

```
AI Agent ──SQL──▶ HiveServer2 ──dịch SQL thành──▶ MapReduce job (chạy LOCAL trong JVM)
                                                        │ map: đọc Parquet trên HDFS
                                                        │ shuffle
                                                        │ reduce: tổng hợp
                                                        ▼
                                                   kết quả ──▶ AI Agent
```

Bằng chứng từ code (`ai-agent/core/hive_client.py`):

```python
# Hive on MapReduce in *local* mode (the default in this dev stack) shares a
# [JVM]; when multiple queries run at the same time, one of them can fail with
# "return code 2 from MapRedTask" ...
_TRANSIENT_MARKERS = [
    "return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask",
    ...
]
```

Vì MapReduce chạy **local** (không có YARN để cô lập tài nguyên), nhiều job MR đồng
thời dùng chung JVM → có thể đụng nhau và fail tạm thời. `hive_client.py` xử lý bằng
**retry với backoff** cho đúng loại lỗi transient này — một chi tiết kỹ thuật hay để
đưa vào đồ án (cho thấy hiểu hạn chế của Hive-on-MR-local và cách khắc phục).

> **Phần hướng phát triển:** thay MapReduce bằng **Hive-on-Tez** hoặc **Hive-on-Spark**,
> hoặc triển khai YARN để cô lập tài nguyên → loại bỏ lỗi tranh chấp JVM.

---

## 10. Hai engine tính toán trên Hadoop: Spark vs MapReduce

Bảng so sánh để làm rõ "Hadoop có hai engine tính toán" trong dự án:

| | **Spark (Standalone)** | **MapReduce (local)** |
|---|---|---|
| Dùng cho | ETL: Bronze, Silver, Gold | Thực thi truy vấn SQL của Hive |
| Quản lý tài nguyên | Spark master riêng (`:7077`) | Không (chạy trong JVM HiveServer2) |
| Quan hệ với YARN | Không dùng | Không dùng |
| Quan hệ với HDFS | Đọc/ghi `/datalake/*` | Đọc `/datalake/gold` |
| Mô hình tính | DAG in-memory (nhanh) | map → shuffle → reduce (chậm hơn, ghi đĩa nhiều) |
| Xuất hiện khi | Airflow trigger ETL | AI Agent gọi SQL qua HiveServer2 |

Điểm cần nhấn: **Spark không thay thế hoàn toàn MapReduce trong dự án** — ETL dùng
Spark, nhưng truy vấn Hive vẫn rơi xuống MapReduce-local. Đây là đặc thù của image
`bde2020/hive:2.3.2` (mặc định `hive.execution.engine=mr`).

---

## 11. Vận hành & sự cố thường gặp

### 11.1 Lệnh kiểm tra Hadoop

| Việc | Lệnh / địa chỉ |
|---|---|
| HDFS Web UI (NameNode) | `http://localhost:9870` |
| Duyệt cây thư mục | `docker exec namenode hdfs dfs -ls -R /datalake` |
| Dung lượng HDFS | `docker exec namenode hdfs dfs -du -h /datalake` |
| Báo cáo DataNode/block | `docker exec namenode hdfs dfsadmin -report` |
| Kiểm tra file lỗi/block | `docker exec namenode hdfs fsck /datalake -files -blocks` |
| Spark cluster UI | `http://localhost:8090` |
| HiveServer2 UI | `http://localhost:10002` |

### 11.2 Bảng triệu chứng → nguyên nhân

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `Connection refused` tới `namenode:9000` | NameNode chưa sẵn sàng / chưa khởi động | Chờ healthcheck; `docker logs namenode` |
| Spark ghi lỗi `SafeModeException` | NameNode đang ở safe mode lúc mới khởi động | Chờ thoát safe mode, hoặc `hdfs dfsadmin -safemode leave` |
| `return code 2 from MapRedTask` khi query | Hive MR-local tranh chấp JVM khi nhiều query đồng thời | `hive_client.py` tự retry; giảm query đồng thời |
| Mất dữ liệu sau khi xóa `./hdfs/datanode` | replication=1, không có bản sao | Phải chạy lại pipeline từ Kafka (replay) |
| DataNode không kết nối NameNode | sai `CORE_CONF_fs_defaultFS` | Kiểm cả 2 service cùng trỏ `hdfs://namenode:9000` |

---

## 12. Tóm tắt một câu

> Trong DataFinch, **"Hadoop" = HDFS (lưu trữ, gồm `namenode` giữ metadata + `datanode`
> giữ block, `dfs.replication=1`) cộng với MapReduce chạy ở chế độ *local* cho Hive —
> còn YARN bị lược bỏ**; dữ liệu từ Kafka đi vào nền tảng này khi **Spark (chạy
> standalone, không qua YARN)** đóng vai HDFS client: nó kéo message từ `kafka:29092`,
> mở RPC tới NameNode `:9000` để xin cấp block, ghi block Parquet thẳng xuống DataNode,
> rồi NameNode cập nhật metadata (fsimage + edits); sau khi "vào Hadoop", dữ liệu
> **không rời Hadoop** mà tiếp tục được Spark tính toán qua các tầng Bronze → Silver →
> Gold ngay trên HDFS, và cuối cùng được **MapReduce-local** thực thi khi AI Agent truy
> vấn bảng Gold qua HiveServer2 (lỗi tranh chấp *"return code 2 MapRedTask"* được
> `hive_client.py` retry tự động) — nghĩa là Hadoop vừa là **kho lưu lâu dài** vừa là
> **sân tính toán** cho toàn bộ vòng đời dữ liệu sau Kafka.
