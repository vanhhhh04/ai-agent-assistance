# DataFinch — AI Agent Assistance Platform

Nền tảng **Big Data + AI** mô phỏng hệ thống thương mại điện tử end-to-end: ingestion đa nguồn (CDC + NiFi), lakehouse Medallion trên HDFS, orchestration Airflow/Spark, và **trợ lý NL→SQL** (DataFinch) cho người dùng nghiệp vụ.

Dự án tốt nghiệp / demo production-like — chạy local hoàn toàn bằng Docker Compose.

---

## Mục lục

- [Tổng quan](#tổng-quan)
- [Kiến trúc](#kiến-trúc)
- [Công nghệ](#công-nghệ)
- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Cấu trúc repository](#cấu-trúc-repository)
- [Cài đặt nhanh](#cài-đặt-nhanh)
- [Cấu hình môi trường](#cấu-hình-môi-trường)
- [Bảng dịch vụ & URL](#bảng-dịch-vụ--url)
- [Luồng dữ liệu](#luồng-dữ-liệu)
- [AI Agent & Frontend](#ai-agent--frontend)
- [CLI & Makefile](#cli--makefile)
- [Vận hành hàng ngày](#vận-hành-hàng-ngày)
- [Xử lý sự cố](#xử-lý-sự-cố)
- [Tài liệu chi tiết](#tài-liệu-chi-tiết)

---

## Tổng quan

Hệ thống gồm **hai lớp chính**:

| Lớp | Mô tả |
|-----|--------|
| **Data Engineering** | PostgreSQL (ERP) → Kafka (Debezium CDC + NiFi) → Spark Bronze/Silver/Gold trên HDFS → Hive catalog |
| **AI Layer (DataFinch)** | OpenSearch semantic layer + FastAPI multi-agent → Hive Gold / Postgres Bronze → Next.js UI |

**Ba nguồn dữ liệu mô phỏng** (`data-source/`):

| Simulator | Cơ chế | Kafka topics |
|-----------|---------|--------------|
| `sim_erp.py` | Ghi OLTP Postgres → Debezium CDC | `erp.public.*` (9 bảng) |
| `sim_warehouse.py` | CSV → NiFi GetFile | `warehouse.events` (+ `.dlq`) |
| `sim_payment.py` | HTTP webhook → NiFi ListenHTTP | `payment.events` (+ `.dlq`) |

Simulator inject ~5% dữ liệu bẩn có kiểm soát để kiểm chứng Silver/DLQ.

---

## Kiến trúc

```mermaid
flowchart TB
    subgraph Sources["Nguồn mô phỏng"]
        ERP[sim_erp.py]
        WH[sim_warehouse.py]
        PAY[sim_payment.py]
    end

    subgraph OLTP["OLTP"]
        PG[(PostgreSQL ecommerce)]
    end

    subgraph Ingest["Ingestion"]
        DEB[Debezium Connect]
        NIFI[Apache NiFi]
    end

    subgraph Stream["Kafka"]
        K[Topics erp / warehouse / payment]
    end

    subgraph Lake["HDFS Medallion"]
        B[/datalake/bronze/]
        S[/datalake/silver/]
        G[/datalake/gold/]
    end

    subgraph Compute["Compute"]
        AF[Airflow]
        SP[Spark 3.5]
        HV[HiveServer2]
    end

    subgraph AI["DataFinch AI"]
        OS[OpenSearch]
        API[FastAPI AI Agent]
        WEB[Next.js UI]
    end

    ERP --> PG
    PG --> DEB --> K
    WH --> NIFI --> K
    PAY --> NIFI --> K
    K --> SP
    AF --> SP
    SP --> B --> S --> G
    G --> HV
    OS --> API
    HV --> API
    PG --> API
    API --> WEB
```

**Star schema Gold** (truy vấn chính): `fact_sales`, `fact_reviews`, `fact_feedback`, `dim_customers`, `dim_products`, `dim_categories`, `dim_addresses`, `dim_coupons`, `dim_payments`, `dim_shipping`.

---

## Công nghệ

| Hạng mục | Stack |
|----------|--------|
| OLTP | PostgreSQL 15 (`wal_level=logical`) |
| Streaming | Kafka 7.5, ZooKeeper, Debezium 2.5 |
| Ingestion | Apache NiFi 1.23 |
| Storage | HDFS (Hadoop 3.2), Parquet |
| Compute | Apache Spark 3.5.1 |
| Catalog | Hive Metastore + HiveServer2 |
| Orchestration | Apache Airflow 2.8 |
| Retrieval | OpenSearch 2.13, sentence-transformers |
| AI Backend | FastAPI, multi-agent (Supervisor → SQL Writer → Execution) |
| LLM | Anthropic / Gemini / OpenAI (cấu hình qua `.env`) |
| Frontend | Next.js 16, React 19, Tailwind CSS 4 |
| DevOps | Docker Compose |

---

## Yêu cầu hệ thống

| Thành phần | Khuyến nghị |
|------------|-------------|
| OS | Windows 10/11 + WSL2/Git Bash, macOS, hoặc Linux |
| Docker | Docker Desktop 4.x+ (bật WSL2 backend trên Windows) |
| RAM | **16 GB+** (toàn stack ~20 container) |
| Disk | **30 GB+** trống (HDFS, Kafka, model cache ~420 MB) |
| Python | 3.10+ (CLI trên host) |
| Git | Clone + bash scripts |

> **Lưu ý Windows:** NiFi, HDFS bind-mount và Next.js hot-reload đã được cấu hình polling trong `docker-compose.yml`. Chạy `bash cli/startup.sh` từ Git Bash hoặc WSL.

---

## Cấu trúc repository

```
ai-agent-assistance/
├── ai-agent/              # FastAPI NL→SQL service (Supervisor, SQL Writer, guardrails)
├── datafinch-web/           # Next.js frontend (landing + app /ask, /saved, …)
├── opensearch/              # Index mappings, embedder, catalog indexers, business docs
├── spark/jobs/              # bronze_ingestion.py, silver_transform.py, gold_transform.py
├── airflow/dags/            # medallion_pipeline DAG (hourly Bronze→Silver→Gold)
├── data-source/             # sim_erp, sim_warehouse, sim_payment
├── data/
│   ├── json/                # Seed JSON (customers, orders, …)
│   ├── initial_table.sql    # Schema ERP + seed load
│   └── postgres-init/       # Init scripts (Airflow DB, …)
├── cli/                     # Shell scripts vận hành (startup, sim-logs, nifi-recover, …)
├── cli.py                   # Python CLI (status, seed, debezium, pipeline, …)
├── documentations/          # Luồng DE, kiến trúc AI, luận văn (MD + assets)
├── docker-compose.yml       # Toàn bộ stack
├── Makefile                 # step1…step10 setup có hướng dẫn
└── .env                     # API keys & overrides (không commit — xem .gitignore)
```

---

## Cài đặt nhanh

### 1. Clone & cấu hình

```bash
git clone https://github.com/vanhhhh04/ai-agent-assistance.git
cd ai-agent-assistance

# Tạo .env từ template AI Agent
cp ai-agent/.env.example .env
# Chỉnh LLM_PROVIDER và API key (Anthropic / Gemini / OpenAI)
```

### 2. Cài CLI dependencies (host)

```bash
pip install -r cli-requirements.txt
# hoặc
make install-cli
```

### 3. Khởi động toàn stack (khuyến nghị)

```bash
bash cli/startup.sh
```

Script idempotent — an toàn chạy lại sau khi restart máy. Thực hiện:

1. `docker compose up -d`
2. Chờ healthcheck các service
3. Fix quyền volume CSV (NiFi)
4. Đăng ký Debezium connector (snapshot ERP → Kafka)
5. Tạo NiFi flow nếu chưa có
6. Start 3 simulators
7. In trạng thái pipeline

**Thời gian:** ~3–5 phút lần đầu; Debezium snapshot ~30–60 giây.

### 4. Luồng setup từng bước (Makefile)

```bash
make step1    # docker compose up -d
make step2    # python cli.py status
make step3    # HDFS /datalake structure
make step4    # seed Postgres (nếu DB trống)
make step5    # Debezium connector
make step6    # NiFi (hướng dẫn / scripts)
make step7    # simulators
make step8    # trigger Airflow DAG
make step9    # verify Gold trên HDFS
make step10   # verify Kafka topics
```

### 5. Index OpenSearch (semantic layer)

Sau Gold đã có dữ liệu:

```bash
bash cli/opensearch-up.sh      # tạo indices + index catalog/docs
bash cli/opensearch-status.sh  # kiểm tra
```

### 6. Truy cập ứng dụng

| Ứng dụng | URL |
|----------|-----|
| **DataFinch Web** | http://localhost:3000 |
| **AI Agent API / Swagger** | http://localhost:8000/docs |
| Demo login | `admin` / `admin` |

---

## Cấu hình môi trường

File **`.env`** ở root project được `docker compose` load cho `ai-agent` và `data-source`.

### LLM (bắt buộc cho AI Agent)

```env
LLM_PROVIDER=gemini          # anthropic | gemini | openai
GEMINI_API_KEY=your-key      # hoặc ANTHROPIC_API_KEY / OPENAI_API_KEY
```

Xem đầy đủ biến trong [`ai-agent/.env.example`](ai-agent/.env.example).

### Frontend (tùy chọn)

[`datafinch-web/.env.local.example`](datafinch-web/.env.local.example):

```env
NEXT_PUBLIC_API_BASE=http://localhost:8000
```

Trong Docker, mặc định đã set qua `docker-compose.yml`.

---

## Bảng dịch vụ & URL

| Service | URL | Credentials |
|---------|-----|-------------|
| DataFinch Web | http://localhost:3000 | admin / admin |
| AI Agent API | http://localhost:8000/docs | — |
| Airflow | http://localhost:8080 | admin / admin123 |
| Kafka UI | http://localhost:8888 | — |
| NiFi | https://localhost:8443/nifi | admin / adminadminadmin |
| HDFS NameNode | http://localhost:9870 | — |
| Spark Master UI | http://localhost:8090 | — |
| HiveServer2 UI | http://localhost:10002 | — |
| Kafka Connect (Debezium) | http://localhost:8083 | — |
| OpenSearch | http://localhost:9200 | (security off — dev only) |
| OpenSearch Dashboards | http://localhost:5601 | — |
| Adminer (Postgres) | http://localhost:8081 | postgres / postgres / ecommerce |
| Postgres (host) | localhost:**5433** | postgres / postgres |

---

## Luồng dữ liệu

### Medallion pipeline (Airflow DAG `medallion_pipeline`)

```
Kafka → Bronze (append Parquet, checkpoint)
     → Silver (clean, dedup CDC, DLQ)
     → Gold (star schema + Hive external tables)
```

| Layer | Path HDFS | Ghi chú |
|-------|-----------|---------|
| Bronze | `/datalake/bronze/erp_raw`, `wh_raw`, `pay_raw` | Raw JSON, append-only |
| Silver | `/datalake/silver/{orders,customers,…}`, `dlq` | Overwrite mỗi lần chạy |
| Gold | `/datalake/gold/fact_*`, `dim_*` | Hive catalog `gold.*` |

Trigger thủ công:

```bash
python cli.py pipeline run
# hoặc Airflow UI → medallion_pipeline → Trigger DAG
```

### Kafka topics chính

| Topic pattern | Nguồn |
|---------------|--------|
| `erp.public.*` | Debezium CDC (9 bảng ERP) |
| `warehouse.events` | NiFi (CSV sản phẩm/kho) |
| `payment.events` | NiFi (HTTP payment/shipping) |
| `*.events.dlq` | Bản ghi DIRTY/QUARANTINE |

---

## AI Agent & Frontend

### Pipeline câu hỏi (FastAPI SSE)

```
POST /api/query/ask
  → Supervisor (intent + backend: hive_gold | postgres_bronze)
  → OpenSearch hybrid retrieval (catalog + docs + query history)
  → SQL Writer (LLM + guardrails)
  → Execute on Hive / Postgres
  → Stream kết quả về UI
```

### Backend chính

| Backend | Khi nào dùng | Engine |
|---------|--------------|--------|
| `hive_gold` | Phân tích, doanh thu, trend (mặc định) | HiveServer2 → `gold.*` |
| `postgres_bronze` | Trạng thái vận hành / OLTP | PostgreSQL `public.*` |

### API hữu ích

| Endpoint | Mô tả |
|----------|--------|
| `GET /api/health` | Health toàn stack |
| `GET /api/schema/full?backend=hive_gold` | Schema cache |
| `POST /api/query/ask` | Hỏi đáp NL→SQL (SSE) |

Chi tiết: [`documentations/AI_AGENT_FLOW.md`](documentations/AI_AGENT_FLOW.md)

---

## CLI & Makefile

### Python CLI (`cli.py`)

```bash
python cli.py status              # health tất cả services
python cli.py seed                # nạp JSON vào Postgres
python cli.py debezium setup      # đăng ký CDC connector
python cli.py pipeline run        # trigger medallion DAG
python cli.py pipeline status
python cli.py kafka topics
python cli.py hdfs ls /datalake/gold
```

### Shell scripts (`cli/`)

| Script | Mục đích |
|--------|----------|
| `startup.sh` | **Khởi động chính** — compose + debezium + nifi + sims |
| `shutdown.sh` | Dừng stack |
| `wipe.sh` | Reset sạch Kafka/ZK (demo từ đầu) |
| `sim-start.sh` / `sim-stop.sh` | Bật/tắt simulators |
| `sim-logs.sh erp\|warehouse\|payment` | Tail log simulator |
| `pipeline-status.sh` | Trạng thái Bronze/Silver/Gold |
| `verify-pipeline.sh` | Kiểm tra end-to-end |
| `nifi-recover.sh` | Sửa NiFi nhẹ |
| `nifi-reset.sh` | Reset NiFi flow (cẩn thận) |
| `opensearch-up.sh` | Bootstrap indices |
| `ai-agent-up.sh` | Rebuild/restart AI Agent |

### Chạy simulator thủ công

```bash
docker compose exec data-source python sim_erp.py
docker compose exec data-source python sim_warehouse.py
docker compose exec data-source python sim_payment.py
```

---

## Vận hành hàng ngày

```bash
# Ngày thường
bash cli/startup.sh
bash cli/pipeline-status.sh

# Debug dữ liệu
bash cli/sim-logs.sh erp
bash cli/sample-data.sh
bash cli/verify-pipeline.sh

# Demo sạch hoàn toàn
bash cli/wipe.sh && bash cli/startup.sh
```

**Sau `startup.sh`**, kiểm tra simulators:

```bash
bash cli/sim-logs.sh erp        # INSERT/UPDATE orders mỗi vài giây
bash cli/sim-logs.sh warehouse  # stock_update ~10s, product mới ~2 phút
bash cli/sim-logs.sh payment    # payment ~3s, shipping ~15s
```

---

## Xử lý sự cố

| Triệu chứng | Hướng xử lý |
|-------------|-------------|
| Kafka corrupt / offset lỗi | `bash cli/kafka-recover.sh` hoặc `bash cli/wipe.sh` |
| NiFi không đọc CSV | `docker exec -u root nifi chmod 777 /opt/nifi/csv_input` |
| NiFi flow lỗi | `bash cli/nifi-recover.sh` → nếu vẫn lỗi: `bash cli/nifi-reset.sh` |
| Docker pull 500 / DNS | Restart Docker Desktop; kiểm tra `nslookup auth.docker.io` |
| Postgres init script CRLF | Chuyển `data/postgres-init/*.sh` sang **LF** (`.gitattributes` đã có `*.sh eol=lf`) |
| AI Agent không trả lời | Kiểm tra API key trong `.env`; `curl localhost:8000/api/health/ping` |
| Gold trống | Chạy simulators → trigger DAG → đợi Spark jobs xong |
| OpenSearch empty | `bash cli/opensearch-up.sh` sau khi Gold đã populate |

### Reset volume runtime (giữ code)

Các thư mục sau **không commit** (xem `.gitignore`) — có thể xóa khi cần reset:

```bash
rm -rf data/kafka/* data/zookeeper/data/* data/zookeeper/log/*
rm -rf ./nifi/flowfile_repository ./nifi/content_repository ./nifi/provenance_repository
```

---

## Tài liệu chi tiết

| Tài liệu | Nội dung |
|----------|----------|
| [`documentations/ARCHITECTURE.md`](documentations/ARCHITECTURE.md) | Kiến trúc tổng thể |
| [`documentations/DATA_FLOW.md`](documentations/DATA_FLOW.md) | Luồng dữ liệu end-to-end |
| [`documentations/LUONG_CHAY_DATA_ENGINEER.md`](documentations/LUONG_CHAY_DATA_ENGINEER.md) | Hướng dẫn DE |
| [`documentations/DATA_ENGINEER_FLOW_SPARK.md`](documentations/DATA_ENGINEER_FLOW_SPARK.md) | Bronze/Silver/Gold Spark |
| [`documentations/DATA_ENGINEER_FLOW_KAFKA.md`](documentations/DATA_ENGINEER_FLOW_KAFKA.md) | Kafka & Debezium |
| [`documentations/DATA_ENGINEER_FLOW_NIFI.md`](documentations/DATA_ENGINEER_FLOW_NIFI.md) | NiFi flows |
| [`documentations/AI_AGENT_FLOW.md`](documentations/AI_AGENT_FLOW.md) | AI Agent pipeline |
| [`documentations/AI_AGENT_SYSTEM.md`](documentations/AI_AGENT_SYSTEM.md) | Multi-agent design |
| [`datafinch-web/README.md`](datafinch-web/README.md) | Frontend riêng |

---

## Phát triển

### Rebuild service

```bash
docker compose build ai-agent datafinch-web
docker compose up -d ai-agent datafinch-web
```

### Spark jobs thủ công

```bash
make spark-bronze
make spark-silver
make spark-gold
```

### Tests

```bash
# OpenSearch query logger
python -m pytest opensearch/test_query_logger.py -q
```

---

## Lưu ý bảo mật

- Cấu hình hiện tại dành cho **local dev**: OpenSearch security off, mật khẩu mặc định, không TLS giữa services.
- **Không** deploy production với `docker-compose.yml` nguyên bản.
- Không commit `.env` chứa API key thật.

---

## License & tác giả

Dự án tốt nghiệp — DataFinch / AI Agent Assistance.
CAO VIỆT ANH 
contact: caovietanhhd@gmail.com

Repository: [github.com/vanhhhh04/ai-agent-assistance](https://github.com/vanhhhh04/ai-agent-assistance)
