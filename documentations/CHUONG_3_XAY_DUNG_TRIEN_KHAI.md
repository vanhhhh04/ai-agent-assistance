# CHƯƠNG 3: XÂY DỰNG VÀ TRIỂN KHAI HỆ THỐNG

Trên cơ sở thiết kế ở Chương 2, Chương 3 trình bày quá trình hiện thực hóa hệ thống DataFinch thành mã nguồn vận hành được: môi trường và công nghệ (3.1), backend FastAPI (3.2), lớp adapter LLM đa nhà cung cấp (3.3), đường ống dữ liệu Medallion (3.4), lớp ngữ nghĩa và catalog indexer (3.5), lõi NL→SQL (3.6), xây dựng và trình diễn frontend (3.7), và đóng gói bằng Docker Compose (3.8); mục 3.9 tổng kết.

## 3.1. Môi trường và Công nghệ triển khai

### 3.1.1. Tech Stack và lựa chọn công nghệ

Hệ thống được xây dựng trên một ngăn xếp công nghệ mã nguồn mở, container hóa hoàn toàn để đảm bảo tính tái lập:

| Lớp | Công nghệ | Phiên bản | Vai trò |
|---|---|---|---|
| Nguồn & OLTP | PostgreSQL | 15 | CSDL ERP nguồn + backend thời gian thực |
| CDC | Debezium Connect | 2.5 | Bắt thay đổi từ WAL của PostgreSQL |
| Streaming | Apache Kafka | 7.5.0 | Hàng đợi sự kiện trung tâm |
| Dataflow | Apache NiFi | 1.23.2 | Tiếp nhận CSV + HTTP |
| Lưu trữ phân tán | HDFS (Hadoop) | 3.2.1 | Data lake Bronze/Silver/Gold |
| Xử lý | Apache Spark (PySpark) | 3.5.1 | ETL Medallion |
| Kho phân tích | Apache Hive | 2.3.2 | Truy vấn star schema bằng SQL |
| Điều phối | Apache Airflow | — | Lập lịch pipeline */15 phút |
| Lớp ngữ nghĩa | OpenSearch | 2.13.0 | Hybrid retrieval kNN + BM25 |
| Embedding | sentence-transformers | mpnet-multilingual 768-d | Vector hóa câu hỏi/schema |
| Backend AI | FastAPI + Uvicorn | Python 3.11 | API Gateway + multi-agent |
| LLM | Anthropic / OpenAI / Google | — | Sinh quyết định & SQL |
| Frontend | Next.js / React / Tailwind | 16.2.6 / 19.2.4 / 4 | Giao diện người dùng |
| Đóng gói | Docker Compose | — | Điều phối 18+ service |

Một số lựa chọn then chốt: **FastAPI (async)** cho backend AI vì bài toán nặng I/O mạng (gọi LLM, truy vấn Hive/OpenSearch) và hỗ trợ StreamingResponse cho SSE tự nhiên; **Spark Structured Streaming** vì cần xử lý cả luồng (Kafka) lẫn lô (rebuild Silver/Gold) trên cùng engine; **OpenSearch** thay vì vector database thuần vì cần kết hợp BM25 lẫn kNN trong cùng truy vấn lai; và **kiến trúc đa nhà cung cấp LLM** để tránh khóa nhà cung cấp.

### 3.1.2. Cấu trúc dự án monorepo và tổ chức mã

Toàn bộ hệ thống tổ chức trong một monorepo, phản ánh ranh giới kiến trúc hai trụ cột:

```
ai-agent-assistance/
├── docker-compose.yml      ← điều phối toàn bộ stack (18+ service)
├── data-source/            ← [DE] sim_erp / sim_warehouse / sim_payment
├── spark/jobs/             ← [DE] bronze / silver / gold_transform.py
├── airflow/dags/           ← [DE] medallion_pipeline.py (*/15)
├── opensearch/             ← [AI] embedder, indexers, query_logger, docs/*.md
├── ai-agent/               ← [AI] FastAPI: main, routers, agents, core/
├── datafinch-web/          ← [UI] Next.js: app/ components/ lib/
└── cli/ , documentations/  ← script vận hành, tài liệu
```

Cách tổ chức này cho phép các thành phần dùng chung mã (ai-agent import opensearch/embedder.py qua sys.path), một file docker-compose.yml dựng được toàn bộ hệ thống, và cấu hình tập trung tại .env cấp gốc được nạp tự động vào từng container.

## 3.2. Xây dựng Backend Services (FastAPI)

### 3.2.1. Triển khai API Gateway với FastAPI và SSE streaming

Backend AI là một ứng dụng FastAPI. Điểm thiết kế quan trọng ở entry point là **vòng đời (lifespan)**: các tài nguyên nặng (mô hình embedding ~420MB, schema cache) được nạp một lần lúc khởi động, không phải mỗi request:

```
@asynccontextmanager
async def lifespan(app):
    await semantic_layer.warmup()   # nạp embedding 1 lần
    await schema_cache.load()        # nạp schema Hive + Postgres song song
    yield
app = FastAPI(lifespan=lifespan)
```

Endpoint lõi `POST /api/query/ask` trả về một StreamingResponse kiểu text/event-stream — bản chất là một async generator phát sự kiện SSE sau từng bước pipeline, với header X-Accel-Buffering: no để vô hiệu hóa buffering của reverse proxy (sự kiện tới giao diện tức thời). Lựa chọn SSE thay vì WebSocket phù hợp với luồng một chiều server→client, nhẹ, chạy trên HTTP thuần.

### 3.2.2. Xác thực và phân quyền (mock auth + lộ trình tích hợp)

Ở giai đoạn hiện tại, xác thực được hiện thực ở mức **mô phỏng (mock)** phía frontend (lib/auth.ts quản lý trạng thái đăng nhập; AuthGate chặn truy cập /app/* khi chưa đăng nhập). Phía backend đã dành sẵn chỗ cho phân quyền: request /ask mang user_id, session_id và cờ allow_pii — các móc nối cho RBAC tương lai. Lộ trình tích hợp đã hoạch định: thay mock auth bằng nhà cung cấp danh tính thật (Clerk/Auth0/SSO), gắn user_id vào ngữ cảnh truy vấn để phân quyền theo vai trò, và liên kết allow_pii với vai trò tại Guardrails. Đây là hạng mục chưa phát triển đầy đủ — đồ án tập trung chứng minh tính khả thi của lõi NL→SQL.

### 3.2.3. Triển khai luồng xử lý NL→SQL pipeline

Hàm _run() trong routers/query.py là trái tim điều phối, phát sự kiện SSE và xử lý các nhánh ý định:

```
async def _run(req):
    yield _sse({"step":"supervisor","status":"running"})
    decision = await classify(req.question, req.conversation_history)
    if decision.intent == "OUT_OF_SCOPE": yield _sse({...}); return   # kết thúc sớm
    if decision.intent == "SCHEMA_INFO":  yield _sse({...}); return
    result, ctx = await generate_sql(question=req.question, backend=decision.backend, ...)
    if not result.valid: yield _sse({"type":"error",...}); return     # Guardrails
    exec_result = await dispatch(decision.backend, result.sql)         # thực thi
    yield _sse({"type":"result","data":{...}})                        # + ghi log
```

Thiết kế xử lý sớm các ý định kết thúc (OUT_OF_SCOPE trả lời lịch sự, SCHEMA_INFO trả schema từ cache) giúp tiết kiệm một lời gọi LLM đắt đỏ và giảm độ trễ cho câu hỏi không cần sinh SQL.

### 3.2.4. Quản lý phiên hội thoại và conversation history

Hệ thống hỗ trợ câu hỏi nối tiếp (FOLLOWUP) mà không cần lưu trạng thái phía server (stateless): lịch sử hội thoại do client gửi kèm mỗi request qua conversation_history. Supervisor chèn tối đa 4 lượt gần nhất để nhận diện FOLLOWUP; SQL Writer chèn tối đa 6 lượt khi intent là FOLLOWUP để hiểu tham chiếu tới câu hỏi trước; session_id gắn nhãn nhật ký, user_id dành cho RBAC. Thiết kế stateless giúp backend dễ scale ngang, phù hợp mô hình container.

## 3.3. Xây dựng adapter cho các LLM provider

### 3.3.1. Giới thiệu bài toán và yêu cầu provider-agnostic

Yêu cầu phi chức năng NFR5 là **độc lập nhà cung cấp LLM**: đổi được giữa Anthropic, OpenAI và Google qua cấu hình mà không sửa mã nghiệp vụ. Động lực: tránh khóa nhà cung cấp, tối ưu chi phí, so sánh hiệu năng (Chương 4), và khả dụng khi một nhà cung cấp gặp sự cố. Khó khăn là mỗi SDK có giao diện riêng (Anthropic có prompt caching/thinking; OpenAI phân biệt model reasoning; Gemini gọi assistant là "model" và truyền system prompt qua trường riêng) — lời giải là tách lớp adapter.

### 3.3.2. Adapter pattern và registry pattern

**Adapter Pattern** định nghĩa giao diện trừu tượng LLMAdapter với phương thức complete() trả về RawCompletion đồng nhất; mỗi nhà cung cấp là một lớp con dịch giao diện chung sang SDK riêng:

```
class LLMAdapter(ABC):
    @abstractmethod
    async def complete(self, *, system, messages, model, max_tokens) -> RawCompletion: ...
    def is_configured(self) -> bool: ...   # có API key chưa
```

**Registry Pattern + lazy import** dùng một sổ đăng ký các factory (`_REGISTRY = {"anthropic", "gemini", "openai"}`), chỉ import SDK khi thực dùng và lưu mỗi adapter dưới dạng singleton; hàm `get_adapter(name)` phân giải tên nhà cung cấp (mặc định từ settings.llm_provider) rồi trả về adapter tương ứng. Nhờ vậy, thêm nhà cung cấp mới = thêm một file adapter + một dòng đăng ký, không đụng gateway hay agent. Việc chọn model cho từng agent được phân giải động trong settings.py theo llm_provider.

### 3.3.3. Xử lý đặc thù từng provider (prompt caching, reasoning, thinking)

Mỗi adapter cài đặt tối ưu đặc thù. **Anthropic** có hai cải tiến cấp sản xuất: *prompt caching* (system prompt gắn cache_control: ephemeral — lần đầu ghi cache ~1.25× chi phí, các lần sau trong ~5 phút đọc lại ~0.1×) và *adaptive thinking* (tự bật cho model hỗ trợ qua cờ thinking: {type: adaptive}, cải thiện SQL phức tạp). **OpenAI** tự phát hiện model reasoning (gpt-5, o1, o3): dùng max_completion_tokens + reasoning_effort thay vì max_tokens + temperature; còn xử lý lỗi thực tế khi docker-compose truyền OPENAI_BASE_URL rỗng. **Gemini** ánh xạ role assistant → model, truyền system prompt qua system_instruction, và phòng vệ bộ lọc an toàn (trích text thủ công từ candidates, raise lỗi nếu bị block_reason).

### 3.3.4. Retry và fallback khi transient error

Phân loại lỗi và thử lại được tập trung tại LLM Gateway: **TransientLLMError** (mạng/5xx/429) → retry với exponential backoff; **PermanentLLMError** (4xx/sai xác thực) → dừng ngay. Gateway thực thi vòng retry với backoff lũy thừa kèm nhiễu (jitter ±25%) chống "thundering herd":

```
delay = 2.0
for attempt in range(1, max_retries + 1):          # mặc định 5 lần
    try: return LLMResponse(await adapter.complete(...))
    except TransientLLMError:
        await asyncio.sleep(min(delay * random.uniform(.75,1.25), 30)); delay *= 2.5
    except PermanentLLMError: raise                  # bail ngay
```

Ngoài retry, gateway còn có bộ nhớ đệm response (LRU + TTL, khóa = sha1 của prompt) và trích xuất JSON mạnh mẽ. Ở tầng agent có lớp fallback ngữ nghĩa: Supervisor khi gặp lỗi mặc định (DATA_QUERY, hive_gold) để không chặn người dùng.

## 3.4. Xây dựng quy trình Data Pipeline Bronze/Silver/Gold

### 3.4.1. Mô phỏng nguồn dữ liệu doanh nghiệp (sim_erp/warehouse/payment)

Vì không có hệ thống doanh nghiệp thật, đồ án xây dựng ba bộ mô phỏng (data-source/) sinh dữ liệu giống thực tế:

| Script | Nguồn | Cơ chế | Đặc điểm |
|---|---|---|---|
| sim_erp.py | ERP (8 thực thể) | Ghi PostgreSQL → Debezium CDC | Bootstrap khối lớn + vòng lặp realtime |
| sim_warehouse.py | Kho vận | CSV vào volume chia sẻ → NiFi | Cập nhật tồn kho, vòng đời sản phẩm |
| sim_payment.py | Thanh toán | POST JSON lồng → NiFi ListenHTTP | Sự kiện payment/shipping |

`sim_erp.py` chạy hai giai đoạn (Bootstrap nạp khối lớn để có dữ liệu lịch sử; Realtime loop mô phỏng hoạt động liên tục), có trần kích thước bảng (đạt ngưỡng thì dừng INSERT nhưng vẫn UPDATE để giữ luồng CDC sống), và đặc biệt **tiêm 5% dữ liệu bẩn có kiểm soát** (email NULL/thiếu "@", lỗi chính tả trạng thái, order_date tương lai, total_amount âm…) — biến pipeline thành hệ thống phải xử lý chất lượng dữ liệu như thật, là cơ sở đánh giá DLQ ở Chương 4.

### 3.4.2. Debezium CDC và NiFi ETL pipelines

**Debezium CDC (cho ERP):** PostgreSQL nguồn cấu hình wal_level=logical; Debezium Connect đăng ký connector theo dõi public.*, dùng SMT ExtractNewRecordState làm phẳng sự kiện rồi phát vào topic erp.public.<table> — đọc log thay vì quét bảng nên đồng bộ gần thời gian thực mà không gây tải nguồn. **NiFi** đảm nhận kho vận (CSV→JSON→warehouse.events) và thanh toán (POST JSON qua ListenHTTP→payment.events). Hai phong cách bổ trợ: CDC log-based cho nguồn quan hệ, dataflow file/HTTP cho nguồn bán cấu trúc.

### 3.4.3. Spark Bronze ingestion với Trigger.AvailableNow

Job bronze_ingestion.py đọc Kafka và ghi thô vào HDFS, dùng Spark Structured Streaming với **Trigger.AvailableNow** — vắt kiệt offset mới rồi tự thoát (vòng đời giống batch) để Airflow lập lịch định kỳ:

```
df = spark.readStream.format("kafka").option("subscribePattern", topic_pattern) \
        .option("startingOffsets","earliest").option("failOnDataLoss","false").load()
(df.select(col("value").cast("string").alias("raw_data"), col("topic"), col("offset"),
           current_timestamp().alias("_bronze_ingested_at"), to_date("timestamp").alias("ingest_date"))
   .writeStream.format("parquet").trigger(availableNow=True)
   .option("checkpointLocation", ckpt).partitionBy("_source_topic","ingest_date").start(sink))
```

Bronze tuân thủ nguyên tắc bất biến: chỉ giữ raw_data + metadata dòng dõi, không áp logic nghiệp vụ. Checkpoint trên HDFS đảm bảo exactly-once và idempotency — chạy lại không đọc trùng.

### 3.4.4. Spark Silver harmonization và surrogate key

Job silver_transform.py biến dữ liệu thô đa định dạng thành bảng sạch có kiểu, xử lý theo từng nguồn với schema StructType tường minh. Các kỹ thuật chính: parse có kiểm soát (from_json theo schema; payload thanh toán lồng dùng get_json_object); ép kiểu & chuẩn hóa thời gian (timestamp Debezium int64 mili-giây → timestamp); harmonization (enum trạng thái, email, tên, SKU; placeholder rác → NULL); và **CDC dedup bằng surrogate key** — window phân theo id sắp theo __source_ts_ms giảm dần, giữ bản ghi mới nhất:

```
def latest_per_id(df, id_col="id", ts_col="__source_ts_ms"):
    w = Window.partitionBy(id_col).orderBy(F.col(ts_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")
```

Cùng với đó là tách DLQ (drop_dirty chia mỗi thực thể thành clean/dirty, gộp bản ghi bẩn vào bảng dlq kèm cột _source). Đầu ra Silver gồm 11 bảng sạch + 1 bảng dlq.

### 3.4.5. Spark Gold star schema và Hive Metastore registration

Job gold_transform.py mô hình hóa Silver thành star schema (3 fact + 7 dimension). **Phi chuẩn hóa fact để giảm JOIN cho AI**: fact_sales JOIN order_items với orders và 6 dimension, nhúng sẵn các thuộc tính hay dùng (customer_name, product_name, brand, payment_method…) cùng measure và cột thời gian phân vùng:

```
fact_sales = (df_order_items.alias("i")
    .join(df_orders.alias("o"), "i.order_id==o.id", "inner")
    .join(dim_customers.alias("c"), "o.customer_id==c.customer_key", "left")
    .join(dim_products.alias("p"), "i.product_id==p.product_key", "left")  # + payments, shipping…
    .select("order_item_key","customer_key","product_key", year("o.order_date").alias("order_year"),
            col("i.total_price").alias("item_total"), "c.customer_name","p.product_name","p.brand"))
```

Hàm write_gold ghi Parquet ra HDFS rồi đăng ký catalog bằng saveAsTable (nguyên tử cho bảng phân vùng); các bảng fact_* phân vùng theo năm/tháng và lọc bỏ bản ghi có khóa phân vùng NULL (tránh lỗi __HIVE_DEFAULT_PARTITION__ của Hive 2.3). Kết quả: AI Agent truy vấn gold.* qua HiveServer2 với rất ít JOIN.

### 3.4.6. Airflow DAG orchestration với schedule 15 phút

DAG medallion_pipeline.py điều phối ba job Spark theo thứ tự phụ thuộc, lập lịch mỗi 15 phút:

```
with DAG("medallion_pipeline", schedule_interval="*/15 * * * *",
         catchup=False, max_active_runs=1) as dag:        # không chạy chồng
    bronze = SparkSubmitOperator(task_id="bronze_ingestion", application=..., packages=KAFKA_PKG)
    silver = SparkSubmitOperator(task_id="silver_transform", ...)
    gold   = SparkSubmitOperator(task_id="gold_transform", ...)
    bronze >> silver >> gold
```

`max_active_runs=1` đảm bảo không hai lần chạy chồng (Bronze streaming + checkpoint không chịu được chạy song song); `catchup=False` tránh "đuổi" hàng loạt lần chạy quá khứ. Độ tươi end-to-end ≈ 15–25 phút — phù hợp phân tích gần thời gian thực mà không cần streaming liên tục tốn tài nguyên.

## 3.5. Triển khai semantic layer và catalog indexer

### 3.5.1. Embedding model sentence-transformers multilingual 768-d

Lớp embedding dùng mô hình paraphrase-multilingual-mpnet-base-v2 (768 chiều, đa ngôn ngữ gồm tiếng Việt, ~420MB, ~50ms/câu sau warmup), với các tối ưu: singleton thread-safe lazy load, cache trong RAM (sha1(text)→vector), chuẩn hóa vector (cosine ≡ dot product), batch encoding, và warmup tại lifespan. Tính đa ngôn ngữ là chìa khóa: câu hỏi tiếng Việt và mô tả schema tiếng Anh về cùng không gian vector, cho phép khớp ngữ nghĩa xuyên ngôn ngữ.

### 3.5.2. Catalog indexer scan Hive Metastore và sinh mô tả

Bộ lập danh mục (catalog_indexer.py) quét Hive Metastore và sinh document cho finch_catalog: mỗi bảng Gold có 1 document mức bảng + 1 document mức cột/cột. Quy trình: SHOW TABLES + DESCRIBE lấy (tên cột, kiểu, comment); cột cardinality thấp chạy SELECT DISTINCT … LIMIT 20 lấy sample values; gắn cờ PII heuristic; tính embedding; bulk upsert theo doc_id ổn định. Tính idempotent cho phép chạy lại mỗi khi Gold cập nhật mà không sinh trùng — đây là điểm nối duy nhất giữa pipeline và dịch vụ AI (hợp đồng dữ liệu ở Chương 2).

### 3.5.3. Hybrid retrieval kNN + BM25 trên 3 indices

Lớp ngữ nghĩa (semantic_layer.py) hiện thực truy hồi lai — một truy vấn bool/should kết hợp BM25 (multi_match có fuzziness AUTO + boost trường) và kNN (vector embedding), gộp dưới minimum_should_match: 1:

```
should = [{"multi_match": {"query": question, "fuzziness": "AUTO",
            "fields": ["table_name^3","column_name^2","description","synonyms","sample_values"]}}]
if q_vec is not None: should.append({"knn": {"embedding": {"vector": q_vec, "k": size}}})
bool_q = {"should": should, "minimum_should_match": 1}
```

Lớp này truy hồi song song trên ba index (finch_catalog lọc backend gold/public, table_docs, query_log lọc status=success); kết quả gói trong RetrievalContext với as_prompt_block() kết xuất Markdown cho prompt SQL Writer. Suy biến mềm: embedding lỗi → lùi về BM25 thuần; OpenSearch không truy cập được → schema cache làm fallback.

### 3.5.4. Self-improving feedback loop từ query_log

Index query_log lưu mỗi truy vấn (câu hỏi NL, SQL, trạng thái, độ trễ, phản hồi 👍/👎), phục vụ hai mục đích: **quan sát** (tỷ lệ thành công, độ trễ từng stage) và **vòng lặp tự cải tiến** — khi truy hồi cho câu hỏi mới, lấy truy vấn quá khứ thành công và được đánh giá tốt làm ví dụ few-shot (bộ lọc chỉ lấy status=success, loại truy vấn bị 👎). Hệ thống nhờ đó chính xác dần theo thời gian sử dụng.

## 3.6. Triển khai lõi hệ thống NL→SQL

### 3.6.1. Supervisor classification và backend selection

Supervisor thực hiện một lời gọi LLM nhỏ trả về JSON nghiêm ngặt. System prompt định nghĩa rõ 4 intent và 2 backend, kèm quy tắc định tuyến đặc thù tiếng Việt:

```
- hive_gold:       Gold star schema (fact_sales, fact_reviews, dim_*).
- postgres_bronze: live ERP — DÙNG khi hỏi STOCK/INVENTORY ("tồn kho","còn hàng"):
      Hive Gold KHÔNG có stock, chỉ public.products.stock_quantity ở Postgres.
# Output strict JSON: {"intent":"DATA_QUERY","backend":"hive_gold","confidence":0.92}
```

Hàm classify() có nhiều lớp phòng vệ: câu hỏi rỗng → OUT_OF_SCOPE ngay; kết quả ngoài tập cho phép → ép về mặc định; mọi exception → fallback (DATA_QUERY, hive_gold). Gộp chọn backend vào đây là có chủ đích vì quyết định này thay đổi schema truy hồi, phương ngữ SQL và client thực thi.

### 3.6.2. Schema augmentation chống hallucination column

Đây là kỹ thuật quan trọng nhất chống ảo giác cột. Truy hồi chỉ trả top-K (8) document, mỗi cột là một document riêng nên thường bỏ sót cột ít liên quan, khiến LLM đoán bừa. Lời giải: với mọi bảng trong truy hồi, bơm **toàn bộ** danh sách cột từ schema_cache (ground truth), mỗi cột một dòng để LLM không trộn cột giữa các bảng:

```
if ctx.catalog:                         # có hit → dump cột các bảng trong scope
    for t in ctx.tables_in_scope(): schema_lines.append(_format_table_schema(t, fallback[t]))
    block += "## Full column schema (GROUND TRUTH — use ONLY these) ..."
else:                                   # truy hồi rỗng (Postgres chưa index) → dump TẤT CẢ
    for t in sorted(schema_fallback): ...
```

Hai nhánh xử lý cả trường hợp cold start / Postgres chưa index — đảm bảo LLM luôn có đủ thông tin cột để không phải đoán.

### 3.6.3. Dialect-specific rules cho HiveQL và PostgreSQL

SQL Writer giữ hai bộ luật prompt riêng — _HIVE_RULES (11 luật) và _POSTGRES_RULES (10 luật) — chọn theo backend, mã hóa tri thức nghiệp vụ và đặc thù phương ngữ, nhiều luật trực tiếp cấm ảo giác:

| Nhóm luật | Ví dụ (HiveQL) | Ví dụ (PostgreSQL) |
|---|---|---|
| Tham chiếu bảng | gold.<table> | Không prefix (search_path=public) |
| An toàn | Không DELETE/UPDATE/DROP; không ; | Tương tự |
| Ánh xạ ngữ nghĩa VN | "bán chạy" → SUM(quantity), "doanh thu" → SUM(item_total) | "doanh thu" → SUM(quantity*unit_price) |
| Đặc thù phương ngữ | Không scalar subquery trong WHERE → dùng CTE | ::date cast; ROUND(x::numeric,2) cho tiền |
| Cấm bịa cột | stock_quantity/phone là Postgres-only | delivered_at ở bảng shipping, không phải orders |

Đặc biệt, luật HiveQL ghi rõ "Hive Gold KHÔNG có dữ liệu tồn kho — nếu hỏi về stock phải hướng sang postgres_bronze; tuyệt đối không bịa cột stock" — cách "mã hóa" tri thức về sự khác biệt giữa hai backend trực tiếp vào prompt.

### 3.6.4. Guardrails: chặn DELETE/UPDATE, enforce LIMIT, PII mask

Guardrails (core/guardrails.py) là lớp phân tích tĩnh độc lập, chạy trên SQL trước khi thực thi. Hàm validate_sql() thực hiện 7 kiểm tra tuần tự:

```
def validate_sql(sql, *, known_tables=None, pii_columns=None, allow_pii=False):
    up = _strip_comments(sql).rstrip(";").upper()
    if not (up.startswith("SELECT") or up.startswith("WITH")): return Result(False,"only SELECT/WITH")
    for kw in FORBIDDEN_KEYWORDS:                       # DELETE/UPDATE/DROP/ALTER/TRUNCATE...
        if re.search(rf"\b{kw}\b", up): return Result(False, f"forbidden {kw}")
    if ";" in cleaned: return Result(False,"multi-statement")          # chống ;-injection
    # 4) JOIN ≤6, UNION ≤3   5) bảng tồn tại   6) bắt buộc LIMIT nếu không aggregate   7) cảnh báo PII
    return Result(True, None, warnings)
```

Một số quyết định cài đặt: khớp theo ranh giới từ (`\b`) để "SELECT" chứa chuỗi con không bị nhầm là từ khóa cấm; kiểm tra bảng tồn tại ở mức bảng (validation cột giao cho chính CSDL); PII ở mức cảnh báo (warn-only) làm tiền đề gate theo vai trò khi RBAC hoàn thiện. Guardrails tách thành module riêng để mọi agent dùng chung và dễ kiểm toán an ninh.

## 3.7. Xây dựng và trình diễn giao diện ứng dụng (Frontend)

Mục này vừa trình bày cách xây dựng vừa trình diễn (demo) từng màn hình. Phân định trung thực mức hoàn thiện: chỉ giao diện **chat NL→SQL (3.7.3)** là chức năng hoàn chỉnh nối backend thật (gọi /api/query/ask) và phản ánh toàn bộ pipeline multi-agent đánh giá ở Chương 4; các màn còn lại ở mức **demo** (dữ liệu mẫu hoặc localStorage), mỗi mục ghi rõ hướng phát triển.

### 3.7.1. Kiến trúc Next.js 16 + React 19 + Tailwind 4

Frontend (datafinch-web/) dùng Next.js 16.2.6 (App Router) + React 19.2.4 + Tailwind CSS 4 + TypeScript 5, biểu đồ bằng Recharts. Tổ chức theo **route groups** để tách ba vùng layout: nhóm `(marketing)` (landing, pricing, how-it-works, docs — TopNav + Footer), nhóm `(auth)` (login, signup onboarding — layout tối giản) và nhóm `app/` (ask, saved, reports, data, settings, billing — Sidebar + AppHeader); thư mục lib/ chứa api.ts (SSE client), auth.ts (mock) và savedQueries.ts (localStorage). Route group (marketing) và (auth) không tạo segment URL, chỉ gom layout — landing ở /, app ở /app/*. Trang landing lắp ghép từ các section (Hero, HowItWorks, Features, Pricing, FAQ); trang /how-it-works vẽ trực quan pipeline 5 agent.

### 3.7.2. Giao diện đăng nhập và onboarding wizard 5 bước (demo)

[Hình 3.19: Giao diện đăng nhập và onboarding wizard 5 bước]

Đăng nhập dùng mock auth (admin/admin lưu localStorage). Wizard làm quen 5 bước: Welcome → chọn loại CSDL (Postgres/MySQL/Snowflake/BigQuery…) → form credentials → màn auto-index với progress mô phỏng → gợi ý câu hỏi mẫu. Toàn bộ dùng dữ liệu mô phỏng, chưa kết nối DB thật.

→ **Hướng phát triển**: thay mock auth bằng nhà cung cấp danh tính thật, gắn user_id cho RBAC, kết nối DB thật ở bước nhập credentials — sẽ phát triển thêm.

### 3.7.3. Giao diện chat NL→SQL với SSE streaming và agent pipeline visualizer (chức năng hoàn chỉnh)

Trang chat (app/app/ask/page.tsx) là giao diện cốt lõi và là màn hình duy nhất nối backend thật end-to-end. Nó tiêu thụ luồng SSE qua askQuery() — một async generator đọc ReadableStream, tách theo \n\n và yield từng sự kiện JSON:

```
const reader = res.body.getReader(); let buf = "";
while (true) {
  const { done, value } = await reader.read(); if (done) break;
  buf += decoder.decode(value, { stream: true });
  const chunks = buf.split("\n\n"); buf = chunks.pop();
  for (const c of chunks) if (c.startsWith("data: ")) yield JSON.parse(c.slice(6));
}
```

[Hình 3.21: Trang /app/ask — chat và agent pipeline visualizer]

Trang cập nhật giao diện theo từng sự kiện: type=step "thắp sáng" agent pipeline visualizer (Supervisor → Truy hồi → Sinh SQL → Thực thi); type=result render bảng số liệu + câu SQL (syntax highlight) + giải thích + biểu đồ. Đây là minh chứng trực quan cho kiến trúc multi-agent và luồng SSE — Chương 4 đánh giá chính trên màn hình này. Các nút hành động: Lưu (localStorage), Share, phản hồi 👍/👎; nút "Pin báo cáo"/"Tạo alert" chưa hiện thực — sẽ phát triển thêm.

### 3.7.4. Giao diện Saved queries (localStorage) (demo)

[Hình 3.22: Trang /app/saved — danh sách câu hỏi đã lưu]

Truy vấn đã lưu được lưu trữ phía client bằng localStorage (lib/savedQueries.ts), chưa cần backend persistence — đủ để trình diễn trải nghiệm. Người dùng có thể lưu một câu hỏi/kết quả, mở lại, gắn sao, sửa, xóa tại /app/saved.

→ **Hướng phát triển**: chuyển lưu trữ sang phía máy chủ gắn user_id để đồng bộ đa thiết bị và chia sẻ trong tổ chức — sẽ phát triển thêm.

### 3.7.5. Giao diện Reports / Dashboard với Recharts (demo)

[Hình 3.23: Trang /app/reports — dashboard KPI và biểu đồ Recharts]

Trang /app/reports trình bày dashboard gồm KPI cards + biểu đồ Recharts (cột/đường/tròn), gom theo tab (Sales/Orders/Customers). Ở giai đoạn này dùng dữ liệu mẫu để trình diễn bố cục và khả năng trực quan hóa.

→ **Hướng phát triển**: ghim kết quả truy vấn thật từ trang chat vào báo cáo khi backend persistence sẵn sàng — sẽ phát triển thêm.

### 3.7.6. Giao diện quản lý Data Sources và Schema Catalog (demo)

[Hình 3.24: Trang /app/data — nguồn dữ liệu và trình duyệt schema catalog]

Danh sách nguồn dữ liệu + trình duyệt schema catalog (bảng → cột → kiểu) + nút "Sửa mô tả". Giao diện dùng dữ liệu mẫu (MOCK_SOURCES, MOCK_TABLES); thao tác sửa mô tả chưa ghi xuống backend. Trong khi đó backend đã có API schema thật (/api/schema/full, /api/schema/tables) đọc từ schema_cache.

→ **Hướng phát triển**: nối UI vào API schema thật và cho phép lưu mô tả nghiệp vụ trở lại finch_catalog/table_docs — sẽ phát triển thêm.

### 3.7.7. Giao diện Settings và Billing (demo)

[Hình 3.25: Trang /app/settings — 4 tab cấu hình]

Trang Settings gồm 4 tab, đáng chú ý tab AI Model: chọn nhà cung cấp LLM + model + API key + max tokens — phản ánh trực quan kiến trúc provider-agnostic ở mục 3.3. Thay đổi ở UI chưa ghi xuống backend (cấu hình thật đặt qua .env). Các tab còn lại: Profile, Integration, Bảo mật (placeholder).

[Hình 3.26: Trang /app/billing — gói cước và usage tracking]

Trang Billing hiển thị gói hiện tại, biểu đồ usage, nút nâng cấp, lịch sử hóa đơn — toàn bộ dữ liệu mẫu, chưa tích hợp cổng thanh toán.

→ **Hướng phát triển**: lưu cấu hình theo người dùng/tổ chức xuống backend; tích hợp cổng thanh toán thật và đo usage thực từ query_log — sẽ phát triển thêm.

## 3.8. Triển khai bằng Docker Compose

Toàn bộ hệ thống (18+ service) được đóng gói bằng một file docker-compose.yml duy nhất, chạy trên mạng bridge chung. Các nhóm service:

| Nhóm | Service | Cổng (host) |
|---|---|---|
| Nguồn & quản trị | postgres, adminer | 5433, 8081 |
| Streaming | zookeeper, kafka, kafka-ui, kafka-connect | 9092, 8888, 8083 |
| Dataflow | nifi | 8443, 8181 |
| Lưu trữ & xử lý | namenode, datanode, spark-master, spark-worker | 9870, 8090, 7077 |
| Kho phân tích | hive-metastore, hiveserver2 | 9083, 10000 |
| Điều phối | airflow-webserver, airflow-scheduler | 8080 |
| Lớp ngữ nghĩa | opensearch, opensearch-dashboards | 9200, 5601 |
| Dịch vụ AI / Giao diện | ai-agent, datafinch-web | 8000, 3000 |

Các điểm đáng chú ý: **health checks + depends_on theo điều kiện** (ai-agent chỉ khởi động sau khi opensearch healthy và hiveserver2 started; datafinch-web chờ ai-agent) xử lý đúng thứ tự khởi động của hệ phân tán; **cấu hình qua .env cấp gốc** (đổi nhà cung cấp LLM chỉ bằng sửa .env, đúng tinh thần provider-agnostic); **volume bền vững** (giữ dữ liệu Postgres/HDFS/OpenSearch/Kafka và cache mô hình embedding qua các lần restart); và **hot-reload** khi phát triển (bind-mount mã nguồn). Quy trình khởi động một lệnh được gói trong cli/startup.sh (dựng stack, đăng ký Debezium connector, tạo NiFi flow, chạy sims).

## 3.9. Tổng kết chương 3

Chương 3 đã hiện thực hóa thiết kế ở Chương 2 thành một hệ thống vận hành được: backend FastAPI với lifespan và SSE; kiến trúc adapter LLM đa nhà cung cấp (Adapter + Registry pattern, retry/cache tập trung); lõi NL→SQL với Supervisor định tuyến, schema augmentation chống ảo giác, hai bộ dialect rules và Guardrails 7 lớp; đường ống Bronze/Silver/Gold với Trigger.AvailableNow, CDC dedup bằng surrogate key và phi chuẩn hóa star schema, điều phối Airflow */15 phút; lớp ngữ nghĩa với embedding đa ngôn ngữ 768-d, catalog indexer idempotent và truy hồi lai kNN+BM25; frontend Next.js 16 với chat SSE hoàn chỉnh nối backend thật cùng sáu màn quản trị mức demo; và đóng gói 18+ service bằng Docker Compose.

Một số hạng mục được nêu trung thực là chưa phát triển đầy đủ (xác thực/phân quyền thật, persistence đa người dùng) — các hướng mở rộng không ảnh hưởng tới việc chứng minh tính khả thi của lõi hệ thống. Trên cơ sở này, Chương 4 tiến hành thực nghiệm và đánh giá.
