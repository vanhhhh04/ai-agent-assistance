# AI AGENT — CÁCH HIỂU SCHEMA & CÂU HỎI NGƯỜI DÙNG

> **Mục đích**: tài liệu chuyên sâu giải thích **chính xác** cách AI Agent trong dự án này
> chuyển câu hỏi tiếng Việt thành SQL hợp lệ — từ việc nạp schema, retrieval semantic, đến
> những gì LLM thực sự thấy trong prompt.
>
> **Bám sát mã nguồn**: mọi mục đều dẫn file/dòng cụ thể, không phải lý thuyết chung chung.
>
> **Đối tượng**: kỹ sư đọc code dự án để debug retrieval / cải tiến prompt / mở rộng semantic
> layer; sinh viên đồ án cần hiểu “bộ não” của agent.

---

## 0. Bức tranh tổng thể — 2 lớp “hiểu” song song

AI Agent gồm **hai cơ chế hiểu schema bổ sung lẫn nhau**:

```
┌────────────────────────────────────────────────────────────────────┐
│           CÂU HỎI NGƯỜI DÙNG (tiếng Việt tự nhiên)                 │
│           "Top 5 brand bán chạy nhất quý 4"                        │
└────────────────────────────┬───────────────────────────────────────┘
                             │
            ┌────────────────┴───────────────┐
            ▼                                ▼
 ┌────────────────────┐         ┌───────────────────────────┐
 │  LỚP 1 — SCHEMA    │         │  LỚP 2 — SEMANTIC LAYER   │
 │  CACHE (RAM)       │         │  (OpenSearch hybrid       │
 │                    │         │   retrieval)              │
 │  • toàn bộ tables  │         │                           │
 │  • dùng cho        │         │  • chỉ top-K bảng + cột   │
 │    guardrails &    │         │    LIÊN QUAN              │
 │    SCHEMA_INFO     │         │  • + business docs        │
 │  • fallback khi    │         │  • + similar past queries │
 │    OpenSearch chết │         │  • dùng để build prompt   │
 └────────────────────┘         └───────────────────────────┘
            │                                │
            └────────────┬───────────────────┘
                         ▼
            ┌─────────────────────────┐
            │  SQL WRITER PROMPT      │
            │  (system + retrieval +  │
            │   dialect rules)        │
            └───────────┬─────────────┘
                        ▼
                ┌──────────────┐
                │ Claude / LLM │ → JSON {sql, explanation, ...}
                └──────────────┘
```

→ **Schema cache không bao giờ được nhét full vào prompt** — nó chỉ phục vụ
guardrails và intent `SCHEMA_INFO`. Prompt LLM **chỉ chứa kết quả semantic retrieval**.

---

## 1. Pipeline xử lý 1 câu hỏi (đối chiếu mã nguồn)

File điều phối: `ai-agent/routers/query.py` → hàm `_run()` (SSE stream).

```
POST /api/query/ask  { question, conversation_history, session_id, allow_pii }
   │
   ▼
[STEP 1] Supervisor.classify()                   ai-agent/agents/supervisor.py
   │     ├─ phân loại intent: DATA_QUERY | SCHEMA_INFO | FOLLOWUP | OUT_OF_SCOPE
   │     └─ chọn backend:    hive_gold | postgres_bronze
   │     → 1 lệnh gọi LLM duy nhất, trả về JSON
   │
   ▼
[Terminal nếu intent ∈ {OUT_OF_SCOPE, SCHEMA_INFO}]
   │     SCHEMA_INFO trả thẳng SchemaCache.get(backend) — không gọi LLM thứ 2
   │
   ▼
[STEP 2] semantic_layer.retrieve(question)        ai-agent/core/semantic_layer.py
   │     ├─ embed câu hỏi 1 lần → vector 768-d   opensearch/embedder.py
   │     ├─ search finch_catalog  (BM25 + kNN)   top_k=8
   │     ├─ search table_docs     (BM25 + kNN)   top_k=3
   │     └─ search query_log      (BM25 + kNN)   top_k=3, lọc status="success"
   │
   ▼
[STEP 3] sql_writer.generate_sql()                ai-agent/agents/sql_writer.py
   │     ├─ as_prompt_block() → render context thành markdown gọn
   │     ├─ build system prompt theo dialect (HiveQL / PostgreSQL)
   │     ├─ gọi LLM (cacheable=True trừ FOLLOWUP)
   │     └─ guardrails.validate_sql()             ai-agent/core/guardrails.py
   │
   ▼
[STEP 4] retrieval_dispatch.dispatch(backend, sql) ai-agent/agents/retrieval/*
   │     └─ hive_agent hoặc postgres_agent thực thi SQL, trả rows
   │
   ▼
[STEP 5] log_query() → ghi query_log              opensearch/query_logger.py
   │     (telemetry — feed back semantic layer ở vòng sau)
   ▼
SSE event {type: "result", data: {sql, rows, columns, explanation, ...}}
```

---

## 2. LỚP 1 — Schema Cache (RAM)

### 2.1 Tại sao cần?

`ai-agent/core/schema_cache.py` nêu rõ 2 lý do:

1. **Supervisor prompt** cần biết “vũ trụ bảng” đang có (để gợi ý backend chính xác).
2. **Guardrails** cần whitelist tables để reject SQL tham chiếu bảng ngoài warehouse
   (chống hallucination cấp bảng).

Schema cache **chỉ load 1 lần** lúc FastAPI startup, không refresh mỗi request.

### 2.2 Cấu trúc trong RAM

```python
class SchemaCache:
    hive:     dict[str, list[dict]]   # {"fact_sales": [{"column": "brand", "type": "string"}, ...]}
    postgres: dict[str, list[dict]]
```

Load song song qua `asyncio.gather(_safe_hive(), _safe_pg())` — nếu Hive chết thì
chỉ rỗng dict đó, Postgres vẫn dùng được (và ngược lại).

### 2.3 Khi nào schema cache được dùng?

| Trường hợp | Mã nguồn |
|---|---|
| `intent == SCHEMA_INFO` — user hỏi “có bảng gì?” | `routers/query.py` dòng ~110 |
| Fallback `schema_fallback=` truyền vào `generate_sql()` để guardrails biết tên bảng hợp lệ | `routers/query.py` → `sql_writer.py` dòng 223 |
| **KHÔNG** dùng để dựng prompt LLM | `sql_writer.py` chỉ inject `retrieval_block` |

### 2.4 Vì sao không nhét full schema vào prompt?

- **Lãng phí token**: 50 bảng × 30 cột × ~80 ký tự = ~120k ký tự noise mỗi request.
- **Nhiễu**: LLM bị phân tâm, dễ chọn bảng sai.
- **Không semantic**: tên cột `c.parent_category_name` không hint cho LLM rằng nó
  chứa “tên ngành hàng cha” khi user hỏi “sản phẩm thuộc ngành nào”.
- **Chậm**: prompt dài → latency tăng, cache hit rate giảm.

→ Đó chính là lý do tồn tại **Lớp 2 — Semantic Layer**.

---

## 3. LỚP 2 — Semantic Layer (trái tim của agent)

File chính: `ai-agent/core/semantic_layer.py`.
Indices: `opensearch/init_indices.sh` tạo ra 3 index.

### 3.1 3 index OpenSearch — vai trò từng cái

| Index | Mỗi document chứa | Vai trò |
|---|---|---|
| **`finch_catalog`** | 1 doc / 1 table-level + 1 doc / 1 column | Mô tả schema kỹ thuật (column type, sample values, PII flag) |
| **`table_docs`** | 1 doc / 1 section markdown (`opensearch/docs/*.md`) | Tri thức nghiệp vụ — “không SUM order_total”, ví dụ SQL chuẩn |
| **`query_log`** | 1 doc / 1 lần user hỏi (NL + SQL + status) | Few-shot từ lịch sử — “user hỏi giống thế này lần trước, SQL đã chạy thành công” |

#### 3.1.1 `finch_catalog` — mỗi cột là 1 document

Builder: `opensearch/indexers/catalog_indexer.py`.

```python
# Table-level doc
{"doc_id": "table::gold.fact_sales", "doc_type": "table",
 "table_name": "fact_sales", "table_kind": "fact",
 "description": "FACT table gold.fact_sales with 35 columns and approximately 1,200,000 rows"}

# Column-level doc (mỗi cột 1 document riêng — quan trọng!)
{"doc_id": "column::gold.fact_sales.brand",
 "doc_type": "column", "table_name": "fact_sales",
 "column_name": "brand", "column_type": "string",
 "sample_values": ["Apple", "Samsung", "Sony", ...],
 "is_pii": false,
 "description": "Column brand of type string in gold.fact_sales"}
```

→ Khi user hỏi “top brand”, BM25 match được **document column `brand`** → kéo theo bảng
`fact_sales` vào prompt.

Mapping `opensearch/mappings/finch_catalog.json`:
- BM25 fields: `description`, `synonyms` (analyzer `snake_and_text` — lowercase + asciifolding).
- kNN field: `embedding` (768-d, HNSW, cosine similarity, ef_construction=256, m=16).

#### 3.1.2 `table_docs` — knowledge nghiệp vụ

Source: `opensearch/docs/{fact_sales,dim_products,dim_customers}.md`.

Đoạn trích từ `opensearch/docs/fact_sales.md`:

```markdown
## Lưu ý quan trọng
- **Tránh nhầm `order_total` và `item_total`**: `order_total` lặp trên các item
  của cùng order (nếu SUM mọi item sẽ nhân đôi). Dùng `item_total` cho aggregation
  theo sản phẩm; dùng DISTINCT order_key rồi SUM order_total cho aggregation theo đơn.
```

→ Đây là **tri thức không nằm trong DDL** — chỉ con người mới biết. Indexer
(`docs_indexer.py`) chia file md thành nhiều section, mỗi section 1 doc với fields
`title`, `content`, `examples`.

#### 3.1.3 `query_log` — vòng feedback loop

Mỗi lần user hỏi, `routers/query.py` gọi `log_query()` (`opensearch/query_logger.py`):

```python
{
  "query_id": "<uuid>",
  "nl_question": "top 10 sản phẩm bán chạy nhất 2026",
  "generated_sql": "SELECT product_name, SUM(quantity) AS qty FROM gold.fact_sales ...",
  "status": "success" | "guardrail_fail" | "exec_error" | "llm_error",
  "tables_used": ["fact_sales"],
  "user_thumbs_up": true | false | null,  # update_feedback() ghi sau
  "exec_ms": 245,
  ...
}
```

Khi search history (`_search_history()`), agent **chỉ lấy `status: success`** —
không bao giờ feed lại SQL hỏng cho LLM.

### 3.2 Hybrid Retrieval — BM25 + kNN cùng lúc

Trái tim của trái tim: hàm `_build_hybrid_query()` (`semantic_layer.py` dòng 349):

```python
{
  "size": K,
  "query": {
    "bool": {
      "should": [
        # Nhánh 1 — BM25 (lexical)
        {"multi_match": {
            "query": question,
            "fields": ["table_name^3", "column_name^2", "description",
                       "synonyms", "sample_values"],
            "type": "best_fields",
            "fuzziness": "AUTO"   # tolerate typo
        }},
        # Nhánh 2 — kNN (semantic vector)
        {"knn": {
            "embedding": {"vector": q_vec, "k": K}
        }}
      ],
      "minimum_should_match": 1,
      "filter": [extra_filter]   # vd: {"term": {"database": "gold"}}
    }
  }
}
```

**Tại sao hybrid?**

| Tín hiệu | Mạnh ở | Yếu ở |
|---|---|---|
| BM25 | từ khoá chính xác: “brand”, “fact_sales”, “DELIVERED” | paraphrase, tiếng Việt tự nhiên |
| kNN (vector mpnet 768-d) | “hãng bán chạy” ↔ `brand`, semantic đa ngôn ngữ | tên kỹ thuật hiếm trong corpus huấn luyện |

`should + minimum_should_match=1` → 1 doc match **bất kỳ** nhánh nào đều được trả về,
OpenSearch tự xếp hạng theo `_score` tổng.

#### Boost trong BM25

```
"table_name^3"   → match tên bảng có trọng số ×3
"column_name^2"  → match tên cột có trọng số ×2
"description"    → trọng số mặc định ×1
```

→ User gõ đúng tên bảng/cột thì rank cao tự nhiên.

#### Embedding model

`opensearch/embedder.py`:
- Model: `sentence-transformers/paraphrase-multilingual-mpnet-base-v2`
- Dim: 768 (khớp `EMBEDDING_DIM` trong `opensearch/config.py`)
- Lazy load + thread-safe singleton + in-memory cache theo sha1(text)
- `normalize_embeddings=True` để cosine ≡ dot product (OpenSearch tính nhanh hơn)
- Warmup được gọi từ FastAPI lifespan để request đầu không bị nuốt 420MB load.

### 3.3 Fallback khi OpenSearch chết

`semantic_layer.retrieve()`:
- OpenSearch ping fail → trả `RetrievalContext()` rỗng, log warning.
- Embedder fail → tiếp tục với chỉ BM25 (`q_vec = None`, nhánh kNN bị skip).
- Bất kỳ index nào lỗi → empty list cho riêng index đó, các index khác vẫn chạy.

`schema_fallback=cache.get(decision.backend)` truyền vào `generate_sql()` đảm bảo
guardrails vẫn whitelist được tables ngay cả khi retrieval rỗng.

### 3.4 Tuning từ env

`ai-agent/core/settings.py`:

```python
retrieval_top_k_catalog = 8     # số catalog hits (table+column)
retrieval_top_k_docs    = 3     # số doc sections
retrieval_top_k_history = 3     # số past queries
retrieval_min_score     = 0.3   # ngưỡng filter
```

Tăng `top_k_catalog` → context dày hơn nhưng noisy hơn. Giảm `min_score` → bắt
được nhiều cột “mơ hồ liên quan” hơn.

---

## 4. AI thực sự nhận gì trong prompt? (cấu trúc cuối cùng)

Hàm `as_prompt_block()` (`semantic_layer.py` dòng 104) render context thành **markdown
gọn**. Đây là phần được nhét vào `SYSTEM_PROMPT_TEMPLATE` của SQL Writer
(`sql_writer.py` dòng 83):

```
You are the SQL Writer Agent. Generate ONE valid HiveQL query for the user's question.

## HiveQL rules (warehouse: gold.*)
1. Reference tables as `gold.<table>` (e.g. gold.fact_sales).
2. Always end with LIMIT 100 unless the query is purely aggregated.
... (8 rules, có cả mapping ngữ nghĩa tiếng Việt)
7. Vietnamese semantic mapping:
   - "bán chạy nhất" → quantity sold (SUM(quantity) hoặc COUNT(DISTINCT order_key))
   - "doanh thu" → SUM(item_total)
   - "đắt nhất" → highest unit price
   - "hiện nay" → bỏ year filter, để GROUP BY + ORDER BY xử lý
...

# Available context (retrieved from semantic layer — do not reference tables not listed here):

## Relevant tables & columns (top matches)
### fact_sales — FACT table gold.fact_sales with 35 columns and approximately 1,200,000 rows
- brand (string)  — Column brand of type string in gold.fact_sales  e.g. Apple, Samsung, Sony
- quantity (int)  — Số lượng item bán  e.g. 1, 2, 3
- item_total (decimal(12,2))  — Doanh thu cấp item

### dim_products
- product_name (string)
- category_name (string)

## Business documentation
### fact_sales — Lưu ý quan trọng
- Tránh nhầm order_total và item_total: order_total lặp trên các item của cùng order...
Example queries from docs:
```sql
SELECT brand, SUM(item_total) AS revenue, COUNT(*) AS items_sold
FROM gold.fact_sales WHERE order_year = 2026 AND order_status = 'DELIVERED'
GROUP BY brand ORDER BY revenue DESC LIMIT 10;
```

## Similar past queries (use as reference, do not copy verbatim)
- 👍 NL: top 10 brand theo doanh số 2026
  SQL: SELECT brand, SUM(item_total) ...

# Output — strict JSON only, no markdown:
{
  "sql": "SELECT ...",
  "explanation": "...",
  "tables_used": ["fact_sales", "dim_products"],
  "complexity": "low|medium|high",
  "has_date_filter": true|false,
  "aggregated": true|false
}
```

**Quan sát then chốt**:
- LLM **không** thấy 50 bảng. Nó chỉ thấy ~5 bảng × vài cột top match.
- LLM **không** thấy DDL. Nó thấy mô tả + sample values → đoán semantic dễ hơn.
- LLM **được “nhắc”** business rule qua doc section đã retrieve.
- LLM **được few-shot** qua past query thumbs-up.
- LLM **bị ép JSON** thuần — parser dùng `resp.parse_json()` (`sql_writer.py` dòng 182).

---

## 5. Supervisor — hiểu “loại câu hỏi” trước khi retrieval

File: `ai-agent/agents/supervisor.py`.

### 5.1 4 intent

| Intent | Định nghĩa | Hành động tiếp |
|---|---|---|
| `DATA_QUERY` | User muốn data: doanh thu, top-N, trends, so sánh | Vào pipeline retrieval + SQL writer |
| `SCHEMA_INFO` | User hỏi “có bảng gì?”, “bảng X có cột nào?” | Return từ `SchemaCache` — KHÔNG gọi SQL writer |
| `FOLLOWUP` | Tiếp nối câu hỏi trước (“so với tháng trước thì sao”) | Inject `conversation_history[-6:]` vào messages, `cacheable=False` |
| `OUT_OF_SCOPE` | Lời chào, đùa, ngoài domain | Trả lời lịch sự, kết thúc |

### 5.2 Backend selection

```
hive_gold       → analytical Gold star schema (mặc định, 95% câu hỏi analytics)
postgres_bronze → ERP OLTP — chỉ khi user yêu cầu live/realtime/current
```

Backend được chọn ngay tại supervisor vì:
- Quyết định **schema nào retrieve** (`backend_filter=` truyền vào `semantic_layer.retrieve()`).
- Quyết định **dialect** SQL (HiveQL vs PostgreSQL — 2 set rule khác nhau).
- Quyết định **agent thực thi** (`hive_agent` vs `postgres_agent`).

### 5.3 Robustness

`supervisor.py` dòng 89-100:
- LLM trả non-JSON → fallback `(DATA_QUERY, hive_gold)`.
- LLM call throw exception → fallback y vậy.
- Question rỗng → trả `OUT_OF_SCOPE` không gọi LLM (tiết kiệm token).

Supervisor dùng **model rẻ hơn** (Haiku) so với SQL Writer (Sonnet). Cấu hình tại
`settings.py`:

```python
anthropic_model_supervisor = "claude-haiku-4-5-20251001"
anthropic_model_sql_writer = "claude-sonnet-4-6"
```

---

## 6. FOLLOWUP — agent hiểu ngữ cảnh hội thoại như thế nào?

Khi `intent = FOLLOWUP`:

1. **Supervisor** đã thấy 4 turn gần nhất (`supervisor.py` dòng 81-86) → phán đoán
   đây là continuation.
2. **SQL Writer** prepend 6 turn gần nhất vào `messages` (`sql_writer.py` dòng 157-163).
3. **Cache LLM tắt** (`cacheable=False`) vì context thay đổi mỗi lần.
4. Retrieval **vẫn chạy** trên câu hỏi mới — nhưng câu hỏi mới có thể quá ngắn
   (“thế còn quý 3?”), nên semantic layer có thể không match đủ context. Lúc này
   LLM dựa vào lịch sử trong `messages` để nhớ subject (“brand bán chạy”).

→ Hạn chế: nếu user follow-up xa khỏi topic, retrieval miss. Tương lai có thể
re-formulate query: ghép hỏi mới với hỏi cũ trước khi retrieve.

---

## 7. Guardrails — tại sao cần dù LLM đã hiểu schema?

File: `ai-agent/core/guardrails.py`. Mọi SQL phải qua `validate_sql()` trước khi
chạm warehouse.

| Check | Lý do |
|---|---|
| Forbidden keywords (`DELETE`, `UPDATE`, `DROP`, ...) | Agent chat KHÔNG được phép DDL/DML |
| Single statement (chặn `;` injection) | Chống smuggling lệnh thứ 2 |
| `MAX_JOINS = 6`, `MAX_UNION = 3` | Chống pathological N-way join làm sập cluster |
| `LIMIT 100` cho non-aggregation | Chống full scan |
| Whitelist tables (`known_tables`) | Chặn hallucination cấp bảng (`SELECT * FROM nonexistent_table`) |
| PII column denial (gated bởi `allow_pii` flag) | Tuân thủ privacy — email, phone, address mặc định warn |

`known_tables` được merge từ:
1. Catalog hits đã retrieve (cao tin cậy).
2. Schema cache fallback (đảm bảo coverage).

→ Nếu LLM cố hallucinate bảng `monthly_revenue_summary` (không tồn tại) → guardrail
reject → response trả `validation_error: unknown_table` và `query_log` ghi
`status: guardrail_fail`.

---

## 8. Vòng feedback — agent học từ chính mình

```
User hỏi → SQL chạy thành công → log_query(status="success")
                                          │
                                          ▼
                                  query_log index
                                          │
                                          ▼
User cấp thumbs-up → update_feedback() → user_thumbs_up=true
                                          │
                                          ▼
Lần sau user hỏi câu tương tự → kNN match cao → đẩy SQL cũ vào prompt
                                          │
                                          ▼
LLM dựa vào pattern thành công → SQL mới chính xác hơn
```

Lưu ý: history search **chỉ lấy `status: success`** và **loại bỏ `user_thumbs_up == false`**
(xem `as_prompt_block()` dòng 149: `if h.user_thumbs_up is not False`).

---

## 9. Mapping file ↔ trách nhiệm

| Layer | File | Trách nhiệm |
|---|---|---|
| Routing | `ai-agent/routers/query.py` | Orchestrator SSE, gọi từng step |
| Routing brain | `ai-agent/agents/supervisor.py` | Phân intent + chọn backend |
| Semantic layer | `ai-agent/core/semantic_layer.py` | Hybrid retrieval (BM25+kNN), render prompt block |
| Schema RAM | `ai-agent/core/schema_cache.py` | Load full schema 1 lần, dùng cho guardrails + SCHEMA_INFO |
| SQL gen | `ai-agent/agents/sql_writer.py` | Build prompt theo dialect, parse JSON |
| Safety | `ai-agent/core/guardrails.py` | Validate forbidden keywords, table whitelist, PII |
| Execute | `ai-agent/agents/retrieval/{hive,postgres}_agent.py` | Chạy SQL trên warehouse tương ứng |
| Embedding | `opensearch/embedder.py` | Encode text → 768-d vector (mpnet multilingual) |
| Catalog index | `opensearch/indexers/catalog_indexer.py` | Hive `DESCRIBE` → finch_catalog docs |
| Docs index | `opensearch/indexers/docs_indexer.py` | Parse markdown → table_docs |
| Query log | `opensearch/query_logger.py` | Ghi telemetry mỗi request |
| Business knowledge | `opensearch/docs/*.md` | Tri thức nghiệp vụ con người viết tay |
| Index mappings | `opensearch/mappings/*.json` | Khai báo BM25 analyzer + kNN HNSW |

---

## 10. Vận hành — kiểm tra agent có “hiểu” đúng không

### 10.1 Trace retrieval đang trả gì

Bật log level DEBUG, sau đó:

```powershell
# Hỏi 1 câu qua API
curl -N -X POST http://localhost:8000/api/query/ask `
     -H "Content-Type: application/json" `
     -d '{"question":"top 5 brand bán chạy nhất quý 4"}'
```

SSE stream sẽ phát từng step:
```
{"type":"step","step":"supervisor","data":{"intent":"DATA_QUERY","backend":"hive_gold",...}}
{"type":"step","step":"metadata","data":{"catalog":8,"docs":2,"history":1,"tables":["fact_sales","dim_products"]}}
{"type":"step","step":"sql_writer","data":{"sql":"SELECT brand, SUM(quantity) ...","complexity":"low"}}
{"type":"step","step":"execution","message":"Trả về 5 hàng trong 245ms"}
{"type":"result","data":{...}}
```

### 10.2 Re-index khi schema Gold đổi

```powershell
# Sau khi Gold pipeline chạy thêm bảng mới hoặc đổi cột
python -m opensearch.indexers.catalog_indexer
# Re-embed nếu có doc nghiệp vụ mới
python -m opensearch.indexers.docs_indexer
# Backfill embedding cho doc cũ
python -m opensearch.indexers.embed_backfill
```

### 10.3 Kiểm tra index OpenSearch

```powershell
curl http://localhost:9200/finch_catalog/_count
curl http://localhost:9200/table_docs/_count
curl http://localhost:9200/query_log/_count
```

### 10.4 Debug retrieval cho 1 câu cụ thể

Có thể chạy semantic layer riêng:

```python
from ai_agent.core.semantic_layer import semantic_layer
ctx = semantic_layer.retrieve("top brand bán chạy nhất")
print(ctx.as_prompt_block())  # đây CHÍNH XÁC những gì LLM sẽ thấy
```

---

## 11. Những điểm dễ hiểu nhầm — checklist tránh sai

1. **Schema cache ≠ prompt context**. Cache phục vụ guardrails; prompt chỉ chứa
   kết quả retrieval. Đừng cố nhét full cache vào prompt — sẽ phá kiến trúc.

2. **BM25 không “hiểu” tiếng Việt** — nó chỉ tokenize + lowercase. Câu hỏi “hãng nào
   bán chạy” match được `dim_products.brand` là **nhờ kNN**, không phải BM25.
   Nếu OpenSearch không có kNN plugin → tiếng Việt sẽ kém hẳn.

3. **`sample_values` cực kỳ quan trọng**. Khi user hỏi “đơn hàng đã giao”,
   BM25 match `sample_values: ['DELIVERED', 'PENDING', ...]` của cột `order_status`
   → LLM biết phải filter `order_status = 'DELIVERED'`.

4. **Doc markdown đóng vai trò “system designer”** — gặp business rule không thể
   suy từ DDL (như “không SUM order_total”), phải viết vào `opensearch/docs/*.md`,
   không hard-code vào prompt.

5. **`query_log` là kho học tập** — nếu agent hay sinh SQL sai cùng pattern, hãy:
   1. Cho thumbs-down để loại khỏi retrieval.
   2. Thêm 1 example đúng vào `opensearch/docs/<table>.md` mục “SQL examples”.
   3. Re-index docs.

6. **PII flag** trong catalog quan trọng cho compliance. `catalog_indexer.py`
   tag heuristic theo keyword (`email`, `phone`, `address`, ...). Cần audit lại
   thủ công cho production.

7. **Hive subquery limitation** đã được nhồi vào prompt SQL Writer rule #9 — bài
   học từ vụ `WHERE order_year = (SELECT MAX(order_year) FROM ...)` fail.
   Rule này nhắc LLM tránh hẳn pattern.

8. **`cacheable=True` cho prompt cache** — supervisor + SQL writer mặc định bật.
   FOLLOWUP tắt vì context phụ thuộc messages. Khi đổi prompt template, cache
   tự invalidate (vì prompt khác = key khác).

---

## 12. TL;DR — 1 đoạn dành cho onboarding nhanh

> AI Agent dùng **2 lớp hiểu schema song song**:
> (1) Schema cache trong RAM nắm toàn bộ tables — phục vụ guardrails reject SQL
>     hallucination + trả lời SCHEMA_INFO trực tiếp;
> (2) **Semantic layer trên OpenSearch** với 3 index (`finch_catalog` = metadata,
>     `table_docs` = tri thức nghiệp vụ, `query_log` = lịch sử) được **hybrid retrieval**
>     (BM25 + kNN 768-d mpnet) để **chỉ kéo top-K bảng/cột/doc liên quan** vào prompt.
> LLM không bao giờ nhìn full schema; nó nhìn 1 block markdown ~5 bảng × vài cột
> có sample values + business doc + 1-3 past query thành công làm few-shot. SQL
> sinh ra **bắt buộc qua guardrails** (forbidden keywords, table whitelist, LIMIT,
> PII gate) trước khi tới warehouse. Mỗi lần chạy đều ghi `query_log` → vòng sau
> retrieval có thêm few-shot tốt hơn (feedback loop).
