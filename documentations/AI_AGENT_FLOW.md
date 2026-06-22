# Luồng xử lý AI Agent — từ UI đến kết quả

> Tài liệu kỹ thuật mô tả chi tiết từng bước trong luồng NL→SQL của DataFinch, kèm giải thích vai trò từng file trong folder `ai-agent/`. Phục vụ developer / data engineer cần hiểu sâu kiến trúc hoặc debug.

**Phiên bản:** 2.0
**Last updated:** 2026-05-26
**Folder bao quát:** `ai-agent/` (FastAPI backend)

---

## Mục lục

1. [Tổng quan luồng end-to-end](#1-tổng-quan-luồng-end-to-end)
2. [Cây thư mục ai-agent/](#2-cây-thư-mục-ai-agent)
3. [Stage 1 — UI layer gửi câu hỏi](#stage-1--ui-layer-gửi-câu-hỏi)
4. [Stage 2 — FastAPI router nhận request](#stage-2--fastapi-router-nhận-request)
5. [Stage 3 — Supervisor phân loại intent](#stage-3--supervisor-phân-loại-intent)
6. [Stage 4 — Retrieval lấy context từ semantic layer](#stage-4--retrieval-lấy-context-từ-semantic-layer)
7. [Stage 5 — SQL Writer sinh câu lệnh SQL](#stage-5--sql-writer-sinh-câu-lệnh-sql)
8. [Stage 6 — Guardrails kiểm tra an toàn](#stage-6--guardrails-kiểm-tra-an-toàn)
9. [Stage 7 — Executor thực thi SQL](#stage-7--executor-thực-thi-sql)
10. [Stage 8 — Stream kết quả về UI](#stage-8--stream-kết-quả-về-ui)
11. [Giải thích từng file](#11-giải-thích-từng-file)
12. [Cách debug khi gặp lỗi](#12-cách-debug-khi-gặp-lỗi)

---

## 1. Tổng quan luồng end-to-end

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  USER (browser)                                                         │
│      │                                                                  │
│      │ gõ câu hỏi: "Top 5 khách hàng đặt nhiều đơn nhất"                 │
│      │                                                                  │
│      ▼                                                                  │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  FRONTEND  (Next.js — datafinch-web)                           │     │
│  │  /app/ask  →  lib/api.ts: askQuery()                           │     │
│  │  POST http://localhost:8000/api/query/ask  (SSE)               │     │
│  └────────────────────────────────────────────────────────────────┘     │
│      │                                                                  │
│      │ HTTP POST (JSON body)                                            │
│      ▼                                                                  │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  FASTAPI  (ai-agent/main.py)                                   │     │
│  │  routers/query.py  →  /ask endpoint                            │     │
│  │  StreamingResponse(text/event-stream)                          │     │
│  └────────────────────────────────────────────────────────────────┘     │
│      │                                                                  │
│      ├──[1]── Supervisor                                                │
│      │       agents/supervisor.py                                       │
│      │       → LLM call → classify intent + backend                     │
│      │                                                                  │
│      ├──[2]── Retriever                                                 │
│      │       core/semantic_layer.py                                     │
│      │       → OpenSearch hybrid kNN + BM25                             │
│      │       → 3 indices: finch_catalog / table_docs / query_log        │
│      │                                                                  │
│      ├──[3]── SQL Writer                                                │
│      │       agents/sql_writer.py                                       │
│      │       → LLM call với retrieval context + dialect rules           │
│      │       → output JSON {sql, explanation, tables_used, ...}         │
│      │                                                                  │
│      ├──[4]── Guardrails                                                │
│      │       core/guardrails.py                                         │
│      │       → block DELETE/UPDATE/DROP + check LIMIT + PII             │
│      │                                                                  │
│      ├──[5]── Executor                                                  │
│      │       agents/retrieval/registry.py → dispatch                    │
│      │       ├─ hive_agent.py → core/hive_client.py (thrift)            │
│      │       └─ postgres_agent.py → core/postgres_client.py (asyncpg)   │
│      │                                                                  │
│      └──[6]── Stream SSE events back                                    │
│              data: {"type": "step", "step": "supervisor", ...}          │
│              data: {"type": "step", "step": "metadata", ...}            │
│              data: {"type": "step", "step": "sql_writer", ...}          │
│              data: {"type": "step", "step": "execution", ...}           │
│              data: {"type": "result", "data": {...rows, sql, ...}}      │
│                                                                         │
│      ▼                                                                  │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │  FRONTEND nhận SSE events real-time                            │     │
│  │  → light up agent pipeline bar                                 │     │
│  │  → render result table, SQL, charts                            │     │
│  │  → user thấy "Đang xử lý → Supervisor ✓ → ... → Kết quả"       │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

Tổng latency end-to-end: 3-10 giây (Hive backend), 1-3 giây (Postgres backend).
LLM chiếm ~50-60% latency, Hive query ~30-40%.
```

---

## 2. Cây thư mục `ai-agent/`

```
ai-agent/
├── main.py                          ← FastAPI entry point + lifespan
├── __init__.py
├── ai-data-assistant.jsx            ← React component (legacy, demo)
├── static/
│   └── index.html                   ← Standalone HTML demo (test mà không cần Next.js)
├── Dockerfile                       ← Build image cho ai-agent container
├── requirements.txt                 ← Python dependencies
├── .env.example                     ← Template config (copy → .env)
│
├── routers/                         ← FastAPI HTTP endpoints
│   ├── __init__.py
│   ├── query.py                     ← POST /api/query/ask  (CORE — SSE stream)
│   ├── schema.py                    ← GET /api/schema/full (schema browser)
│   └── health.py                    ← GET /api/health, /api/health/ping
│
├── agents/                          ← Multi-agent pipeline (Finch architecture)
│   ├── __init__.py                  ← Re-export retrieval.dispatch
│   ├── supervisor.py                ← [1] classify intent + backend
│   ├── sql_writer.py                ← [3] LLM sinh SQL + augment schema
│   └── retrieval/                   ← Data Retrieval Agents
│       ├── __init__.py
│       ├── registry.py              ← dispatch(backend, sql) → hive or postgres
│       ├── hive_agent.py            ← thrift execute với Hive
│       └── postgres_agent.py        ← asyncpg execute với Postgres
│
└── core/                            ← Shared infrastructure
    ├── __init__.py
    ├── settings.py                  ← Config từ env vars (Pydantic-style dataclass)
    ├── schema_cache.py              ← Load Hive+Postgres schema lúc startup
    ├── semantic_layer.py            ← [2] OpenSearch retrieval (kNN + BM25)
    ├── guardrails.py                ← [4] Validate SQL trước khi execute
    ├── hive_client.py               ← Low-level Hive thrift wrapper + retry
    ├── postgres_client.py           ← Low-level asyncpg pool wrapper
    ├── llm_gateway.py               ← Provider-agnostic LLM call + cache
    └── llm_adapters/                ← Provider-specific implementations
        ├── __init__.py              ← Factory + registry pattern
        ├── base.py                  ← Abstract LLMAdapter + RawCompletion
        ├── anthropic_adapter.py     ← Claude (Anthropic SDK)
        ├── openai_adapter.py        ← GPT-5 / o-series (OpenAI SDK)
        └── gemini_adapter.py        ← Gemini 2.5 Flash (Google GenAI SDK)
```

---

## Stage 1 — UI layer gửi câu hỏi

### Khi user nhấn "Gửi" trong chat

**Frontend file:** `datafinch-web/app/app/ask/page.tsx`

```typescript
// User input
const question = "Top 5 khách hàng đặt nhiều đơn nhất";

// Call SSE stream
for await (const event of askQuery(question, history)) {
  // event.type === "step" | "result" | "error"
}
```

**Helper:** `datafinch-web/lib/api.ts`

```typescript
const res = await fetch(`${API_BASE}/api/query/ask`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    question,
    conversation_history: [
      { role: "user", content: "..." },
      { role: "assistant", content: "..." }
    ],
    session_id: "default",
    user_id: null,
    allow_pii: false,
  }),
});
const reader = res.body.getReader();   // Đọc stream SSE
```

**Request body schema** (định nghĩa ở `routers/query.py`):
- `question` (string, required) — câu hỏi tiếng Việt
- `conversation_history` (array, optional) — context cho FOLLOWUP intent
- `session_id` (string, optional) — track session để log
- `user_id` (string, optional) — RBAC sau này
- `allow_pii` (boolean, default false) — nếu false, PII columns sẽ bị mask

---

## Stage 2 — FastAPI router nhận request

### File: `ai-agent/routers/query.py`

**Endpoint:** `POST /api/query/ask`

```python
@router.post("/ask")
async def ask(request: QueryRequest):
    return StreamingResponse(
        _run(request),                       # async generator yielding SSE events
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",       # disable nginx buffering
            "Connection": "keep-alive",
        },
    )
```

**`_run()` là async generator** orchestrate pipeline 5 stages:

```python
async def _run(req: QueryRequest):
    # SSE helper: chuyển dict → "data: {...}\n\n"
    def _sse(d): return f"data: {json.dumps(d, ensure_ascii=False)}\n\n"

    # [1] Supervisor
    yield _sse({"type": "step", "step": "supervisor", "status": "running"})
    decision = await classify(question, conversation_history)
    yield _sse({"type": "step", "step": "supervisor", "status": "done", "data": ...})

    # Terminal intents
    if decision.intent == "OUT_OF_SCOPE":
        yield _sse({"type": "result", "data": {"explanation": "Out of scope..."}})
        return
    if decision.intent == "SCHEMA_INFO":
        schema = cache.get(decision.backend)
        yield _sse({"type": "result", "data": {"schema_info": schema}})
        return

    # [2-3] Retrieval + SQL Writer (combined in generate_sql)
    yield _sse({"type": "step", "step": "metadata", "status": "running"})
    sql_result, retrieval_ctx = await generate_sql(...)
    yield _sse({"type": "step", "step": "metadata", "status": "done", "data": {...}})

    yield _sse({"type": "step", "step": "sql_writer", "status": "running"})
    # ... yields done with SQL + warnings

    # [4-5] Guardrails đã validate trong generate_sql; nếu fail → return error
    if not sql_result.valid:
        yield _sse({"type": "error", "message": sql_result.validation_error})
        return

    # [6] Execute
    yield _sse({"type": "step", "step": "execution", "status": "running"})
    exec_result = await retrieval_dispatch.dispatch(decision.backend, sql_result.sql)
    yield _sse({"type": "step", "step": "execution", "status": "done"})

    # [7] Log telemetry to OpenSearch query_log
    query_id = log_query(...)

    # [8] Final result
    yield _sse({"type": "result", "data": {sql, rows, columns, exec_ms, total_ms, ...}})
```

---

## Stage 3 — Supervisor phân loại intent

### File: `ai-agent/agents/supervisor.py`

**Vai trò:** 1 LLM call (nhỏ, nhanh — Haiku/GPT-5-mini) để **phân loại** + **chọn backend** trong cùng 1 lần.

**Output:**

```python
@dataclass
class SupervisorDecision:
    intent:     str   # DATA_QUERY | SCHEMA_INFO | FOLLOWUP | OUT_OF_SCOPE
    backend:    str   # hive_gold | postgres_bronze
    confidence: float # 0-1
    reasoning:  str   # 1-line explanation
```

**4 loại intent:**

| Intent | Mô tả | Action |
|---|---|---|
| `DATA_QUERY` | Hỏi dữ liệu — top, sum, count, trend | Đi tiếp pipeline |
| `SCHEMA_INFO` | "Có bảng nào?", "Cột nào?" | Return từ `schema_cache` luôn, skip LLM |
| `FOLLOWUP` | "Còn Q2 thì sao?" — kế tiếp câu trước | Inject conversation_history vào prompt |
| `OUT_OF_SCOPE` | Chitchat, hỏi thời tiết, không liên quan data | Return polite refuse |

**2 backends:**

- `hive_gold` — analytical (80% câu hỏi). Aggregate, top-K, trend theo thời gian.
- `postgres_bronze` — operational realtime. Stock, đơn pending, status hiện tại.

**Special rules trong prompt:**

```
- "tồn kho" / "stock" / "còn hàng" → BẮT BUỘC postgres_bronze
  (Hive Gold không có stock_quantity)
- "live" / "hiện tại" / "vừa rồi" → ưu tiên postgres_bronze
- Còn lại → hive_gold (mặc định)
```

**Khi LLM fail** (timeout, JSON malformed): fallback về `(DATA_QUERY, hive_gold)` để không block user.

---

## Stage 4 — Retrieval lấy context từ semantic layer

### File: `ai-agent/core/semantic_layer.py`

**Vai trò:** Tìm các bảng/cột/business docs **liên quan** đến câu hỏi từ OpenSearch — KHÔNG dump full schema.

### 3 OpenSearch indices được query

| Index | Nội dung | Top-K mặc định |
|---|---|---|
| `finch_catalog` | Table + column metadata, sample values, PII flag | 8 |
| `table_docs` | Markdown business docs (1 doc/section × 10 tables = 50 docs) | 3 |
| `query_log` | Past queries với thumbs up/down → "similar past query" hints | 3 |

### Hybrid retrieval: kNN + BM25

```python
def _build_hybrid_query(question, q_vec, text_fields, size, extra_filter):
    return {
        "size": size,
        "query": {
            "bool": {
                "should": [
                    # BM25 (keyword)
                    {"multi_match": {"query": question, "fields": text_fields, "fuzziness": "AUTO"}},
                    # kNN (semantic embedding)
                    {"knn": {"embedding": {"vector": q_vec, "k": size}}},
                ],
                "minimum_should_match": 1,
                "filter": [extra_filter] if extra_filter else []
            }
        }
    }
```

- **BM25** match khi có từ khóa overlap (vd "doanh thu" trong query → match "doanh thu" trong description)
- **kNN** match semantically (vd "thu nhập" sẽ match "revenue" qua multilingual embeddings 768-d)
- Cả 2 chạy **song song**, OpenSearch reciprocal-rank fuse top-K

### Output

```python
@dataclass
class RetrievalContext:
    catalog: list[CatalogHit]   # tables + columns matched
    docs:    list[DocHit]        # business docs matched
    history: list[HistoryHit]    # similar past queries

    def as_prompt_block(self) -> str:
        # Render thành Markdown để inject vào prompt SQL Writer
        ...
```

### Embedder

File: `opensearch/embedder.py` (project root, được import từ ai-agent)

- Model: `sentence-transformers/paraphrase-multilingual-mpnet-base-v2`
- Dimension: 768
- Multilingual (English + Vietnamese)
- Lazy-load 1 lần lúc startup (warmup ở lifespan)

---

## Stage 5 — SQL Writer sinh câu lệnh SQL

### File: `ai-agent/agents/sql_writer.py`

**Vai trò:** LLM mạnh hơn (Sonnet / GPT-5 / Gemini Pro) nhận context + sinh SQL.

### Flow trong `generate_sql()`

```python
async def generate_sql(question, backend, intent, conversation_history, schema_fallback):
    backend_db = "gold" if backend == "hive_gold" else "public"
    dialect    = "HiveQL" if backend == "hive_gold" else "PostgreSQL"
    rules      = _HIVE_RULES if backend == "hive_gold" else _POSTGRES_RULES

    # 1. Retrieve context từ semantic layer (luôn filter theo backend)
    ctx = semantic_layer.retrieve(question, backend_filter=backend_db)
    retrieval_block = ctx.as_prompt_block()

    # 2. SCHEMA AUGMENTATION (critical anti-hallucination)
    if schema_fallback:
        if ctx.catalog:
            # Có retrieval hits → dump full column list cho các tables hit được
            for tname in ctx.tables_in_scope():
                schema_lines.append(_format_table_schema(tname, schema_fallback[tname]))
        else:
            # Retrieval rỗng → dump TẤT CẢ tables (cold start / postgres không indexed)
            for tname in schema_fallback:
                lines.append(_format_table_schema(tname, schema_fallback[tname]))

    # 3. Build prompt
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        dialect=dialect,
        dialect_rules=rules,
        retrieval_block=retrieval_block,
    )
    messages = [{"role": "user", "content": question}]
    if intent == "FOLLOWUP":
        messages = conversation_history[-6:] + messages

    # 4. LLM call qua gateway (cache + retry + provider-agnostic)
    resp = await gateway.complete(system, messages, model, max_tokens, cacheable=...)
    parsed = resp.parse_json()   # extract JSON từ markdown / prose

    # 5. Guardrails validate
    validation = guardrails.validate_sql(parsed["sql"], known_tables, pii_columns)
    if not validation.valid:
        return SQLWriterResult(valid=False, validation_error=validation.error)

    return SQLWriterResult(sql=..., explanation=..., tables_used=..., complexity=...)
```

### Dialect rules — file gốc

11 rules cho **HiveQL** (vd: dùng `gold.<table>`, partition filter, không scalar subquery, không bịa `event_type`, không có `stock_quantity`...)
10 rules cho **PostgreSQL** (vd: order status enum, không dùng `delivered_at` trên orders...)

### Anti-hallucination layers

| Layer | Cơ chế |
|---|---|
| 1 | **Retrieval filter by backend** — không lẫn Hive tables vào Postgres query |
| 2 | **Schema augmentation** — dump FULL column list cho tables trong scope |
| 3 | **Multi-line schema format** — mỗi column 1 dòng → LLM không merge giữa tables |
| 4 | **Dialect rules cấm fabricate** — explicit liệt kê column KHÔNG có |
| 5 | **Guardrails post-validation** — reject SQL reference table không tồn tại |

---

## Stage 6 — Guardrails kiểm tra an toàn

### File: `ai-agent/core/guardrails.py`

**Vai trò:** Static analysis trên SQL **TRƯỚC khi execute** — chặn destructive operations, enforce LIMIT, PII mask.

### Các check chính

```python
def validate_sql(sql, known_tables, pii_columns, max_joins=5):
    # 1. Forbidden keywords (case-insensitive)
    FORBIDDEN = ["DELETE", "UPDATE", "INSERT", "DROP", "TRUNCATE", "ALTER", "CREATE"]
    for kw in FORBIDDEN:
        if re.search(rf"\b{kw}\b", sql, re.IGNORECASE):
            return ValidationError(f"Forbidden keyword: {kw}")

    # 2. Only SELECT/WITH
    if not re.match(r"^\s*(SELECT|WITH)\b", sql, re.IGNORECASE):
        return ValidationError("Only SELECT/WITH allowed")

    # 3. No semicolons (prevent multi-statement injection)
    if ";" in sql.rstrip(";"):
        return ValidationError("No semicolons inside query")

    # 4. JOIN cap (prevent explosion)
    join_count = len(re.findall(r"\bJOIN\b", sql, re.IGNORECASE))
    if join_count > max_joins:
        return ValidationError(f"Too many JOINs ({join_count} > {max_joins})")

    # 5. LIMIT enforcement (non-aggregated queries)
    if not has_aggregation(sql) and not has_limit(sql):
        return ValidationError("Must have LIMIT for non-aggregated query")

    # 6. PII column reference (if allow_pii=False)
    for pii_col in pii_columns:
        if re.search(rf"\b{pii_col}\b", sql):
            return ValidationError(f"PII column not allowed: {pii_col}")

    return ValidationOK(warnings=[...])
```

### Output

```python
@dataclass
class ValidationResult:
    valid: bool
    error: str | None
    warnings: list[str]   # vd "LIMIT 1000 → tự thêm vì missing"
```

---

## Stage 7 — Executor thực thi SQL

### File: `ai-agent/agents/retrieval/registry.py`

**Vai trò:** Dispatcher — route SQL đến đúng client based on backend.

```python
async def dispatch(backend: str, sql: str) -> dict:
    if backend == "hive_gold":
        return await hive_agent.execute(sql)
    elif backend == "postgres_bronze":
        return await postgres_agent.execute(sql)
    raise ValueError(f"Unknown backend: {backend}")
```

### File: `ai-agent/agents/retrieval/hive_agent.py`

```python
async def execute(sql: str) -> dict:
    try:
        result = await hive_client.execute_query(sql)
        return result   # {columns, rows, row_count, exec_ms}
    except OperationalError as e:
        log.warning("hive execute failed: %s", e)
        return {"error": str(e), ...}
```

### File: `ai-agent/core/hive_client.py`

Low-level wrapper sử dụng **pyhive** + **thrift**:

```python
def _execute_sync(sql, max_rows):
    with hive_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0].split(".")[-1] for d in cur.description]
        rows = []
        for i, row in enumerate(cur):
            if i >= max_rows: break
            rows.append(list(row))
        return cols, rows

async def execute_query(sql):
    started = time.time()
    for attempt in range(1, 4):   # 3 retries
        try:
            cols, rows = await asyncio.wait_for(
                asyncio.to_thread(_execute_sync, sql, settings.hive_max_rows),
                timeout=settings.hive_query_timeout_sec,
            )
            break
        except Exception as e:
            if _is_transient_hive_error(e) and attempt < 3:
                await asyncio.sleep(1.5 * attempt)   # 1.5s, 3s
                continue
            raise

    return {"columns": cols, "rows": rows_dict, "row_count": len(rows), "exec_ms": ...}
```

**Đặc điểm:**
- **Thrift transport** đến HiveServer2 (port 10000)
- **auth=NONE** (SASL/PLAIN với `pure-sasl`)
- **Sync execute** trong thread → wrap bằng `asyncio.to_thread`
- **Retry 3 lần** cho transient errors (MapRedTask return code 2, thrift blip)

### File: `ai-agent/core/postgres_client.py`

Low-level wrapper sử dụng **asyncpg**:

```python
_pool: asyncpg.Pool | None = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(settings.postgres_url, min_size=1, max_size=10)
    return _pool

async def execute_query(sql) -> dict:
    pool = await get_pool()
    started = time.time()
    rows = await pool.fetch(sql)   # asyncpg native — fast
    cols = list(rows[0].keys()) if rows else []
    rows_dict = [dict(r) for r in rows]
    return {"columns": cols, "rows": rows_dict, ...}
```

**Đặc điểm:**
- **Async native** (asyncpg) — không cần thread
- **Connection pool** (1-10 connections)
- Latency thường 10-100ms cho query đơn giản
- Không cần retry phức tạp như Hive

---

## Stage 8 — Stream kết quả về UI

### SSE event sequence

Mỗi event là 1 dòng `data: {...}\n\n` qua HTTP stream:

```
data: {"type": "step", "step": "supervisor", "status": "running", "message": "Phân tích yêu cầu..."}
data: {"type": "step", "step": "supervisor", "status": "done", "data": {"intent": "DATA_QUERY", "backend": "hive_gold", ...}}
data: {"type": "step", "step": "metadata", "status": "running", "message": "Tìm bảng & cột..."}
data: {"type": "step", "step": "metadata", "status": "done", "data": {"catalog": 8, "docs": 3, "history": 2}}
data: {"type": "step", "step": "sql_writer", "status": "running"}
data: {"type": "step", "step": "sql_writer", "status": "done", "data": {"sql": "SELECT ...", "complexity": "medium"}}
data: {"type": "step", "step": "execution", "status": "running"}
data: {"type": "step", "step": "execution", "status": "done", "message": "Trả về 5 hàng trong 5400ms"}
data: {"type": "step", "step": "result_formatter", "status": "done"}
data: {"type": "result", "data": {
    "intent": "DATA_QUERY",
    "backend": "hive_gold",
    "sql": "SELECT c.customer_name, COUNT(DISTINCT s.order_key) AS orders FROM gold.fact_sales s JOIN gold.dim_customers c ON s.customer_key = c.customer_key GROUP BY c.customer_name ORDER BY orders DESC LIMIT 5",
    "explanation": "Truy vấn tìm top 5 khách hàng có nhiều đơn nhất",
    "tables_used": ["fact_sales", "dim_customers"],
    "columns": ["customer_name", "orders"],
    "rows": [{"customer_name": "Vân Hoàng", "orders": 14}, ...],
    "row_count": 5,
    "exec_ms": 5400,
    "total_ms": 8600,
    "query_id": "uuid",
    "retrieval": {"catalog": 8, "docs": 3, "history": 2}
}}
```

### Frontend nhận và render

`datafinch-web/lib/api.ts`:

```typescript
const reader = res.body.getReader();
const decoder = new TextDecoder();
let buf = "";

while (true) {
  const { done, value } = await reader.read();
  if (done) break;
  buf += decoder.decode(value, { stream: true });

  const chunks = buf.split("\n\n");
  buf = chunks.pop();   // last incomplete chunk

  for (const chunk of chunks) {
    if (chunk.startsWith("data: ")) {
      yield JSON.parse(chunk.slice(6));   // async generator yield
    }
  }
}
```

`/app/ask` page:

```typescript
for await (const event of askQuery(question, history)) {
  if (event.type === "step") {
    setSteps(p => ({ ...p, [event.step]: event.status }));   // light up agent bar
  } else if (event.type === "result") {
    setResult(event.data);   // render table + SQL + chart
  }
}
```

---

## 11. Giải thích từng file

### Root files

#### `ai-agent/main.py`

**Vai trò:** Entry point FastAPI app.

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await semantic_layer.warmup()       # load embedder model (~420MB)
    await schema_cache.load()           # load Hive + Postgres schema
    yield
    # Shutdown (cleanup if needed)

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"])

app.include_router(health.router, prefix="/api/health")
app.include_router(schema.router, prefix="/api/schema")
app.include_router(query.router,  prefix="/api/query")

app.mount("/ui", StaticFiles(directory="static", html=True))
@app.get("/", include_in_schema=False)
async def root(): return RedirectResponse("/ui/")
```

**Khi nào chạy:** Lúc container startup, uvicorn pickup.

---

### `routers/` — HTTP endpoints

#### `routers/query.py`

**Vai trò:** Endpoint chính `POST /api/query/ask` — async generator stream SSE qua orchestrator `_run()` (đã giải thích Stage 2).

Cũng có `POST /api/query/feedback` cho thumbs up/down → update `query_log` index.

#### `routers/schema.py`

**Vai trò:** `GET /api/schema/full` — return toàn bộ schema từ schema_cache.

```python
@router.get("/full")
async def get_full_schema(backend: str = "hive_gold"):
    return {"schema": cache.get(backend)}
```

UI dùng cho **Schema browser** trang `/app/data`.

#### `routers/health.py`

**Vai trò:** Health check endpoints.

- `GET /api/health/ping` — lightweight, chỉ check process alive. Dùng cho Docker healthcheck.
- `GET /api/health` — aggregate: kiểm tra Hive + Postgres + OpenSearch + LLM gateway. Frontend dùng cho indicator "Backend LIVE/OFFLINE".

---

### `agents/` — Multi-agent pipeline

#### `agents/__init__.py`

Re-export `retrieval.dispatch` để router import sạch sẽ:

```python
from .retrieval import dispatch
```

#### `agents/supervisor.py`

**Vai trò:** Stage 1 — classify intent + chọn backend (đã giải thích).

Key function: `classify(question, conversation_history)`. Output: `SupervisorDecision`.

#### `agents/sql_writer.py`

**Vai trò:** Stage 5 — sinh SQL với context retrieval + guardrails (đã giải thích).

Key function: `generate_sql(question, backend, intent, conversation_history, schema_fallback)`. Output: `(SQLWriterResult, RetrievalContext)`.

Chứa 2 prompt templates lớn: `_HIVE_RULES` (11 rules) và `_POSTGRES_RULES` (10 rules).

#### `agents/retrieval/registry.py`

**Vai trò:** Dispatcher routing SQL theo backend.

```python
async def dispatch(backend: str, sql: str) -> dict:
    if backend == "hive_gold":     return await hive_agent.execute(sql)
    if backend == "postgres_bronze": return await postgres_agent.execute(sql)
    raise ValueError(f"Unknown backend: {backend}")
```

#### `agents/retrieval/hive_agent.py`

Thin wrapper trên `core/hive_client.py` — catch exceptions thành dict format cho router.

#### `agents/retrieval/postgres_agent.py`

Thin wrapper trên `core/postgres_client.py`.

---

### `core/` — Shared infrastructure

#### `core/settings.py`

**Vai trò:** Centralized config từ env vars. Tránh `os.getenv()` rải rác.

```python
@dataclass
class Settings:
    # LLM
    llm_provider: str = field(default_factory=lambda: _env("LLM_PROVIDER", "anthropic"))
    anthropic_api_key: str | None = ...
    openai_api_key: str | None = ...
    gemini_api_key: str | None = ...

    # Backends
    hive_host: str
    hive_port: int
    postgres_url: str

    # OpenSearch
    opensearch_url: str

    # Retrieval
    retrieval_top_k_catalog: int = 8
    retrieval_top_k_docs: int = 3

    # Provider-resolved properties
    @property
    def llm_model_supervisor(self) -> str:
        if self.llm_provider == "openai":    return self.openai_model_supervisor
        if self.llm_provider == "gemini":    return self.gemini_model_supervisor
        return self.anthropic_model_supervisor

settings = Settings()  # singleton
```

**Khi nào chạy:** Import 1 lần lúc startup, giá trị fix cho cả lifetime.

#### `core/schema_cache.py`

**Vai trò:** Load schema 1 lần lúc startup, lưu trong RAM, share giữa các requests.

```python
class SchemaCache:
    hive: dict[str, list[dict]]      = {}  # {table_name: [{column, type}, ...]}
    postgres: dict[str, list[dict]]  = {}

    async def load(self):
        # Load song song để giảm startup time
        hive_task     = asyncio.create_task(hive_client.list_schema())
        postgres_task = asyncio.create_task(postgres_client.list_schema())
        self.hive, self.postgres = await asyncio.gather(hive_task, postgres_task)

    def get(self, backend: str) -> dict:
        return self.hive if backend == "hive_gold" else self.postgres

cache = SchemaCache()
```

**Khi nào dùng:** Mọi câu hỏi đều dùng để `schema_fallback` truyền vào `generate_sql` → augment prompt LLM.

#### `core/semantic_layer.py`

**Vai trò:** Stage 4 — OpenSearch hybrid retrieval (đã giải thích chi tiết).

Class chính: `SemanticLayer`. Singleton `semantic_layer` instance.

#### `core/guardrails.py`

**Vai trò:** Stage 6 — validate SQL trước khi execute (đã giải thích).

Key function: `validate_sql(sql, known_tables, pii_columns)`. Output: `ValidationResult`.

#### `core/hive_client.py`

**Vai trò:** Low-level pyhive wrapper + retry logic cho transient errors (đã giải thích Stage 7).

Functions chính:
- `hive_connection()` — context manager mở thrift connection
- `list_tables(db)` — danh sách tables
- `describe_table(table, db)` — schema 1 bảng
- `list_schema(db)` — full schema dict
- `execute_query(sql)` — execute với retry + timeout

#### `core/postgres_client.py`

**Vai trò:** Low-level asyncpg wrapper với connection pool (đã giải thích Stage 7).

Functions song song hive_client để abstract đồng nhất.

#### `core/llm_gateway.py`

**Vai trò:** Provider-agnostic LLM call. Cache + retry + telemetry.

```python
class LLMGateway:
    def __init__(self):
        self._cache = _ResponseCache(max_entries=256, ttl_seconds=300)

    @property
    def provider(self) -> str:
        return get_adapter().name

    async def complete(self, *, system, messages, model=None, max_tokens=1000,
                       cacheable=True, max_retries=5) -> LLMResponse:
        adapter = get_adapter()
        model = model or adapter.default_sql_writer_model

        # Check cache (key = sha1(provider+model+system+messages))
        if cacheable:
            cached = self._cache.get(...)
            if cached: return cached

        # Retry with exponential backoff + jitter
        delay = 2.0
        for attempt in range(1, max_retries + 1):
            try:
                raw = await adapter.complete(system=system, messages=messages, ...)
                ...
                if cacheable:
                    self._cache.put(...)
                return out
            except TransientLLMError as e:
                if attempt < max_retries:
                    await asyncio.sleep(delay * random.uniform(0.75, 1.25))
                    delay *= 2.5
            except PermanentLLMError as e:
                raise

gateway = LLMGateway()  # singleton
```

**Tính năng:**
- **Cache 256 entries, TTL 300s** — same prompt → same response trong 5 phút (không tính tiền lần 2)
- **Retry 5 lần** với exponential backoff (2s → 5s → 12s → 30s) + ±25% jitter
- **TransientLLMError** retry; **PermanentLLMError** bail out ngay

#### `core/llm_adapters/base.py`

```python
@dataclass
class RawCompletion:
    text: str
    model: str
    input_tokens: int
    output_tokens: int

class TransientLLMError(Exception): pass     # 5xx, timeout, rate limit → retry
class PermanentLLMError(Exception): pass     # 4xx, auth fail → bail out

class LLMAdapter(ABC):
    name: str
    default_supervisor_model: str
    default_sql_writer_model: str

    @abstractmethod
    async def complete(self, *, system, messages, model, max_tokens) -> RawCompletion:
        raise NotImplementedError

    def is_configured(self) -> bool: ...
```

#### `core/llm_adapters/__init__.py`

Factory + lazy import registry:

```python
def _make_anthropic(): from .anthropic_adapter import AnthropicAdapter; return AnthropicAdapter()
def _make_openai():    from .openai_adapter import OpenAIAdapter; return OpenAIAdapter()
def _make_gemini():    from .gemini_adapter import GeminiAdapter; return GeminiAdapter()

_REGISTRY = {"anthropic": _make_anthropic, "openai": _make_openai, "gemini": _make_gemini}
_cache: dict[str, LLMAdapter] = {}   # singleton per provider

def get_adapter(name=None) -> LLMAdapter:
    name = (name or settings.llm_provider).lower()
    if name not in _cache:
        _cache[name] = _REGISTRY[name]()
    return _cache[name]
```

**Lý do lazy:** không cần install tất cả SDK — user dùng OpenAI thì không cần `anthropic` package.

#### `core/llm_adapters/anthropic_adapter.py`

Wrap `anthropic.AsyncAnthropic`. Đặc trưng:
- **Prompt caching** với `cache_control: ephemeral` trên system prompt (tiết kiệm ~90% cost cho repeated calls)
- **Adaptive thinking** auto-enabled cho Sonnet 4.6+ / Opus 4.6+

Error mapping:
- `APIConnectionError`, `RateLimitError`, 5xx → `TransientLLMError`
- 4xx → `PermanentLLMError`

#### `core/llm_adapters/openai_adapter.py`

Wrap `openai.AsyncOpenAI`. Đặc trưng:
- **Auto-detect reasoning models** (gpt-5*, o-series) → dùng `max_completion_tokens` + `reasoning_effort` thay vì `max_tokens` + `temperature`
- **Scrub `OPENAI_BASE_URL=""` env** trước khi init client (workaround SDK bug)

#### `core/llm_adapters/gemini_adapter.py`

Wrap `google.genai.Client`. Đặc trưng:
- Role mapping: "assistant" → "model"
- System prompt via `GenerateContentConfig(system_instruction=...)`
- Safety filter guard — extract text từ `response.candidates[].content.parts` thay vì `response.text` (sau cùng raise nếu safety blocked)

---

### `static/` và frontend artifacts

#### `ai-agent/static/index.html`

Standalone HTML test tool (không qua Next.js). Useful khi muốn test API mà chưa start frontend. Mount tại `/ui/` route.

#### `ai-agent/ai-data-assistant.jsx`

React component legacy (trước khi Next.js frontend được tách ra). Còn lưu lại để reference, sẽ remove sau khi cleanup.

---

### Config & build

#### `ai-agent/Dockerfile`

Multi-stage build:
1. Base `python:3.11-slim` + build-essential
2. `pip install -r requirements.txt`
3. Pre-download sentence-transformers model vào `/cache`
4. Copy source
5. CMD `uvicorn main:app --host 0.0.0.0 --port 8000`

#### `ai-agent/requirements.txt`

Dependencies chính:
- `fastapi==0.115.0` + `uvicorn[standard]==0.30.0`
- `anthropic==0.40.0` + `google-genai==0.8.0` + `openai==1.99.0`
- `pyhive==0.7.0` + `thrift==0.16.0` + `pure-sasl==0.6.2`
- `asyncpg==0.29.0`
- `opensearch-py==2.6.0` + `sentence-transformers==2.7.0`

#### `ai-agent/.env.example`

Template cho env vars. Copy → `.env`, fill API keys.

---

## 12. Cách debug khi gặp lỗi

### Lỗi thường gặp + nơi check

| Triệu chứng | Check file/log | Cách fix |
|---|---|---|
| **"Cannot reach backend"** trên UI | `docker logs ai-agent` | Check ai-agent container đang chạy không |
| **"LLM gateway failed after 5 attempts"** | `docker logs ai-agent \| grep llm_gateway` | Check API key, network, free tier rate limit |
| **"Invalid column reference"** | `query_log` index trong OpenSearch | LLM hallucinate — thêm rule vào `_HIVE_RULES`/`_POSTGRES_RULES` |
| **"return code 2 from MapRedTask"** | `docker logs hiveserver2` | Hive transient — retry tự fix, nếu liên tục thì check resources |
| **"relation does not exist"** | Check `tables_used` trong query_log | Supervisor chọn sai backend — update routing rule |
| **"retrieval empty"** | `curl http://localhost:9200/finch_catalog/_count` | Re-index: `docker exec -w /app ai-agent python -m opensearch.indexers.catalog_indexer` |
| **Backend OFFLINE indicator** | `curl http://localhost:8000/api/health` | Check Hive/Postgres/OpenSearch healthy |

### Bật log debug

```bash
# Sửa .env
LOG_LEVEL=DEBUG

# Restart container
docker compose restart ai-agent

# Tail logs
docker logs ai-agent -f
```

### Verify từng stage độc lập

```bash
# 1. Supervisor only — test classify
docker exec ai-agent python -c "
import asyncio
from agents.supervisor import classify
print(asyncio.run(classify('Top 5 sản phẩm')))
"

# 2. Semantic retrieval only
docker exec ai-agent python -c "
from core.semantic_layer import semantic_layer
ctx = semantic_layer.retrieve('khách hàng VIP')
print('catalog:', len(ctx.catalog), 'docs:', len(ctx.docs))
"

# 3. SQL Writer only
docker exec ai-agent python -c "
import asyncio
from agents.sql_writer import generate_sql
from core.schema_cache import cache
asyncio.run(cache.load())
r, _ = asyncio.run(generate_sql(
    question='Top 5 brands',
    backend='hive_gold',
    intent='DATA_QUERY',
    schema_fallback=cache.get('hive_gold'),
))
print(r.sql)
"

# 4. End-to-end via curl
curl -sN -X POST http://localhost:8000/api/query/ask \
  -H 'Content-Type: application/json' \
  -d '{"question":"Top 5 sản phẩm bán chạy"}'
```

### Inspect SQL được generate

Mọi câu hỏi đều được log vào OpenSearch `query_log` index. Truy vấn để xem SQL gần nhất:

```bash
curl -s -X POST "http://localhost:9200/query_log/_search" \
  -H "Content-Type: application/json" \
  -d '{"size":5,"sort":[{"created_at":{"order":"desc"}}],"query":{"match_all":{}}}' \
  | python -c "
import json, sys
hits = json.load(sys.stdin)['hits']['hits']
for h in hits:
    s = h['_source']
    print('Q:', s.get('nl_question'))
    print('  SQL:', s.get('generated_sql', '')[:200])
    print('  Status:', s.get('status'), '| Exec:', s.get('exec_ms'), 'ms')
    print()
"
```

---

## Phụ lục — Latency breakdown điển hình

Câu hỏi: **"Top 5 khách hàng đặt nhiều đơn nhất"**

| Stage | File | Latency |
|---|---|---|
| Frontend → backend HTTP | `lib/api.ts` → `routers/query.py` | ~50ms |
| Supervisor LLM | `agents/supervisor.py` → OpenAI | ~2500ms |
| Semantic retrieval | `core/semantic_layer.py` → OpenSearch | ~150ms (3 indices song song) |
| SQL Writer LLM | `agents/sql_writer.py` → OpenAI | ~3000ms |
| Guardrails | `core/guardrails.py` | <10ms |
| Hive execute | `core/hive_client.py` → HiveServer2 | ~5000ms |
| Log to query_log | `opensearch/query_logger.py` | ~30ms |
| Stream back to UI | SSE | ~10ms |
| **TOTAL** |  | **~11s** |

LLM chiếm ~50% (Supervisor + SQL Writer ~5.5s). Hive execute ~45% (MapReduce slow trong dev local mode). Có thể giảm bằng:
- Switch Hive engine sang **Spark** (3-5x nhanh hơn MR)
- Use **prompt caching** Anthropic (~90% giảm latency cho Supervisor)
- Use **GPT-5-nano** thay vì GPT-5-mini cho Supervisor (~50% giảm)

---

**Hết tài liệu.** Đây là tài liệu kỹ thuật mô tả luồng + giải thích từng file trong `ai-agent/`. Khi cần thay đổi prompt, thêm LLM provider mới, hoặc debug specific stage — đọc section liên quan ở trên.
