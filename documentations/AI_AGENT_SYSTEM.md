# AI_AGENT_SYSTEM.md — Natural-Language Query Layer

> **Scope**: the FastAPI service in `ai-agent/main.py`, its prompt design,
> the CLI orchestration in `cli.py`, and how clarifying questions are auto-generated.

---

## 1. Service Inventory

| Service | File | Purpose |
|---|---|---|
| FastAPI app | `ai-agent/main.py` | Hosts `/query`, `/health`, `/schema` endpoints |
| LangChain wiring | inline | `SQLDatabase`, `Ollama` (currently — to be swapped), `QuerySQLDataBaseTool` |
| CLI | `cli.py` | Click-based; calls AI Agent and platform APIs |
| Prompt templates | inline | `SQL_PROMPT` |
| Container | `ai-agent/Dockerfile` | Python 3.11-slim + uvicorn |

> **Current state**: `ai-agent/main.py` still references `langchain_community.llms.Ollama`. The user has indicated a third-party LLM provider will be chosen later (OpenAI / Anthropic / Google / Groq). The provider abstraction exists in `.env.example` (`LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL`) — the code will be swapped when a choice is made.

---

## 2. Endpoint Catalog (current implementation)

| Method | Path | Body / Query | Purpose |
|---|---|---|---|
| GET  | `/`        | — | liveness ping |
| GET  | `/health`  | — | reports model, ollama_host, list of usable tables |
| GET  | `/schema`  | — | returns `db.get_table_info()` (full DDL) |
| POST | `/query`   | `{question: str}` | NL→SQL→result |

The `/query` endpoint is the only real workhorse. The CLI extends this with `/clarify` and `/chat` semantics by composing multiple `/query` calls and wrapping local logic — but those endpoints are not yet present in `main.py`. The intended shape (per the now-rolled-back rewrite plan) was:

| POST | `/query`   | `{question, role, session_id?, context?}` | NL→SQL→result + auto follow-up questions |
| POST | `/clarify` | `{question, role, num_questions}` | generate clarifying questions only |
| POST | `/chat`    | `{message, role, session_id?}` | multi-turn |

---

## 3. Prompt Architecture (current `main.py`)

```python
SQL_PROMPT = PromptTemplate.from_template("""
You are a PostgreSQL expert. Given the database schema below, write ONLY a valid SQL SELECT query.
Do NOT explain anything. Do NOT write English sentences. Output ONLY the raw SQL query.

Database schema:
{schema}

Question: {question}

SQL Query (SELECT only, no markdown, no explanation):
""")
```

Notable design decisions:
- **Schema injected via `db.get_table_info()`** — LangChain serializes DDL + 3 sample rows per table. With 6 tables (customers/orders/products/order_items/payments/categories), this is ~3-5 KB. Fits well within any modern LLM context window.
- **No few-shot examples** — relies on instruction-following + dialect cue ("PostgreSQL").
- **`temperature=0`** — deterministic output for reproducible SQL.
- **No multi-turn memory** — each `/query` is independent.

### Recommended prompt extensions (when LLM provider is wired)

When swapping to a hosted provider, prompt this 3-section structure:

```
[SYSTEM]
You are a {role}-context SQL analyst. {role_specific_guardrails}.

[SCHEMA]
{db.get_table_info()}

[USER]
{question}
{optional clarifying-answer context}
```

Roles & guardrails (already drafted in earlier work):

| Role | Guardrail |
|---|---|
| `business_owner` | "Prefer aggregates, %, YoY comparisons. Tables: fact_sales, dim_customers, dim_products." |
| `staff` | "Prefer detailed records, status filters. Tables: orders, shipping, payments." |
| `customer` | "ALWAYS filter by the provided customer_id. Never return rows from other customers." |

The customer guardrail is **especially important** for security — see `SECURITY_REVIEW.md`.

---

## 4. SQL Sanitization Pipeline

The current `/query` flow in `main.py`:

```
question
  ↓ check DANGEROUS_KEYWORDS in question.upper()
  ↓ build prompt with schema
  ↓ llm.invoke(prompt) → raw_sql
  ↓ strip markdown fences (```sql ... ```)
  ↓ keep lines starting with SELECT/WITH/FROM/WHERE/GROUP/ORDER/HAVING/LIMIT/(
  ↓ append "LIMIT 100" if missing
  ↓ check DANGEROUS_KEYWORDS in sql.upper()
  ↓ QuerySQLDataBaseTool.run(sql)
  ↓ return {question, sql, result}
```

`DANGEROUS_KEYWORDS = ["DROP","DELETE","UPDATE","INSERT","TRUNCATE","ALTER"]`

### Sanitizer weaknesses (must fix before production)

1. **`if keyword in question.upper()`** — substring match. Question "What was DELETED yesterday?" would 403, but "Show me the deletes log" would not. Worse, an SQL keyword embedded in a column name (`update_history`) inside an SQL response would trigger 403.

2. **No SQL parsing** — the regex line filter is structural string-matching. A model that emits `SELECT ... FROM users; DROP TABLE users;--` may slip through if the second statement's leading word is filtered out only by line-prefix logic. (In practice, the keyword check before execution catches this — but a true SQL parser like `sqlparse` is the right tool.)

3. **No schema scoping** — model can `SELECT * FROM pg_catalog.pg_user`. Best practice: use a dedicated read-only PG role with `GRANT SELECT ON specific_tables`.

4. **`LIMIT 100` is appended unconditionally**, even when the model already wrote `LIMIT 5`. Result: `LIMIT 5 LIMIT 100;` — invalid SQL. Bug.

---

## 5. Clarifying Question Generation (designed flow)

Although not yet in `main.py`, the design for auto-generated clarifying questions is:

```
POST /query {question, role}
   ↓
LLM call #1 — CLARIFY_PROMPT(question, role, schema, num=4)
   → LLM returns JSON array ["Q1?", "Q2?", "Q3?", "Q4?"]
   ↓
LLM call #2 — SQL_PROMPT(question, role, schema, optional context)
   → LLM returns raw SQL
   ↓
execute SQL → result rows
   ↓
LLM call #3 — SUMMARY_PROMPT(question, sql, result, role)
   → LLM returns 2-3 sentence summary
   ↓
return {question, sql, result, summary, clarifying_questions[]}
```

**Three LLM calls per query** is the cost. With prompt caching (Anthropic SDK supports it on system + schema portions), calls 2 and 3 can hit cache for the schema chunk and be much cheaper. For OpenAI, manual context-replay is needed.

**CLI side** (in `cli.py ask`):
```
1. POST /clarify → display the 4 questions to user
2. user types answers (or hits Enter to skip)
3. POST /query with context = user's answers
4. display result + summary + follow-up questions
```

**Why two-stage (`clarify` then `query`)?** Single-stage forces the user to wait through 3 LLM calls before seeing anything. Two-stage shows the questions immediately (after 1 LLM call), letting the user refine before paying for the SQL roundtrip.

---

## 6. Session & Memory

The current `main.py` is **stateless**. The earlier rewrite plan included an in-memory `sessions` dict keyed by `session_id`. Limitations of in-memory:

- Lost on restart
- Single-instance only (cannot scale horizontally)
- No TTL → unbounded memory growth

**Production fix**: use Redis or PostgreSQL for session state, keyed by `session_id`, with rolling-window of the last N turns.

**RAG potential**: not implemented. There is no vector DB, no embeddings, no retrieval step. The schema fits in-prompt so it isn't needed for SQL generation. RAG would become useful for:
- Looking up business definitions ("What does 'churn' mean to us?")
- Finding past similar questions (cache layer for common queries)
- Routing role-specific glossaries

---

## 7. Tool Calling — Currently None

The current implementation is **not** an "agent" in the modern tool-calling sense. It's a **prompt chain**:

```
prompt → LLM → SQL string → SQL execute → result string → return
```

There is no:
- Function/tool calling (`anthropic.messages.create(tools=...)`)
- ReAct loop
- Self-correction (LLM doesn't see the SQL error and retry)
- Multi-step planning

Adding tool calling would let the LLM:
- Inspect schema (`describe_table`)
- Run intermediate queries to disambiguate
- Validate SQL before executing
- Refine if execution fails

This is the natural next step once a provider is chosen — Anthropic's tool use API is well-suited.

---

## 8. Hallucination Prevention

Current safeguards:
- ✓ `temperature=0` reduces drift
- ✓ Strict prompt: "Output ONLY the raw SQL"
- ✓ Schema-grounded: model sees actual DDL
- ✓ Post-LLM regex filters markdown / English

Gaps:
- ✗ No verification that referenced tables/columns exist
- ✗ No validation that `JOIN` keys are real FKs
- ✗ Model can invent column names; failure surfaces only as a Postgres error string in the response

**Recommended**: parse the generated SQL with `sqlparse`, extract table/column references, validate against `db.get_usable_table_names()` and `inspect(engine).get_columns(table)`. Reject and re-prompt on mismatch.

---

## 9. Context Window Management

Schema dump = ~3 KB. Question + role = ~200 tokens. Result row dump for summary = capped at 2000 chars in the planned `SUMMARY_PROMPT`. So:

| Provider window | Headroom |
|---|---|
| GPT-4o (128k) | enormous |
| Claude Sonnet 4.6 (200k or 1M) | enormous |
| Gemini 1.5 Pro (1M / 2M) | enormous |
| Groq Llama 3 70B (8k) | tight — schema alone is 3k+, careful with result size |

For **Groq's 8k context**, prompt-truncate the schema to only relevant tables (use a router LLM call first, or simple keyword match `if "order" in question: include orders+order_items only`).

---

## 10. Failure Modes & Fallbacks

| Failure | Current behavior | Recommended |
|---|---|---|
| LLM down | exception → caught → `error` field in response | retry once + circuit-breaker |
| LLM returns invalid SQL | execute fails → exception caught → error returned | parse error, send back to LLM with error context, retry once |
| LLM returns dangerous SQL | 403 raised pre-execute | log to security audit |
| DB down | exception caught → error returned | retry with exp. backoff for transient errors only |
| Empty result | returned as `"[]"` | summary path should detect and say "no rows match" |
| Result too big | `result[:2000]` truncation in summary prompt only | also truncate at API response level (`result[:50000]`) and include `truncated: true` flag |

---

## 11. Multi-LLM Routing (future)

When two or more providers are wired, a routing strategy could be:

```python
if question_complexity == "simple":
   model = "claude-haiku-4-5"      # cheap, fast
elif involves_aggregation:
   model = "gpt-4o"                # strong reasoning
elif customer_role:
   model = "claude-sonnet-4-6"     # strong instruction-following for guardrails
```

Or a much simpler "primary + fallback" pattern:
1. Try Claude Sonnet 4.6 (best quality)
2. On 5xx / timeout, fallback to GPT-4o-mini

---

## 12. Embeddings & Vector Search — Not Implemented

There is **no** vector DB (no Chroma, Pinecone, pgvector, FAISS). The schema fits in-prompt and the queries are SQL-typed, so vector search isn't needed for the core use case.

If the user wants to add a "search by description" feature for products (e.g., "show me red leather wallets" against the `products.description` text column), the right addition is:
- pgvector extension on PostgreSQL (`postgres:15` supports it via the `pgvector/pgvector:pg15` image)
- embedding job in the Spark Silver pipeline that adds `description_embedding` column
- AI agent gains a `vector_search()` tool

---

## 13. Observability

Current implementation has:
- ✓ FastAPI's default access log (uvicorn)
- ✗ No structured logging
- ✗ No request tracing (no OpenTelemetry)
- ✗ No prompt/response logging (critical for prompt regression detection)
- ✗ No token usage metering
- ✗ No cost tracking

**Bare minimum to add**:
1. Structured JSON logs with `request_id`, `session_id`, `model`, `tokens_in`, `tokens_out`, `sql_executed`, `result_rows`, `latency_ms`
2. Persist a sample (10%) of `(question, sql, result, summary)` to a `query_audit` table for offline review and regression testing

---

## 14. Concrete File-Reference Map

| Artifact | File:Line |
|---|---|
| FastAPI app declaration | `ai-agent/main.py:9` |
| DB connection | `ai-agent/main.py:18-22` |
| LLM init | `ai-agent/main.py:24-28` |
| `DANGEROUS_KEYWORDS` list | `ai-agent/main.py:30` |
| SQL prompt | `ai-agent/main.py:32-43` |
| `/query` endpoint | `ai-agent/main.py:60-89` |
| Markdown stripper | `ai-agent/main.py:69-72` |
| `LIMIT 100` injection | `ai-agent/main.py:79-80` |
| CLI `ask` command | `cli.py` (planned) |
| CLI `clarify` command | `cli.py` (planned) |

The CLI (`cli.py`) currently focuses on platform operations (status / debezium / pipeline / kafka) — the `ask` / `chat` end-user commands will be added once the LLM provider is chosen.
