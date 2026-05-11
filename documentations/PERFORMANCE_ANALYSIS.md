# PERFORMANCE_ANALYSIS.md

> Where the platform will choke and what to do about it.

---

## 1. Top Bottlenecks (ranked)

| # | Hot path | Symptom under load | Why | Fix |
|---|---|---|---|---|
| 1 | Bronze ingestion is `read.format("kafka")` batch with `earliest→latest` | Hourly job grows linearly with topic size; will exceed Airflow timeout | full re-scan + overwrite every run | switch to **Structured Streaming** with checkpointing, append-mode |
| 2 | 1 partition per Kafka topic | Spark cannot parallelize Kafka reads | default partition count | `kafka-topics --alter --partitions 6` per topic; reproduce simulator with key-aware producer |
| 3 | Spark worker = 2 cores / 2 GB RAM | OOM on Silver dedup / Gold join | `SPARK_WORKER_MEMORY=2G` in compose | increase to 4-8 GB, add a second worker |
| 4 | Every Silver dedup uses `dropDuplicates(["id"])` after `filter` | Causes wide shuffle | no `repartition` before dedup | partition by hash(id) or use `Window` + `row_number` |
| 5 | Gold `fact_sales` join is 5-way LEFT JOIN | Slow for >10M order_items | broadcast joins not enforced | `broadcast(dim_customers)`, `broadcast(dim_payments)`, etc. — dims are small |
| 6 | `df.count()` called 6+ times per Silver run | Each `.count()` triggers a full job | logging convenience | cache once: `df.persist(); df.count(); ... ; df.unpersist()` |
| 7 | DLQ union iterates over 6 entities then `.union()` | Cascading shuffle | union with mismatched schemas | normalize all to a common DLQ schema, then `unionByName` |
| 8 | Spark Hive integration uses `spark.sql.warehouse.dir` over HDFS bind mount on Windows | Disk I/O 10× slower vs Linux | Docker Desktop bind mounts | use named volume on Windows, or move warehouse to S3-compatible (MinIO) |
| 9 | AI Agent recreates `SQLDatabase` per app start with full schema introspection | Cold start ~3-5s | one-time cost, but blocks startup | introspect lazily on first query, then cache |
| 10 | LangChain `db.get_table_info()` runs `SELECT * FROM table LIMIT 3` per table | Multiple roundtrips for every query | LangChain default | cache result in module-level var or Redis with 5-min TTL |
| 11 | Three LLM calls per question in planned `/query` flow | $$ + ~6-15s end-to-end | clarify + sql + summary | merge clarify + sql; or stream summary; or skip clarify by default |
| 12 | Postgres metadata DB shared with Airflow + ERP | Contention during heavy DAG runs | one Postgres instance | split out Airflow DB to its own container (already supported) |
| 13 | NiFi runs in single-node mode | Cannot scale ingestion throughput | image is single-instance | move to NiFi cluster, or replace simple flows with Kafka Connect SMTs |
| 14 | Faker simulators are single-threaded | Can saturate at ~5k inserts/min | Python GIL + sequential commit | use `execute_values` (already done) + multi-process workers |

---

## 2. Spark Job Profiles

### Bronze (`bronze_ingestion.py`)

**What it does**: reads 6 Kafka topics into a flat Bronze table, partitioned by `(_source_topic, _kafka_partition)`.

**Cost driver**: full topic scan per run.

**Numbers** (estimated for graduation-project scale):
- 100k customers + 500k orders + 1.2M order_items + 100k coupons = ~1.9M ERP events
- ~10k warehouse events
- ~20k payment events
- Total ~2M Kafka messages → 1 partition each → single executor reads serially
- Read time: ~30-60s once, write to Parquet ~30-60s
- **Bottleneck**: serial Kafka read per topic

**Fix path**:
```python
# Move to streaming
spark.readStream.format("kafka")
   .option("subscribe", "...")
   .load()
   ...
   .writeStream
   .format("parquet")
   .option("checkpointLocation", "hdfs://.../checkpoints/bronze")
   .outputMode("append")
   .start()
```

This requires a long-running streaming job, which means:
- Replace Airflow's `SparkSubmitOperator` with a one-shot start, monitored externally
- Or keep a streaming + a periodic batch reconciliation job
- Or stay batch but increase Kafka partitions and use `numPartitions` option

### Silver (`silver_transform.py`)

**What it does**: per source, parse JSON → typed columns → dedup → split into clean + dirty.

**Cost driver**:
1. `from_json()` per row × 6 schemas
2. `dropDuplicates(["id"])` does a full shuffle
3. `df.count()` calls trigger 6+ extra jobs
4. DLQ `.union()` is wide

**Numbers**: 
- ERP: ~1.9M rows → after dedup ~1.7M rows
- Warehouse: ~100k rows
- Payment: ~30k rows
- Total Silver run: ~2-5 minutes on graduation-project hardware

**Specific lines to optimize**:
- `silver_transform.py:159-167` — `df_orders.dropDuplicates(["id"])` is a full shuffle on 1M+ rows; would benefit from `df.repartition("id").dropDuplicates(["id"])` to localize duplicate detection.
- `silver_transform.py:193-194` — `df_customers.dropDuplicates(["id"])` after the email regex filter; same as above.
- `silver_transform.py:281-298` — DLQ union of 6 dirty DataFrames with mismatched columns. Cleaner: define a uniform DLQ schema upfront and have each `drop_dirty()` produce that shape.

### Gold (`gold_transform.py`)

**What it does**: builds star schema with 5-way LEFT JOIN on `fact_sales`.

**Cost driver**: the 5-way join. Specifically:
```python
df_order_items.alias("i")
  .join(df_orders.alias("o"),    F.col("i.order_id")    == F.col("o.id"), "inner")
  .join(dim_customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_key"), "left")
  .join(dim_products.alias("p"),  F.col("i.product_id")  == F.col("p.product_key"), "left")
  .join(dim_payments.alias("pm"), F.col("o.id")          == F.col("pm.order_id"), "left")
  .join(dim_shipping.alias("s"),  F.col("o.id")          == F.col("s.order_id"), "left")
```

`dim_customers`, `dim_products`, `dim_payments`, `dim_shipping` are all **small** (<1M rows each). They should be **broadcast**:

```python
from pyspark.sql.functions import broadcast
.join(broadcast(dim_customers), ..., "left")
```

This converts each LEFT JOIN from a sort-merge join (O(n log n) + shuffle) to a hash broadcast join (O(n) + no shuffle). Expected speedup: **3-5×** on the Gold step.

`fact_sales` partitionBy `(order_year, order_month)` is correct — supports partition pruning when AI agent queries with date filters.

---

## 3. AI Agent Latency Decomposition

For a single `/query` call (planned three-LLM-call flow):

| Step | Time (Anthropic Sonnet) | Time (Groq Llama-3-70B) |
|---|---|---|
| Schema introspection (first call only) | 100-500 ms | same |
| LLM #1 — clarify | 1.0-2.0 s | 0.3-0.8 s |
| LLM #2 — SQL gen | 1.5-3.0 s | 0.5-1.0 s |
| Postgres execute (with `LIMIT 100`) | 50-500 ms | same |
| LLM #3 — summary | 1.0-2.0 s | 0.3-0.8 s |
| **Total** | **~4-8 s** | **~1.5-3 s** |

**Optimizations**:
- Skip clarify by default; only call when the user opts in (or when the question is short / ambiguous).
- Cache schema introspection in-process.
- Stream the summary back to the CLI via SSE so the user sees text appearing immediately.
- For Anthropic: use **prompt caching** on the schema portion (saves ~30% per call after the first).

---

## 4. Database Query Patterns

The AI agent constructs queries blindly from natural language. There is **no query optimizer hint** layer. Given the schema:

```sql
-- Likely heavy queries the AI will produce:

-- Top 10 products by revenue
SELECT p.name, SUM(oi.total_price) AS revenue
FROM order_items oi JOIN products p ON oi.product_id = p.id
GROUP BY p.id, p.name
ORDER BY revenue DESC
LIMIT 10;
```

This needs an index on `order_items(product_id)`. Currently the schema only has the FK declared — Postgres does **not** auto-index FK columns (unlike MySQL InnoDB). **Required indexes**:

```sql
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_order_items_order_id   ON order_items(order_id);
CREATE INDEX idx_orders_customer_id     ON orders(customer_id);
CREATE INDEX idx_orders_order_date      ON orders(order_date);
CREATE INDEX idx_orders_status          ON orders(status);
CREATE INDEX idx_payments_order_id      ON payments(order_id);
CREATE INDEX idx_shipping_order_id      ON shipping(order_id);
CREATE INDEX idx_reviews_product_id     ON reviews(product_id);
CREATE INDEX idx_addresses_customer_id  ON addresses(customer_id);
```

These are not in `data/initial_table.sql` — add them.

---

## 5. N+1 Patterns

None observed in the codebase. Spark jobs are vectorized PySpark, no row-by-row processing. The AI agent runs single SQL statements — but the **risk** is the LLM emitting a query with a correlated subquery that the planner can't unnest:

```sql
SELECT * FROM customers c
WHERE EXISTS (
  SELECT 1 FROM orders o WHERE o.customer_id = c.id AND ...
);
```

Postgres usually unnests these well. But for safety the AI agent should `EXPLAIN` the SQL first when running unattended; if `cost > threshold`, refuse and ask the user to narrow the query.

---

## 6. Caching Strategy

Currently **no caching layer**. Recommendations by benefit/effort:

| Layer | Cache | TTL | Backend |
|---|---|---|---|
| Schema introspection | full DDL string | 5 min | in-process |
| Common questions | `(role, question_hash)` → `(sql, result, summary)` | 60 s | Redis |
| LLM responses | `(model, prompt_hash)` → response | 10 min | Redis or anthropic prompt-caching |
| Hive Gold table queries | rare-changing aggregates | minutes-hours | materialized views or ClickHouse rollups |

A 60-second Redis cache on `(role, question_hash)` will absorb the long tail of "show me revenue today" being asked 10 times.

---

## 7. Concurrency Model

### Spark
Cluster mode, but resources defined for ONE worker (2 cores, 2 GB). Effective parallelism = 2 tasks at once. Add workers in compose for real parallelism.

### Airflow
`LocalExecutor` — runs tasks in subprocesses on the scheduler container. Limited by scheduler container CPU. For ≤10 DAG runs/hour this is fine; for more, switch to `CeleryExecutor` or `KubernetesExecutor`.

### AI Agent (FastAPI)
Started by `uvicorn main:app --host 0.0.0.0 --port 8000`. Single-process, single-event-loop. CPU-bound work (LLM calls are I/O-bound — fine) but DB queries via `QuerySQLDataBaseTool` are **synchronous psycopg2** under an `asyncio` endpoint — this **blocks the event loop**.

**Fix**:
```python
from fastapi.concurrency import run_in_threadpool

@app.post("/query")
async def query(req):
    ...
    result = await run_in_threadpool(execute_tool.run, sql)
    ...
```

Or run uvicorn with `--workers 4` for OS-level concurrency (but in-memory `sessions` dict won't be shared — needs Redis).

---

## 8. Memory-Heavy Operations

| Operation | Memory pattern |
|---|---|
| `silver_transform.py` `from_json` × 6 schemas in same job | each adds an in-memory column; consider repartitioning by `_source_topic` then doing one branch per partition |
| `gold_transform.py` 5-way join | shuffle spills to disk if memory exceeds — increase `spark.executor.memory` or use `broadcast` |
| `migrate.py` (initial load) | reads tables into Pandas DataFrames in 50k row batches — bounded ✓ |
| Bronze `df.count()` after read | full pass over Parquet — fine for graduation; minutes for production scale |
| AI Agent in-memory sessions (planned) | unbounded; 1 KB per turn × 1000 turns × 1000 users = 1 GB. Replace with Redis. |

---

## 9. Async / Async-Inappropriate Patterns

The current AI agent declares `async def query(...)` but inside calls `db.get_table_info()` and `execute_tool.run(sql)` synchronously. Each request **blocks** the entire event loop. Under 10 concurrent requests, latency multiplies.

**Recommended pattern**:
```python
@app.post("/query")
async def query(req):
    schema = await run_in_threadpool(db.get_table_info)
    sql    = await asyncio.to_thread(llm.invoke, prompt)
    result = await run_in_threadpool(execute_tool.run, sql)
    return ...
```

For LLM calls, prefer the **async client** offered by each provider SDK (Anthropic and OpenAI both ship `AsyncAnthropic` / `AsyncOpenAI`).

---

## 10. Storage Sizing

Estimated steady-state Bronze/Silver/Gold sizes for the simulator default load:

| Layer | Source | Approx size |
|---|---|---|
| PostgreSQL `ecommerce` | bootstrap + 1 day of realtime | 200-500 MB |
| Bronze Parquet | 1 day of events | 100-300 MB |
| Silver Parquet | cleaned, typed, deduplicated | 70-200 MB |
| Gold Parquet | star schema (denormalized fact_sales) | 200-400 MB |
| Hive Metastore DB | metadata only | <10 MB |
| Kafka logs (retention 7 days) | depends — at simulator rate | ~1 GB/day |

A 16 GB volume is the practical minimum for HDFS bind mount.

---

## 11. Tuning Cheat Sheet

```yaml
# docker-compose.yml — spark-worker
SPARK_WORKER_MEMORY: 4G        # was 2G
SPARK_WORKER_CORES:  4         # was 2

# scale up
docker compose up -d --scale spark-worker=2
```

```python
# bronze_ingestion.py — for streaming evolution
.option("maxOffsetsPerTrigger", "10000")     # cap per micro-batch
.option("kafka.fetch.max.bytes", "10485760") # 10 MB

# silver_transform.py — pre-dedup repartition
df = df.repartition(8, "id").dropDuplicates(["id"])

# gold_transform.py — broadcast small dims
from pyspark.sql.functions import broadcast
.join(broadcast(dim_customers), ..., "left")
.join(broadcast(dim_payments),  ..., "left")
```

```sql
-- Postgres — required indexes
CREATE INDEX CONCURRENTLY idx_order_items_product_id ON order_items(product_id);
CREATE INDEX CONCURRENTLY idx_orders_customer_id     ON orders(customer_id);
-- ... (see section 4 above)

-- Postgres — read-only role with timeout
ALTER ROLE ai_agent_ro SET statement_timeout = '5s';
ALTER ROLE ai_agent_ro SET work_mem = '64MB';
```

---

## 12. Concrete Latency Targets (production-ready)

| Tier | Goal |
|---|---|
| Source → Bronze | <5 min (streaming) |
| Bronze → Silver | <15 min (hourly) |
| Silver → Gold | <15 min |
| AI Agent simple query | <2 s p95 |
| AI Agent complex query (with summary) | <5 s p95 |
| Full pipeline (cold) | <60 min |
