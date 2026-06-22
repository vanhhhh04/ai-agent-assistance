import Link from "next/link";
import {
  Callout,
  CodeBlock,
  DocsLayout,
  EndpointCard,
  H2,
  H3,
  ToolCard,
  type TocItem,
} from "@/components/docs/DocsLayout";

const TOC: TocItem[] = [
  { id: "overview", label: "Tổng quan" },
  { id: "stack", label: "Stack" },
  {
    id: "data-pipeline",
    label: "Data Pipeline",
    children: [
      { id: "sources",   label: "Data Sources" },
      { id: "debezium",  label: "Debezium CDC" },
      { id: "nifi",      label: "NiFi" },
      { id: "kafka",     label: "Kafka" },
      { id: "spark",     label: "Spark Medallion" },
      { id: "hive",      label: "Hive Gold" },
      { id: "airflow",   label: "Airflow" },
    ],
  },
  {
    id: "ai-agent",
    label: "AI Agent",
    children: [
      { id: "supervisor",  label: "Supervisor" },
      { id: "retriever",   label: "Retriever" },
      { id: "sql-writer",  label: "SQL Writer" },
      { id: "guardrails",  label: "Guardrails" },
      { id: "executor",    label: "Executor" },
    ],
  },
  {
    id: "semantic-layer",
    label: "Semantic Layer",
    children: [
      { id: "opensearch", label: "OpenSearch" },
      { id: "embedder",   label: "Embedder" },
      { id: "indexer",    label: "Catalog indexer" },
    ],
  },
  {
    id: "llm-adapters",
    label: "LLM Adapters",
    children: [
      { id: "anthropic", label: "Anthropic" },
      { id: "openai",    label: "OpenAI" },
      { id: "gemini",    label: "Gemini" },
    ],
  },
  {
    id: "api-reference",
    label: "API Reference",
    children: [
      { id: "api-ask",      label: "POST /query/ask" },
      { id: "api-feedback", label: "POST /query/feedback" },
      { id: "api-schema",   label: "GET /schema/full" },
      { id: "api-health",   label: "GET /health" },
    ],
  },
  { id: "deployment", label: "Deployment" },
];

export const metadata = {
  title: "Tài liệu — DataFinch",
  description: "Kiến trúc, API reference, và hướng dẫn từng tool của DataFinch.",
};

export default function DocsPage() {
  return (
    <DocsLayout toc={TOC}>
      {/* Header */}
      <div className="mb-12 not-prose">
        <p className="text-xs uppercase tracking-wider font-bold text-[color:var(--color-primary)] mb-3">
          Tài liệu kỹ thuật
        </p>
        <h1 className="text-4xl md:text-5xl font-bold tracking-tight text-[color:var(--color-text)] mb-4">
          DataFinch Architecture & API Reference
        </h1>
        <p className="text-lg text-[color:var(--color-text-muted)]">
          Tài liệu chi tiết cho dev/ops/data engineers. Mô tả từng component của hệ thống, vai trò
          trong pipeline, và REST API endpoints.
        </p>
        <div className="mt-6 flex flex-wrap gap-2 text-xs">
          <span className="px-2.5 py-1 rounded-md bg-[color:var(--color-bg-subtle)] font-mono text-[color:var(--color-text-muted)]">
            v2.0.0
          </span>
          <span className="px-2.5 py-1 rounded-md bg-[color:var(--color-bg-subtle)] font-mono text-[color:var(--color-text-muted)]">
            Last updated: 2026-05-13
          </span>
        </div>
      </div>

      {/* ========== OVERVIEW ========== */}
      <H2 id="overview">Tổng quan</H2>
      <p className="text-base text-[color:var(--color-text-muted)] leading-relaxed mb-4">
        DataFinch là hệ thống <strong>multi-agent NL→SQL</strong> inspired by Uber Finch. Người dùng
        hỏi bằng tiếng Việt → 5 AI agents phối hợp để viết SQL chính xác → chạy trên data warehouse
        của bạn → trả về kết quả + biểu đồ trong &lt; 10s.
      </p>
      <p className="text-base text-[color:var(--color-text-muted)] leading-relaxed mb-4">
        Hệ thống chia làm <strong>2 phần chính</strong>:
      </p>
      <ul className="list-disc pl-6 mb-6 text-base text-[color:var(--color-text-muted)] space-y-2">
        <li>
          <strong>Data Engineering Pipeline</strong> — Medallion (Bronze/Silver/Gold) cho phép
          DataFinch query data warehouse có sẵn của customer
        </li>
        <li>
          <strong>AI Agent Service</strong> — FastAPI với 5 agents + OpenSearch semantic layer + 3
          LLM provider adapters
        </li>
      </ul>

      <div className="not-prose grid sm:grid-cols-2 lg:grid-cols-3 gap-3 my-6">
        <ToolCard icon="🐘" name="Postgres" role="Source DB"        link="sources"   color="#2563eb" />
        <ToolCard icon="⚡" name="Debezium" role="CDC capture"     link="debezium"  color="#dc2626" />
        <ToolCard icon="💧" name="NiFi"     role="Pipeline ETL"    link="nifi"      color="#0891b2" />
        <ToolCard icon="📨" name="Kafka"    role="Message broker"  link="kafka"     color="#0f172a" />
        <ToolCard icon="🔥" name="Spark"    role="Transform"       link="spark"     color="#ea580c" />
        <ToolCard icon="🏛" name="Hive"     role="Data warehouse"  link="hive"      color="#d97706" />
        <ToolCard icon="🤖" name="AI Agent" role="NL→SQL pipeline" link="ai-agent"  color="#0891b2" />
        <ToolCard icon="🔍" name="OpenSearch" role="Semantic layer" link="semantic-layer" color="#7c3aed" />
        <ToolCard icon="🧠" name="LLM Adapters" role="Anthropic/OpenAI/Gemini" link="llm-adapters" color="#059669" />
      </div>

      <Callout type="info">
        Tài liệu này tập trung vào <strong>tool/component</strong>. Cho user-facing docs (cách dùng
        product), xem{" "}
        <Link href="/how-it-works" className="text-[color:var(--color-primary)] hover:underline font-semibold">
          /how-it-works
        </Link>.
      </Callout>

      {/* ========== STACK ========== */}
      <H2 id="stack">Stack</H2>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        Toàn bộ stack chạy bằng <strong>docker compose</strong> — single command để spin up cả 13+
        containers cho local dev.
      </p>
      <div className="not-prose my-4 rounded-xl border border-[color:var(--color-border)] bg-white overflow-x-auto">
        <table className="w-full text-sm min-w-[600px]">
          <thead className="bg-[color:var(--color-bg-muted)] border-b border-[color:var(--color-border)]">
            <tr>
              <th className="text-left px-4 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">Layer</th>
              <th className="text-left px-4 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">Technology</th>
              <th className="text-left px-4 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">Version</th>
              <th className="text-left px-4 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">Port</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[color:var(--color-border)] text-[color:var(--color-text)]">
            {[
              ["Source DB",      "PostgreSQL",            "15",     "5432"],
              ["CDC",            "Debezium (Kafka Connect)", "2.x", "8083"],
              ["Pipeline ETL",   "Apache NiFi",           "1.27",   "8443"],
              ["Message broker", "Kafka + ZooKeeper",     "7.5.0",  "9092"],
              ["Storage",        "HDFS (Namenode + Datanode)", "2.7.4", "9870"],
              ["Compute",        "Apache Spark",          "3.5.1",  "8090"],
              ["Metastore",      "Hive Metastore + Server2", "3.1.3", "10000"],
              ["Orchestrator",   "Apache Airflow",        "2.7.x",  "8080"],
              ["Retrieval",      "OpenSearch",            "2.13.0", "9200"],
              ["AI Agent",       "FastAPI + Python 3.11", "—",      "8000"],
              ["Frontend",       "Next.js 16 + React 19", "—",      "3000"],
            ].map(([layer, tech, ver, port]) => (
              <tr key={layer} className="hover:bg-[color:var(--color-bg-muted)]/50">
                <td className="px-4 py-2.5 font-semibold">{layer}</td>
                <td className="px-4 py-2.5">{tech}</td>
                <td className="px-4 py-2.5 font-mono text-xs">{ver}</td>
                <td className="px-4 py-2.5 font-mono text-xs text-[color:var(--color-text-muted)]">{port}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ========== DATA PIPELINE ========== */}
      <H2 id="data-pipeline">Data Pipeline (Medallion)</H2>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        Bronze → Silver → Gold pattern. Realtime CDC từ Postgres + push/pull connectors từ
        Warehouse/Payment GW → Kafka → Spark transform → Hive Gold.
      </p>
      <CodeBlock language="ascii flow">
{`Postgres ERP ─── Debezium CDC ──┐
                                 ├──► Kafka ─── Spark Bronze ─── HDFS /datalake/bronze/
Warehouse ─── NiFi GetFile  ────┤                                       │
(CSV)                            │                                       ▼
                                 │                                  Spark Silver ─── HDFS /silver/
Payment GW ─── NiFi ListenHTTP ─┘                                       │
(HTTP POST)                                                              ▼
                                                                   Spark Gold ─── Hive Metastore
                                                                                     │
                                                                                     ▼
                                                                              gold.fact_*, dim_*`}
      </CodeBlock>

      <H3 id="sources">Data Sources</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        3 simulator để generate dữ liệu giống production thực tế (có 5% dirty data có chủ đích để
        Spark Silver phải handle dedup/quarantine).
      </p>
      <ul className="list-disc pl-6 mb-4 text-sm text-[color:var(--color-text-muted)] space-y-1.5">
        <li><code className="font-mono text-[color:var(--color-primary)]">data-source/sim_warehouse.py</code> — 100k products + 70 categories. Dual-write Postgres + CSV cho NiFi GetFile.</li>
        <li><code className="font-mono text-[color:var(--color-primary)]">data-source/sim_erp.py</code> — 100k customers + 500k orders + items/reviews/feedback. INSERT/UPDATE qua Postgres WAL.</li>
        <li><code className="font-mono text-[color:var(--color-primary)]">data-source/sim_payment.py</code> — Payment events realtime. POST HTTP JSON đến NiFi ListenHTTP.</li>
      </ul>

      <H3 id="debezium">Debezium CDC</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        Capture WAL từ Postgres → publish event tới Kafka topics. Config tại{" "}
        <code className="font-mono">nifi/init_debezium.sh</code>.
      </p>
      <CodeBlock language="json">
{`{
  "name": "erp-postgres-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "slot.name": "erp_debezium_slot",
    "topic.prefix": "erp",
    "table.include.list": "public.customers,public.orders,public.order_items,...",
    "snapshot.mode": "initial",
    "transforms": "unwrap",
    "transforms.unwrap.add.fields": "op,table,source.ts_ms"
  }
}`}
      </CodeBlock>
      <p className="text-sm text-[color:var(--color-text-muted)]">
        Output topics: <code className="font-mono">erp.public.customers</code>,{" "}
        <code className="font-mono">erp.public.orders</code>, ... — mỗi message có{" "}
        <code className="font-mono">__op</code> (c/u/d) cho Spark biết là INSERT vs UPDATE.
      </p>

      <H3 id="nifi">NiFi (Pipeline ETL)</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        2 pipelines auto-setup qua REST API (script <code className="font-mono">nifi/setup_flows.py</code>):
      </p>
      <ul className="list-disc pl-6 mb-4 text-sm text-[color:var(--color-text-muted)] space-y-1.5">
        <li>
          <strong>Warehouse pipeline</strong> (CSV → Kafka): GetFile → ConvertRecord → SplitJson →
          EvaluateJsonPath → RouteOnAttribute → PublishKafka (clean / dlq)
        </li>
        <li>
          <strong>Payment pipeline</strong> (HTTP → Kafka): ListenHTTP :8181 → EvaluateJsonPath →
          RouteOnAttribute → PublishKafka (clean / dlq)
        </li>
      </ul>
      <Callout type="info">
        NiFi cũng có <strong>back-pressure</strong> — Kafka chậm thì ListenHTTP từ chối request mới
        (HTTP 503), không OOM. Persistent queue cho phép replay event sau crash.
      </Callout>

      <H3 id="kafka">Kafka (Message Broker)</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        13 topics: 11 từ Debezium (<code className="font-mono">erp.public.*</code>) + 2 từ NiFi (
        <code className="font-mono">warehouse.events</code>,{" "}
        <code className="font-mono">payment.events</code>) + DLQ tương ứng.
      </p>
      <CodeBlock language="bash">
{`# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume realtime
docker exec kafka kafka-console-consumer \\
  --bootstrap-server localhost:9092 \\
  --topic erp.public.orders --from-beginning`}
      </CodeBlock>

      <H3 id="spark">Spark Medallion Jobs</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        3 Spark jobs ở <code className="font-mono">spark/jobs/</code>:
      </p>
      <ul className="list-disc pl-6 mb-4 text-sm text-[color:var(--color-text-muted)] space-y-1.5">
        <li>
          <code className="font-mono text-[color:var(--color-primary)]">bronze_ingestion.py</code> —
          Structured Streaming với <code>Trigger.AvailableNow</code> — drain Kafka offsets mới rồi
          exit. Ghi parquet vào <code>/datalake/bronze/{`{erp_raw,wh_raw,pay_raw}`}</code>.
        </li>
        <li>
          <code className="font-mono text-[color:var(--color-primary)]">silver_transform.py</code> —
          Dedup by event_id, harmonization, surrogate key MD5(source:id), routing CLEAN/DIRTY/QUARANTINE.
        </li>
        <li>
          <code className="font-mono text-[color:var(--color-primary)]">gold_transform.py</code> —
          Build star schema (1 fact_sales + 4 dims), register vào Hive Metastore.
        </li>
      </ul>

      <H3 id="hive">Hive Gold (Data Warehouse)</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        Star schema cho analytical queries. Partitioned by <code>order_year</code>,{" "}
        <code>order_month</code>.
      </p>
      <CodeBlock language="sql">
{`-- 1 fact + 4 dims
gold.fact_sales       (134k rows, partitioned by order_year/order_month)
gold.dim_customers    (96k rows)
gold.dim_products     (10k rows)
gold.dim_coupons      (1.4k rows)
gold.dim_addresses    (1.4k rows)

-- Connect via HiveServer2 (thrift)
beeline -u "jdbc:hive2://hiveserver2:10000/gold"`}
      </CodeBlock>

      <H3 id="airflow">Airflow Orchestration</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        DAG <code className="font-mono">medallion_pipeline</code> chạy mỗi <strong>15 phút</strong>:
      </p>
      <CodeBlock language="python">
{`schedule_interval = "*/15 * * * *"
max_active_runs = 1   # never overlap

bronze >> silver >> gold   # sequential dependency`}
      </CodeBlock>
      <p className="text-sm text-[color:var(--color-text-muted)]">
        End-to-end freshness: source write → Hive Gold visible trong ~15-25 phút.
      </p>

      {/* ========== AI AGENT ========== */}
      <H2 id="ai-agent">AI Agent (DataFinch Backend)</H2>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        FastAPI server ở <code className="font-mono">ai-agent/</code> implement multi-agent NL→SQL
        pipeline với 5 agents phối hợp. Entry point: <code className="font-mono">ai-agent/main.py</code>.
      </p>
      <CodeBlock language="ascii flow">
{`POST /api/query/ask
   ↓
[1] Supervisor    ─── classify intent → DATA_QUERY | SCHEMA_INFO | OUT_OF_SCOPE
   ↓
[2] Retriever     ─── OpenSearch hybrid kNN + BM25 → 8 catalog hits
   ↓
[3] SQL Writer    ─── LLM generate JSON {sql, explanation, tables_used, ...}
   ↓
[4] Guardrails    ─── block DELETE/UPDATE/DROP, check JOIN count, enforce LIMIT
   ↓
[5] Executor      ─── dispatch to Hive (thrift) or Postgres (asyncpg)
   ↓
Stream SSE events to frontend`}
      </CodeBlock>

      <H3 id="supervisor">[1] Supervisor Agent</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-3">
        <strong>File:</strong>{" "}
        <code className="font-mono text-[color:var(--color-primary)]">ai-agent/agents/supervisor.py</code>
      </p>
      <p className="text-sm text-[color:var(--color-text-muted)] mb-3">
        LLM nhỏ (Haiku / GPT-5-nano) phân loại intent + chọn backend. 1 LLM call → JSON với 4 fields.
      </p>
      <CodeBlock language="json">
{`{
  "intent": "DATA_QUERY",
  "backend": "hive_gold",
  "confidence": 0.95,
  "reasoning": "Câu hỏi về aggregation top-K, phù hợp gold layer"
}`}
      </CodeBlock>

      <H3 id="retriever">[2] Metadata Retriever</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-3">
        <strong>File:</strong>{" "}
        <code className="font-mono text-[color:var(--color-primary)]">ai-agent/core/semantic_layer.py</code>
      </p>
      <p className="text-sm text-[color:var(--color-text-muted)] mb-3">
        Hybrid retrieval qua 3 OpenSearch indices: <code>finch_catalog</code> (schema),{" "}
        <code>table_docs</code> (markdown docs), <code>query_log</code> (past queries).
      </p>
      <CodeBlock language="python">
{`# Hybrid search: kNN (semantic) + BM25 (keyword)
ctx = semantic_layer.retrieve(
    question="Top khách hàng đặt nhiều đơn nhất",
    backend_filter="gold",
)
# → ctx.catalog (8 hits), ctx.docs (0), ctx.history (2)
# Augmented with full schema_cache for tables in scope`}
      </CodeBlock>

      <H3 id="sql-writer">[3] SQL Writer</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-3">
        <strong>File:</strong>{" "}
        <code className="font-mono text-[color:var(--color-primary)]">ai-agent/agents/sql_writer.py</code>
      </p>
      <p className="text-sm text-[color:var(--color-text-muted)] mb-3">
        LLM mạnh (Sonnet/GPT-5/Gemini 2.5) nhận: question + retrieved schema + dialect rules
        (HiveQL/PostgreSQL) + similar past queries. Output strict JSON.
      </p>
      <CodeBlock language="json">
{`{
  "sql": "SELECT c.customer_name, COUNT(DISTINCT s.order_key) AS orders...",
  "explanation": "Truy vấn lấy top khách hàng có nhiều đơn nhất",
  "tables_used": ["fact_sales", "dim_customers"],
  "complexity": "medium",
  "has_date_filter": false,
  "aggregated": true
}`}
      </CodeBlock>

      <H3 id="guardrails">[4] Guardrails</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-3">
        <strong>File:</strong>{" "}
        <code className="font-mono text-[color:var(--color-primary)]">ai-agent/core/guardrails.py</code>
      </p>
      <p className="text-sm text-[color:var(--color-text-muted)] mb-3">Static analysis trước khi execute:</p>
      <ul className="list-disc pl-6 mb-4 text-sm text-[color:var(--color-text-muted)] space-y-1">
        <li>Block DELETE / UPDATE / INSERT / DROP / TRUNCATE / ALTER / CREATE</li>
        <li>Reject multiple statements (no semicolons mid-query)</li>
        <li>Enforce <code>LIMIT</code> trên non-aggregated queries (max 1000)</li>
        <li>JOIN cap (max 5 tables)</li>
        <li>PII column masking nếu user role không có quyền</li>
      </ul>

      <H3 id="executor">[5] Executor</H3>
      <p className="text-base text-[color:var(--color-text-muted)] mb-3">
        <strong>Files:</strong>{" "}
        <code className="font-mono text-[color:var(--color-primary)]">ai-agent/agents/retrieval/{`{hive,postgres}_agent.py`}</code>
      </p>
      <p className="text-sm text-[color:var(--color-text-muted)] mb-3">
        Dispatch SQL đến đúng backend. Async execution với timeout 60s.
      </p>
      <CodeBlock language="python">
{`# Backend dispatch (registry pattern)
if backend == "hive_gold":
    result = await hive_client.execute_query(sql)  # pyhive over thrift
elif backend == "postgres_bronze":
    result = await postgres_client.execute_query(sql)  # asyncpg

# Log to OpenSearch query_log (telemetry + self-improving)
log_query(question, sql, status, exec_ms, ...)`}
      </CodeBlock>

      {/* ========== SEMANTIC LAYER ========== */}
      <H2 id="semantic-layer">Semantic Layer (OpenSearch)</H2>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        OpenSearch lưu schema catalog + business docs + query history. Hybrid retrieval (kNN + BM25)
        cung cấp context cho SQL Writer agent.
      </p>

      <H3 id="opensearch">3 OpenSearch Indices</H3>
      <ul className="list-disc pl-6 mb-4 text-sm text-[color:var(--color-text-muted)] space-y-1.5">
        <li>
          <code className="font-mono text-[color:var(--color-primary)]">finch_catalog</code> — table
          + column metadata (table_name, column_name, type, description, sample_values, is_pii,
          embedding 768-d).
        </li>
        <li>
          <code className="font-mono text-[color:var(--color-primary)]">table_docs</code> — markdown
          business docs với examples queries.
        </li>
        <li>
          <code className="font-mono text-[color:var(--color-primary)]">query_log</code> —
          (nl_question, generated_sql, status, thumbs_up, exec_ms, tables_used) cho telemetry +
          retrieval hint (past queries).
        </li>
      </ul>

      <H3 id="embedder">Embedder Model</H3>
      <p className="text-sm text-[color:var(--color-text-muted)] mb-3">
        Multilingual model (Việt + English) — chạy local trong ai-agent container, cache trong{" "}
        <code className="font-mono">/cache/sentence-transformers</code>.
      </p>
      <CodeBlock language="python">
{`from sentence_transformers import SentenceTransformer
model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
# Output dim: 768`}
      </CodeBlock>

      <H3 id="indexer">Catalog Indexer</H3>
      <p className="text-sm text-[color:var(--color-text-muted)] mb-3">
        Script <code className="font-mono">opensearch/indexers/catalog_indexer.py</code> scan Hive
        Metastore + Postgres → generate descriptions → embed → bulk upsert vào{" "}
        <code className="font-mono">finch_catalog</code>.
      </p>
      <CodeBlock language="bash">
{`# Run anytime schema changes (or on first install)
docker exec -w /app ai-agent python -m opensearch.indexers.catalog_indexer

# Check
curl -s http://localhost:9200/finch_catalog/_count
# {"count": 132, ...}`}
      </CodeBlock>
      <Callout type="warn">
        Sau khi schema Hive thay đổi (thêm/xóa cột) — phải re-index catalog để LLM hiểu schema mới.
      </Callout>

      {/* ========== LLM ADAPTERS ========== */}
      <H2 id="llm-adapters">LLM Adapters</H2>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        Provider-agnostic abstraction. Đổi provider chỉ cần thay env <code className="font-mono">LLM_PROVIDER</code> trong{" "}
        <code className="font-mono">.env</code> — không cần đổi code.
      </p>
      <CodeBlock language="python">
{`# ai-agent/core/llm_adapters/__init__.py
_REGISTRY = {
    "anthropic": _make_anthropic,
    "gemini":    _make_gemini,
    "openai":    _make_openai,
}

# All implement same interface (base.py):
async def complete(*, system, messages, model, max_tokens) -> RawCompletion: ...`}
      </CodeBlock>

      <H3 id="anthropic">Anthropic Claude</H3>
      <p className="text-sm text-[color:var(--color-text-muted)] mb-3">
        File <code className="font-mono text-[color:var(--color-primary)]">core/llm_adapters/anthropic_adapter.py</code>.
        Hỗ trợ prompt caching + adaptive thinking (Sonnet 4.6+).
      </p>
      <CodeBlock language="env">
{`LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL_SUPERVISOR=claude-haiku-4-5-20251001
ANTHROPIC_MODEL_SQL_WRITER=claude-sonnet-4-6
ANTHROPIC_ADAPTIVE_THINKING=true`}
      </CodeBlock>

      <H3 id="openai">OpenAI GPT</H3>
      <p className="text-sm text-[color:var(--color-text-muted)] mb-3">
        File <code className="font-mono text-[color:var(--color-primary)]">core/llm_adapters/openai_adapter.py</code>.
        Auto-detect reasoning models (gpt-5*, o-series) → dùng{" "}
        <code className="font-mono">max_completion_tokens</code> +{" "}
        <code className="font-mono">reasoning_effort</code>.
      </p>
      <CodeBlock language="env">
{`LLM_PROVIDER=openai
OPENAI_API_KEY=sk-proj-...
OPENAI_MODEL_SUPERVISOR=gpt-5-mini
OPENAI_MODEL_SQL_WRITER=gpt-5-mini
OPENAI_REASONING_EFFORT=minimal`}
      </CodeBlock>

      <H3 id="gemini">Google Gemini</H3>
      <p className="text-sm text-[color:var(--color-text-muted)] mb-3">
        File <code className="font-mono text-[color:var(--color-primary)]">core/llm_adapters/gemini_adapter.py</code>.
        Lưu ý: Gemini 2.5 Flash có "thinking tokens" ăn vào max_output_tokens — cần set ≥ 5000.
      </p>
      <CodeBlock language="env">
{`LLM_PROVIDER=gemini
GEMINI_API_KEY=AIza...
GEMINI_MODEL_SUPERVISOR=gemini-2.5-flash
GEMINI_MODEL_SQL_WRITER=gemini-2.5-flash
LLM_MAX_TOKENS_SQL_WRITER=5000   # leave room for thinking`}
      </CodeBlock>

      {/* ========== API REFERENCE ========== */}
      <H2 id="api-reference">REST API Reference</H2>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        Base URL: <code className="font-mono">http://localhost:8000</code> (default) hoặc env{" "}
        <code className="font-mono">NEXT_PUBLIC_API_BASE</code>.
      </p>

      <H3 id="api-ask">POST /api/query/ask — Stream NL→SQL pipeline</H3>
      <EndpointCard
        method="POST"
        path="/api/query/ask"
        description="Main endpoint. Stream Server-Sent Events (SSE) cho từng step của pipeline. Frontend dùng để show realtime agent progress."
      >
        <p className="text-xs font-bold uppercase tracking-wider text-[color:var(--color-text-subtle)] mb-2">Request body</p>
        <CodeBlock language="json">
{`{
  "question": "Top 5 khách hàng đặt nhiều đơn nhất",
  "conversation_history": [
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "..."}
  ],
  "session_id": "abc-123",
  "user_id": "user-456",
  "allow_pii": false
}`}
        </CodeBlock>

        <p className="text-xs font-bold uppercase tracking-wider text-[color:var(--color-text-subtle)] mb-2 mt-4">SSE event types</p>
        <CodeBlock language="javascript">
{`// Step event — pipeline progress
data: {"type": "step", "step": "supervisor", "status": "done", "data": {...}}

// Result event — final SQL + rows
data: {"type": "result", "data": {
  "intent": "DATA_QUERY",
  "backend": "hive_gold",
  "sql": "SELECT ...",
  "explanation": "...",
  "tables_used": ["fact_sales", "dim_customers"],
  "columns": [...],
  "rows": [...],
  "row_count": 5,
  "exec_ms": 5400,
  "total_ms": 8600,
  "query_id": "uuid"
}}

// Error event — when something fails
data: {"type": "error", "message": "..."}`}
        </CodeBlock>
      </EndpointCard>

      <H3 id="api-feedback">POST /api/query/feedback — Thumbs up/down</H3>
      <EndpointCard
        method="POST"
        path="/api/query/feedback"
        description="Update past query với feedback. Feeds vào self-improving loop."
      >
        <CodeBlock language="json">
{`// Request
{
  "query_id": "uuid-from-previous-result",
  "thumbs_up": true,
  "feedback_text": "Câu trả lời chính xác"
}

// Response
{ "ok": true, "query_id": "..." }`}
        </CodeBlock>
      </EndpointCard>

      <H3 id="api-schema">GET /api/schema/full — Full warehouse schema</H3>
      <EndpointCard
        method="GET"
        path="/api/schema/full"
        description="Return toàn bộ schema từ Hive Metastore + Postgres. Dùng cho schema browser UI."
      >
        <CodeBlock language="json">
{`{
  "schema": {
    "fact_sales": [
      { "column": "order_key", "type": "int" },
      { "column": "customer_key", "type": "int" },
      ...
    ],
    "dim_customers": [...]
  }
}`}
        </CodeBlock>
      </EndpointCard>

      <H3 id="api-health">GET /api/health — Health check</H3>
      <EndpointCard
        method="GET"
        path="/api/health"
        description="Aggregate health status. Frontend dùng để show backend LIVE/OFFLINE indicator."
      >
        <CodeBlock language="json">
{`{
  "status": "ok",
  "components": {
    "hive_gold": { "ok": true, "tables": 10 },
    "postgres_bronze": { "ok": true, "tables": 11 },
    "opensearch": { "reachable": true },
    "schema_cache": { "hive_tables": 10, "postgres_tables": 11 }
  },
  "llm": {
    "provider": "openai",
    "supervisor_model": "gpt-5-mini",
    "sql_writer_model": "gpt-5-mini",
    "configured": true,
    "openai_key_set": true
  }
}`}
        </CodeBlock>
      </EndpointCard>

      <EndpointCard
        method="GET"
        path="/api/health/ping"
        description="Lightweight health check — chỉ check process alive, không touch backends. Dùng cho Docker healthcheck."
      >
        <CodeBlock language="json">{`{ "pong": true }`}</CodeBlock>
      </EndpointCard>

      {/* ========== DEPLOYMENT ========== */}
      <H2 id="deployment">Deployment</H2>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">Single-command bootstrap:</p>
      <CodeBlock language="bash">
{`# 1. Start full stack (Postgres, Kafka, Hive, Spark, OpenSearch, ai-agent...)
docker compose up -d

# 2. Wait ~2-3 min for services healthy, then:
bash cli/startup.sh

# 3. Frontend (separate)
cd datafinch-web && npm install && npm run dev`}
      </CodeBlock>
      <p className="text-base text-[color:var(--color-text-muted)] mb-4">
        Services accessible after startup:
      </p>
      <ul className="list-disc pl-6 mb-6 text-sm text-[color:var(--color-text-muted)] space-y-1">
        <li><strong>Frontend</strong>: <code className="font-mono">http://localhost:3000</code></li>
        <li><strong>AI Agent API</strong>: <code className="font-mono">http://localhost:8000/docs</code> (Swagger UI)</li>
        <li><strong>NiFi UI</strong>: <code className="font-mono">https://localhost:8443/nifi</code> (admin/adminadminadmin)</li>
        <li><strong>Airflow</strong>: <code className="font-mono">http://localhost:8080</code> (admin/admin123)</li>
        <li><strong>HDFS Namenode</strong>: <code className="font-mono">http://localhost:9870</code></li>
        <li><strong>Kafka UI</strong>: <code className="font-mono">http://localhost:8888</code></li>
        <li><strong>OpenSearch</strong>: <code className="font-mono">http://localhost:9200</code></li>
        <li><strong>Pipeline Dashboard</strong>: <code className="font-mono">http://localhost:5555</code></li>
      </ul>

      <Callout type="success">
        Pipeline live dashboard ở port 5555 — single-page HTML show realtime count cho từng layer
        (Postgres → CDC → NiFi → Kafka → HDFS → Airflow → Gold/Hive). Tốt nhất để debug pipeline
        flow.
      </Callout>

      {/* Footer */}
      <div className="mt-16 pt-8 border-t border-[color:var(--color-border)] not-prose">
        <p className="text-sm text-[color:var(--color-text-muted)] mb-4">
          Cần thêm thông tin? Check source code repo hoặc liên hệ team.
        </p>
        <div className="flex flex-wrap gap-3">
          <Link
            href="/how-it-works"
            className="px-4 py-2 rounded-lg border border-[color:var(--color-border-strong)] text-sm font-medium text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]"
          >
            ← User-facing How It Works
          </Link>
          <Link
            href="/pricing"
            className="px-4 py-2 rounded-lg border border-[color:var(--color-border-strong)] text-sm font-medium text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]"
          >
            Bảng giá →
          </Link>
          <a
            href="mailto:dev@datafinch.app"
            className="px-4 py-2 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)]"
          >
            Liên hệ dev team
          </a>
        </div>
      </div>
    </DocsLayout>
  );
}
