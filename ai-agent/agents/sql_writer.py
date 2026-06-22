"""
agents/sql_writer.py — SQL Writer Agent.

Pipeline (per request):
  1. Hybrid-retrieve relevant catalog/docs/history from OpenSearch
  2. Build a focused system prompt: only the tables/columns we need
  3. Call Claude via the Generative AI Gateway
  4. Parse JSON, run guardrails, return result + retrieval metadata

Two SQL dialects supported:
  - hive_gold: HiveQL (no semicolons, no DISTINCT in COUNT(*) tricks, FROM gold.fact_*)
  - postgres_bronze: standard PostgreSQL

Output shape (after guardrails):
  {
    "sql": "...", "explanation": "...", "tables_used": [...],
    "complexity": "low|medium|high", "valid": true|false,
    "validation_error": str|null, "warnings": [...],
    "retrieval": {"catalog": N, "docs": N, "history": N},
  }
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from core import guardrails
from core.llm_gateway import gateway
from core.semantic_layer import RetrievalContext, semantic_layer
from core.settings import settings

log = logging.getLogger("sql_writer")


# ─────────────────────────────────────────────────────────────
#  Prompts (one per dialect)
# ─────────────────────────────────────────────────────────────
_HIVE_RULES = """## HiveQL rules (warehouse: gold.*)
1. Reference tables as `gold.<table>` (e.g. gold.fact_sales).
2. Always end with LIMIT 100 unless the query is purely aggregated (SUM/COUNT/AVG).
3. Filter by partition columns when present: `order_year`, `order_month` on fact_sales.
4. Never emit DELETE/UPDATE/INSERT/DROP/CREATE/ALTER/TRUNCATE — read-only.
5. No semicolons inside the SQL — single statement only.
6. Use `item_total` for revenue per product, `order_total` only with DISTINCT order_key (it duplicates across items).
7. Vietnamese semantic mapping (DEFAULT interpretations — pick the metric the user most likely means):
   - "bán chạy nhất" / "best-selling" / "phổ biến" / "được mua nhiều"
       → quantity sold. Use `SUM(quantity)` if a quantity column exists,
         else `COUNT(DISTINCT order_key)` (number of orders containing the product),
         else `COUNT(*)` on the line items. Do NOT use revenue here.
   - "doanh thu" / "revenue" / "doanh số" → `SUM(item_total)`.
   - "lợi nhuận" → profit (only if profit column exists; else say not available).
   - "đắt nhất" → highest unit price.
   - "đơn hàng nhiều nhất" → COUNT(DISTINCT order_key).
   - Other terms: "đơn hàng"=order, "sản phẩm"=product, "khách hàng"=customer,
     "đánh giá"=review, "hoàn tất"/"đã giao"=DELIVERED status.
8. "hiện nay" / "currently" / "gần đây" → for top-K queries, OMIT the year filter
   entirely (the warehouse already only contains recent data). Do NOT hard-code
   2023/2024.
9. Hive subquery limitation: scalar subqueries in WHERE comparisons are NOT
   supported (e.g. `WHERE order_year = (SELECT MAX(order_year) FROM ...)` fails
   with "Unsupported SubQuery Expression"). If you need the latest year:
     - Preferred: omit the filter — let GROUP BY + ORDER BY handle ranking.
     - If you really need it: use a CTE join, e.g.
         `WITH m AS (SELECT MAX(order_year) AS y FROM gold.fact_sales)
          SELECT ... FROM gold.fact_sales s JOIN m ON s.order_year = m.y ...`
   Subqueries in IN/EXISTS at the top-level of a WHERE conjunct ARE supported.
10. **NEVER FABRICATE COLUMNS** — every column you reference must appear in the
    schema section below for that table. Common LLM traps to avoid:
    - Do NOT invent discriminator columns like `event_type`, `event_kind`,
      `record_type`, `is_stock_event`, `transaction_kind`. `fact_sales` is a
      pure line-item fact, not an event log.
    - Do NOT self-JOIN `fact_sales` to itself with a fake filter to fabricate
      a different "view" (stock vs sales). There is one row per order_item, full stop.
    - Do NOT assume Postgres column names exist in Hive — e.g. `stock_quantity`,
      `customer_id` (Hive uses `customer_key`), `order_id` (Hive uses `order_key`),
      `phone`, `address`, `first_name`, `last_name` are all Postgres-only.
    - Do NOT join two dim tables and call it a fact — facts are `gold.fact_*`.
11. **Hive Gold does NOT have stock/inventory data** — `dim_products` is a
    product master (sku/name/brand/category/list_price/cost/is_active) with NO
    stock_quantity column. If the user asks about "tồn kho", "stock", "inventory",
    "còn hàng", "hết hàng", you MUST respond with an explanation that this query
    needs the `postgres_bronze` backend (where `public.products.stock_quantity`
    lives). Set `intent` to OUT_OF_SCOPE in the supervisor for now, OR generate
    a query against Postgres if backend=postgres_bronze. Never invent a stock
    column on a Hive table."""


_POSTGRES_RULES = """## PostgreSQL rules (operational ERP: public.*)
1. Reference tables without schema prefix (search_path=public).
2. Always end with LIMIT 100 unless the query is purely aggregated.
3. Use `ROUND(value::numeric, 2)` when displaying money.
4. Never emit DELETE/UPDATE/INSERT/DROP/CREATE/ALTER/TRUNCATE — read-only.
5. No semicolons — single statement only.
6. Use ::date casts when filtering on timestamp columns by date.
7. Vietnamese semantic mapping:
   - "bán chạy nhất" → quantity (`SUM(quantity)` or `COUNT(DISTINCT order_id)`), not revenue.
   - "doanh thu" → `SUM(quantity * unit_price)` from order_items, NOT from orders.
   - "hiện nay" → no fixed year filter (use most-recent data or skip the filter).
   - "đơn hàng" / "order" → orders table.
   - "khách hàng" → customers table.
   - "sản phẩm" → products table.
8. Order status semantics — `orders.status` enum:
   ('pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned').
   - "chưa giao" / "undelivered" / "đang xử lý" → `status NOT IN ('delivered','cancelled','returned')`
     (i.e. pending / processing / shipped). Do NOT use a non-existent `delivered_at` column on
     the orders table — that column lives on `shipping`, not `orders`.
   - "đã giao" / "delivered" → `status = 'delivered'`.
   - "đã hủy" → `status = 'cancelled'`.
9. Shipping vs orders — these are SEPARATE tables joined by order_id:
   - `orders.status` is the high-level workflow status.
   - `shipping.status` is the carrier-side detail (picked_up, in_transit, ...).
   - `shipping.delivered_at` is the timestamp from carrier; does NOT exist on orders.
   - Use orders.status for most "đơn hàng" questions. JOIN shipping only when the
     user asks about carrier, tracking number, or actual delivery time.
10. CRITICAL — do NOT borrow columns across tables. Every column you reference
    must be listed under that table's "### public.<table>" block in the schema
    section below. If a column you need is missing there, it does NOT exist."""


SYSTEM_PROMPT_TEMPLATE = """You are the SQL Writer Agent. Generate ONE valid {dialect} query for the user's question.

{dialect_rules}

# Available context (retrieved from semantic layer — do not reference tables not listed here):
{retrieval_block}

# Conversation context (for FOLLOWUP intent only):
The previous user/assistant turns are in messages — when present, the user's new question may refer to them.

# Output — strict JSON only, no markdown:
{{
  "sql": "SELECT ...",
  "explanation": "1-sentence in the user's language explaining what the SQL does",
  "tables_used": ["fact_sales", "dim_products"],
  "complexity": "low|medium|high",
  "has_date_filter": true|false,
  "aggregated": true|false
}}
"""


@dataclass
class SQLWriterResult:
    sql: str | None
    explanation: str
    tables_used: list[str]
    complexity: str
    valid: bool
    validation_error: str | None
    warnings: list[str]
    retrieval_summary: dict
    aggregated: bool = False
    has_date_filter: bool = False

    def to_dict(self) -> dict:
        return {
            "sql": self.sql,
            "explanation": self.explanation,
            "tables_used": self.tables_used,
            "complexity": self.complexity,
            "valid": self.valid,
            "validation_error": self.validation_error,
            "warnings": self.warnings,
            "retrieval": self.retrieval_summary,
            "aggregated": self.aggregated,
            "has_date_filter": self.has_date_filter,
        }


async def generate_sql(
    *,
    question: str,
    backend: str,
    intent: str,
    conversation_history: list[dict] | None = None,
    schema_fallback: dict[str, list[dict]] | None = None,
) -> tuple[SQLWriterResult, RetrievalContext]:
    """
    Returns (result, retrieval_context). The router uses both — context is
    written into the query_log so we can debug why a particular SQL was generated.
    """
    backend_db = "gold" if backend == "hive_gold" else "public"
    dialect = "HiveQL" if backend == "hive_gold" else "PostgreSQL"
    dialect_rules = _HIVE_RULES if backend == "hive_gold" else _POSTGRES_RULES

    # 1. Retrieve focused context.
    # ALWAYS filter by backend — otherwise a postgres query may retrieve gold
    # tables (since both share the same catalog index) and the LLM ends up
    # writing SQL with Hive table names like `fact_sales` against Postgres.
    # If retrieval returns nothing for that backend (e.g. Postgres not indexed
    # yet), the schema augmentation block below will dump the full schema_cache
    # instead.
    ctx = semantic_layer.retrieve(
        question,
        backend_filter=backend_db,
    )
    retrieval_block = ctx.as_prompt_block()

    # ── Schema augmentation — critical for preventing column hallucination ──
    # Semantic retrieval returns only top-K (default 8) catalog docs. Each column
    # is a separate doc, so 8 hits typically cover the table doc + a few
    # "semantic-match" columns (e.g. `customer_name` when asked about "khách hàng")
    # but miss less-relevant columns (e.g. `gender`, `date_of_birth`). The LLM
    # then guesses missing columns (`first_name`, `phone`, `address`, ...).
    #
    # Fix: for every table that appears in retrieval, dump its FULL column list
    # from schema_cache (loaded at startup from Hive Metastore). This is ground
    # truth — the LLM no longer needs to guess what columns exist.
    def _format_table_schema(tname: str, cols: list[dict]) -> str:
        """Multi-line format — one column per line. Easier for LLM to parse than
        a dense comma-separated list (especially when many tables exist; LLM
        was previously merging columns across tables, e.g. taking `delivered_at`
        from `shipping` and pretending it lives on `orders`).
        """
        lines = [f"### {backend_db}.{tname}"]
        for c in cols:
            lines.append(f"  - {c['column']}: {c['type']}")
        return "\n".join(lines)

    if schema_fallback:
        if ctx.catalog:
            tables_in_scope = ctx.tables_in_scope()
            schema_lines: list[str] = []
            for tname in tables_in_scope:
                if tname in schema_fallback:
                    schema_lines.append(_format_table_schema(tname, schema_fallback[tname]))
            if schema_lines:
                retrieval_block += (
                    "\n\n## Full column schema for retrieved tables (GROUND TRUTH — "
                    "use ONLY columns listed below for each table; do NOT borrow "
                    "columns from one table to use on another):\n\n"
                    + "\n\n".join(schema_lines)
                )
        else:
            # Retrieval empty (cold start / index wiped / postgres not indexed)
            # — dump ALL tables. This is the path Postgres queries take today
            # since the catalog indexer currently only walks Hive Metastore.
            lines = [
                f"# Available tables in `{backend_db}` (full schema — retrieval returned 0 hits)",
                "Use ONLY these tables/columns. Do NOT invent column names or borrow",
                "columns from one table to use on another.\n",
            ]
            for tname in sorted(schema_fallback.keys()):
                lines.append(_format_table_schema(tname, schema_fallback[tname]))
                lines.append("")
            retrieval_block = "\n".join(lines)
            log.warning(
                "sql_writer: retrieval empty for %r — using full schema_fallback (%d tables)",
                question[:60], len(schema_fallback),
            )
    elif not retrieval_block:
        retrieval_block = "(no catalog matches and no schema fallback — LLM will guess)"

    # 2. Build messages — for FOLLOWUP, prepend recent turns
    messages: list[dict] = []
    if intent == "FOLLOWUP" and conversation_history:
        for turn in conversation_history[-6:]:
            role = turn.get("role")
            content = turn.get("content")
            if role in ("user", "assistant") and content:
                messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": question})

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        dialect=dialect,
        dialect_rules=dialect_rules,
        retrieval_block=retrieval_block,
    )

    # 3. Call Claude
    cacheable = intent != "FOLLOWUP"  # follow-ups must always re-evaluate context
    try:
        resp = await gateway.complete(
            system=system_prompt,
            messages=messages,
            model=settings.llm_model_sql_writer,
            max_tokens=settings.llm_max_tokens_sql_writer,
            cacheable=cacheable,
        )
        parsed = resp.parse_json()
    except Exception as e:
        log.exception("LLM call failed: %s", e)
        return SQLWriterResult(
            sql=None,
            explanation="Không thể kết nối tới LLM. Vui lòng thử lại sau.",
            tables_used=[],
            complexity="unknown",
            valid=False,
            validation_error=f"llm_error: {type(e).__name__}",
            warnings=[],
            retrieval_summary=_summarise(ctx),
        ), ctx

    if not parsed or not parsed.get("sql"):
        log.warning(
            "sql_writer: LLM returned invalid/empty JSON. parsed=%r raw_text=%r",
            parsed, resp.text[:2000],
        )
        return SQLWriterResult(
            sql=None,
            explanation="Không thể tạo câu truy vấn từ phản hồi của AI.",
            tables_used=[],
            complexity="unknown",
            valid=False,
            validation_error="llm_returned_invalid_json",
            warnings=[],
            retrieval_summary=_summarise(ctx),
        ), ctx

    sql = (parsed.get("sql") or "").strip().rstrip(";")

    # 4. Build the validation context — known tables come from BOTH the retrieval
    #    catalog (high-confidence) and the schema fallback loaded at startup.
    known_tables: set[str] = set()
    pii_columns: set[str] = set()
    for h in ctx.catalog:
        if h.table:
            known_tables.add(h.table.lower())
        if h.is_pii and h.column:
            pii_columns.add(f"{h.table}.{h.column}".lower())
    if schema_fallback:
        for t, cols in schema_fallback.items():
            known_tables.add(t.lower())

    # 5. Run guardrails
    val = guardrails.validate_sql(sql, known_tables=known_tables or None, pii_columns=pii_columns)

    return SQLWriterResult(
        sql=sql,
        explanation=str(parsed.get("explanation") or ""),
        tables_used=list(parsed.get("tables_used") or []),
        complexity=str(parsed.get("complexity") or "unknown"),
        valid=val.valid,
        validation_error=val.error,
        warnings=val.warnings or [],
        retrieval_summary=_summarise(ctx),
        aggregated=bool(parsed.get("aggregated")),
        has_date_filter=bool(parsed.get("has_date_filter")),
    ), ctx


def _summarise(ctx: RetrievalContext) -> dict:
    return {
        "catalog": len(ctx.catalog),
        "docs": len(ctx.docs),
        "history": len(ctx.history),
        "tables": ctx.tables_in_scope(),
    }
