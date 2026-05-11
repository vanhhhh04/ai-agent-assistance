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
   Subqueries in IN/EXISTS at the top-level of a WHERE conjunct ARE supported."""


_POSTGRES_RULES = """## PostgreSQL rules (operational ERP: public.*)
1. Reference tables without schema prefix (search_path=public).
2. Always end with LIMIT 100 unless the query is purely aggregated.
3. Use `ROUND(value::numeric, 2)` when displaying money.
4. Never emit DELETE/UPDATE/INSERT/DROP/CREATE/ALTER/TRUNCATE — read-only.
5. No semicolons — single statement only.
6. Use ::date casts when filtering on timestamp columns by date.
7. Vietnamese semantic mapping (same as HiveQL rules):
   - "bán chạy nhất" → quantity (`SUM(quantity)` or `COUNT(DISTINCT order_id)`), not revenue.
   - "doanh thu" → `SUM(item_total)` or `SUM(quantity * unit_price)`.
   - "hiện nay" → no fixed year filter (use most-recent data or skip the filter)."""


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

    # 1. Retrieve focused context
    ctx = semantic_layer.retrieve(
        question,
        backend_filter=backend_db if backend == "hive_gold" else None,
    )
    retrieval_block = ctx.as_prompt_block() or "(no catalog matches — use general SQL knowledge)"

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
