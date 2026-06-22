"""
agents/supervisor.py — Supervisor Agent (Finch's "brain").

Two routing decisions in one LLM call:
  - intent:   DATA_QUERY | SCHEMA_INFO | FOLLOWUP | OUT_OF_SCOPE
  - backend:  hive_gold (analytical) | postgres_bronze (operational)

Responses are JSON-only. Failure modes:
  - LLM returns non-JSON  → fall back to (DATA_QUERY, hive_gold) — most common case
  - User question is empty → OUT_OF_SCOPE without an LLM call

Why include backend selection here instead of in SQL Writer? Because the
choice changes which schema we retrieve from OpenSearch, which dialect
the SQL Writer should produce, and which Data Retrieval Agent runs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from core.llm_gateway import gateway
from core.settings import settings

log = logging.getLogger("supervisor")


@dataclass
class SupervisorDecision:
    intent: str           # DATA_QUERY | SCHEMA_INFO | FOLLOWUP | OUT_OF_SCOPE
    backend: str          # hive_gold | postgres_bronze
    confidence: float
    reasoning: str

    def to_dict(self) -> dict:
        return {
            "intent": self.intent,
            "backend": self.backend,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
        }


SYSTEM_PROMPT = """You are the Supervisor Agent in an AI Data Assistant for an e-commerce data warehouse.

Your ONLY job is to route the user's question. Output strict JSON, nothing else.

# Intents
- DATA_QUERY:   user wants data (revenue, top products, orders, customers, trends, comparisons)
- SCHEMA_INFO:  user asks what tables/columns exist or what data is available
- FOLLOWUP:     user is continuing a previous question ("compare to last month", "and by region", "same but for X")
- OUT_OF_SCOPE: not about business data (greetings, jokes, unrelated topics)

# Backends
- hive_gold:        analytical Gold star schema (fact_sales, fact_reviews, fact_feedback, dim_*).
                    Use this for revenue, aggregations, historical trends, top-K, by-period analysis.
- postgres_bronze:  live operational ERP (customers, orders, products, payments, shipping).
                    Use this for:
                    - live/realtime/current state ("đơn hàng chưa giao hiện tại", "trạng thái pending")
                    - **STOCK / INVENTORY queries** ("tồn kho", "stock", "còn hàng", "hết hàng",
                      "sắp hết hàng", "stock thấp", "stock_quantity") — Hive Gold has NO stock data,
                      ONLY `public.products.stock_quantity` in Postgres tracks inventory levels.
                    - operational transaction lookup by exact ID

If unsure, prefer `hive_gold` (it covers 95% of analytical questions). But if the question
mentions inventory/stock in any form, you MUST pick `postgres_bronze`.

# Mixed questions
If the user asks for BOTH analytical aggregation AND realtime stock in one question
(e.g. "sản phẩm tồn kho thấp nhưng bán chạy"), pick `postgres_bronze` — the SQL writer
can JOIN products with order_items there. Hive cannot serve stock at all.

# Output format — strict JSON, no markdown, no prose:
{"intent":"DATA_QUERY","backend":"hive_gold","confidence":0.92,"reasoning":"asks for monthly revenue aggregation"}
"""


async def classify(
    question: str,
    conversation_history: list[dict] | None = None,
) -> SupervisorDecision:
    question = (question or "").strip()
    if not question:
        return SupervisorDecision(
            intent="OUT_OF_SCOPE", backend="hive_gold",
            confidence=1.0, reasoning="empty question",
        )

    # Inject up to last 4 turns for FOLLOWUP detection
    msgs: list[dict] = []
    if conversation_history:
        for turn in conversation_history[-4:]:
            role = turn.get("role")
            content = turn.get("content")
            if role in ("user", "assistant") and content:
                msgs.append({"role": role, "content": content})
    msgs.append({"role": "user", "content": question})

    try:
        resp = await gateway.complete(
            system=SYSTEM_PROMPT,
            messages=msgs,
            model=settings.llm_model_supervisor,
            max_tokens=settings.llm_max_tokens_supervisor,
            cacheable=True,
        )
        parsed = resp.parse_json() or {}
    except Exception as e:
        log.warning("supervisor LLM call failed (%s) — defaulting to DATA_QUERY/hive_gold", e)
        parsed = {}

    intent = (parsed.get("intent") or "DATA_QUERY").upper()
    if intent not in ("DATA_QUERY", "SCHEMA_INFO", "FOLLOWUP", "OUT_OF_SCOPE"):
        intent = "DATA_QUERY"

    backend = (parsed.get("backend") or "hive_gold").lower()
    if backend not in ("hive_gold", "postgres_bronze"):
        backend = "hive_gold"
    if backend == "postgres_bronze" and not settings.enable_postgres_backend:
        # Falling back keeps the assistant useful even if Bronze is down for maintenance
        log.info("supervisor wanted postgres_bronze but it is disabled — using hive_gold")
        backend = "hive_gold"

    decision = SupervisorDecision(
        intent=intent,
        backend=backend,
        confidence=float(parsed.get("confidence") or 0.7),
        reasoning=str(parsed.get("reasoning") or ""),
    )
    log.info("supervisor → %s", decision.to_dict())
    return decision
