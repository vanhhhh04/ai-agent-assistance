"""
core/semantic_layer.py — Hybrid retrieval over OpenSearch.

Equivalent of Finch's "semantic layer" — maps a natural-language question
to the *minimal* set of catalog entries / docs / past queries that the
SQL Writer Agent actually needs to see in its prompt.

Without this layer, the SQL Writer would be forced to either:
  (a) see the full schema in every prompt — wasteful, noisy, and slow
  (b) see no schema at all and hallucinate column names

Hybrid retrieval = kNN (semantic) + BM25 (lexical). We run both, merge by
reciprocal-rank fusion, and return top-K.

Three indices (created by opensearch/init_indices.sh):
  - finch_catalog : table + column metadata          (table_name, column_name, sample_values, …)
  - table_docs    : business documentation chunks     (markdown sections)
  - query_log     : NL question + SQL history         (for "similar past query" hints)

Embeddings: 768-d sentence-transformers (paraphrase-multilingual-mpnet-base-v2).
We import the embedder lazily — the model is ~420MB and we want it loaded
exactly once at startup, not per-request.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from opensearchpy import OpenSearch

from .settings import settings

# Make the project's `opensearch/` package importable. The AI agent is a
# sibling folder, so without this the relative path to embedder.py / config.py
# is not on sys.path. cli/startup.sh does the same for indexers.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from opensearch import config as os_config  # noqa: E402
from opensearch.embedder import embed_one, warmup as embed_warmup  # noqa: E402

log = logging.getLogger("semantic_layer")


# ─────────────────────────────────────────────────────────────
#  Result shapes
# ─────────────────────────────────────────────────────────────
@dataclass
class CatalogHit:
    table: str
    column: str | None
    column_type: str | None
    description: str | None
    sample_values: list[str]
    is_pii: bool
    score: float
    doc_type: str  # "table" | "column"


@dataclass
class DocHit:
    table: str
    title: str
    content: str
    examples: str
    score: float


@dataclass
class HistoryHit:
    nl_question: str
    generated_sql: str
    tables_used: list[str]
    user_thumbs_up: bool | None
    score: float


@dataclass
class RetrievalContext:
    """
    Bundle the SQL Writer prompt actually consumes. Use `as_prompt_block()`
    to render into a compact string the LLM can read.
    """
    catalog: list[CatalogHit] = field(default_factory=list)
    docs: list[DocHit] = field(default_factory=list)
    history: list[HistoryHit] = field(default_factory=list)

    def tables_in_scope(self) -> list[str]:
        """Distinct table names mentioned by any retrieved doc — used by guardrails."""
        seen: set[str] = set()
        out: list[str] = []
        for h in self.catalog + self.docs:  # type: ignore[operator]
            t = h.table
            if t and t not in seen:
                seen.add(t)
                out.append(t)
        return out

    def as_prompt_block(self) -> str:
        """Render into the system-prompt section the SQL Writer reads."""
        lines: list[str] = []

        # ── Tables & columns ──
        if self.catalog:
            # Group by table
            by_table: dict[str, list[CatalogHit]] = {}
            for h in self.catalog:
                by_table.setdefault(h.table, []).append(h)

            lines.append("## Relevant tables & columns (top matches)")
            for table, hits in by_table.items():
                hits.sort(key=lambda h: 0 if h.doc_type == "table" else 1)
                table_doc = next((h for h in hits if h.doc_type == "table"), None)
                hdr = f"### {table}"
                if table_doc and table_doc.description:
                    hdr += f"  — {table_doc.description}"
                lines.append(hdr)
                for h in hits:
                    if h.doc_type != "column":
                        continue
                    pii_tag = "  [PII]" if h.is_pii else ""
                    desc = f"  — {h.description}" if h.description else ""
                    samples = ""
                    if h.sample_values:
                        s = ", ".join(h.sample_values[:5])
                        samples = f"  e.g. {s}"
                    lines.append(f"- {h.column} ({h.column_type or '?'}){pii_tag}{desc}{samples}")
            lines.append("")

        # ── Business docs ──
        if self.docs:
            lines.append("## Business documentation")
            for d in self.docs:
                lines.append(f"### {d.table} — {d.title}")
                lines.append(d.content.strip())
                if d.examples:
                    lines.append("Example queries from docs:")
                    lines.append("```sql")
                    lines.append(d.examples.strip())
                    lines.append("```")
                lines.append("")

        # ── Similar past queries (only successful ones with thumbs up) ──
        good_history = [h for h in self.history if h.user_thumbs_up is not False]
        if good_history:
            lines.append("## Similar past queries (use as reference, do not copy verbatim)")
            for h in good_history:
                marker = "👍 " if h.user_thumbs_up else ""
                lines.append(f"- {marker}NL: {h.nl_question}")
                lines.append(f"  SQL: {h.generated_sql}")
            lines.append("")

        return "\n".join(lines).strip()


# ─────────────────────────────────────────────────────────────
#  Semantic Layer
# ─────────────────────────────────────────────────────────────
class SemanticLayer:
    """
    Retrieves the K most relevant catalog entries / docs / past queries
    for a given NL question. One instance per process.
    """

    def __init__(self):
        self._client: OpenSearch | None = None

    # --- client lifecycle ----------------------------------------
    def _ensure_client(self) -> OpenSearch:
        if self._client is None:
            auth = None
            if settings.opensearch_user and settings.opensearch_password:
                auth = (settings.opensearch_user, settings.opensearch_password)
            self._client = OpenSearch(
                hosts=[settings.opensearch_url],
                http_auth=auth,
                verify_certs=False,
                ssl_show_warn=False,
                request_timeout=30,
            )
        return self._client

    async def warmup(self) -> None:
        """Force-load the embedder model (called from FastAPI lifespan)."""
        try:
            embed_warmup()
            log.info("semantic layer ready (embedder warm)")
        except Exception as e:
            log.warning("embedder warmup failed: %s — kNN retrieval will fall back to BM25", e)

    def is_available(self) -> bool:
        try:
            return self._ensure_client().ping()
        except Exception:
            return False

    # --- retrieval -----------------------------------------------
    def retrieve(
        self,
        question: str,
        *,
        top_k_catalog: int | None = None,
        top_k_docs: int | None = None,
        top_k_history: int | None = None,
        backend_filter: str | None = None,
    ) -> RetrievalContext:
        """
        Run hybrid retrieval. Backend_filter = "gold" / "bronze" filters
        catalog hits by database name (catalog docs carry `database` field).
        """
        top_k_catalog = top_k_catalog or settings.retrieval_top_k_catalog
        top_k_docs = top_k_docs or settings.retrieval_top_k_docs
        top_k_history = top_k_history or settings.retrieval_top_k_history

        try:
            client = self._ensure_client()
        except Exception as e:
            log.warning("OpenSearch unreachable: %s — returning empty retrieval", e)
            return RetrievalContext()

        # 1. Embed the question once — share across kNN queries
        try:
            q_vec = embed_one(question)
        except Exception as e:
            log.warning("embedding failed (%s) — falling back to pure BM25", e)
            q_vec = None

        ctx = RetrievalContext()
        ctx.catalog = self._search_catalog(client, question, q_vec, top_k_catalog, backend_filter)
        ctx.docs = self._search_docs(client, question, q_vec, top_k_docs)
        ctx.history = self._search_history(client, question, q_vec, top_k_history)

        log.info(
            "retrieval | q=%s.. | catalog=%d docs=%d history=%d",
            question[:60], len(ctx.catalog), len(ctx.docs), len(ctx.history),
        )
        return ctx

    # ---- per-index search helpers ----
    def _search_catalog(
        self,
        client: OpenSearch,
        question: str,
        q_vec: list[float] | None,
        size: int,
        backend_filter: str | None,
    ) -> list[CatalogHit]:
        body = self._build_hybrid_query(
            question=question,
            q_vec=q_vec,
            text_fields=["table_name^3", "column_name^2", "description", "synonyms", "sample_values"],
            size=size,
            extra_filter=({"term": {"database": backend_filter}} if backend_filter else None),
        )
        try:
            res = client.search(index=os_config.INDEX_CATALOG, body=body)
        except Exception as e:
            log.warning("catalog search failed: %s", e)
            return []

        out: list[CatalogHit] = []
        for h in res["hits"]["hits"]:
            s = h["_source"]
            score = float(h.get("_score") or 0.0)
            if score < settings.retrieval_min_score and h.get("_score") is not None:
                continue
            out.append(CatalogHit(
                table=s.get("table_name") or "",
                column=s.get("column_name"),
                column_type=s.get("column_type"),
                description=s.get("description"),
                sample_values=s.get("sample_values") or [],
                is_pii=bool(s.get("is_pii")),
                score=score,
                doc_type=s.get("doc_type") or "column",
            ))
        return out

    def _search_docs(
        self,
        client: OpenSearch,
        question: str,
        q_vec: list[float] | None,
        size: int,
    ) -> list[DocHit]:
        body = self._build_hybrid_query(
            question=question,
            q_vec=q_vec,
            text_fields=["title^2", "content", "examples"],
            size=size,
        )
        try:
            res = client.search(index=os_config.INDEX_TABLE_DOCS, body=body)
        except Exception as e:
            log.warning("docs search failed: %s", e)
            return []

        out: list[DocHit] = []
        for h in res["hits"]["hits"]:
            s = h["_source"]
            out.append(DocHit(
                table=s.get("table_name") or "",
                title=s.get("title") or s.get("section") or "doc",
                content=s.get("content") or "",
                examples=s.get("examples") or "",
                score=float(h.get("_score") or 0.0),
            ))
        return out

    def _search_history(
        self,
        client: OpenSearch,
        question: str,
        q_vec: list[float] | None,
        size: int,
    ) -> list[HistoryHit]:
        # Only return successful past queries — never feed bad SQL back to the LLM.
        body = self._build_hybrid_query(
            question=question,
            q_vec=q_vec,
            text_fields=["nl_question"],
            size=size,
            extra_filter={"term": {"status": "success"}},
        )
        try:
            res = client.search(index=os_config.INDEX_QUERY_LOG, body=body)
        except Exception as e:
            log.warning("history search failed: %s", e)
            return []

        out: list[HistoryHit] = []
        for h in res["hits"]["hits"]:
            s = h["_source"]
            out.append(HistoryHit(
                nl_question=s.get("nl_question") or "",
                generated_sql=s.get("generated_sql") or "",
                tables_used=s.get("tables_used") or [],
                user_thumbs_up=s.get("user_thumbs_up"),
                score=float(h.get("_score") or 0.0),
            ))
        return out

    # ---- query builder ----
    @staticmethod
    def _build_hybrid_query(
        *,
        question: str,
        q_vec: list[float] | None,
        text_fields: list[str],
        size: int,
        extra_filter: dict | None = None,
    ) -> dict:
        """
        BM25 multi_match + kNN cosine, joined under a `should` clause so
        OpenSearch returns docs that match either signal. We pre-filter
        with extra_filter (e.g. backend=gold) to keep both sub-queries
        on the same candidate set.
        """
        should: list[dict] = [
            {"multi_match": {
                "query": question,
                "fields": text_fields,
                "type": "best_fields",
                "fuzziness": "AUTO",
            }}
        ]
        if q_vec is not None:
            should.append({
                "knn": {
                    "embedding": {
                        "vector": q_vec,
                        "k": size,
                    }
                }
            })

        bool_q: dict[str, Any] = {"should": should, "minimum_should_match": 1}
        if extra_filter:
            bool_q["filter"] = [extra_filter]

        return {"size": size, "query": {"bool": bool_q}}


semantic_layer = SemanticLayer()
