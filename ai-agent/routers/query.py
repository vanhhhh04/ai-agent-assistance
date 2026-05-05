"""
routers/query.py — main pipeline orchestrator (SSE streamed).

Pipeline (mirrors Finch end-to-end):
   client → POST /api/query/ask
   ↓
   Supervisor.classify(question)         [step: supervisor]
   ↓
   if SCHEMA_INFO  → return schema dict   [terminal]
   if OUT_OF_SCOPE → return polite no    [terminal]
   ↓
   SQLWriter.generate_sql(question)      [step: retrieval, sql_writer]
       └── semantic_layer.retrieve()
       └── llm gateway → Claude
       └── guardrails.validate_sql()
   ↓
   retrieval.dispatch(backend, sql)      [step: execution]
   ↓
   query_log.log_query()                 [step: telemetry]
   ↓
   final SSE event {type: "result", data: {...}}

Each step yields a Server-Sent Event so the React UI can light up the
"agent pipeline" sidebar in real time. The same shape Finch uses for its
Slack callbacks.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from agents import retrieval as retrieval_dispatch
from agents.sql_writer import generate_sql
from agents.supervisor import classify
from core.schema_cache import cache

# Make the project's `opensearch/` package importable for query_logger
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from opensearch.query_logger import log_query, update_feedback  # noqa: E402

log = logging.getLogger("router.query")
router = APIRouter()


# ─────────────────────────────────────────────────────────────
#  Request shape
# ─────────────────────────────────────────────────────────────
class QueryRequest(BaseModel):
    question: str
    conversation_history: list[dict] = Field(default_factory=list)
    session_id: str = "default"
    user_id: str | None = None
    allow_pii: bool = False


class FeedbackRequest(BaseModel):
    query_id: str
    thumbs_up: bool
    feedback_text: str | None = None


# ─────────────────────────────────────────────────────────────
#  SSE helper
# ─────────────────────────────────────────────────────────────
def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


# ─────────────────────────────────────────────────────────────
#  Pipeline
# ─────────────────────────────────────────────────────────────
async def _run(req: QueryRequest):
    """Async generator yielding SSE events. Wrapped by StreamingResponse."""
    started = time.time()
    question = (req.question or "").strip()

    # ── STEP 1: Supervisor ───────────────────────────────
    yield _sse({"type": "step", "step": "supervisor", "status": "running",
                "message": "Phân tích yêu cầu..."})
    decision = await classify(question, req.conversation_history)
    yield _sse({"type": "step", "step": "supervisor", "status": "done",
                "data": decision.to_dict(),
                "message": f"Intent={decision.intent} · Backend={decision.backend}"})

    # ── Terminal intents ─────────────────────────────────
    if decision.intent == "OUT_OF_SCOPE":
        yield _sse({"type": "result", "data": {
            "intent": decision.intent,
            "backend": decision.backend,
            "explanation": (
                "Câu hỏi này nằm ngoài phạm vi dữ liệu kinh doanh. "
                "Hãy hỏi về doanh thu, sản phẩm, đơn hàng, khách hàng, đánh giá hoặc feedback."
            ),
            "sql": None, "rows": [], "columns": [], "row_count": 0,
        }})
        return

    if decision.intent == "SCHEMA_INFO":
        schema = cache.get(decision.backend)
        # Compact list view: {table: [col, …]} (drop types here — full types come from /api/schema/full)
        compact = {t: [c["column"] for c in cols] for t, cols in schema.items()}
        yield _sse({"type": "result", "data": {
            "intent": decision.intent,
            "backend": decision.backend,
            "explanation": f"Hiện có {len(compact)} bảng trong {decision.backend}.",
            "schema_info": compact,
            "sql": None, "rows": [], "columns": [], "row_count": 0,
        }})
        return

    # ── STEP 2: Retrieval + SQL Writer ────────────────────
    yield _sse({"type": "step", "step": "metadata", "status": "running",
                "message": "Tìm bảng & cột liên quan trong semantic layer..."})

    try:
        sql_result, retrieval_ctx = await generate_sql(
            question=question,
            backend=decision.backend,
            intent=decision.intent,
            conversation_history=req.conversation_history,
            schema_fallback=cache.get(decision.backend),
        )
    except Exception as e:
        log.exception("sql_writer failed")
        yield _sse({"type": "step", "step": "sql_writer", "status": "error",
                    "message": f"SQL Writer error: {type(e).__name__}"})
        yield _sse({"type": "error", "message": f"Không tạo được SQL: {e}"})
        return

    yield _sse({"type": "step", "step": "metadata", "status": "done",
                "data": sql_result.retrieval_summary,
                "message": (
                    f"Catalog: {sql_result.retrieval_summary['catalog']} hits · "
                    f"Docs: {sql_result.retrieval_summary['docs']} · "
                    f"Past queries: {sql_result.retrieval_summary['history']}"
                )})

    yield _sse({"type": "step", "step": "sql_writer", "status": "running",
                "message": "Đang tạo câu lệnh SQL..."})

    if not sql_result.valid or not sql_result.sql:
        err = sql_result.validation_error or "SQL không hợp lệ"
        yield _sse({"type": "step", "step": "sql_writer", "status": "error",
                    "message": err})
        # Telemetry: log the failure for future retrieval improvement
        log_query(
            nl_question=question,
            generated_sql=sql_result.sql or "",
            status="guardrail_fail" if sql_result.sql else "llm_error",
            tables_used=sql_result.tables_used,
            user_id=req.user_id,
            session_id=req.session_id,
            error_message=err,
            retrieval_top_k=sql_result.retrieval_summary,
        )
        yield _sse({"type": "error", "message": f"Không tạo được SQL hợp lệ: {err}"})
        return

    yield _sse({"type": "step", "step": "sql_writer", "status": "done",
                "data": {
                    "sql": sql_result.sql,
                    "complexity": sql_result.complexity,
                    "tables_used": sql_result.tables_used,
                    "warnings": sql_result.warnings,
                },
                "message": f"SQL sẵn sàng (độ phức tạp: {sql_result.complexity})"})

    # ── STEP 3: Execution via Data Retrieval Agent ──────
    yield _sse({"type": "step", "step": "execution", "status": "running",
                "message": f"Thực thi trên {decision.backend}..."})

    exec_started = time.time()
    exec_result = await retrieval_dispatch.dispatch(decision.backend, sql_result.sql)
    exec_ms = int((time.time() - exec_started) * 1000)

    if "error" in exec_result:
        yield _sse({"type": "step", "step": "execution", "status": "error",
                    "message": exec_result["error"]})
        log_query(
            nl_question=question,
            generated_sql=sql_result.sql,
            status="exec_error",
            tables_used=sql_result.tables_used,
            user_id=req.user_id,
            session_id=req.session_id,
            error_message=exec_result["error"],
            exec_ms=exec_ms,
            retrieval_top_k=sql_result.retrieval_summary,
        )
        yield _sse({"type": "error",
                    "message": f"Lỗi thực thi: {exec_result['error']}"})
        return

    yield _sse({"type": "step", "step": "execution", "status": "done",
                "message": f"Trả về {exec_result['row_count']} hàng trong {exec_ms}ms"})

    # ── STEP 4: Telemetry — write to query_log ──────────
    yield _sse({"type": "step", "step": "result_formatter", "status": "running",
                "message": "Định dạng kết quả..."})

    query_id = log_query(
        nl_question=question,
        generated_sql=sql_result.sql,
        status="success",
        tables_used=sql_result.tables_used,
        row_count=exec_result["row_count"],
        exec_ms=exec_ms,
        user_id=req.user_id,
        session_id=req.session_id,
        retrieval_top_k=sql_result.retrieval_summary,
    )

    yield _sse({"type": "step", "step": "result_formatter", "status": "done"})

    total_ms = int((time.time() - started) * 1000)
    yield _sse({"type": "result", "data": {
        "intent":         decision.intent,
        "backend":        decision.backend,
        "sql":            sql_result.sql,
        "explanation":    sql_result.explanation,
        "tables_used":    sql_result.tables_used,
        "complexity":     sql_result.complexity,
        "warnings":       sql_result.warnings,
        "columns":        exec_result["columns"],
        "rows":           exec_result["rows"],
        "row_count":      exec_result["row_count"],
        "exec_ms":        exec_ms,
        "total_ms":       total_ms,
        "query_id":       query_id,
        "retrieval":      sql_result.retrieval_summary,
        "aggregated":     sql_result.aggregated,
    }})


# ─────────────────────────────────────────────────────────────
#  Endpoints
# ─────────────────────────────────────────────────────────────
@router.post("/ask")
async def ask(request: QueryRequest):
    return StreamingResponse(
        _run(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control":   "no-cache",
            "X-Accel-Buffering": "no",   # disable Nginx buffering
            "Connection":      "keep-alive",
        },
    )


@router.post("/feedback")
async def feedback(req: FeedbackRequest):
    """Thumbs-up/down on a past query. Updates query_log so future retrieval can prefer good SQL."""
    ok = update_feedback(req.query_id, req.thumbs_up, req.feedback_text)
    return {"ok": ok, "query_id": req.query_id}
