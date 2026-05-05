"""
Query log writer — library used by the AI agent runtime.

Each successful (or failed) query produces 1 doc in the `query_log` index.
The agent calls log_query() after execution; the doc later feeds into the
"similar past queries" retrieval signal at Phase 3.

Embedding is left empty here — Phase 3 will add it. The text fields are still
searchable via BM25 immediately.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

from . import config
from .client import get_client

log = logging.getLogger("query_logger")

QueryStatus = Literal["success", "guardrail_fail", "exec_error", "llm_error"]


def log_query(
    nl_question: str,
    generated_sql: str,
    status: QueryStatus,
    *,
    tables_used: Optional[list[str]] = None,
    columns_used: Optional[list[str]] = None,
    row_count: Optional[int] = None,
    exec_ms: Optional[int] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    model: Optional[str] = None,
    error_message: Optional[str] = None,
    user_thumbs_up: Optional[bool] = None,
    user_feedback: Optional[str] = None,
    retrieval_top_k: Optional[dict] = None,
) -> str:
    """
    POST 1 doc into the query_log index. Returns the generated query_id.

    Failure to write to OpenSearch is logged but NOT raised — the agent should
    never fail just because telemetry is down. Caller can treat empty return
    as "log skipped" if needed.
    """
    query_id = str(uuid.uuid4())
    doc = {
        "query_id":        query_id,
        "nl_question":     nl_question,
        "generated_sql":   generated_sql,
        "status":          status,
        "tables_used":     tables_used or [],
        "columns_used":    columns_used or [],
        "row_count":       row_count,
        "exec_ms":         exec_ms,
        "user_id":         user_id,
        "session_id":      session_id,
        "model":           model,
        "error_message":   error_message,
        "user_thumbs_up":  user_thumbs_up,
        "user_feedback":   user_feedback,
        "retrieval_top_k": retrieval_top_k or {},
        "created_at":      datetime.now(timezone.utc).isoformat(),
    }

    try:
        client = get_client()
        client.index(
            index=config.INDEX_QUERY_LOG,
            id=query_id,
            body=doc,
            refresh=False,    # async — don't block on refresh
        )
    except Exception as e:
        # Telemetry must never break the agent. Log + swallow.
        log.warning("query_log write failed (%s): %s", type(e).__name__, e)
        return ""

    return query_id


def update_feedback(query_id: str, thumbs_up: bool,
                    feedback_text: Optional[str] = None) -> bool:
    """
    Update an existing query_log doc with user feedback. Used by the API
    endpoint that handles thumbs-up/down clicks in the UI.
    """
    if not query_id:
        return False
    body = {"doc": {"user_thumbs_up": thumbs_up}}
    if feedback_text is not None:
        body["doc"]["user_feedback"] = feedback_text
    try:
        client = get_client()
        client.update(index=config.INDEX_QUERY_LOG, id=query_id, body=body, refresh=True)
        return True
    except Exception as e:
        log.warning("query_log update failed (%s): %s", type(e).__name__, e)
        return False
