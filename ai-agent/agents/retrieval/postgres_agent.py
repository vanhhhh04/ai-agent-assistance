"""
Postgres Data Retrieval Agent — speaks to the Bronze ERP source.
Used when Supervisor routes to backend=postgres_bronze.
"""

from __future__ import annotations

import logging

from core import postgres_client
from core.settings import settings

log = logging.getLogger("retrieval.postgres")


async def execute(sql: str) -> dict:
    if not settings.enable_postgres_backend:
        return {
            "error": "postgres backend is disabled (set ENABLE_POSTGRES_BACKEND=true to enable)",
            "backend": "postgres_bronze",
        }
    try:
        result = await postgres_client.execute_query(sql)
        result["backend"] = "postgres_bronze"
        return result
    except Exception as e:
        log.warning("postgres execute failed: %s", e, exc_info=True)
        return {"error": f"{type(e).__name__}: {e}", "backend": "postgres_bronze"}
