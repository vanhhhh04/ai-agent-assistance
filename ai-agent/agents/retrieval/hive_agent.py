"""
Hive Data Retrieval Agent — speaks to HiveServer2 (gold warehouse).
"""

from __future__ import annotations

import logging

from core import hive_client

log = logging.getLogger("retrieval.hive")


async def execute(sql: str) -> dict:
    """
    Run SQL against gold.* tables. Returns:
      {"columns": [...], "rows": [...], "row_count": N, "exec_ms": ms}
    or {"error": "..."} on failure.
    """
    try:
        result = await hive_client.execute_query(sql)
        result["backend"] = "hive_gold"
        return result
    except Exception as e:
        log.warning("hive execute failed: %s", e, exc_info=True)
        return {"error": f"{type(e).__name__}: {e}", "backend": "hive_gold"}
