"""
core/hive_client.py — HiveServer2 connection + query execution.

The Gold layer (gold.fact_sales, gold.dim_*) is the primary warehouse the
AI Agent queries. HiveServer2 listens on port 10000 with NOSASL auth
in our local dev compose.

Two responsibilities:
  1. Schema introspection — SHOW TABLES + DESCRIBE for the warmup catalog
  2. SELECT execution — pyhive cursor → list[dict]

Hive is synchronous and blocking; we wrap calls in `asyncio.to_thread` so
the FastAPI event loop is never stalled during a long query.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import contextmanager
from typing import Any

from .settings import settings

log = logging.getLogger("hive_client")


def _import_hive():
    """Import lazily — avoids hard dep when running unit tests off-platform."""
    try:
        from pyhive import hive  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "pyhive not installed — pip install pyhive thrift sasl thrift_sasl. "
            "On Windows wheels for sasl are tricky; the project's Dockerfile "
            "installs the system libs needed."
        ) from e
    return hive


# ─────────────────────────────────────────────────────────────
#  Connection helper
# ─────────────────────────────────────────────────────────────
@contextmanager
def hive_connection():
    """
    Open a fresh HiveServer2 connection. We don't pool — pyhive connections
    are cheap to open against a local container and avoid connection-leak
    surprises across async event loops.
    """
    hive = _import_hive()
    # auth=NONE uses SASL/PLAIN and requires a username (anonymous on this server,
    # but pyhive errors if not provided). 'hive' matches the doAs=false runtime user
    # so any HDFS access is consistent across paths.
    kwargs = {
        "host":     settings.hive_host,
        "port":     settings.hive_port,
        "database": settings.hive_database,
        "auth":     settings.hive_auth,
    }
    if settings.hive_auth.upper() in ("NONE", "LDAP", "CUSTOM"):
        kwargs["username"] = "hive"
    conn = hive.Connection(**kwargs)
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
#  Schema introspection
# ─────────────────────────────────────────────────────────────
def _list_tables_sync(database: str) -> list[str]:
    with hive_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES IN {database}")
        return [row[0] for row in cur.fetchall()]


def _describe_sync(database: str, table: str) -> list[dict]:
    """
    Returns columns as [{"column": …, "type": …, "comment": …}, …].
    DESCRIBE rows past the first blank/comment line are partition info Hive
    repeats — we stop there to avoid duplicates.
    """
    cols: list[dict] = []
    with hive_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"DESCRIBE {database}.{table}")
        for row in cur.fetchall():
            if not row or not row[0] or row[0].startswith("#") or row[0].strip() == "":
                break
            cols.append({
                "column": row[0],
                "type": (row[1] or "").lower(),
                "comment": (row[2] or "").strip() or None,
            })
    return cols


async def list_tables(database: str | None = None) -> list[str]:
    db = database or settings.hive_database
    return await asyncio.to_thread(_list_tables_sync, db)


async def describe_table(table: str, database: str | None = None) -> list[dict]:
    db = database or settings.hive_database
    return await asyncio.to_thread(_describe_sync, db, table)


async def load_full_schema(database: str | None = None) -> dict[str, list[dict]]:
    """
    {table_name: [{column, type, comment}, …]} for every table in the database.
    Called from main.py lifespan and exposed via /api/schema/full.
    """
    db = database or settings.hive_database
    tables = await list_tables(db)
    schema: dict[str, list[dict]] = {}
    for t in tables:
        try:
            schema[t] = await describe_table(t, db)
        except Exception as e:
            log.warning("describe_table %s failed: %s", t, e)
            schema[t] = []
    return schema


# ─────────────────────────────────────────────────────────────
#  Query execution
# ─────────────────────────────────────────────────────────────
def _execute_sync(sql: str, max_rows: int) -> tuple[list[str], list[list[Any]]]:
    """
    Returns (column_names, rows). Caller is responsible for converting rows
    into JSON-friendly dicts. We cap at max_rows to defend against runaway
    queries that LIMIT was supposed to bound.
    """
    with hive_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0].split(".")[-1] for d in (cur.description or [])]
        rows: list[list[Any]] = []
        for i, row in enumerate(cur):
            if i >= max_rows:
                break
            rows.append(list(row))
        return cols, rows


async def execute_query(sql: str) -> dict:
    """
    Run a SELECT against Hive. Returns:
      {"columns": [...], "rows": [{col: val, ...}], "row_count": N, "exec_ms": …}

    Raises on connection / SQL syntax errors — caller (the data retrieval
    agent) catches and surfaces a friendly error.
    """
    started = time.time()
    try:
        cols, rows = await asyncio.wait_for(
            asyncio.to_thread(_execute_sync, sql, settings.hive_max_rows),
            timeout=settings.hive_query_timeout_sec,
        )
    except asyncio.TimeoutError as e:
        raise RuntimeError(
            f"Hive query exceeded {settings.hive_query_timeout_sec}s timeout"
        ) from e

    elapsed_ms = int((time.time() - started) * 1000)
    rows_dict: list[dict] = [
        {c: _serialize(v) for c, v in zip(cols, r)} for r in rows
    ]
    return {
        "columns": cols,
        "rows": rows_dict,
        "row_count": len(rows_dict),
        "exec_ms": elapsed_ms,
    }


def _serialize(v: Any) -> Any:
    """Make values JSON-safe (Decimal → float, datetime → iso, bytes → str)."""
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return v.hex()
    try:
        # Decimal → float for JSON
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
    except ImportError:
        pass
    return v


# ─────────────────────────────────────────────────────────────
#  Health check
# ─────────────────────────────────────────────────────────────
async def health_check() -> dict:
    """Used by /api/health — never raises."""
    try:
        tables = await asyncio.wait_for(list_tables(), timeout=5.0)
        return {"reachable": True, "table_count": len(tables)}
    except Exception as e:
        return {"reachable": False, "error": f"{type(e).__name__}: {e}"}


# ─────────────────────────────────────────────────────────────
#  SQL utilities for guardrails
# ─────────────────────────────────────────────────────────────
def extract_referenced_tables(sql: str) -> list[str]:
    """
    Pull table names from FROM/JOIN clauses. Lowercased, dedup-preserved order.
    Used by guardrails to verify referenced tables actually exist in the catalog.
    """
    pattern = re.compile(r"(?:FROM|JOIN)\s+([\w\.]+)", re.IGNORECASE)
    seen: set[str] = set()
    out: list[str] = []
    for m in pattern.finditer(sql):
        t = m.group(1).lower()
        # strip database prefix if present (gold.fact_sales → fact_sales for catalog lookup)
        bare = t.split(".")[-1]
        if bare not in seen:
            seen.add(bare)
            out.append(bare)
    return out
