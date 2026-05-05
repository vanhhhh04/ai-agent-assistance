"""
core/postgres_client.py — Postgres Bronze (ERP source) connection.

This is the secondary backend. The primary warehouse is Hive Gold.
Postgres is queried only when:
  - User asks about live operational state ("how many orders right now?")
  - User explicitly references the OLTP schema (customers, orders, products)

Schema introspection here mirrors hive_client.load_full_schema() so the
same router can serve `/api/schema/full?backend=postgres`.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import asyncpg

from .settings import settings

log = logging.getLogger("postgres_client")

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not settings.postgres_url:
            raise RuntimeError("POSTGRES_URL not configured")
        _pool = await asyncpg.create_pool(
            settings.postgres_url,
            min_size=1,
            max_size=5,
            timeout=10,
            command_timeout=30,
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def load_full_schema() -> dict[str, list[dict]]:
    """
    Read information_schema.columns + pg_description to build the same
    {table: [{column, type, comment}]} shape as Hive's loader.
    """
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT
            c.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            col_description(
                (quote_ident(c.table_schema)||'.'||quote_ident(c.table_name))::regclass,
                c.ordinal_position
            ) AS column_comment
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
        ORDER BY c.table_name, c.ordinal_position
    """)
    schema: dict[str, list[dict]] = {}
    for row in rows:
        t = row["table_name"]
        schema.setdefault(t, []).append({
            "column": row["column_name"],
            "type": (row["data_type"] or "").lower(),
            "comment": row["column_comment"],
            "nullable": row["is_nullable"] == "YES",
        })
    return schema


async def execute_query(sql: str) -> dict:
    """
    SELECT-only execution against Postgres bronze. Same return shape as hive.
    """
    pool = await get_pool()
    started = time.time()
    rows = await pool.fetch(sql)
    cols = list(rows[0].keys()) if rows else []
    rows_dict = [{c: _serialize(r[c]) for c in cols} for r in rows]
    return {
        "columns": cols,
        "rows": rows_dict,
        "row_count": len(rows_dict),
        "exec_ms": int((time.time() - started) * 1000),
    }


def _serialize(v):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
    except ImportError:
        pass
    return v


async def health_check() -> dict:
    if not settings.enable_postgres_backend:
        return {"reachable": False, "error": "postgres backend disabled"}
    try:
        pool = await get_pool()
        async with pool.acquire() as con:
            n = await con.fetchval("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'")
        return {"reachable": True, "table_count": int(n or 0)}
    except Exception as e:
        return {"reachable": False, "error": f"{type(e).__name__}: {e}"}
