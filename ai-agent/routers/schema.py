"""
routers/schema.py — schema introspection endpoints.

Used by the React UI's Schema tab and by the SCHEMA_INFO intent path.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from core.schema_cache import cache

router = APIRouter()


@router.get("/full")
async def full_schema(backend: str = Query("hive_gold", regex="^(hive_gold|postgres_bronze)$")):
    schema = cache.get(backend)
    return {
        "backend": backend,
        "table_count": len(schema),
        "schema": schema,
    }


@router.get("/tables")
async def tables(backend: str = Query("hive_gold", regex="^(hive_gold|postgres_bronze)$")):
    return {"backend": backend, "tables": cache.tables_for(backend)}
