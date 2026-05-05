"""
routers/health.py — /api/health and /api/ping
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter

from core import hive_client, postgres_client
from core.llm_gateway import gateway
from core.schema_cache import cache
from core.semantic_layer import semantic_layer
from core.settings import settings

router = APIRouter()


@router.get("")
async def health():
    """
    Aggregate health for the AI Agent's backends. Never fails — each
    component reports its own state. Frontend reads this to color the
    "● LIVE / ○ OFFLINE" indicator.
    """
    hive, postgres = await asyncio.gather(
        hive_client.health_check(),
        postgres_client.health_check(),
    )
    return {
        "status": "ok",
        "components": {
            "hive_gold":       hive,
            "postgres_bronze": postgres,
            "opensearch":      {"reachable": semantic_layer.is_available()},
            "schema_cache":    {
                "hive_tables": len(cache.hive),
                "postgres_tables": len(cache.postgres),
            },
        },
        "llm": {
            "provider":          gateway.provider,
            "supervisor_model":  settings.llm_model_supervisor,
            "sql_writer_model":  settings.llm_model_sql_writer,
            "configured":        gateway.is_configured(),
            "anthropic_key_set": bool(settings.anthropic_api_key),
            "gemini_key_set":    bool(settings.gemini_api_key),
        },
    }


@router.get("/ping")
async def ping():
    return {"pong": True}
