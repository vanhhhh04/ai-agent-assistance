"""
core/schema_cache.py — In-memory schema cache loaded at app startup.

Why cache? The semantic layer is good for *focused* retrieval, but two
things need the FULL list of tables:
  - The supervisor prompt mentions the universe of available tables
  - Guardrails reject SQL that references tables outside the warehouse

Loading both Hive Gold and Postgres Bronze schemas takes a few seconds
each — we do it once during FastAPI lifespan, then keep it in memory.
A periodic refresher could be added later if schemas drift in production.
"""

from __future__ import annotations

import asyncio
import logging

from core import hive_client, postgres_client
from core.settings import settings

log = logging.getLogger("schema_cache")


class SchemaCache:
    def __init__(self):
        self.hive: dict[str, list[dict]] = {}
        self.postgres: dict[str, list[dict]] = {}
        self._loaded_at: float | None = None

    async def load(self) -> None:
        """Run both loaders in parallel; tolerate one failing."""
        async def _safe_hive():
            try:
                return await hive_client.load_full_schema()
            except Exception as e:
                log.warning("hive schema load failed (%s) — Gold queries will fall back to retrieval", e)
                return {}

        async def _safe_pg():
            if not settings.enable_postgres_backend:
                return {}
            try:
                return await postgres_client.load_full_schema()
            except Exception as e:
                log.warning("postgres schema load failed: %s", e)
                return {}

        self.hive, self.postgres = await asyncio.gather(_safe_hive(), _safe_pg())
        log.info(
            "schema cache loaded | hive=%d tables | postgres=%d tables",
            len(self.hive), len(self.postgres),
        )

    def get(self, backend: str) -> dict[str, list[dict]]:
        return self.hive if backend == "hive_gold" else self.postgres

    def tables_for(self, backend: str) -> list[str]:
        return list(self.get(backend).keys())

    def all_tables(self) -> list[str]:
        return sorted(set(self.hive) | set(self.postgres))


cache = SchemaCache()
