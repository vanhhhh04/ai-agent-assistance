"""
agents/retrieval/registry.py — dispatch SQL to the right specialist.

Adding a new backend later (e.g. Presto, Snowflake, Druid) only requires
adding a new module + an entry in `_AGENTS`. Supervisor returns a backend
name; this module turns that name into an `execute()` call.
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable

from . import hive_agent, postgres_agent

log = logging.getLogger("retrieval.registry")


_AGENTS: dict[str, Callable[[str], Awaitable[dict]]] = {
    "hive_gold":       hive_agent.execute,
    "postgres_bronze": postgres_agent.execute,
}


async def dispatch(backend: str, sql: str) -> dict:
    fn = _AGENTS.get(backend)
    if fn is None:
        return {"error": f"Unknown backend '{backend}'", "backend": backend}
    return await fn(sql)


def known_backends() -> list[str]:
    return list(_AGENTS.keys())
