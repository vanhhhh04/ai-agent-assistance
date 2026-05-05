"""
agents/retrieval — Data Retrieval Agents (Finch's specialists per backend).

Each agent encapsulates the integration logic for one data system so the
Supervisor + SQL Writer never have to deal with vendor quirks (Hive's
NOSASL auth, asyncpg's pool lifecycle, etc.).

Entry point: `agents.retrieval.dispatch(backend, sql)` returns a uniform
result dict {columns, rows, row_count, exec_ms, error?}.
"""

from .registry import dispatch  # noqa: F401
