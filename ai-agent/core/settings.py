"""
core/settings.py
Centralised configuration. All env knobs live here so other modules
don't sprinkle os.getenv() around.

Two backends:
  - Hive Gold (HiveServer2 :10000)   → main analytics warehouse
  - Postgres Bronze (ERP source)     → optional, for live operational queries

OpenSearch is the retrieval layer — finch_catalog, table_docs, query_log.

LLM provider is pluggable via `LLM_PROVIDER` env (anthropic | gemini).
Each provider has its own API key + default model names; the active
provider's models are exposed through the `llm_model_*` properties so
agents stay provider-agnostic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else default


@dataclass
class Settings:
    # ── LLM Gateway ────────────────────────────────────────────
    llm_provider: str = field(default_factory=lambda: (_env("LLM_PROVIDER", "anthropic") or "anthropic").lower())

    # Anthropic
    anthropic_api_key: str | None = field(default_factory=lambda: _env("ANTHROPIC_API_KEY"))
    anthropic_model_supervisor: str = field(default_factory=lambda: _env("ANTHROPIC_MODEL_SUPERVISOR", "claude-haiku-4-5-20251001"))
    anthropic_model_sql_writer: str = field(default_factory=lambda: _env("ANTHROPIC_MODEL_SQL_WRITER", "claude-sonnet-4-6"))
    # Adaptive thinking on the SQL writer (Opus 4.6+ / Sonnet 4.6+ only).
    # Skipped automatically on models that don't support it.
    anthropic_adaptive_thinking: bool = field(default_factory=lambda: _env("ANTHROPIC_ADAPTIVE_THINKING", "true").lower() == "true")

    # Gemini (Google AI Studio)
    gemini_api_key: str | None = field(default_factory=lambda: _env("GEMINI_API_KEY"))
    gemini_model_supervisor: str = field(default_factory=lambda: _env("GEMINI_MODEL_SUPERVISOR", "gemini-2.5-flash"))
    gemini_model_sql_writer: str = field(default_factory=lambda: _env("GEMINI_MODEL_SQL_WRITER", "gemini-2.5-flash"))

    # Token + cache budgets (provider-agnostic)
    llm_max_tokens_supervisor: int = field(default_factory=lambda: int(_env("LLM_MAX_TOKENS_SUPERVISOR", "300")))
    llm_max_tokens_sql_writer: int = field(default_factory=lambda: int(_env("LLM_MAX_TOKENS_SQL_WRITER", "1500")))
    llm_cache_ttl_seconds: int = field(default_factory=lambda: int(_env("LLM_CACHE_TTL_SECONDS", "300")))
    llm_cache_max_entries: int = field(default_factory=lambda: int(_env("LLM_CACHE_MAX_ENTRIES", "256")))

    # ── Hive Gold warehouse (primary backend) ──────────────────
    hive_host: str = field(default_factory=lambda: _env("HIVE_HOST", "localhost"))
    hive_port: int = field(default_factory=lambda: int(_env("HIVE_PORT", "10000")))
    hive_database: str = field(default_factory=lambda: _env("HIVE_DB", "gold"))
    hive_auth: str = field(default_factory=lambda: _env("HIVE_AUTH", "NONE"))
    hive_query_timeout_sec: int = field(default_factory=lambda: int(_env("HIVE_QUERY_TIMEOUT_SEC", "60")))
    hive_max_rows: int = field(default_factory=lambda: int(_env("HIVE_MAX_ROWS", "1000")))

    # ── Postgres Bronze (operational source) ───────────────────
    postgres_url: str | None = field(default_factory=lambda: _env(
        "POSTGRES_URL", "postgresql://postgres:postgres@localhost:5433/ecommerce"
    ))
    enable_postgres_backend: bool = field(default_factory=lambda: _env("ENABLE_POSTGRES_BACKEND", "true") == "true")

    # ── OpenSearch retrieval ───────────────────────────────────
    opensearch_url: str = field(default_factory=lambda: _env("OPENSEARCH_URL", "http://localhost:9200"))
    opensearch_user: str | None = field(default_factory=lambda: _env("OPENSEARCH_USER"))
    opensearch_password: str | None = field(default_factory=lambda: _env("OPENSEARCH_PASSWORD"))

    # ── Retrieval tuning ───────────────────────────────────────
    retrieval_top_k_catalog: int = field(default_factory=lambda: int(_env("RETRIEVAL_TOP_K_CATALOG", "8")))
    retrieval_top_k_docs: int = field(default_factory=lambda: int(_env("RETRIEVAL_TOP_K_DOCS", "3")))
    retrieval_top_k_history: int = field(default_factory=lambda: int(_env("RETRIEVAL_TOP_K_HISTORY", "3")))
    retrieval_min_score: float = field(default_factory=lambda: float(_env("RETRIEVAL_MIN_SCORE", "0.3")))

    # ── Server ─────────────────────────────────────────────────
    host: str = field(default_factory=lambda: _env("HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(_env("PORT", "8000")))
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))

    # ── Provider-resolved properties ───────────────────────────
    # Agents read `settings.llm_model_supervisor` without caring which
    # provider is active. Switching provider = change LLM_PROVIDER in .env.
    @property
    def llm_model_supervisor(self) -> str:
        if self.llm_provider == "gemini":
            return self.gemini_model_supervisor
        return self.anthropic_model_supervisor

    @property
    def llm_model_sql_writer(self) -> str:
        if self.llm_provider == "gemini":
            return self.gemini_model_sql_writer
        return self.anthropic_model_sql_writer

    @property
    def active_api_key(self) -> str | None:
        if self.llm_provider == "gemini":
            return self.gemini_api_key
        return self.anthropic_api_key


settings = Settings()
