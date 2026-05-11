"""
core/llm_gateway.py — Generative AI Gateway (Finch component).

The single mediator between agents and LLM providers. It does NOT speak
any provider-specific SDK directly — that's the job of `llm_adapters/`.
Responsibilities owned here:

  - Pick the active adapter (anthropic | gemini) via settings.llm_provider
  - In-memory response cache (idempotent prompts → 1 LLM call)
  - Retry with exponential backoff on TransientLLMError
  - JSON extraction helper that handles markdown-wrapped output
  - Token usage telemetry

Adding a new provider = add a file in `core/llm_adapters/` and register it
in `core/llm_adapters/__init__.py`. Nothing here changes.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
import time
from collections import OrderedDict
from dataclasses import dataclass

from .llm_adapters import (
    LLMAdapter,
    PermanentLLMError,
    TransientLLMError,
    get_adapter,
)
from .settings import settings

log = logging.getLogger("llm_gateway")


# ─────────────────────────────────────────────────────────────
#  Response shape
# ─────────────────────────────────────────────────────────────
@dataclass
class LLMResponse:
    text: str
    model: str
    provider: str
    input_tokens: int
    output_tokens: int
    latency_ms: int
    cache_hit: bool = False

    def parse_json(self) -> dict | None:
        """
        Extract JSON. Handles three common shapes:
          - bare JSON
          - ```json ... ``` markdown fence
          - JSON embedded in prose (first `{...}` block)
        Returns None if nothing parseable.
        """
        txt = self.text.strip()
        txt = re.sub(r"^```(?:json)?\s*", "", txt)
        txt = re.sub(r"\s*```$", "", txt)
        try:
            return json.loads(txt)
        except json.JSONDecodeError:
            pass
        m = re.search(r"\{.*\}", txt, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                return None
        return None


# ─────────────────────────────────────────────────────────────
#  Cache (LRU + TTL)
# ─────────────────────────────────────────────────────────────
class _ResponseCache:
    """sha1(provider+model+system+messages) → (response, expiry)."""

    def __init__(self, max_entries: int, ttl_seconds: int):
        self._store: OrderedDict[str, tuple[LLMResponse, float]] = OrderedDict()
        self._max = max_entries
        self._ttl = ttl_seconds

    @staticmethod
    def _key(provider: str, model: str, system: str, messages: list[dict]) -> str:
        payload = json.dumps(
            {"p": provider, "m": model, "s": system, "msgs": messages},
            sort_keys=True, ensure_ascii=False,
        )
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def get(self, provider: str, model: str, system: str, messages: list[dict]) -> LLMResponse | None:
        k = self._key(provider, model, system, messages)
        entry = self._store.get(k)
        if not entry:
            return None
        resp, expires_at = entry
        if time.time() > expires_at:
            del self._store[k]
            return None
        self._store.move_to_end(k)
        return LLMResponse(**{**resp.__dict__, "cache_hit": True})

    def put(self, provider: str, model: str, system: str, messages: list[dict], resp: LLMResponse):
        k = self._key(provider, model, system, messages)
        self._store[k] = (resp, time.time() + self._ttl)
        self._store.move_to_end(k)
        while len(self._store) > self._max:
            self._store.popitem(last=False)


# ─────────────────────────────────────────────────────────────
#  Gateway
# ─────────────────────────────────────────────────────────────
class GenerativeAIGateway:
    def __init__(self):
        self._cache = _ResponseCache(
            max_entries=settings.llm_cache_max_entries,
            ttl_seconds=settings.llm_cache_ttl_seconds,
        )

    @staticmethod
    def adapter() -> LLMAdapter:
        """Currently-active adapter (provider chosen by settings.llm_provider)."""
        return get_adapter()

    @property
    def provider(self) -> str:
        return self.adapter().name

    def is_configured(self) -> bool:
        return self.adapter().is_configured()

    async def complete(
        self,
        *,
        system: str,
        messages: list[dict],
        model: str | None = None,
        max_tokens: int = 1000,
        cacheable: bool = True,
        max_retries: int = 5,
    ) -> LLMResponse:
        adapter = self.adapter()
        model = model or adapter.default_sql_writer_model

        if cacheable:
            cached = self._cache.get(adapter.name, model, system, messages)
            if cached is not None:
                log.debug("LLM cache hit (provider=%s model=%s)", adapter.name, model)
                return cached

        # Exponential backoff with jitter: 2s, 5s, 12s, 30s (capped). Total ~50s
        # max wait. Gemini free tier 503s often clear within ~10-30s.
        delay = 2.0
        last_err: Exception | None = None
        started = time.time()

        for attempt in range(1, max_retries + 1):
            try:
                raw = await adapter.complete(
                    system=system,
                    messages=messages,
                    model=model,
                    max_tokens=max_tokens,
                )
                latency = int((time.time() - started) * 1000)
                out = LLMResponse(
                    text=raw.text,
                    model=raw.model,
                    provider=adapter.name,
                    input_tokens=raw.input_tokens,
                    output_tokens=raw.output_tokens,
                    latency_ms=latency,
                )
                if cacheable:
                    self._cache.put(adapter.name, model, system, messages, out)
                log.info(
                    "LLM ok | provider=%s model=%s in_tok=%d out_tok=%d ms=%d",
                    adapter.name, model, out.input_tokens, out.output_tokens, latency,
                )
                return out
            except TransientLLMError as e:
                last_err = e
                log.warning("LLM transient (attempt %d/%d, provider=%s): %s",
                            attempt, max_retries, adapter.name, e)
                if attempt < max_retries:
                    # Jitter ±25% to avoid thundering-herd retries against the same shard
                    jitter = random.uniform(0.75, 1.25)
                    sleep_s = min(delay * jitter, 30.0)
                    log.info("LLM backoff %.1fs before retry", sleep_s)
                    await asyncio.sleep(sleep_s)
                    delay *= 2.5
            except PermanentLLMError as e:
                # Don't retry — bail out fast so the caller can surface a clean error.
                log.error("LLM permanent (provider=%s): %s", adapter.name, e)
                raise RuntimeError(f"LLM ({adapter.name}) error: {e}") from e

        raise RuntimeError(
            f"LLM gateway ({adapter.name}) failed after {max_retries} attempts: {last_err}"
        )


gateway = GenerativeAIGateway()
