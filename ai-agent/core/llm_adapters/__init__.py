"""
core/llm_adapters — provider-agnostic LLM adapters.

Each adapter speaks one provider's SDK and exposes a uniform `complete()`.
The gateway picks one based on `settings.llm_provider`. Adding a new provider
later = drop a new file in here + register it in `_REGISTRY`.

Today:
  - anthropic  → Claude family
  - gemini     → Google Gemini family
  - openai     → OpenAI GPT family (GPT-5*, GPT-4*, o-series)

Public surface: `get_adapter(name=None)`. With name=None the active provider
from settings is used.
"""

from __future__ import annotations

import logging
from typing import Callable

from core.settings import settings

from .base import LLMAdapter, PermanentLLMError, RawCompletion, TransientLLMError

log = logging.getLogger("llm_adapter")


# Lazy import — we don't want to require both SDKs to be installed at once.
# A user running provider=gemini shouldn't need anthropic in their env, and
# vice versa. The registry is a dict of factories that import on first use.
def _make_anthropic() -> LLMAdapter:
    from .anthropic_adapter import AnthropicAdapter
    return AnthropicAdapter()


def _make_gemini() -> LLMAdapter:
    from .gemini_adapter import GeminiAdapter
    return GeminiAdapter()


def _make_openai() -> LLMAdapter:
    from .openai_adapter import OpenAIAdapter
    return OpenAIAdapter()


_REGISTRY: dict[str, Callable[[], LLMAdapter]] = {
    "anthropic": _make_anthropic,
    "gemini":    _make_gemini,
    "openai":    _make_openai,
}

_cache: dict[str, LLMAdapter] = {}


def get_adapter(name: str | None = None) -> LLMAdapter:
    """
    Returns a singleton adapter for the named provider.
    name=None → use settings.llm_provider.
    Raises ValueError on unknown provider.
    """
    name = (name or settings.llm_provider).lower()
    if name not in _REGISTRY:
        raise ValueError(
            f"Unknown LLM provider '{name}'. Known: {', '.join(_REGISTRY)}"
        )
    if name not in _cache:
        log.info("Initialising LLM adapter: %s", name)
        _cache[name] = _REGISTRY[name]()
    return _cache[name]


def known_providers() -> list[str]:
    return list(_REGISTRY.keys())


__all__ = [
    "LLMAdapter",
    "RawCompletion",
    "TransientLLMError",
    "PermanentLLMError",
    "get_adapter",
    "known_providers",
]
