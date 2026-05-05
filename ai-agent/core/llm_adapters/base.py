"""
core/llm_adapters/base.py — Adapter interface + shared error types.

Every concrete adapter:
  1. Implements `complete(...)` returning a `RawCompletion`.
  2. Maps native SDK errors into TransientLLMError (gateway will retry)
     or PermanentLLMError (gateway gives up).
  3. Knows nothing about caching, JSON parsing, or pipeline orchestration —
     those live in the gateway.

Why dataclass + ABC instead of Protocol? — runtime isinstance checks for
better error messages, and forced abstract methods catch incomplete
implementations at instantiation, not at first call.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class RawCompletion:
    """
    The raw output from a single LLM call. Provider-agnostic shape — the
    gateway wraps this with cache metadata + JSON parsing helpers.
    """
    text: str
    model: str
    input_tokens: int
    output_tokens: int


# ─────────────────────────────────────────────────────────────
#  Error taxonomy
# ─────────────────────────────────────────────────────────────
class LLMError(Exception):
    """Base for everything that can go wrong inside an adapter."""


class TransientLLMError(LLMError):
    """Network blip / 5xx / rate-limit. Gateway retries with backoff."""


class PermanentLLMError(LLMError):
    """4xx / auth fail / malformed request. Gateway gives up immediately."""


# ─────────────────────────────────────────────────────────────
#  Adapter ABC
# ─────────────────────────────────────────────────────────────
class LLMAdapter(ABC):
    """One concrete subclass per provider."""

    name: str = "base"
    default_supervisor_model: str = ""
    default_sql_writer_model: str = ""

    @abstractmethod
    async def complete(
        self,
        *,
        system: str,
        messages: list[dict],
        model: str,
        max_tokens: int,
    ) -> RawCompletion:
        """
        Run a single completion.

        `messages` is provider-agnostic: a list of {"role": "user"|"assistant", "content": str}.
        Adapters translate role names to whatever the provider expects
        (e.g. Gemini wants "model" instead of "assistant").
        """
        raise NotImplementedError

    def is_configured(self) -> bool:
        """True if API credentials are present. Used by /api/health."""
        return False
