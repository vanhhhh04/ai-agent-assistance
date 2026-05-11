"""
Anthropic adapter — wraps `anthropic.AsyncAnthropic`.

Two production-grade enhancements layered on top of the basic SDK call:

  1. Prompt caching — the system prompt is sent as a single text block with
     `cache_control: ephemeral`. The first request writes the cache (~1.25× cost);
     subsequent requests within ~5 min read it (~0.1× cost). For the SQL writer's
     ~2-3KB system prompt, this is a major win on bursts of similar questions.
     If the prefix is below the model's minimum (4096 tok for Opus 4.7,
     2048 for Sonnet 4.6, 4096 for Haiku 4.5), the marker is silently a no-op
     — no error, no extra cost.

  2. Adaptive thinking — auto-enabled for models that support it (Opus 4.6+,
     Sonnet 4.6+). Claude decides how much to think; produces noticeably better
     SQL on complex multi-table joins. Toggleable via ANTHROPIC_ADAPTIVE_THINKING.

Error mapping (unchanged):
  - APIConnectionError / RateLimitError / 5xx → TransientLLMError (gateway retries)
  - 4xx / auth / malformed                    → PermanentLLMError (gateway gives up)
"""

from __future__ import annotations

import logging

import anthropic

from core.settings import settings

from .base import LLMAdapter, PermanentLLMError, RawCompletion, TransientLLMError

log = logging.getLogger("llm_adapter.anthropic")


# Models that support adaptive thinking. Excludes Haiku 4.5 and Sonnet 4.5
# (effort/adaptive thinking errors there).
_ADAPTIVE_THINKING_MODELS_PREFIXES = (
    "claude-opus-4-7",
    "claude-opus-4-6",
    "claude-sonnet-4-6",
)


def _supports_adaptive_thinking(model: str) -> bool:
    return any(model.startswith(p) for p in _ADAPTIVE_THINKING_MODELS_PREFIXES)


class AnthropicAdapter(LLMAdapter):
    name = "anthropic"
    default_supervisor_model = "claude-haiku-4-5-20251001"
    default_sql_writer_model = "claude-sonnet-4-6"

    def __init__(self):
        self._client: anthropic.AsyncAnthropic | None = None

    def is_configured(self) -> bool:
        return bool(settings.anthropic_api_key)

    def _ensure_client(self) -> anthropic.AsyncAnthropic:
        if self._client is None:
            if not settings.anthropic_api_key:
                raise PermanentLLMError(
                    "ANTHROPIC_API_KEY is not set in .env"
                )
            self._client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        return self._client

    async def complete(
        self,
        *,
        system: str,
        messages: list[dict],
        model: str,
        max_tokens: int,
    ) -> RawCompletion:
        client = self._ensure_client()

        # Wrap system prompt with cache_control. The marker is stable (no
        # timestamps/UUIDs in the system string), so repeated requests with the
        # same provider+model+system+messages prefix reuse the cached prefix.
        # The agents/* code already keeps system content stable per-request.
        system_blocks = [
            {
                "type": "text",
                "text": system,
                "cache_control": {"type": "ephemeral"},
            }
        ]

        kwargs = {
            "model":      model,
            "max_tokens": max_tokens,
            "system":     system_blocks,
            "messages":   messages,
        }

        # Adaptive thinking — only on models that support it, and only when
        # the operator has not opted out. Adaptive thinking is the recommended
        # mode for Claude 4.6+ models; budget_tokens is deprecated and 400s on
        # Opus 4.7.
        if settings.anthropic_adaptive_thinking and _supports_adaptive_thinking(model):
            kwargs["thinking"] = {"type": "adaptive"}

        try:
            resp = await client.messages.create(**kwargs)
        except anthropic.APIConnectionError as e:
            raise TransientLLMError(f"connection error: {e}") from e
        except anthropic.RateLimitError as e:
            raise TransientLLMError(f"rate limit: {e}") from e
        except anthropic.APIStatusError as e:
            # 5xx = transient, 4xx = permanent
            status = getattr(e, "status_code", None)
            if status is not None and status >= 500:
                raise TransientLLMError(f"server error {status}: {e}") from e
            raise PermanentLLMError(f"client error {status}: {e}") from e
        except Exception as e:
            raise PermanentLLMError(f"unexpected error: {e}") from e

        # Extract the text — when adaptive thinking is on, response.content
        # contains thinking blocks before the text block. Pick the first text.
        text = ""
        for block in resp.content or []:
            if getattr(block, "type", None) == "text":
                text = block.text
                break

        usage = resp.usage
        cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0 if usage else 0
        cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0 if usage else 0
        if cache_read or cache_write:
            log.info(
                "prompt cache | model=%s read=%d write=%d",
                model, cache_read, cache_write,
            )

        return RawCompletion(
            text=text,
            model=model,
            input_tokens=getattr(usage, "input_tokens", 0) if usage else 0,
            output_tokens=getattr(usage, "output_tokens", 0) if usage else 0,
        )
