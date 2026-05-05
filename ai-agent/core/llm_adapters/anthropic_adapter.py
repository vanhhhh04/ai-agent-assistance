"""
Anthropic adapter — wraps `anthropic.AsyncAnthropic`.

Maps:
  - `anthropic.APIConnectionError`  → TransientLLMError
  - `anthropic.RateLimitError`      → TransientLLMError
  - `anthropic.APIStatusError(5xx)` → TransientLLMError
  - everything else                 → PermanentLLMError
"""

from __future__ import annotations

import logging

import anthropic

from core.settings import settings

from .base import LLMAdapter, PermanentLLMError, RawCompletion, TransientLLMError

log = logging.getLogger("llm_adapter.anthropic")


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
        try:
            resp = await client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system,
                messages=messages,
            )
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

        text = resp.content[0].text if resp.content else ""
        usage = resp.usage
        return RawCompletion(
            text=text,
            model=model,
            input_tokens=getattr(usage, "input_tokens", 0) if usage else 0,
            output_tokens=getattr(usage, "output_tokens", 0) if usage else 0,
        )
