"""
OpenAI adapter — wraps `openai.AsyncOpenAI` (Chat Completions API).

Works with any GPT model the user puts in `.env`:
  - gpt-5 / gpt-5-mini / gpt-5-nano    (current frontier, supports reasoning_effort)
  - gpt-4.1 / gpt-4.1-mini
  - gpt-4o / gpt-4o-mini
  - older 3.5-turbo / etc. (no reasoning)

The user picks the model via `OPENAI_MODEL_SUPERVISOR` / `OPENAI_MODEL_SQL_WRITER`
in .env, so no model list is hard-coded here.

Key notes per the OpenAI SDK:
  - GPT-5* models accept `reasoning_effort` (minimal|low|medium|high) — controls
    chain-of-thought depth. We pass "minimal" so the JSON-only response isn't
    starved by reasoning tokens (analogous to the Gemini thinking-budget issue).
    Older models reject this param → we strip it and retry once.
  - GPT-5* models also expect `max_completion_tokens`, not `max_tokens`. The
    SDK is forgiving on most models but newer ones 400 on `max_tokens`. We send
    both candidates with a fallback.
  - System prompt is the first message with role="system" in chat.completions
    (no separate config field like Gemini).

Error mapping:
  - openai.APIConnectionError / RateLimitError / APIStatusError(5xx) → TransientLLMError
  - APIStatusError(4xx) / AuthenticationError / BadRequestError    → PermanentLLMError
"""

from __future__ import annotations

import logging

from core.settings import settings

from .base import LLMAdapter, PermanentLLMError, RawCompletion, TransientLLMError

log = logging.getLogger("llm_adapter.openai")


# Model name prefixes that support the `reasoning_effort` parameter + require
# `max_completion_tokens`. Anything else uses classic `max_tokens` and no
# reasoning param.
_REASONING_MODEL_PREFIXES = ("gpt-5", "o1", "o3", "o4")


def _is_reasoning_model(model: str) -> bool:
    return any(model.startswith(p) for p in _REASONING_MODEL_PREFIXES)


class OpenAIAdapter(LLMAdapter):
    name = "openai"
    default_supervisor_model = "gpt-5-mini"
    default_sql_writer_model = "gpt-5-mini"

    def __init__(self):
        self._client = None
        self._errors = None

    def is_configured(self) -> bool:
        return bool(settings.openai_api_key)

    def _ensure_client(self):
        if self._client is None:
            try:
                import openai
                from openai import AsyncOpenAI
            except ImportError as e:
                raise PermanentLLMError(
                    "openai SDK not installed. Run: pip install openai"
                ) from e
            if not settings.openai_api_key:
                raise PermanentLLMError("OPENAI_API_KEY is not set in .env")
            # The OpenAI SDK auto-reads OPENAI_BASE_URL from os.environ even
            # when we don't pass base_url explicitly. docker-compose passes
            # OPENAI_BASE_URL="" (empty string) when not set, which overrides
            # the SDK's default https://api.openai.com/v1 with garbage and
            # triggers httpx.UnsupportedProtocol. Scrub the empty env var so
            # the SDK falls back to its real default.
            import os
            if os.environ.get("OPENAI_BASE_URL", "").strip() == "":
                os.environ.pop("OPENAI_BASE_URL", None)
            kwargs = {"api_key": settings.openai_api_key}
            if settings.openai_base_url:
                # Allows pointing at Azure OpenAI / proxy gateways
                kwargs["base_url"] = settings.openai_base_url
            self._client = AsyncOpenAI(**kwargs)
            self._errors = openai
        return self._client

    @staticmethod
    def _to_openai_messages(system: str, messages: list[dict]) -> list[dict]:
        out: list[dict] = [{"role": "system", "content": system}]
        for m in messages:
            role = m.get("role") or "user"
            # OpenAI accepts user/assistant/system/tool. Map anything else → user.
            if role not in ("user", "assistant", "system", "tool"):
                role = "user"
            content = m.get("content")
            if content:
                out.append({"role": role, "content": str(content)})
        return out

    async def complete(
        self,
        *,
        system: str,
        messages: list[dict],
        model: str,
        max_tokens: int,
    ) -> RawCompletion:
        client = self._ensure_client()
        openai_mod = self._errors

        oai_messages = self._to_openai_messages(system, messages)
        reasoning = _is_reasoning_model(model)

        # Build kwargs that match the model's API surface
        kwargs: dict = {
            "model":    model,
            "messages": oai_messages,
        }
        if reasoning:
            # GPT-5* / o-series: use max_completion_tokens + reasoning_effort.
            # "minimal" leaves the full budget for the JSON response.
            kwargs["max_completion_tokens"] = max_tokens
            kwargs["reasoning_effort"]      = settings.openai_reasoning_effort
        else:
            kwargs["max_tokens"]  = max_tokens
            kwargs["temperature"] = 0.0   # reasoning models don't accept temperature

        try:
            resp = await client.chat.completions.create(**kwargs)
        except openai_mod.APIConnectionError as e:
            raise TransientLLMError(f"openai connection error: {e}") from e
        except openai_mod.RateLimitError as e:
            raise TransientLLMError(f"openai rate limit: {e}") from e
        except openai_mod.APIStatusError as e:
            status = getattr(e, "status_code", None)
            if status is not None and status >= 500:
                raise TransientLLMError(f"openai server error {status}: {e}") from e
            raise PermanentLLMError(f"openai client error {status}: {e}") from e
        except openai_mod.AuthenticationError as e:
            raise PermanentLLMError(f"openai auth: {e}") from e
        except openai_mod.BadRequestError as e:
            raise PermanentLLMError(f"openai bad request: {e}") from e
        except Exception as e:
            raise PermanentLLMError(f"unexpected openai error: {e}") from e

        choice = (resp.choices or [None])[0]
        text   = (getattr(getattr(choice, "message", None), "content", None) or "") if choice else ""

        usage = getattr(resp, "usage", None)
        return RawCompletion(
            text=text,
            model=model,
            input_tokens=int(getattr(usage, "prompt_tokens", 0) or 0),
            output_tokens=int(getattr(usage, "completion_tokens", 0) or 0),
        )
