"""
Gemini adapter — Google Gen AI SDK (`google-genai`).

Notes per Google's SDK:
  - Role mapping: Gemini calls the assistant "model", not "assistant".
  - System prompt is passed via `config.system_instruction`, not as a message.
  - The SDK has built-in retries on 5xx; we still wrap our own for parity
    with the Anthropic adapter.
  - `response.text` raises if the response was blocked by safety filters or
    if the candidate has no parts. We defensively check before access.

Errors:
  - `google.genai.errors.ServerError`  (5xx)        → TransientLLMError
  - `google.genai.errors.ClientError`  (4xx, incl 429) → check rate-limit
"""

from __future__ import annotations

import logging

from core.settings import settings

from .base import LLMAdapter, PermanentLLMError, RawCompletion, TransientLLMError

log = logging.getLogger("llm_adapter.gemini")


class GeminiAdapter(LLMAdapter):
    name = "gemini"
    default_supervisor_model = "gemini-2.5-flash"
    default_sql_writer_model = "gemini-2.5-flash"

    def __init__(self):
        self._client = None
        self._types = None
        self._errors = None

    def is_configured(self) -> bool:
        return bool(settings.gemini_api_key)

    def _ensure_client(self):
        if self._client is None:
            try:
                from google import genai
                from google.genai import errors as genai_errors
                from google.genai import types
            except ImportError as e:
                raise PermanentLLMError(
                    "google-genai not installed. Run: pip install google-genai"
                ) from e
            if not settings.gemini_api_key:
                raise PermanentLLMError("GEMINI_API_KEY is not set in .env")
            self._client = genai.Client(api_key=settings.gemini_api_key)
            self._types = types
            self._errors = genai_errors
        return self._client

    @staticmethod
    def _to_gemini_role(role: str) -> str:
        # Gemini accepts "user" and "model"; we keep "user" but rename "assistant" → "model"
        return "model" if role == "assistant" else "user"

    def _to_contents(self, messages: list[dict]) -> list[dict]:
        return [
            {
                "role":  self._to_gemini_role(m.get("role") or "user"),
                "parts": [{"text": str(m.get("content") or "")}],
            }
            for m in messages
            if m.get("content")
        ]

    @staticmethod
    def _safe_extract_text(response, types_mod) -> str:
        """
        Pull text from a Gemini response without triggering the safety-filter
        ValueError that `.text` raises when there are no parts.
        """
        try:
            text = response.text
            if text:
                return text
        except (ValueError, AttributeError):
            pass
        # Fallback — concatenate text parts manually
        out: list[str] = []
        for cand in (getattr(response, "candidates", None) or []):
            content = getattr(cand, "content", None)
            for part in (getattr(content, "parts", None) or []):
                t = getattr(part, "text", None)
                if t:
                    out.append(t)
        return "".join(out)

    async def complete(
        self,
        *,
        system: str,
        messages: list[dict],
        model: str,
        max_tokens: int,
    ) -> RawCompletion:
        client = self._ensure_client()
        types = self._types
        errors = self._errors

        contents = self._to_contents(messages)
        config = types.GenerateContentConfig(
            system_instruction=system,
            max_output_tokens=max_tokens,
            temperature=0.0,
        )

        try:
            resp = await client.aio.models.generate_content(
                model=model,
                contents=contents,
                config=config,
            )
        except errors.ServerError as e:
            raise TransientLLMError(f"gemini 5xx: {e}") from e
        except errors.ClientError as e:
            # 429 (rate limit) is technically 4xx but transient
            code = getattr(e, "code", None) or getattr(e, "status_code", None)
            if code == 429:
                raise TransientLLMError(f"gemini rate-limit: {e}") from e
            raise PermanentLLMError(f"gemini client error: {e}") from e
        except Exception as e:
            raise PermanentLLMError(f"unexpected gemini error: {e}") from e

        # Safety-filter guard
        block_reason = getattr(getattr(resp, "prompt_feedback", None), "block_reason", None)
        if block_reason:
            raise PermanentLLMError(f"gemini blocked by safety filter: {block_reason}")

        text = self._safe_extract_text(resp, types)
        usage = getattr(resp, "usage_metadata", None)
        return RawCompletion(
            text=text,
            model=model,
            input_tokens=int(getattr(usage, "prompt_token_count", 0) or 0),
            output_tokens=int(getattr(usage, "candidates_token_count", 0) or 0),
        )
