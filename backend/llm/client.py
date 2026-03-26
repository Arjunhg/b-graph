"""Optional Groq client helpers for SQL and answer generation."""

from __future__ import annotations

from dataclasses import dataclass

from backend.config import GROQ_API_KEY, GROQ_MODEL

try:
    from groq import Groq
except ModuleNotFoundError:  # pragma: no cover - dependency is optional at runtime
    Groq = None


@dataclass(frozen=True)
class LLMResponse:
    content: str
    model: str


def is_llm_available() -> bool:
    return bool(GROQ_API_KEY and Groq is not None)


def generate_completion(system_prompt: str, user_prompt: str) -> LLMResponse | None:
    """Call Groq when configured, otherwise return None."""
    if not is_llm_available():
        return None

    client = Groq(api_key=GROQ_API_KEY)
    response = client.chat.completions.create(
        model=GROQ_MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    content = response.choices[0].message.content or ""
    return LLMResponse(content=content.strip(), model=GROQ_MODEL)
