"""Schémas Pydantic partagés entre chat et sessions."""

from pydantic import BaseModel, Field


class ToolCallLog(BaseModel):
    name: str
    args: dict
    result_summary: str
    # raw_result : mémoire uniquement (non persisté, exclu des réponses API)
    raw_result: dict | None = Field(default=None, exclude=True)


CONTEXT_TOKEN_LIMIT = 250_000


class Usage(BaseModel):
    prompt_tokens: int | None = None
    candidate_tokens: int | None = None
    total_tokens: int | None = None
    tokens_source: str | None = None
    input_usd: float | None = None
    output_usd: float | None = None
    cost_usd: float | None = None
    price_input_per_m: float | None = None
    price_output_per_m: float | None = None
    context_tokens: int | None = None
    """Dernier appel LLM : taille réelle de la fenêtre (prompt)."""


def context_limit_fields(context_tokens: int | None) -> dict:
    n = int(context_tokens or 0)
    return {
        "context_tokens": n or None,
        "context_limit": CONTEXT_TOKEN_LIMIT,
        "context_limit_reached": n >= CONTEXT_TOKEN_LIMIT,
    }


class SessionMessageItem(BaseModel):
    id: str
    role: str
    content: str
    tool_calls: list[dict] | None = None
    created_at: str | None = None
    has_raw_context: bool = False
    usage: Usage | None = None


class RawLlmContextResponse(BaseModel):
    message_id: str
    session_id: str
    raw_context: dict
