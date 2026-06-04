"""Schémas Pydantic partagés entre chat et sessions."""

from pydantic import BaseModel, Field


class ToolCallLog(BaseModel):
    name: str
    args: dict
    result_summary: str
    # raw_result : mémoire uniquement (non persisté, exclu des réponses API)
    raw_result: dict | None = Field(default=None, exclude=True)


class Usage(BaseModel):
    prompt_tokens: int | None = None
    candidate_tokens: int | None = None
    total_tokens: int | None = None


class SessionMessageItem(BaseModel):
    id: str
    role: str
    content: str
    tool_calls: list[dict] | None = None
    created_at: str | None = None
    has_raw_context: bool = False


class RawLlmContextResponse(BaseModel):
    message_id: str
    session_id: str
    raw_context: dict
