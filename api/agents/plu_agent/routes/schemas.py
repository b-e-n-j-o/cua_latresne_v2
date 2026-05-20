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
