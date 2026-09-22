"""API playground : prompt système + prompt utilisateur → réponse Mistral."""

from __future__ import annotations

import asyncio
import logging

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from .client import (
    CHARS_PER_TOKEN,
    DEFAULT_EFFORT,
    DEFAULT_MAX_TOKENS,
    DEFAULT_MODEL,
    MAX_PROMPT_CHARS,
    complete_chat,
    mistral_configured,
    models_catalog,
    validate_effort,
    validate_model,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/appel-llm", tags=["appel-llm"])


class TokenUsage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class CostBreakdown(BaseModel):
    input_usd: float = 0
    output_usd: float = 0
    total_usd: float = 0
    price_input_per_m: float
    price_output_per_m: float


class ChatRequest(BaseModel):
    system_prompt: str = Field(default="", max_length=MAX_PROMPT_CHARS)
    user_prompt: str = Field(..., min_length=1, max_length=MAX_PROMPT_CHARS)
    model: str = DEFAULT_MODEL
    temperature: float = Field(default=0.0, ge=0.0, le=1.0)
    json_mode: bool = True
    max_tokens: int = Field(default=DEFAULT_MAX_TOKENS, ge=1, le=32_768)
    reasoning_effort: str = DEFAULT_EFFORT

    @field_validator("user_prompt")
    @classmethod
    def user_prompt_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Le prompt utilisateur est vide.")
        return value


class ChatResponse(BaseModel):
    content: str
    parsed: Any = None
    model: str
    reasoning_effort: str = DEFAULT_EFFORT
    duration_s: float
    tokens: TokenUsage
    cost: CostBreakdown


class ModelInfo(BaseModel):
    id: str
    price_input_per_m: float
    price_output_per_m: float
    reasoning_efforts: list[str] = Field(default_factory=lambda: ["none", "high"])


class ModelsResponse(BaseModel):
    models: list[ModelInfo]
    default: str
    default_effort: str = DEFAULT_EFFORT
    configured: bool
    chars_per_token: float = 4.0


@router.get("/models", response_model=ModelsResponse)
def list_models() -> ModelsResponse:
    return ModelsResponse(
        models=[ModelInfo(**item) for item in models_catalog()],
        default=DEFAULT_MODEL,
        default_effort=DEFAULT_EFFORT,
        configured=mistral_configured(),
        chars_per_token=CHARS_PER_TOKEN,
    )


@router.post("/chat", response_model=ChatResponse)
async def chat(payload: ChatRequest) -> ChatResponse:
    if not mistral_configured():
        raise HTTPException(status_code=503, detail="MISTRAL_API_KEY absente du .env backend.")

    try:
        model = validate_model(payload.model)
        effort = validate_effort(model, payload.reasoning_effort)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    total_chars = len(payload.system_prompt) + len(payload.user_prompt)
    if total_chars > MAX_PROMPT_CHARS:
        raise HTTPException(
            status_code=400,
            detail=f"Prompts trop longs ({total_chars} caractères, max {MAX_PROMPT_CHARS}).",
        )

    try:
        result = await asyncio.to_thread(
            complete_chat,
            system_prompt=payload.system_prompt,
            user_prompt=payload.user_prompt,
            model=model,
            temperature=payload.temperature,
            json_mode=payload.json_mode,
            max_tokens=payload.max_tokens,
            reasoning_effort=effort,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Échec appel Mistral")
        text = str(exc)
        if "429" in text or "rate_limited" in text.lower():
            raise HTTPException(
                status_code=429,
                detail="Quota Mistral dépassé pour ce modèle. Réessayez dans quelques secondes, ou choisissez un autre modèle.",
            ) from exc
        raise HTTPException(status_code=502, detail=f"Échec de l'appel Mistral : {exc}") from exc

    return ChatResponse(**result)
