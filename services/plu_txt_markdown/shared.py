import logging
import os
from typing import Any

from google import genai

from dotenv import load_dotenv

load_dotenv()

ALLOWED_GEMINI_MODELS: tuple[str, ...] = (
    "gemini-3.1-pro-preview",
    "gemini-3.5-flash",
    "gemini-3.1-flash-lite",
)
DEFAULT_GEMINI_MODEL = "gemini-3.1-pro-preview"
# Rétrocompatibilité des imports existants
MODEL = DEFAULT_GEMINI_MODEL

TOKEN_USAGE_KEYS = (
    "prompt_token_count",
    "candidates_token_count",
    "thoughts_token_count",
    "cached_content_token_count",
    "total_token_count",
)


def empty_token_usage() -> dict[str, int]:
    return {k: 0 for k in TOKEN_USAGE_KEYS}


def parse_usage_metadata(usage: Any) -> dict[str, int]:
    """Normalise ``response.usage_metadata`` Gemini (doc Google AI)."""
    if usage is None:
        return empty_token_usage()

    prompt = int(getattr(usage, "prompt_token_count", 0) or 0)
    candidates = int(getattr(usage, "candidates_token_count", 0) or 0)
    thoughts = int(getattr(usage, "thoughts_token_count", 0) or 0)
    cached = int(getattr(usage, "cached_content_token_count", 0) or 0)
    total = getattr(usage, "total_token_count", None)
    if total is None:
        total = prompt + candidates + thoughts
    else:
        total = int(total)

    return {
        "prompt_token_count": prompt,
        "candidates_token_count": candidates,
        "thoughts_token_count": thoughts,
        "cached_content_token_count": cached,
        "total_token_count": total,
    }


def validate_gemini_model(model: str | None) -> str:
    m = (model or DEFAULT_GEMINI_MODEL).strip()
    if m not in ALLOWED_GEMINI_MODELS:
        allowed = ", ".join(ALLOWED_GEMINI_MODELS)
        raise ValueError(f"Modèle Gemini invalide : {m!r}. Valeurs acceptées : {allowed}")
    return m


def log_gemini_tokens(
    logger: logging.Logger,
    *,
    context: str,
    phase: str,
    tokens: dict[str, int] | None,
) -> None:
    if not tokens:
        return
    logger.info(
        "%s | %s — entrée=%s sortie=%s réflexion=%s cache=%s (total=%s)",
        context,
        phase,
        tokens.get("prompt_token_count", 0),
        tokens.get("candidates_token_count", 0),
        tokens.get("thoughts_token_count", 0),
        tokens.get("cached_content_token_count", 0),
        tokens.get("total_token_count", 0),
    )


def merge_token_usages(*parts: dict[str, int] | None) -> dict[str, int]:
    out = empty_token_usage()
    for part in parts:
        if not part:
            continue
        for key in TOKEN_USAGE_KEYS:
            out[key] += int(part.get(key, 0) or 0)
    return out

_client: genai.Client | None = None


def get_client() -> genai.Client:
    global _client
    if _client is None:
        api_key = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY ou GOOGLE_API_KEY requis pour le pipeline markdown PLU")
        _client = genai.Client(api_key=api_key)
    return _client


def compute_cost(
    input_tokens: int,
    output_tokens: int,
    cached_tokens: int = 0,
) -> dict:
    """Coût en USD pour Gemini 3.1 Pro Preview."""

    large = input_tokens > 200_000

    price_in = 4.0 if large else 2.0
    price_out = 18.0 if large else 12.0
    price_cache = 0.40 if large else 0.20

    billable_in = input_tokens - cached_tokens

    cost_in = billable_in * price_in / 1_000_000
    cost_out = output_tokens * price_out / 1_000_000
    cost_cache = cached_tokens * price_cache / 1_000_000
    total = cost_in + cost_out + cost_cache

    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cached_tokens": cached_tokens,
        "cost_input_usd": cost_in,
        "cost_output_usd": cost_out,
        "cost_cache_usd": cost_cache,
        "total_usd": total,
        "total_eur_approx": total * 0.92,
    }
