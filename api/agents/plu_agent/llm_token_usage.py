"""
Utilitaires tokens Gemini — alignés sur services/plu_txt_markdown/shared.py.

``parse_usage_metadata`` : métadonnées officielles d'une réponse ``generate_content``.
"""

from __future__ import annotations

from typing import Any

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
    """Normalise ``response.usage_metadata`` Gemini."""
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


def merge_token_usages(*parts: dict[str, int] | None) -> dict[str, int]:
    out = empty_token_usage()
    for part in parts:
        if not part:
            continue
        for key in TOKEN_USAGE_KEYS:
            out[key] += int(part.get(key, 0) or 0)
    return out
