"""
Estimation de tokens et coût USD — stack Mistral / GLM 5.2.

L'API Mistral ne fournit pas toujours un ``usage`` fiable (contrairement à
Gemini ``usage_metadata``). On estime alors avec tiktoken ``cl100k_base``
(approximation, pas le tokenizer GLM officiel), repli ~4 caractères / token.

Tarifs GLM 5.2 (API Mistral, sept. 2026) : 1,40 $ / M tokens in, 4,40 $ / M out.
"""

from __future__ import annotations

import json
from typing import Any

CHARS_PER_TOKEN = 4.0
TIKTOKEN_ENCODING = "cl100k_base"

# USD / million de jetons (entrée, sortie). Cached GLM 0,14 $ non utilisé ici.
PRIX_USD_PAR_M: dict[str, tuple[float, float]] = {
    "zai-glm-5-2": (1.40, 4.40),
    "mistral-small-latest": (0.15, 0.60),
    "mistral-small-2603": (0.15, 0.60),
    "mistral-medium-latest": (1.50, 7.50),
    "mistral-large-latest": (0.50, 1.50),
    "ministral-8b-latest": (0.15, 0.15),
    "ministral-14b-latest": (0.15, 0.15),
}
DEFAULT_PRIX = (1.40, 4.40)

_encoder = None
_encoder_failed = False


def prix_modele(model: str | None) -> tuple[float, float]:
    if not model:
        return DEFAULT_PRIX
    key = model.strip()
    if key in PRIX_USD_PAR_M:
        return PRIX_USD_PAR_M[key]
    if key.startswith("mistral-small"):
        return PRIX_USD_PAR_M["mistral-small-2603"]
    return DEFAULT_PRIX


def _get_encoder():
    global _encoder, _encoder_failed
    if _encoder is not None or _encoder_failed:
        return _encoder
    try:
        import tiktoken

        _encoder = tiktoken.get_encoding(TIKTOKEN_ENCODING)
    except Exception:
        _encoder_failed = True
        _encoder = None
    return _encoder


def estimate_tokens(text: str | None) -> int:
    """Compte approximatif (tiktoken cl100k, sinon chars/4)."""
    if not text:
        return 0
    enc = _get_encoder()
    if enc is not None:
        try:
            return len(enc.encode(text))
        except Exception:
            pass
    return max(1, round(len(text) / CHARS_PER_TOKEN))


def dump_for_estimate(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


def cost_usd(
    model: str | None,
    prompt_tokens: int,
    completion_tokens: int,
) -> dict[str, float]:
    price_in, price_out = prix_modele(model)
    input_usd = prompt_tokens / 1_000_000 * price_in
    output_usd = completion_tokens / 1_000_000 * price_out
    return {
        "input_usd": round(input_usd, 6),
        "output_usd": round(output_usd, 6),
        "total_usd": round(input_usd + output_usd, 6),
        "price_input_per_m": price_in,
        "price_output_per_m": price_out,
    }


def parse_mistral_api_usage(usage: dict | None) -> dict[str, int]:
    usage = usage or {}
    prompt = int(usage.get("prompt_tokens") or 0)
    completion = int(usage.get("completion_tokens") or 0)
    total = int(usage.get("total_tokens") or 0)
    if total <= 0:
        total = prompt + completion
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
    }


def estimate_mistral_round_tokens(
    messages: list[dict],
    result: dict,
    *,
    tools: list[dict] | None = None,
) -> dict[str, int]:
    payload: dict[str, Any] = {"messages": messages}
    if tools:
        payload["tools"] = tools
    prompt = estimate_tokens(dump_for_estimate(payload))
    raw = result.get("raw_message") or {}
    output_payload = {
        "content": raw.get("content") if raw.get("content") is not None else result.get("content"),
        "tool_calls": result.get("tool_calls") or raw.get("tool_calls") or [],
    }
    completion = estimate_tokens(dump_for_estimate(output_payload))
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
    }


def resolve_mistral_round_usage(
    *,
    api_usage: dict | None,
    messages: list[dict],
    result: dict,
    model: str | None,
    tools: list[dict] | None = None,
) -> dict[str, Any]:
    """
    Tokens + coût d'un appel chat Mistral.

    Prefère ``usage`` API s'il est non nul ; sinon estimateur tiktoken.
    """
    api = parse_mistral_api_usage(api_usage)
    estimated = estimate_mistral_round_tokens(messages, result, tools=tools)
    if api["prompt_tokens"] > 0 or api["completion_tokens"] > 0:
        prompt = api["prompt_tokens"]
        completion = api["completion_tokens"]
        total = api["total_tokens"] or (prompt + completion)
        source = "api"
    else:
        prompt = estimated["prompt_tokens"]
        completion = estimated["completion_tokens"]
        total = estimated["total_tokens"]
        source = "estimate"

    cost = cost_usd(model, prompt, completion)
    return {
        "prompt_token_count": prompt,
        "candidates_token_count": completion,
        "thoughts_token_count": 0,
        "cached_content_token_count": 0,
        "total_token_count": total,
        "tokens_source": source,
        "estimated_prompt_token_count": estimated["prompt_tokens"],
        "estimated_candidates_token_count": estimated["completion_tokens"],
        **cost,
    }


def merge_round_usages(*parts: dict[str, Any] | None) -> dict[str, Any]:
    """Agrège les rounds Mistral (tokens + USD)."""
    prompt = candidates = thoughts = cached = total = 0
    input_usd = output_usd = total_usd = 0.0
    price_in = price_out = None
    model = None
    sources: list[str] = []
    for part in parts:
        if not part:
            continue
        prompt += int(part.get("prompt_token_count") or 0)
        candidates += int(part.get("candidates_token_count") or 0)
        thoughts += int(part.get("thoughts_token_count") or 0)
        cached += int(part.get("cached_content_token_count") or 0)
        total += int(part.get("total_token_count") or 0)
        input_usd += float(part.get("input_usd") or 0)
        output_usd += float(part.get("output_usd") or 0)
        total_usd += float(part.get("total_usd") or 0)
        if part.get("price_input_per_m") is not None:
            price_in = part.get("price_input_per_m")
        if part.get("price_output_per_m") is not None:
            price_out = part.get("price_output_per_m")
        if part.get("tokens_source"):
            sources.append(str(part["tokens_source"]))
        if part.get("model"):
            model = part.get("model")
    if not total:
        total = prompt + candidates + thoughts
    source = "mixed" if sources and len(set(sources)) > 1 else (sources[0] if sources else "estimate")
    return {
        "prompt_token_count": prompt,
        "candidates_token_count": candidates,
        "thoughts_token_count": thoughts,
        "cached_content_token_count": cached,
        "total_token_count": total,
        "tokens_source": source,
        "input_usd": round(input_usd, 6),
        "output_usd": round(output_usd, 6),
        "total_usd": round(total_usd, 6),
        "price_input_per_m": price_in,
        "price_output_per_m": price_out,
        "model": model,
    }


__all__ = [
    "CHARS_PER_TOKEN",
    "DEFAULT_PRIX",
    "PRIX_USD_PAR_M",
    "TIKTOKEN_ENCODING",
    "cost_usd",
    "dump_for_estimate",
    "estimate_mistral_round_tokens",
    "estimate_tokens",
    "merge_round_usages",
    "parse_mistral_api_usage",
    "prix_modele",
    "resolve_mistral_round_usage",
]
