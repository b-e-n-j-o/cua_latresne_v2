"""Client Mistral pour un appel chat unique (system + user)."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

from mistralai import Mistral
import httpx

logger = logging.getLogger(__name__)

ALLOWED_MODELS = (
    "mistral-small-latest",
    "mistral-medium-latest",
    "mistral-large-latest",
    "ministral-8b-latest",
    "zai-glm-5-2",
)
DEFAULT_MODEL = "mistral-small-latest"
DEFAULT_EFFORT = "none"

# Alias CLI / UI → id API.
ALIAS_MODELES = {
    "glm": "zai-glm-5-2",
    "glm-5-2": "zai-glm-5-2",
}

# Small / medium / large : l'API n'accepte que none|high.
# GLM chez Mistral : reasoning_effort (pas le champ Z.ai `thinking`).
REASONING_EFFORTS: dict[str, tuple[str, ...]] = {
    "mistral-small-latest": ("none", "high"),
    "mistral-medium-latest": ("none", "high"),
    "mistral-large-latest": ("none", "high"),
    "ministral-8b-latest": ("none", "high"),
    "zai-glm-5-2": ("none", "high", "xhigh"),
}
ALIAS_EFFORT = {"max": "xhigh"}

# USD / million de jetons (entrée, sortie) — tarifs API Mistral, sept. 2026.
# GLM 5.2 : $1.4 in / $4.4 out (cached $0.14, non utilisé ici).
PRIX_USD_PAR_M: dict[str, tuple[float, float]] = {
    "mistral-small-latest": (0.15, 0.60),
    "mistral-medium-latest": (1.50, 7.50),
    "mistral-large-latest": (0.50, 1.50),
    "ministral-8b-latest": (0.15, 0.15),
    "zai-glm-5-2": (1.40, 4.40),
}
CHARS_PER_TOKEN = 4.0

MAX_PROMPT_CHARS = 300_000
DEFAULT_MAX_TOKENS = 8192
MAX_TOKENS_CAP = 32_768
TIMEOUT_MS = 180_000
API_URL = "https://api.mistral.ai/v1/chat/completions"
MAX_ATTEMPTS = 5
RETRY_BASE_S = 1.5
_HTTP_STATUS_RE = re.compile(r"http\s+(\d{3})", re.IGNORECASE)
_RETRYABLE_HTTP = {408, 409, 425, 429, 500, 502, 503, 504, 529}
_RETRYABLE_MARKERS = (
    "timeout",
    "timed out",
    "rate_limited",
    "overloaded",
    "temporarily unavailable",
    "connection",
    "connecterror",
    "reset by peer",
    "server disconnected",
    "network",
    "429",
)

JSON_HINT = (
    "Réponds uniquement par un objet JSON valide, sans texte autour, "
    "sans balises markdown."
)


def mistral_configured() -> bool:
    return bool((os.getenv("MISTRAL_API_KEY_BEN") or "").strip())


def validate_model(model: str) -> str:
    model = ALIAS_MODELES.get(model.strip(), model.strip())
    if model not in ALLOWED_MODELS:
        raise ValueError(
            f"Modèle non autorisé : {model}. "
            f"Choix : {', '.join(ALLOWED_MODELS)}"
        )
    return model


def efforts_pour(model: str) -> tuple[str, ...]:
    return REASONING_EFFORTS.get(model, (DEFAULT_EFFORT,))


def validate_effort(model: str, effort: str) -> str:
    effort = ALIAS_EFFORT.get(effort.strip().lower(), effort.strip().lower())
    autorises = efforts_pour(model)
    if effort not in autorises:
        raise ValueError(
            f"Raisonnement '{effort}' non supporté pour {model}. "
            f"Choix : {', '.join(autorises)}"
        )
    return effort


def prix_modele(model: str) -> tuple[float, float]:
    return PRIX_USD_PAR_M.get(model, PRIX_USD_PAR_M[DEFAULT_MODEL])


def estimate_tokens(text: str) -> int:
    n = len(text)
    if n == 0:
        return 0
    return max(1, round(n / CHARS_PER_TOKEN))


def cost_usd(model: str, prompt_tokens: int, completion_tokens: int) -> dict[str, float]:
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


def models_catalog() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for model_id in ALLOWED_MODELS:
        price_in, price_out = prix_modele(model_id)
        items.append(
            {
                "id": model_id,
                "price_input_per_m": price_in,
                "price_output_per_m": price_out,
                "reasoning_efforts": list(efforts_pour(model_id)),
            }
        )
    return items


def _chunk_type(chunk: Any) -> str | None:
    if isinstance(chunk, dict):
        t = chunk.get("type")
        return t if isinstance(t, str) else None
    t = getattr(chunk, "type", None)
    return t if isinstance(t, str) else None


def _chunk_text(chunk: Any) -> str:
    if isinstance(chunk, str):
        return chunk
    if isinstance(chunk, dict):
        text = chunk.get("text")
        return text if isinstance(text, str) else ""
    text = getattr(chunk, "text", None)
    return text if isinstance(text, str) else ""


def _message_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for chunk in content:
            if _chunk_type(chunk) in ("thinking", "think"):
                continue
            parts.append(_chunk_text(chunk))
        return "".join(parts)
    return str(content)


def _parse_json(raw: str) -> Any | None:
    txt = raw.strip()
    if not txt:
        return None
    if txt.startswith("```"):
        txt = txt.split("```", 2)[1]
        if txt.lstrip().lower().startswith("json"):
            txt = txt.lstrip()[4:]
        txt = txt.strip()
    start_obj, end_obj = txt.find("{"), txt.rfind("}")
    start_arr, end_arr = txt.find("["), txt.rfind("]")
    if start_obj != -1 and end_obj != -1 and (start_arr == -1 or start_obj < start_arr):
        txt = txt[start_obj : end_obj + 1]
    elif start_arr != -1 and end_arr != -1:
        txt = txt[start_arr : end_arr + 1]
    try:
        return json.loads(txt)
    except json.JSONDecodeError:
        return None


def _ensure_json_hint(system_prompt: str, user_prompt: str) -> str:
    combined = f"{system_prompt}\n{user_prompt}".lower()
    if "json" in combined:
        return system_prompt
    if system_prompt.strip():
        return f"{system_prompt.rstrip()}\n\n{JSON_HINT}"
    return JSON_HINT


def _usage_tokens(usage: Any) -> dict[str, int]:
    if usage is None:
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    getter = usage.get if isinstance(usage, dict) else lambda k, d=0: getattr(usage, k, d)
    return {
        "prompt_tokens": int(getter("prompt_tokens", 0) or 0),
        "completion_tokens": int(getter("completion_tokens", 0) or 0),
        "total_tokens": int(getter("total_tokens", 0) or 0),
    }


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.RequestError)):
        return True
    text = str(exc).lower()
    match = _HTTP_STATUS_RE.search(text)
    if match:
        return int(match.group(1)) in _RETRYABLE_HTTP
    return any(marker in text for marker in _RETRYABLE_MARKERS)


def _complete_http(api_key: str, payload: dict) -> tuple[Any, dict]:
    """Appel brut : le SDK 1.x refuse reasoning_effort (champ extra)."""
    timeout = httpx.Timeout(connect=30.0, read=TIMEOUT_MS / 1000, write=30.0, pool=30.0)
    with httpx.Client(timeout=timeout) as http:
        r = http.post(
            API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code} : {r.text[:500]}")
        data = r.json()
    content = ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
    return content, data.get("usage") or {}


def complete_chat(
    *,
    system_prompt: str,
    user_prompt: str,
    model: str = DEFAULT_MODEL,
    temperature: float = 0.0,
    json_mode: bool = True,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    reasoning_effort: str = DEFAULT_EFFORT,
) -> dict[str, Any]:
    api_key = (os.getenv("MISTRAL_API_KEY_BEN") or "").strip()
    if not api_key:
        raise RuntimeError("MISTRAL_API_KEY_BEN absente du .env backend.")

    model = validate_model(model)
    effort = validate_effort(model, reasoning_effort)
    max_tokens = max(1, min(int(max_tokens), MAX_TOKENS_CAP))
    temperature = min(1.0, max(0.0, float(temperature)))

    system = system_prompt.strip()
    user = user_prompt.strip()
    if json_mode:
        system = _ensure_json_hint(system, user)

    messages: list[dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": user})

    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "reasoning_effort": effort,
    }
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    client = Mistral(api_key=api_key, timeout_ms=TIMEOUT_MS)
    t0 = time.perf_counter()
    last_err: Exception | None = None
    content: Any = None
    usage: Any = None
    http_brut = False
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            if http_brut:
                content, usage = _complete_http(api_key, kwargs)
            else:
                response = client.chat.complete(**kwargs)
                content = response.choices[0].message.content
                usage = getattr(response, "usage", None)
            break
        except Exception as exc:
            last_err = exc
            text = str(exc).lower()
            sdk_refuse_effort = (
                not http_brut
                and "reasoning_effort" in text
                and any(
                    k in text
                    for k in ("unexpected", "extra", "not permitted", "not valid", "unknown")
                )
            )
            if sdk_refuse_effort:
                logger.warning(
                    "SDK sans reasoning_effort, appel HTTP brut pour %s effort=%s",
                    model,
                    effort,
                )
                http_brut = True
                continue
            if attempt < MAX_ATTEMPTS and _is_retryable(exc):
                wait_s = RETRY_BASE_S * attempt
                logger.warning(
                    "appel_llm retry %s/%s model=%s dans %.1fs : %s",
                    attempt,
                    MAX_ATTEMPTS,
                    model,
                    wait_s,
                    exc,
                )
                time.sleep(wait_s)
                continue
            raise
    else:
        raise last_err or RuntimeError("Réponse Mistral vide")
    if content is None and last_err:
        raise last_err
    duration_s = round(time.perf_counter() - t0, 3)

    raw = _message_text(content).strip()
    parsed = _parse_json(raw) if json_mode else None
    tokens = _usage_tokens(usage)

    cost = cost_usd(model, tokens["prompt_tokens"], tokens["completion_tokens"])

    logger.info(
        "appel_llm model=%s effort=%s duration=%.2fs tokens=%s cost=$%.6f json_mode=%s",
        model,
        effort,
        duration_s,
        tokens["total_tokens"],
        cost["total_usd"],
        json_mode,
    )

    return {
        "content": raw,
        "parsed": parsed,
        "model": model,
        "reasoning_effort": effort,
        "duration_s": duration_s,
        "tokens": tokens,
        "cost": cost,
    }
