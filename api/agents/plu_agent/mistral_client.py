"""
Client Mistral parallèle à Vertex — chat (tool calling) + embeddings.

Modèles (doc Mistral, 2026) :
  - chat / tools : ``zai-glm-5-2`` (GLM 5.2 hébergé Mistral, bon raisonnement agentique)
  - embeddings   : ``mistral-embed`` (1024d) — ne pas mixer avec les vecteurs Gemini 768d

Pas de Gemini Developer ni de client Z.ai direct : tout passe par ``api.mistral.ai``.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx
import numpy as np

try:
    from ._env import (
        MISTRAL_API_KEY,
        MISTRAL_CHAT_MODEL,
        MISTRAL_EMBED_MODEL,
        MISTRAL_REASONING_EFFORT,
        PLU_LLM_PROVIDER,
    )
except ImportError:
    MISTRAL_API_KEY = (
        (os.environ.get("MISTRAL_API_KEY") or "").strip()
    )
    MISTRAL_CHAT_MODEL = os.environ.get("MISTRAL_CHAT_MODEL", "zai-glm-5-3")
    MISTRAL_EMBED_MODEL = os.environ.get("MISTRAL_EMBED_MODEL", "mistral-embed")
    MISTRAL_REASONING_EFFORT = os.environ.get("MISTRAL_REASONING_EFFORT", "high")
    PLU_LLM_PROVIDER = (os.environ.get("PLU_LLM_PROVIDER") or "mistral").strip().lower()

logger = logging.getLogger("plu_api")

MISTRAL_CHAT_URL = "https://api.mistral.ai/v1/chat/completions"
MISTRAL_EMBED_URL = "https://api.mistral.ai/v1/embeddings"
MISTRAL_PARTS_PROVIDER = "mistral"
# GLM 5.2 est le préféré (raisonnement) mais souvent hors palier → fallback natif.
CHAT_MODEL_FALLBACKS = (
    "ministral-14b-latest",
    "mistral-small-latest",
    "mistral-medium-latest",
)
_REASONING_MODELS = {
    "zai-glm-5-2",
    "magistral-small-latest",
    "magistral-medium-latest",
}
_REASONING_CONTENT_TYPES = frozenset({"thinking", "think", "reasoning", "reasoning_content"})
_REASONING_MSG_KEYS = ("thinking", "reasoning_content", "reasoning")
_DOTENV_PATH = Path(__file__).resolve().parents[3] / ".env"
_MISTRAL_MODEL_MARKERS = (
    "zai-",
    "mistral-",
    "ministral-",
    "magistral-",
    "codestral-",
    "voxtral-",
    "devstral-",
)

_TYPE_MAP = {
    "STRING": "string",
    "INTEGER": "integer",
    "NUMBER": "number",
    "BOOLEAN": "boolean",
    "ARRAY": "array",
    "OBJECT": "object",
}


def mistral_configured() -> bool:
    return bool(MISTRAL_API_KEY)


def is_mistral_model(name: str | None) -> bool:
    n = (name or "").strip().lower()
    return bool(n) and n.startswith(_MISTRAL_MODEL_MARKERS)


def resolve_provider(
    requested: str | None = None,
    *,
    session_model: str | None = None,
) -> str:
    """``mistral`` | ``gemini``. Défaut ``PLU_LLM_PROVIDER`` (mistral)."""
    if requested in ("mistral", "gemini"):
        return requested
    if is_mistral_model(session_model):
        return "mistral"
    if session_model and not is_mistral_model(session_model):
        return "gemini"
    return PLU_LLM_PROVIDER if PLU_LLM_PROVIDER in ("mistral", "gemini") else "mistral"


def _require_key() -> str:
    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY absente — stack Mistral indisponible.")
    return MISTRAL_API_KEY


def _gemini_type_name(schema) -> str:
    t = getattr(schema, "type", None)
    if t is None:
        return "STRING"
    return t.name if hasattr(t, "name") else str(t).split(".")[-1]


def gemini_schema_to_json(schema) -> dict[str, Any]:
    """Convertit un ``types.Schema`` Gemini en JSON Schema (tools Mistral)."""
    if schema is None:
        return {"type": "object", "properties": {}}
    name = _gemini_type_name(schema)
    out: dict[str, Any] = {"type": _TYPE_MAP.get(name, "string")}
    desc = getattr(schema, "description", None)
    if desc:
        out["description"] = desc
    if name == "OBJECT":
        props = getattr(schema, "properties", None) or {}
        out["properties"] = {k: gemini_schema_to_json(v) for k, v in props.items()}
        required = list(getattr(schema, "required", None) or [])
        if required:
            out["required"] = required
    if name == "ARRAY":
        items = getattr(schema, "items", None)
        if items is not None:
            out["items"] = gemini_schema_to_json(items)
    return out


def gemini_decls_to_mistral_tools(function_declarations: list) -> list[dict[str, Any]]:
    """Même catalogue de tools que Gemini, format Chat Completions Mistral."""
    tools: list[dict[str, Any]] = []
    for fd in function_declarations:
        tools.append(
            {
                "type": "function",
                "function": {
                    "name": fd.name,
                    "description": (fd.description or "").strip(),
                    "parameters": gemini_schema_to_json(fd.parameters),
                },
            }
        )
    return tools


def _message_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for chunk in content:
            if isinstance(chunk, dict):
                if chunk.get("type") in ("thinking", "think"):
                    continue
                text = chunk.get("text")
                if isinstance(text, str):
                    parts.append(text)
            elif isinstance(chunk, str):
                parts.append(chunk)
        return "".join(parts)
    return str(content)


def parse_tool_args(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _is_tier_blocked(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "tier_not_allowed" in text or "not available in your subscription" in text


def _is_rate_limited(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "429" in text or "rate_limited" in text or "rate limit" in text


def _sync_mistral_env_from_dotenv() -> None:
    """Relit le .env projet : uvicorn --reload ne recharge pas les variables."""
    try:
        from dotenv import dotenv_values
    except ImportError:
        return
    values = dotenv_values(_DOTENV_PATH)
    for key in ("MISTRAL_CHAT_MODEL", "MISTRAL_REASONING_EFFORT"):
        val = (values.get(key) or "").strip()
        if val:
            os.environ[key] = val


def resolve_chat_model(preferred: str | None = None) -> str:
    """Modèle chat effectif (``.env`` à jour, pas seulement la constante d'import)."""
    _sync_mistral_env_from_dotenv()
    if preferred and preferred.strip():
        return preferred.strip()
    return (os.environ.get("MISTRAL_CHAT_MODEL") or MISTRAL_CHAT_MODEL or "zai-glm-5-2").strip()


def resolve_reasoning_effort(explicit: str | None = None) -> str:
    _sync_mistral_env_from_dotenv()
    if explicit is not None:
        return explicit
    return (os.environ.get("MISTRAL_REASONING_EFFORT") or MISTRAL_REASONING_EFFORT or "high").strip()


def _chat_model_candidates(preferred: str | None) -> list[str]:
    first = preferred or resolve_chat_model()
    out: list[str] = []
    for name in (first, *CHAT_MODEL_FALLBACKS):
        if name and name not in out:
            out.append(name)
    return out


def _strip_reasoning_from_content(content: Any) -> Any:
    if not isinstance(content, list):
        return content
    cleaned: list[Any] = []
    for chunk in content:
        if isinstance(chunk, dict) and (
            chunk.get("type") in _REASONING_CONTENT_TYPES
            or "thinking" in chunk
            and chunk.get("type") != "text"
        ):
            continue
        cleaned.append(chunk)
    if not cleaned:
        return ""
    if len(cleaned) == 1 and isinstance(cleaned[0], dict) and cleaned[0].get("type") == "text":
        return cleaned[0].get("text") or ""
    return cleaned


def _messages_for_model(messages: list[dict], model: str) -> list[dict]:
    if model in _REASONING_MODELS:
        return messages
    out: list[dict] = []
    for msg in messages:
        if not isinstance(msg, dict):
            out.append(msg)
            continue
        item = {k: v for k, v in msg.items() if k not in _REASONING_MSG_KEYS}
        item["content"] = _strip_reasoning_from_content(item.get("content"))
        out.append(item)
    return out


def mistral_chat_complete(
    messages: list[dict],
    *,
    tools: list[dict] | None = None,
    model: str | None = None,
    temperature: float = 0.1,
    reasoning_effort: str | None = None,
    timeout_s: float = 120.0,
    allow_fallback: bool = True,
) -> dict[str, Any]:
    """
    POST /v1/chat/completions (httpx : le SDK refuse parfois reasoning_effort).

    Retourne {content, tool_calls, usage, raw_message}.
    Si GLM 5.2 est hors palier et ``allow_fallback``, bascule vers un modèle free tier.
    """
    key = _require_key()
    effort = resolve_reasoning_effort(reasoning_effort)
    last_err: Exception | None = None
    preferred = resolve_chat_model(model)
    candidates = _chat_model_candidates(preferred) if allow_fallback else [preferred]
    logger.info("Mistral chat → %s (fallbacks=%s)", preferred, allow_fallback)

    def _post(body: dict) -> dict:
        with httpx.Client(timeout=timeout_s) as http:
            resp = http.post(
                MISTRAL_CHAT_URL,
                headers={
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                },
                json=body,
            )
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code} : {resp.text[:800]}")
            return resp.json()

    for candidate in candidates:
        payload: dict[str, Any] = {
            "model": candidate,
            "messages": _messages_for_model(messages, candidate),
            "temperature": temperature,
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"
        if effort and effort != "none" and candidate in _REASONING_MODELS:
            payload["reasoning_effort"] = effort

        for attempt in range(3):
            try:
                data = _post(payload)
                if candidate != preferred:
                    logger.warning(
                        "Mistral : modèle %s indisponible, fallback %s",
                        preferred,
                        candidate,
                    )
                choice = (data.get("choices") or [{}])[0]
                raw_message = choice.get("message") or {}
                usage = data.get("usage") or {}
                return {
                    "content": _message_text(raw_message.get("content")),
                    "tool_calls": raw_message.get("tool_calls") or [],
                    "usage": usage,
                    "raw_message": raw_message,
                    "model": data.get("model") or candidate,
                }
            except RuntimeError as exc:
                last_err = exc
                err_l = str(exc).lower()
                if "reasoning_effort" in payload and (
                    "422" in err_l or "reasoning" in err_l
                ):
                    payload.pop("reasoning_effort", None)
                    logger.warning("Mistral : retry sans reasoning_effort (%s)", exc)
                    continue
                if "reasoning input" in err_l or '"code":"3051"' in str(exc):
                    payload["messages"] = _messages_for_model(messages, candidate)
                    payload.pop("reasoning_effort", None)
                    logger.warning(
                        "Mistral : retry sans reasoning input (%s)", candidate
                    )
                    continue
                if _is_rate_limited(exc) and attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                if _is_tier_blocked(exc) or _is_rate_limited(exc):
                    logger.warning(
                        "Mistral : %s indisponible (%s), essai suivant",
                        candidate,
                        "quota" if _is_rate_limited(exc) else "palier",
                    )
                    break
                raise

    raise last_err or RuntimeError("Aucun modèle Mistral disponible.")


def embed_query_mistral(text: str) -> list[float]:
    """Embedding requête via ``mistral-embed`` (normalisé L2)."""
    key = _require_key()
    with httpx.Client(timeout=30.0) as http:
        resp = http.post(
            MISTRAL_EMBED_URL,
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            json={"model": MISTRAL_EMBED_MODEL, "input": [text]},
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code} : {resp.text[:800]}")
        data = resp.json()
    items = data.get("data") or []
    if not items:
        raise RuntimeError("Réponse embeddings Mistral vide")
    values = items[0].get("embedding") or []
    arr = np.asarray(values, dtype=np.float32)
    norm = np.linalg.norm(arr)
    return (arr / norm).tolist() if norm else arr.tolist()


def serialize_mistral_turn(messages: list[dict]) -> dict:
    return {"provider": MISTRAL_PARTS_PROVIDER, "messages": messages}


def is_mistral_parts(blob) -> bool:
    return isinstance(blob, dict) and blob.get("provider") == MISTRAL_PARTS_PROVIDER


def build_mistral_messages_from_db(messages: list[dict], system: str) -> list[dict]:
    """Rejoue l'historique Mistral ; fallback texte plat pour les sessions Gemini."""
    out: list[dict] = [{"role": "system", "content": system}]
    for msg in messages:
        parts = msg.get("gemini_parts")
        if msg.get("role") == "model" and is_mistral_parts(parts):
            for item in parts.get("messages") or []:
                if isinstance(item, dict) and item.get("role") != "system":
                    out.append(item)
        else:
            role = "assistant" if msg.get("role") == "model" else "user"
            out.append({"role": role, "content": msg.get("content") or ""})
    return out
