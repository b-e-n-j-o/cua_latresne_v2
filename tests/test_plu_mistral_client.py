# -*- coding: utf-8 -*-
"""
Test unitaire du client Mistral PLU — GLM d'abord, free tier seulement en fallback.

Enchaîne les mêmes scénarios avec ``MISTRAL_API_KEY`` puis ``MISTRAL_API_KEY_BEN``
pour comparer les paliers.

    pytest tests/test_plu_mistral_client.py -v -s
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

import pytest

from tests.test_env import load_app_env

load_app_env()

GLM_MODEL = "zai-glm-5-2"
FREE_TIER_MODELS = ("ministral-14b-latest", "mistral-small-latest")
MISTRAL_KEY_VARS = ("MISTRAL_API_KEY", "MISTRAL_API_KEY_BEN")
PING = "Réponds uniquement par le mot PONG."
TOOL_PROMPT = (
    "Quelle heure est-il maintenant ? Tu dois appeler l'outil get_heure, "
    "sans inventer l'heure toi-même."
)
GET_HEURE_TOOL = [
    {
        "type": "function",
        "function": {
            "name": "get_heure",
            "description": "Renvoie l'heure actuelle (ISO-8601).",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    }
]


@contextmanager
def _use_mistral_key_var(var_name: str) -> Iterator[str]:
    """Active temporairement la clé lue depuis ``var_name`` (sans l'afficher)."""
    key = (os.getenv(var_name) or "").strip()
    if not key:
        pytest.skip(f"{var_name} absente")
    import api.agents.plu_agent.mistral_client as mc

    previous = mc.MISTRAL_API_KEY
    mc.MISTRAL_API_KEY = key
    try:
        yield var_name
    finally:
        mc.MISTRAL_API_KEY = previous


def _classify(exc: BaseException) -> str:
    from api.agents.plu_agent.routes.chat import classify_llm_error

    return classify_llm_error(exc)


def _try_chat(
    model: str,
    *,
    with_tools: bool = False,
) -> dict[str, Any]:
    from api.agents.plu_agent.mistral_client import mistral_chat_complete

    try:
        result = mistral_chat_complete(
            [{"role": "user", "content": TOOL_PROMPT if with_tools else PING}],
            tools=GET_HEURE_TOOL if with_tools else None,
            model=model,
            temperature=0,
            reasoning_effort="none",
            allow_fallback=False,
        )
        tool_calls = result.get("tool_calls") or []
        return {
            "ok": True,
            "model": result.get("model") or model,
            "requested": model,
            "content": (result.get("content") or "").strip(),
            "tool_calls": [((tc.get("function") or {}).get("name")) for tc in tool_calls],
            "error": None,
            "reason": None,
        }
    except Exception as exc:
        return {
            "ok": False,
            "model": model,
            "requested": model,
            "content": "",
            "tool_calls": [],
            "error": str(exc),
            "reason": _classify(exc),
        }


def _try_embed() -> dict[str, Any]:
    from api.agents.plu_agent.mistral_client import embed_query_mistral

    try:
        vec = embed_query_mistral("ping urbanisme")
        return {"ok": bool(vec), "dim": len(vec), "error": None, "reason": None}
    except Exception as exc:
        return {"ok": False, "dim": 0, "error": str(exc), "reason": _classify(exc)}


def _print_row(label: str, row: dict[str, Any]) -> None:
    status = "OK" if row.get("ok") else f"KO ({row.get('reason') or 'error'})"
    extra = ""
    if row.get("tool_calls"):
        extra = f" tools={row['tool_calls']}"
    elif row.get("content"):
        extra = f" answer={row['content'][:40]!r}"
    if row.get("dim"):
        extra = f" dim={row['dim']}"
    err = f" | {row['error'][:160]}" if row.get("error") else ""
    print(f"  {label:28} {status}{extra}{err}")


def _fallback_models() -> list[str]:
    return list(FREE_TIER_MODELS)


@pytest.mark.llm
@pytest.mark.smoke
@pytest.mark.parametrize("key_var", MISTRAL_KEY_VARS)
def test_mistral_client_glm_ping_then_free_tier(key_var: str) -> None:
    """Generate : GLM 5.2 d'abord ; free tier seulement si GLM échoue."""
    with _use_mistral_key_var(key_var):
        rows = [_try_chat(GLM_MODEL)]
        if not rows[0]["ok"]:
            for model in _fallback_models():
                rows.append(_try_chat(model))

        print(f"\n=== Mistral chat (sans tools) — {key_var} ===")
        for row in rows:
            _print_row(row["requested"], row)

        glm = rows[0]
        if glm["ok"]:
            assert "PONG" in (glm.get("content") or "").upper()
            return

        ok_fallback = next((r for r in rows[1:] if r["ok"]), None)
        assert ok_fallback is not None, (
            f"[{key_var}] GLM {GLM_MODEL} KO ({glm.get('reason')}): {glm.get('error')} "
            f"— aucun free tier n'a répondu non plus : {rows}"
        )


@pytest.mark.llm
@pytest.mark.smoke
@pytest.mark.parametrize("key_var", MISTRAL_KEY_VARS)
def test_mistral_client_glm_tools_then_free_tier(key_var: str) -> None:
    """Tool calling : GLM 5.2 d'abord ; free tier si GLM échoue."""
    with _use_mistral_key_var(key_var):
        rows = [_try_chat(GLM_MODEL, with_tools=True)]
        if not rows[0]["ok"]:
            for model in _fallback_models():
                rows.append(_try_chat(model, with_tools=True))

        print(f"\n=== Mistral tool calling — {key_var} ===")
        for row in rows:
            _print_row(row["requested"], row)

        glm = rows[0]
        if glm["ok"]:
            assert "get_heure" in glm["tool_calls"], (
                f"[{key_var}] GLM a répondu sans tool_call get_heure : {glm}"
            )
            return

        ok_fallback = next((r for r in rows[1:] if r["ok"]), None)
        assert ok_fallback is not None, (
            f"[{key_var}] GLM tools KO ({glm.get('reason')}): {glm.get('error')} "
            f"— aucun free tier n'a répondu : {rows}"
        )
        if ok_fallback["ok"] and "get_heure" not in ok_fallback["tool_calls"]:
            pytest.xfail(
                f"[{key_var}] GLM indisponible ({glm.get('reason')}) ; "
                f"{ok_fallback['requested']} répond mais sans tool_call "
                f"(content={ok_fallback.get('content')!r})"
            )


@pytest.mark.llm
@pytest.mark.smoke
@pytest.mark.parametrize("key_var", MISTRAL_KEY_VARS)
def test_mistral_client_embed(key_var: str) -> None:
    """Embeddings : mistral-embed (indépendant du modèle chat GLM)."""
    with _use_mistral_key_var(key_var):
        row = _try_embed()
        print(f"\n=== Mistral embed — {key_var} ===")
        _print_row("mistral-embed", row)
        assert row["ok"], (
            f"[{key_var}] mistral-embed KO ({row.get('reason')}): {row.get('error')}"
        )
        assert row["dim"] == 1024
