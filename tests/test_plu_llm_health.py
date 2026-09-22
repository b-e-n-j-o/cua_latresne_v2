# -*- coding: utf-8 -*-
"""
Ping LLM de l'agent PLU — Argelès et Latresne.

Vérifie que Vertex / Gemini (et optionnellement Mistral via ?stack=mistral)
répond, via GET /llm-health, sans créer de session ni appeler les tools.

Client Mistral unitaire (GLM puis free tier) :
    pytest tests/test_plu_mistral_client.py -v -s

    pytest tests/test_plu_llm_health.py -v
    pytest -m llm -v
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from tests.test_env import load_app_env

load_app_env()

PLU_COMMUNES = ("argeles", "latresne")


def test_classify_llm_error_quota() -> None:
    from api.agents.plu_agent.routes.chat import classify_llm_error

    class QuotaErr(Exception):
        status_code = 429

    assert classify_llm_error(QuotaErr("RESOURCE_EXHAUSTED")) == "quota"
    assert classify_llm_error(RuntimeError("quota exceeded")) == "quota"


def test_classify_llm_error_auth() -> None:
    from api.agents.plu_agent.routes.chat import classify_llm_error

    assert classify_llm_error(RuntimeError("API key not valid")) == "auth"


def test_vertex_client_is_vertexai() -> None:
    from api.agents.plu_agent.vertex_client import build_vertex_client

    client = build_vertex_client()
    vertexai = getattr(client, "vertexai", None)
    if vertexai is None:
        cfg = getattr(client, "_api_client", None) or getattr(client, "api_client", None)
        vertexai = getattr(cfg, "vertexai", None)
    assert vertexai is True


def test_classify_llm_error_service_disabled() -> None:
    from api.agents.plu_agent.routes.chat import classify_llm_error

    msg = (
        "403 PERMISSION_DENIED. Gemini API has not been used in project "
        "316677074624 before or it is disabled. SERVICE_DISABLED"
    )
    assert classify_llm_error(RuntimeError(msg)) == "service_disabled"


@pytest.fixture(scope="module")
def plu_app():
    from api.agents.plu_agent.api import create_standalone_app

    return create_standalone_app()


def _get_llm_health(app, slug: str, *, stack: str = "vertex") -> httpx.Response:
    """GET /llm-health via ASGI (TestClient httpx 0.28 + Starlette 0.27 incompatibles)."""

    async def _call() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            timeout=120.0,
        ) as client:
            return await client.get(
                f"/api/plu/{slug}/llm-health",
                params={"stack": stack},
            )

    return asyncio.run(_call())


@pytest.mark.llm
@pytest.mark.smoke
@pytest.mark.parametrize("slug", PLU_COMMUNES)
def test_llm_api_responds(plu_app, slug: str) -> None:
    """GET /api/plu/{slug}/llm-health : generate + embedding Vertex doivent répondre."""
    resp = _get_llm_health(plu_app, slug)
    body = resp.json()
    if resp.status_code != 200 or not body.get("ok"):
        reason = body.get("reason") or "error"
        detail = body.get("error") or body
        embedding = body.get("embedding") or {}
        pytest.fail(
            f"API Vertex indisponible pour {slug} ({reason}) : {detail} "
            f"| embedding={embedding}"
        )
    assert body["commune"] == slug
    assert body.get("stack") == "vertex"
    assert body.get("model")
    assert (body.get("generate") or {}).get("ok") is True, body.get("generate")
    embed = body.get("embedding") or {}
    assert embed.get("ok") is True, embed
    assert embed.get("stack") == "vertex"
    assert embed.get("dim") == 768


def test_resolve_provider_defaults_gemini() -> None:
    from api.agents.plu_agent.mistral_client import is_mistral_model, resolve_provider

    assert resolve_provider(None) == "mistral"
    assert resolve_provider("gemini") == "gemini"
    assert resolve_provider("mistral") == "mistral"
    assert resolve_provider(None, session_model="zai-glm-5-2") == "mistral"
    assert resolve_provider(None, session_model="gemini-3.5-flash") == "gemini"
    assert is_mistral_model("zai-glm-5-2")
    assert not is_mistral_model("gemini-3.5-flash")


def test_mistral_tools_from_gemini_decls() -> None:
    from api.agents.plu_agent.mistral_client import gemini_decls_to_mistral_tools
    from api.agents.plu_agent.tools import TOOL_DECLARATIONS_BY_NAME

    tools = gemini_decls_to_mistral_tools(
        [
            TOOL_DECLARATIONS_BY_NAME["get_parcelle"],
            TOOL_DECLARATIONS_BY_NAME["search_articles_urbanisme"],
        ]
    )
    assert len(tools) == 2
    by_name = {t["function"]["name"]: t for t in tools}
    assert by_name["get_parcelle"]["type"] == "function"
    params = by_name["search_articles_urbanisme"]["function"]["parameters"]
    assert params["type"] == "object"
    assert "query" in params["properties"]
    assert "query" in params.get("required", [])


@pytest.mark.llm
@pytest.mark.smoke
@pytest.mark.parametrize("key_var", ("MISTRAL_API_KEY", "MISTRAL_API_KEY_BEN"))
@pytest.mark.parametrize("slug", PLU_COMMUNES)
def test_mistral_api_responds(plu_app, slug: str, key_var: str) -> None:
    """GET /llm-health?stack=mistral : GLM 5.2 + mistral-embed (chaque clé)."""
    import os

    from api.agents.plu_agent import mistral_client as mc

    key = (os.getenv(key_var) or "").strip()
    if not key:
        pytest.skip(f"{key_var} absente")
    previous = mc.MISTRAL_API_KEY
    mc.MISTRAL_API_KEY = key
    try:
        resp = _get_llm_health(plu_app, slug, stack="mistral")
    finally:
        mc.MISTRAL_API_KEY = previous
    body = resp.json()
    if resp.status_code != 200 or not body.get("ok"):
        pytest.fail(
            f"API Mistral indisponible pour {slug} [{key_var}] "
            f"({body.get('reason')}) : {body.get('error') or body}"
        )
    assert body.get("stack") == "mistral"
    assert (body.get("generate") or {}).get("ok") is True
    embed = body.get("embedding") or {}
    assert embed.get("ok") is True, embed
    assert embed.get("stack") == "mistral"
    assert embed.get("dim", 0) > 0
