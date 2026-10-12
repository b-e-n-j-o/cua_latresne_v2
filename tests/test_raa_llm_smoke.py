# -*- coding: utf-8 -*-
"""
Smoke LLM du pipeline d'analyse RAA.

Ping Vertex / Gemini avec le même client et le même modèle que
`api.raa.service_analyse_raa` (pas de PDF, pas de base).

Détecte : clé / auth cassée, modèle obsolète, SDK generate_content incompatible.

Usage (depuis cua_latresne_v4/) :

    pytest tests/test_raa_llm_smoke.py -v -s
    pytest -m llm tests/test_raa_llm_smoke.py -v -s
    python -m tests.test_raa_llm_smoke
"""

from __future__ import annotations

import pytest

from tests.test_env import load_app_env

load_app_env()


def _require_key() -> None:
    from api._env import GEMINI_API_KEY

    if not (GEMINI_API_KEY or "").strip():
        pytest.skip("GEMINI_API_KEY (ou GOOGLE_API_KEY) absente")


@pytest.mark.llm
def test_raa_vertex_client_is_vertexai() -> None:
    _require_key()
    from api.raa.service_analyse_raa import get_client

    client = get_client()
    vertexai = getattr(client, "vertexai", None)
    if vertexai is None:
        cfg = getattr(client, "_api_client", None) or getattr(client, "api_client", None)
        vertexai = getattr(cfg, "vertexai", None)
    assert vertexai is True, "Le client RAA doit être Vertex (vertexai=True)"


@pytest.mark.llm
def test_raa_llm_generate_content_responds() -> None:
    """Appel réel : même SDK + GEMINI_MODEL que _call_gemini (texte, pas PDF)."""
    _require_key()
    from google.genai import types

    from api._env import GEMINI_MODEL
    from api.raa.service_analyse_raa import _parse_gemini_json, get_client

    client = get_client()
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=(
            'Réponds uniquement avec ce JSON exact, sans markdown : '
            '{"ok": true, "service": "raa"}'
        ),
        config=types.GenerateContentConfig(temperature=0),
    )
    text = (response.text or "").strip()
    assert text, f"Réponse vide du modèle {GEMINI_MODEL!r}"

    parsed = _parse_gemini_json(text)
    assert isinstance(parsed, dict), f"JSON invalide : {text[:200]!r}"
    assert parsed.get("ok") is True, f"Payload inattendu : {parsed}"


def main() -> int:
    load_app_env()
    from api._env import GEMINI_API_KEY, GEMINI_MODEL

    print(f"Modèle : {GEMINI_MODEL}")
    if not (GEMINI_API_KEY or "").strip():
        print("ÉCHEC — GEMINI_API_KEY absente")
        return 1
    try:
        test_raa_vertex_client_is_vertexai()
        test_raa_llm_generate_content_responds()
    except Exception as e:
        print(f"ÉCHEC — {type(e).__name__}: {e}")
        return 2
    print("OK — Vertex/Gemini RAA répond")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
