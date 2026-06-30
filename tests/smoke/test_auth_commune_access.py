# -*- coding: utf-8 -*-
"""
Smoke tests auth — login Supabase → JWT → endpoints protégés.

Lancer en local (uvicorn sur :8000) :
    cp .env.test.example .env.test.local   # remplir AUTH_TEST_*
    pytest tests/smoke -v

Après deploy Render :
    pytest tests/smoke --prod -v

Creds tests : .env.test.local (gitignoré), pas le .env applicatif.
"""

from __future__ import annotations

import pytest

from tests.smoke.auth_e2e import api_get, run_checks, supabase_config

pytestmark = pytest.mark.smoke


def test_commune_access_requires_bearer(api_base: str) -> None:
    """Sans Authorization → 401 (pas 422 = ancien backend)."""
    resp = api_get(api_base, "/account/commune-access")
    assert resp.status_code == 401, resp.text
    assert resp.status_code != 422, (
        "422 = ancien backend (user_id en query). Redéployer le code auth."
    )


def test_commune_access_rejects_legacy_query_user_id(
    api_base: str, auth_session: dict
) -> None:
    """?user_id= seul ne doit plus suffire."""
    resp = api_get(
        api_base,
        "/account/commune-access",
        query_user_id=auth_session["user_id"],
    )
    assert resp.status_code in (401, 403), resp.text


def test_commune_access_with_bearer(api_base: str, auth_session: dict) -> None:
    resp = api_get(
        api_base,
        "/account/commune-access",
        token=auth_session["access_token"],
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("success") is True
    assert body.get("unrestricted") or body.get("allowed_commune_slugs") is not None


def test_superadmin_when_expected(
    api_base: str,
    auth_session: dict,
    expect_superadmin: bool,
) -> None:
    if not expect_superadmin:
        pytest.skip("AUTH_TEST_EXPECT_SUPERADMIN non défini")
    resp = api_get(
        api_base,
        "/account/commune-access",
        token=auth_session["access_token"],
    )
    body = resp.json()
    assert body.get("is_superadmin") is True
    assert body.get("unrestricted") is True


def test_supabase_token_valid_like_backend(auth_session: dict) -> None:
    """Même validation que services/auth/current_user.py."""
    import httpx

    url, key = supabase_config()
    with httpx.Client(timeout=20.0) as client:
        resp = client.get(
            f"{url}/auth/v1/user",
            headers={
                "apikey": key,
                "Authorization": f"Bearer {auth_session['access_token']}",
            },
        )
    assert resp.status_code == 200, resp.text
    assert resp.json().get("id") == auth_session["user_id"]


def test_pipelines_by_user(
    api_base: str,
    auth_session: dict,
    commune_slug: str | None,
) -> None:
    params = {"commune_slug": commune_slug} if commune_slug else None
    resp = api_get(
        api_base,
        "/pipelines/by_user",
        token=auth_session["access_token"],
        extra_params=params,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("success") is True


def test_auth_e2e_suite(
    api_base: str,
    auth_session: dict,
    expect_superadmin: bool,
    commune_slug: str | None,
) -> None:
    """Résumé groupé (équivalent au script CLI)."""
    results = run_checks(
        api_base,
        auth_session,
        expect_superadmin=expect_superadmin,
        commune_slug=commune_slug,
    )
    failed = [r for r in results if not r.ok]
    if failed:
        lines = "\n".join(f"  - {r.name}: {r.detail}" for r in failed)
        pytest.fail(f"{len(failed)} check(s) en échec:\n{lines}")
