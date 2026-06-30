# -*- coding: utf-8 -*-
"""Helpers partagés — smoke tests auth (pytest + script CLI)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

import httpx

DEFAULT_API_BASE = os.getenv("AUTH_TEST_API_BASE", "http://127.0.0.1:8000").rstrip("/")
PROD_API_BASE = os.getenv("AUTH_TEST_PROD_API_BASE", "https://api.kerelia.fr").rstrip("/")
UA = "Mozilla/5.0 (Kerelia auth test)"


class AuthE2EError(RuntimeError):
    """Erreur login ou config Supabase."""


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str


def supabase_config() -> tuple[str, str]:
    url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    key = (
        os.getenv("SUPABASE_ANON_KEY")
        or os.getenv("SUPABASE_KEY")
        or os.getenv("SERVICE_KEY")
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or ""
    ).strip()
    if not url or not key:
        raise AuthE2EError(
            "SUPABASE_URL et SUPABASE_KEY (anon) requis dans .env ou l'environnement."
        )
    return url, key


def supabase_sign_in(email: str, password: str) -> dict:
    """Login password Supabase → access_token + user."""
    url, key = supabase_config()
    with httpx.Client(timeout=20.0) as client:
        resp = client.post(
            f"{url}/auth/v1/token?grant_type=password",
            headers={"apikey": key, "Content-Type": "application/json"},
            json={"email": email, "password": password},
        )
    if resp.status_code != 200:
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        raise AuthE2EError(f"Login Supabase échoué ({resp.status_code}) : {body}")

    data = resp.json()
    token = (data.get("access_token") or "").strip()
    user = data.get("user") or {}
    user_id = (user.get("id") or "").strip()
    if not token or not user_id:
        raise AuthE2EError(f"Réponse Supabase incomplète : {data}")
    return {
        "access_token": token,
        "user_id": user_id,
        "email": user.get("email"),
        "expires_in": data.get("expires_in"),
    }


def api_get(
    api_base: str,
    path: str,
    *,
    token: str | None = None,
    query_user_id: str | None = None,
    extra_params: dict | None = None,
) -> httpx.Response:
    headers = {"User-Agent": UA}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    params = dict(extra_params or {})
    if query_user_id:
        params["user_id"] = query_user_id
    url = f"{api_base}{path}"
    with httpx.Client(timeout=20.0) as client:
        return client.get(url, headers=headers, params=params or None)


def _json_body(resp: httpx.Response) -> dict:
    if resp.headers.get("content-type", "").startswith("application/json"):
        return resp.json()
    return {}


def run_checks(
    api_base: str,
    session: dict,
    *,
    expect_superadmin: bool,
    commune_slug: str | None,
) -> list[CheckResult]:
    token = session["access_token"]
    user_id = session["user_id"]
    results: list[CheckResult] = []

    r = api_get(api_base, "/account/commune-access")
    results.append(
        CheckResult(
            name="sans Authorization",
            ok=r.status_code == 401,
            detail=f"HTTP {r.status_code} — {_json_body(r) or r.text[:200]}",
        )
    )
    if r.status_code == 422:
        results.append(
            CheckResult(
                name="backend à jour (pas de user_id query requis)",
                ok=False,
                detail="422 sans token = ancien backend (user_id en query). Redéployer le code auth.",
            )
        )

    r = api_get(api_base, "/account/commune-access", query_user_id=user_id)
    results.append(
        CheckResult(
            name="query ?user_id= sans Bearer (doit refuser)",
            ok=r.status_code in (401, 403),
            detail=f"HTTP {r.status_code} — attendu 401/403 sur backend JWT",
        )
    )

    r = api_get(api_base, "/account/commune-access", token=token)
    body = _json_body(r)
    ok = r.status_code == 200 and body.get("success") is True
    results.append(
        CheckResult(
            name="GET /account/commune-access (Bearer)",
            ok=ok,
            detail=f"HTTP {r.status_code} — {json.dumps(body, ensure_ascii=False)}",
        )
    )

    if ok:
        is_super = bool(body.get("is_superadmin"))
        unrestricted = bool(body.get("unrestricted"))
        if expect_superadmin:
            results.append(
                CheckResult(
                    name="superadmin attendu",
                    ok=is_super and unrestricted,
                    detail=f"is_superadmin={is_super}, unrestricted={unrestricted}",
                )
            )
        slugs = body.get("allowed_commune_slugs")
        insee = body.get("allowed_insee_codes")
        results.append(
            CheckResult(
                name="droits renvoyés",
                ok=unrestricted
                or (isinstance(slugs, list) and len(slugs) > 0)
                or (isinstance(insee, list) and len(insee) > 0),
                detail=f"slugs={slugs}, insee={insee}, unrestricted={unrestricted}",
            )
        )

    url, key = supabase_config()
    with httpx.Client(timeout=20.0) as client:
        vr = client.get(
            f"{url}/auth/v1/user",
            headers={"apikey": key, "Authorization": f"Bearer {token}"},
        )
    results.append(
        CheckResult(
            name="Supabase /auth/v1/user (comme le backend)",
            ok=vr.status_code == 200 and (vr.json().get("id") or "") == user_id,
            detail=(
                f"HTTP {vr.status_code}, id={vr.json().get('id')}"
                if vr.status_code == 200
                else vr.text[:120]
            ),
        )
    )

    params: dict = {}
    if commune_slug:
        params["commune_slug"] = commune_slug
    r = api_get(api_base, "/pipelines/by_user", token=token, extra_params=params)
    body = _json_body(r)
    suffix = f"?commune_slug={commune_slug}" if commune_slug else ""
    results.append(
        CheckResult(
            name=f"GET /pipelines/by_user{suffix}",
            ok=r.status_code == 200 and body.get("success") is True,
            detail=f"HTTP {r.status_code}, count={body.get('count', '?')}",
        )
    )

    return results
