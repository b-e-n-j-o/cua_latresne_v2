# -*- coding: utf-8 -*-
"""Identité utilisateur vérifiée (JWT Supabase) — dépendance partagée."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache

import httpx
from fastapi import Header, HTTPException

logger = logging.getLogger("auth.current_user")

_DEV_USER_ID = "00000000-0000-0000-0000-000000000000"


@dataclass(frozen=True)
class AuthenticatedUser:
    id: str
    email: str | None = None


def auth_required() -> bool:
    """False seulement si REQUIRE_AUTH=0 (dev local). En prod : toujours True."""
    return (os.environ.get("REQUIRE_AUTH") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )


@lru_cache(maxsize=1)
def _supabase_auth_config() -> tuple[str, str]:
    url = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
    key = (
        os.environ.get("SERVICE_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_KEY")
        or ""
    ).strip()
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL et SERVICE_KEY requis pour valider les JWT."
        )
    return url, key


def verify_supabase_access_token(token: str) -> AuthenticatedUser:
    """Valide le JWT utilisateur ; retourne id + email depuis Supabase Auth."""
    url, key = _supabase_auth_config()
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{url}/auth/v1/user",
                headers={
                    "apikey": key,
                    "Authorization": f"Bearer {token}",
                },
            )
    except httpx.HTTPError as e:
        logger.warning("auth — erreur réseau Supabase : %s", e)
        raise HTTPException(status_code=503, detail="Auth indisponible.") from e

    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Token invalide ou expiré.")

    data = resp.json()
    user_id = (data.get("id") or "").strip()
    if not user_id:
        raise HTTPException(status_code=401, detail="Utilisateur introuvable.")

    email = (data.get("email") or "").strip() or None
    return AuthenticatedUser(id=user_id, email=email)


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Authentification requise.")
    token = authorization[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token manquant.")
    return token


def get_current_user(authorization: str | None = Header(None)) -> AuthenticatedUser:
    """Dépendance FastAPI : utilisateur authentifié (UUID + email vérifiés)."""
    if not auth_required():
        return AuthenticatedUser(id=_DEV_USER_ID, email=None)

    return verify_supabase_access_token(_extract_bearer_token(authorization))


def get_current_user_id(authorization: str | None = Header(None)) -> str:
    """Dépendance FastAPI : UUID Supabase de l'utilisateur authentifié."""
    return get_current_user(authorization).id
