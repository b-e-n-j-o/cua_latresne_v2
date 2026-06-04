"""
Authentification Supabase pour l'agent PLU — isolation des sessions par utilisateur.

Chaque requête protégée exige ``Authorization: Bearer <access_token>`` (session
frontend Supabase). Le JWT est validé via l'API Auth ; seules les sessions dont
``plu_sessions.user_id`` correspond sont listées / lues / modifiées.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache

import httpx
from fastapi import Header, HTTPException

logger = logging.getLogger("plu_api")

_SCHEMAS_USER_COLUMN_READY: set[str] = set()


def plu_auth_required() -> bool:
    """False uniquement si PLU_REQUIRE_AUTH=0 (dev local sans JWT)."""
    return (os.environ.get("PLU_REQUIRE_AUTH") or "1").strip().lower() not in (
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
            "SUPABASE_URL et SERVICE_KEY (ou SUPABASE_SERVICE_ROLE_KEY) requis "
            "pour valider les JWT PLU."
        )
    return url, key


def _user_id_column_exists(conn, schema: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = 'plu_sessions'
              AND column_name = 'user_id'
            LIMIT 1
            """,
            (schema,),
        )
        return cur.fetchone() is not None


def ensure_user_id_column(conn, schema: str) -> None:
    """
    Vérifie que la colonne user_id existe (pas d'ALTER en requête HTTP :
    sur Supabase, ALTER TABLE peut dépasser statement_timeout).
    """
    if schema in _SCHEMAS_USER_COLUMN_READY:
        return
    if _user_id_column_exists(conn, schema):
        _SCHEMAS_USER_COLUMN_READY.add(schema)
        return
    logger.error(
        "Migration manquante : %s.plu_sessions.user_id — voir "
        "api/agents/plu_agent/migrations/001_plu_sessions_user_id.sql",
        schema,
    )
    raise HTTPException(
        status_code=503,
        detail=(
            f'Migration SQL requise sur "{schema}.plu_sessions" : '
            f'ALTER TABLE "{schema}".plu_sessions '
            "ADD COLUMN IF NOT EXISTS user_id UUID;"
        ),
    )


def verify_supabase_access_token(token: str) -> str:
    """Valide le JWT utilisateur ; retourne l'UUID Supabase Auth."""
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
        logger.warning("plu_auth — erreur réseau Supabase : %s", e)
        raise HTTPException(status_code=503, detail="Auth indisponible.") from e

    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Token invalide ou expiré.")

    data = resp.json()
    user_id = (data.get("id") or "").strip()
    if not user_id:
        raise HTTPException(status_code=401, detail="Utilisateur introuvable.")
    return user_id


def get_plu_user_id(authorization: str | None = Header(None)) -> str:
    """
    Dépendance FastAPI : identifiant utilisateur courant (UUID Supabase).
    """
    if not plu_auth_required():
        return "00000000-0000-0000-0000-000000000000"

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=401,
            detail="Authentification requise (Bearer token Supabase).",
        )
    token = authorization[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token manquant.")
    return verify_supabase_access_token(token)


def session_belongs_to_user(session: dict | None, user_id: str) -> bool:
    if not session:
        return False
    if not plu_auth_required():
        return True
    owner = session.get("user_id")
    if owner is None:
        return False
    return str(owner) == str(user_id)
