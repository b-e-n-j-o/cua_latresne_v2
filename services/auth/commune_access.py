# -*- coding: utf-8 -*-
"""
commune_access.py — Droits utilisateur par commune (INSEE / slug).

Source de vérité (par priorité) :
1. Table public.user_commune_access (après migration SQL)
2. Fallback : user_metadata.insee dans Supabase Auth (legacy Latresne)

Convention :
- get_authorized_insee_codes() renvoie None → accès à toutes les communes
- get_authorized_insee_codes() renvoie une liste non vide → accès restreint
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from dotenv import load_dotenv
from fastapi import HTTPException
from supabase import create_client

load_dotenv()

logger = logging.getLogger("commune_access")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Registre des communes portail Kerelia (slug → INSEE).
COMMUNE_REGISTRY: dict[str, dict[str, str]] = {
    "latresne": {"code_insee": "33234", "nom": "Latresne"},
    "argeles": {"code_insee": "66008", "nom": "Argelès-sur-Mer"},
    "mios": {"code_insee": "33531", "nom": "Mios"},
}

INSEE_TO_SLUG: dict[str, str] = {
    meta["code_insee"]: slug for slug, meta in COMMUNE_REGISTRY.items()
}

ROLES = ("user", "admin_commune", "superadmin")


def _get_supabase():
    if not (SUPABASE_URL and SUPABASE_KEY):
        raise RuntimeError("SUPABASE_URL et SERVICE_KEY requis pour commune_access.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _insee_from_metadata(meta: dict[str, Any]) -> list[str]:
    insee_field = meta.get("insee")
    if isinstance(insee_field, str) and insee_field.strip():
        return [insee_field.strip()]
    if isinstance(insee_field, list):
        return [str(x).strip() for x in insee_field if str(x).strip()]
    return []


def _fetch_user_commune_access_rows(user_id: str) -> list[dict[str, Any]] | None:
    """Lit public.user_commune_access. None si table absente ou erreur transitoire."""
    try:
        sb = _get_supabase()
        response = (
            sb.schema("public")
            .table("user_commune_access")
            .select("user_id, commune_slug, code_insee, role")
            .eq("user_id", user_id)
            .execute()
        )
        return response.data or []
    except Exception as exc:
        logger.debug("user_commune_access indisponible (%s) — fallback metadata", exc)
        return None


def _fetch_metadata_insee(user_id: str) -> list[str]:
    try:
        sb = _get_supabase()
        user = sb.auth.admin.get_user_by_id(user_id)
        meta = user.user.user_metadata or {}
        return _insee_from_metadata(meta)
    except Exception as exc:
        logger.warning("Erreur lecture metadata INSEE pour %s : %s", user_id, exc)
        return []


def get_user_commune_access(user_id: str) -> list[dict[str, Any]]:
    """
    Retourne les lignes d'accès commune pour un utilisateur.
    Liste vide si aucune restriction explicite (accès global).
    """
    rows = _fetch_user_commune_access_rows(user_id)
    if rows is not None:
        return rows

    insee_codes = _fetch_metadata_insee(user_id)
    if not insee_codes:
        return []

    access: list[dict[str, Any]] = []
    for code in insee_codes:
        slug = INSEE_TO_SLUG.get(code, "")
        access.append(
            {
                "user_id": user_id,
                "commune_slug": slug,
                "code_insee": code,
                "role": "user",
            }
        )
    return access


def get_authorized_insee_codes(user_id: str) -> Optional[list[str]]:
    """
    None → toutes les communes.
    Liste non vide → restriction à ces codes INSEE.
    """
    rows = _fetch_user_commune_access_rows(user_id)
    if rows is not None:
        if not rows:
            return None
        if any((r.get("role") or "").lower() == "superadmin" for r in rows):
            return None
        codes = sorted({str(r["code_insee"]).strip() for r in rows if r.get("code_insee")})
        return codes or None

    insee_codes = _fetch_metadata_insee(user_id)
    if not insee_codes:
        return None
    return sorted(set(insee_codes))


def get_authorized_commune_slugs(user_id: str) -> Optional[list[str]]:
    """None → toutes les communes ; sinon liste de slugs autorisés."""
    codes = get_authorized_insee_codes(user_id)
    if codes is None:
        return None
    slugs = sorted({INSEE_TO_SLUG[c] for c in codes if c in INSEE_TO_SLUG})
    return slugs or None


def is_authorized_for_insee(user_id: str, commune_insee: str) -> bool:
    if not user_id:
        return True
    allowed = get_authorized_insee_codes(user_id)
    if allowed is None:
        return True
    return commune_insee in allowed


def is_authorized_for_commune_slug(user_id: str, commune_slug: str) -> bool:
    slug = (commune_slug or "").strip().lower()
    meta = COMMUNE_REGISTRY.get(slug)
    if not meta:
        return False
    return is_authorized_for_insee(user_id, meta["code_insee"])


def assert_authorized_for_insee(user_id: Optional[str], commune_insee: str) -> None:
    if not user_id:
        return
    if not is_authorized_for_insee(user_id, commune_insee):
        allowed = get_authorized_insee_codes(user_id) or []
        raise HTTPException(
            status_code=403,
            detail=(
                "Accès refusé : vous n'êtes autorisé qu'à accéder aux communes "
                f"suivantes (INSEE) : {', '.join(allowed)}"
            ),
        )


def assert_authorized_for_commune_slug(user_id: Optional[str], commune_slug: str) -> None:
    if not user_id:
        return
    slug = (commune_slug or "").strip().lower()
    meta = COMMUNE_REGISTRY.get(slug)
    if not meta:
        raise HTTPException(status_code=404, detail=f"Commune inconnue : {commune_slug}")
    assert_authorized_for_insee(user_id, meta["code_insee"])
