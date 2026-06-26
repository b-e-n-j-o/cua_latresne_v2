# -*- coding: utf-8 -*-
"""Helpers de requête sur la table pipelines (public ou legacy latresne)."""

from __future__ import annotations

import os
from typing import Any, Optional

from services.auth.commune_access import filter_pipelines_for_viewer, get_authorized_insee_codes

PIPELINES_SCHEMA = os.getenv("PIPELINES_SCHEMA", "public")


def pipelines_schema() -> str:
    return (os.getenv("PIPELINES_SCHEMA") or PIPELINES_SCHEMA).strip() or "public"


def apply_access_filters(
    query: Any,
    *,
    user_id: str,
    commune_slug: Optional[str] = None,
    allowed_insee: Optional[list[str]] = None,
) -> Any:
    """
    Filtre par droits commune (code_insee) et optionnellement par commune affichée.
    """
    if allowed_insee is None:
        allowed_insee = get_authorized_insee_codes(user_id)
    if allowed_insee:
        query = query.in_("code_insee", allowed_insee)

    if commune_slug:
        slug = commune_slug.strip().lower()
        # public.pipelines : commune_slug ; legacy latresne.pipelines : champ commune
        query = query.or_(f"commune_slug.eq.{slug},commune.ilike.{slug}")

    return query


def select_pipelines_for_user(
    supabase_client: Any,
    *,
    user_id: str,
    limit: int = 15,
    commune_slug: Optional[str] = None,
    columns: str = "*",
) -> list[dict]:
    """
    Pipelines visibles pour un utilisateur, scopées par commune (INSEE), pas par auteur.

    None dans allowed_insee = superadmin / accès global (pas de filtre INSEE).
    Liste vide = aucun droit explicite → aucun résultat.
    """
    allowed_insee = get_authorized_insee_codes(user_id)
    if allowed_insee is not None and not allowed_insee:
        return []

    schema = pipelines_schema()
    query = (
        supabase_client.schema(schema)
        .table("pipelines")
        .select(columns)
        .order("created_at", desc=True)
        .limit(limit)
    )
    query = apply_access_filters(
        query,
        user_id=user_id,
        commune_slug=commune_slug,
        allowed_insee=allowed_insee,
    )
    response = query.execute()
    pipelines = response.data or []
    return filter_pipelines_for_viewer(pipelines, user_id)
