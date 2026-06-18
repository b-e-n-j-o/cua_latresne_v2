# -*- coding: utf-8 -*-
"""Helpers de requête sur la table pipelines (public ou legacy latresne)."""

from __future__ import annotations

import os
from typing import Any, Optional

from services.auth.commune_access import get_authorized_insee_codes

PIPELINES_SCHEMA = os.getenv("PIPELINES_SCHEMA", "public")


def pipelines_schema() -> str:
    return (os.getenv("PIPELINES_SCHEMA") or PIPELINES_SCHEMA).strip() or "public"


def apply_access_filters(
    query: Any,
    *,
    user_id: str,
    commune_slug: Optional[str] = None,
) -> Any:
    """
    Filtre par droits utilisateur (code_insee) et optionnellement par commune affichée.
    """
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
    schema = pipelines_schema()
    query = (
        supabase_client.schema(schema)
        .table("pipelines")
        .select(columns)
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .limit(limit)
    )
    query = apply_access_filters(query, user_id=user_id, commune_slug=commune_slug)
    response = query.execute()
    return response.data or []
