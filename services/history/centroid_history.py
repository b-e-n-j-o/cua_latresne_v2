# -*- coding: utf-8 -*-
"""
centroid_history.py — Endpoints dédiés à l'historique des pipelines (centroïdes pour la carte)
---------------------------------------------------------------------------------------------
Récupère les pipelines d'un utilisateur avec centroid (lon/lat) et cerfa_data
pour afficher les pings sur la carte et les infos au clic.

Filtrage par droits commune (public.user_commune_access ou metadata Auth legacy).
"""

from fastapi import APIRouter, Depends

from services.auth.commune_access import assert_authorized_for_commune_slug
from services.auth.current_user import get_current_user_id
from services.auth.pipelines_query import select_pipelines_for_user
from services.history.pipeline_enrichment import enrich_pipelines_for_history

# Client Supabase injecté depuis main.py
supabase = None

router = APIRouter(prefix="/pipelines", tags=["pipelines"])

_MAP_HISTORY_COLUMNS = (
    "slug, centroid, cerfa_data, commune, commune_slug, code_insee, "
    "output_cua, qr_url, created_at, suivi"
)


@router.get("/by_user")
def get_pipelines_by_user(
    limit: int = 15,
    commune_slug: str | None = None,
    user_id: str = Depends(get_current_user_id),
):
    """
    Historique des pipelines visibles pour l'utilisateur (scope commune, pas auteur).
    Option commune_slug : ne retourne que les CUAs de la commune affichée.
    """
    if commune_slug:
        assert_authorized_for_commune_slug(user_id, commune_slug)

    try:
        pipelines = enrich_pipelines_for_history(
            select_pipelines_for_user(
                supabase,
                user_id=user_id,
                limit=limit,
                commune_slug=commune_slug,
                columns="*",
            )
        )
        return {
            "success": True,
            "count": len(pipelines),
            "pipelines": pipelines,
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


@router.get("/map-history")
def get_pipelines_map_history(
    limit: int = 15,
    commune_slug: str | None = None,
    user_id: str = Depends(get_current_user_id),
):
    """
    Variante légère : slug, centroid, cerfa_data pour l'affichage carte.
    """
    if commune_slug:
        assert_authorized_for_commune_slug(user_id, commune_slug)

    try:
        pipelines = enrich_pipelines_for_history(
            select_pipelines_for_user(
                supabase,
                user_id=user_id,
                limit=limit,
                commune_slug=commune_slug,
                columns=_MAP_HISTORY_COLUMNS,
            )
        )
        return {
            "success": True,
            "count": len(pipelines),
            "pipelines": pipelines,
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }
