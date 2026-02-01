# -*- coding: utf-8 -*-
"""
centroid_history.py — Endpoints dédiés à l'historique des pipelines (centroïdes pour la carte)
---------------------------------------------------------------------------------------------
Récupère les pipelines d'un utilisateur avec centroid (lon/lat) et cerfa_data
pour afficher les pings sur la carte et les infos au clic.
"""

from fastapi import APIRouter

# Client Supabase injecté depuis main.py
supabase = None

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.get("/by_user")
def get_pipelines_by_user(user_id: str, limit: int = 15):
    """
    Récupère les pipelines d'un utilisateur spécifique avec centroid et cerfa_data.
    Utilisé par la carte Latresne pour afficher les pings d'historique.
    """
    try:
        response = (
            supabase
            .schema("latresne")
            .table("pipelines")
            .select("*")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )

        pipelines = response.data or []
        return {
            "success": True,
            "count": len(pipelines),
            "pipelines": pipelines
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.get("/map-history")
def get_pipelines_map_history(user_id: str, limit: int = 15):
    """
    Variante légère : retourne uniquement slug, centroid, cerfa_data pour l'affichage carte.
    Optionnel si on veut optimiser le payload.
    """
    try:
        response = (
            supabase
            .schema("latresne")
            .table("pipelines")
            .select("slug, centroid, cerfa_data, commune, code_insee, output_cua, qr_url, created_at, suivi")
            .eq("user_id", user_id)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )

        pipelines = response.data or []
        return {
            "success": True,
            "count": len(pipelines),
            "pipelines": pipelines
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
