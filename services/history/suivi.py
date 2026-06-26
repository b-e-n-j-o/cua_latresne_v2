# -*- coding: utf-8 -*-
"""
suivi.py — Endpoints pour le suivi d'avancée des dossiers (pipelines)
---------------------------------------------------------------------
GET  /pipelines/{slug}/suivi  → récupère l'étape de suivi
PATCH /pipelines/{slug}/suivi → met à jour l'étape (1 à 4)
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from services.auth.current_user import get_current_user_id
from services.history.project_management import _assert_can_modify, _fetch_pipeline_by_slug

# Client Supabase injecté depuis main.py
supabase = None

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


class SuiviUpdate(BaseModel):
    suivi: int = Field(..., ge=1, le=4, description="Étape : 1=Dossier reçu, 2=Dossier traité, 3=Validé/corrigé, 4=CUA délivré")


@router.get("/{slug}/suivi")
def get_pipeline_suivi(slug: str, user_id: str = Depends(get_current_user_id)):
    """
    Récupère l'étape de suivi d'un pipeline par son slug.
    """
    try:
        found = _fetch_pipeline_by_slug(slug)
        if not found:
            raise HTTPException(status_code=404, detail=f"Pipeline {slug} introuvable")

        schema, row = found
        _assert_can_modify(row, user_id)
        suivi = row.get("suivi")
        return {
            "success": True,
            "slug": slug,
            "suivi": int(suivi) if suivi is not None else 2,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{slug}/suivi")
def update_pipeline_suivi(
    slug: str,
    body: SuiviUpdate,
    user_id: str = Depends(get_current_user_id),
):
    """
    Met à jour l'étape de suivi d'un pipeline.
    """
    try:
        found = _fetch_pipeline_by_slug(slug)
        if not found:
            raise HTTPException(status_code=404, detail=f"Pipeline {slug} introuvable")

        schema, row = found
        _assert_can_modify(row, user_id)

        response = (
            supabase.schema(schema)
            .table("pipelines")
            .update({"suivi": body.suivi})
            .eq("slug", slug)
            .execute()
        )
        if not response.data:
            raise HTTPException(status_code=404, detail=f"Pipeline {slug} introuvable")
        return {
            "success": True,
            "slug": slug,
            "suivi": body.suivi,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
