# -*- coding: utf-8 -*-
"""
project_management.py — Gestion CRUD légère des projets d'historique
--------------------------------------------------------------------
PATCH  /pipelines/{slug}  -> met à jour des champs éditables (cerfa_data)
DELETE /pipelines/{slug}  -> supprime un projet de l'historique
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

supabase = None

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


class AdresseTerrainUpdate(BaseModel):
    numero: Optional[str] = None
    voie: Optional[str] = None
    code_postal: Optional[str] = None
    ville: Optional[str] = None


class CerfaDataUpdate(BaseModel):
    demandeur: Optional[str] = None
    numero_cu: Optional[str] = None
    adresse_terrain: Optional[AdresseTerrainUpdate] = None


class PipelineUpdateBody(BaseModel):
    cerfa_data: CerfaDataUpdate


@router.patch("/{slug}")
def update_pipeline_fields(slug: str, body: PipelineUpdateBody):
    try:
        existing = (
            supabase.schema("latresne")
            .table("pipelines")
            .select("slug, cerfa_data")
            .eq("slug", slug)
            .limit(1)
            .execute()
        )
        rows = existing.data or []
        if not rows:
            raise HTTPException(status_code=404, detail=f"Pipeline {slug} introuvable")

        current = rows[0].get("cerfa_data") or {}
        incoming = body.cerfa_data.model_dump(exclude_unset=True)
        incoming_addr = incoming.get("adresse_terrain") or {}
        current_addr = current.get("adresse_terrain") or {}

        merged_cerfa = {
            **current,
            **incoming,
            "adresse_terrain": {
                **current_addr,
                **incoming_addr,
            },
        }

        response = (
            supabase.schema("latresne")
            .table("pipelines")
            .update({"cerfa_data": merged_cerfa})
            .eq("slug", slug)
            .execute()
        )
        if not response.data:
            raise HTTPException(status_code=404, detail=f"Pipeline {slug} introuvable")

        return {"success": True, "slug": slug, "pipeline": response.data[0]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{slug}")
def delete_pipeline(slug: str):
    try:
        existing = (
            supabase.schema("latresne")
            .table("pipelines")
            .select("slug")
            .eq("slug", slug)
            .limit(1)
            .execute()
        )
        rows = existing.data or []
        if not rows:
            raise HTTPException(status_code=404, detail=f"Pipeline {slug} introuvable")

        response = (
            supabase.schema("latresne")
            .table("pipelines")
            .delete()
            .eq("slug", slug)
            .execute()
        )

        return {
            "success": True,
            "slug": slug,
            "deleted_count": len(response.data or []),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

