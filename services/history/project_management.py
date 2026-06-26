# -*- coding: utf-8 -*-
"""
project_management.py — Gestion CRUD légère des projets d'historique
--------------------------------------------------------------------
PATCH  /pipelines/{slug}  -> met à jour des champs éditables (cerfa_data)
DELETE /pipelines/{slug}  -> supprime un projet de l'historique
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.auth.commune_access import (
    COMMUNE_REGISTRY,
    assert_authorized_for_commune_slug,
    assert_authorized_for_insee,
    assert_pipeline_visible_to_viewer,
    get_authorized_insee_codes,
    is_pipeline_visible_to_viewer,
)
from services.auth.current_user import get_current_user_id
from services.auth.pipelines_query import pipelines_schema

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


def _pipeline_schemas() -> list[str]:
    primary = pipelines_schema()
    schemas = [primary]
    if primary != "latresne":
        schemas.append("latresne")
    return schemas


def _fetch_pipeline_by_slug(slug: str) -> tuple[str, dict[str, Any]] | None:
    for schema in _pipeline_schemas():
        response = (
            supabase.schema(schema)
            .table("pipelines")
            .select("*")
            .eq("slug", slug)
            .limit(1)
            .execute()
        )
        rows = response.data or []
        if rows:
            return schema, rows[0]
    return None


def assert_can_view_pipeline(slug: str, user_id: str) -> dict[str, Any]:
    """
    Charge une pipeline et vérifie droits commune + visibilité superadmin.
    Répond 404 en cas de refus (ne révèle pas l'existence du projet).
    """
    found = _fetch_pipeline_by_slug((slug or "").strip())
    if not found:
        raise HTTPException(status_code=404, detail="Projet introuvable")

    _schema, pipeline = found

    if not is_pipeline_visible_to_viewer(pipeline, user_id):
        raise HTTPException(status_code=404, detail="Projet introuvable")

    allowed = get_authorized_insee_codes(user_id)
    if allowed is not None:
        if not allowed:
            raise HTTPException(status_code=404, detail="Projet introuvable")

        code_insee = (pipeline.get("code_insee") or "").strip()
        if code_insee:
            if code_insee not in allowed:
                raise HTTPException(status_code=404, detail="Projet introuvable")
        else:
            commune_slug = (pipeline.get("commune_slug") or pipeline.get("commune") or "").strip().lower()
            meta = COMMUNE_REGISTRY.get(commune_slug)
            pipeline_insee = meta["code_insee"] if meta else ""
            if not pipeline_insee or pipeline_insee not in allowed:
                raise HTTPException(status_code=404, detail="Projet introuvable")

    return pipeline


def _assert_can_modify(row: dict[str, Any], user_id: str | None) -> None:
    """Autorise si le user a accès à la commune de la pipeline (pas seulement s'il l'a créée)."""
    if not user_id:
        raise HTTPException(status_code=401, detail="user_id requis")

    assert_pipeline_visible_to_viewer(row, user_id)

    code_insee = row.get("code_insee") or ""
    if code_insee:
        assert_authorized_for_insee(user_id, str(code_insee))
        return

    commune_slug = (row.get("commune_slug") or row.get("commune") or "").strip().lower()
    if commune_slug:
        assert_authorized_for_commune_slug(user_id, commune_slug)
        return

    raise HTTPException(
        status_code=403,
        detail="Accès refusé : commune de la pipeline non identifiable.",
    )


def _delete_project_artifacts(slug: str) -> None:
    """Nettoie project_files / project_directories (schéma latresne, transition)."""
    try:
        supabase.schema("latresne").table("project_files").delete().eq("project_slug", slug).execute()
        supabase.schema("latresne").table("project_directories").delete().eq("project_slug", slug).execute()
    except Exception:
        pass


@router.patch("/{slug}")
def update_pipeline_fields(
    slug: str,
    body: PipelineUpdateBody,
    user_id: str = Depends(get_current_user_id),
):
    try:
        found = _fetch_pipeline_by_slug(slug)
        if not found:
            raise HTTPException(status_code=404, detail=f"Pipeline {slug} introuvable")

        schema, row = found
        _assert_can_modify(row, user_id)

        current = row.get("cerfa_data") or {}
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
            supabase.schema(schema)
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
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete("/{slug}")
def delete_pipeline(slug: str, user_id: str = Depends(get_current_user_id)):
    try:
        found = _fetch_pipeline_by_slug(slug)
        if not found:
            raise HTTPException(status_code=404, detail=f"Pipeline {slug} introuvable")

        schema, row = found
        _assert_can_modify(row, user_id)

        response = (
            supabase.schema(schema)
            .table("pipelines")
            .delete()
            .eq("slug", slug)
            .execute()
        )

        _delete_project_artifacts(slug)

        return {
            "success": True,
            "slug": slug,
            "schema": schema,
            "deleted_count": len(response.data or []),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
