# -*- coding: utf-8 -*-
"""Génération CUA (intersections + builder DOCX) par commune."""

from __future__ import annotations

import asyncio
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from api.cuas.argeles.generate_cua import COMMUNE_CUA_CATALOGUE, generate_cua_for_parcelles
from services.auth.commune_access import assert_authorized_for_commune_slug

router = APIRouter(prefix="/communes", tags=["cua-generation"])


class ParcelleRefIn(BaseModel):
    section: str
    numero: str


class DossierIn(BaseModel):
    demandeur: Optional[str] = None
    demandeur_adresse: Optional[str] = None
    terrain: Optional[str] = None
    date_depot: Optional[str] = None
    numero_cu: Optional[str] = None
    superficie: Optional[float] = None
    cadastre: Optional[str] = None


class GenerateCuaRequest(BaseModel):
    refs: list[ParcelleRefIn] = Field(..., min_length=1, max_length=20)
    dossier: Optional[DossierIn] = None
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    persist: bool = True


class GenerateCuaResponse(BaseModel):
    success: bool = True
    slug: str
    commune_slug: str
    code_insee: Optional[str] = None
    parcelles: list[dict[str, str]]
    n_parcelles: int
    surface_m2: Optional[float] = None
    surface_indicative: Optional[float] = None
    n_couches: int
    n_couches_concernees: int
    output_cua: Optional[str] = None
    cua_viewer_url: Optional[str] = None
    bucket_path: Optional[str] = None
    computed_at: str


@router.post("/{commune_slug}/cua/generate", response_model=GenerateCuaResponse)
async def generate_cua(commune_slug: str, body: GenerateCuaRequest):
    """
    Génère un certificat d'urbanisme pour une parcelle ou une UF contiguë.

    Pipeline : build_uf → intersections catalogue → builder DOCX → upload Supabase.
    """
    slug = (commune_slug or "").strip().lower()
    if slug not in COMMUNE_CUA_CATALOGUE:
        raise HTTPException(
            status_code=404,
            detail=f"Génération CUA indisponible pour {commune_slug}",
        )

    assert_authorized_for_commune_slug(body.user_id, slug)

    refs = [{"section": r.section.strip(), "numero": r.numero.strip()} for r in body.refs]
    dossier = body.dossier.model_dump(exclude_none=True) if body.dossier else None

    try:
        result = await asyncio.to_thread(
            generate_cua_for_parcelles,
            refs,
            commune_slug=slug,
            dossier=dossier,
            persist=body.persist,
            user_id=body.user_id,
            user_email=body.user_email,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Échec génération CUA : {exc}",
        ) from exc

    return GenerateCuaResponse(
        slug=result["slug"],
        commune_slug=result["commune_slug"],
        code_insee=result.get("code_insee"),
        parcelles=result.get("parcelles", []),
        n_parcelles=result.get("n_parcelles", len(refs)),
        surface_m2=result.get("surface_m2"),
        surface_indicative=result.get("surface_indicative"),
        n_couches=result.get("n_couches", 0),
        n_couches_concernees=result.get("n_couches_concernees", 0),
        output_cua=result.get("output_cua"),
        cua_viewer_url=result.get("cua_viewer_url"),
        bucket_path=result.get("bucket_path"),
        computed_at=result["computed_at"],
    )
