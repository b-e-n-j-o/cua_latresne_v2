# -*- coding: utf-8 -*-
"""Intersections à la demande parcelle / UF × catalogue CUA complet."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from api.communes.latresne.parcelles_geojson import _resolve_table_for_commune
from api.cuas.argeles.carto_context import (
    DEFAULT_CONTEXT_BUFFER_M,
    DEFAULT_DISPLAY_CLIP_M,
    load_carto_catalogue,
    run_carto_context,
)
from api.cuas.argeles.intersections import load_catalogue, run_intersections
from api.cuas.argeles.uf import build_uf

router = APIRouter(prefix="/communes", tags=["parcelles-intersections"])

_CUAS_DIR = Path(__file__).resolve().parent

COMMUNE_CUA_CATALOGUE: dict[str, Path] = {
    "argeles": _CUAS_DIR / "catalogue_cua_argeles.json",
}
COMMUNE_CARTO_CATALOGUE: dict[str, Path] = {
    "argeles": _CUAS_DIR / "catalogue_carto_argeles.json",
}


class ParcelleRefIn(BaseModel):
    section: str
    numero: str


class IntersectionsRequest(BaseModel):
    refs: list[ParcelleRefIn] = Field(..., min_length=1, max_length=20)


class CartoContextRequest(BaseModel):
    refs: list[ParcelleRefIn] = Field(..., min_length=1, max_length=20)
    context_buffer_m: float = Field(default=DEFAULT_CONTEXT_BUFFER_M, ge=0, le=500)
    display_clip_m: float = Field(default=DEFAULT_DISPLAY_CLIP_M, ge=50, le=2000)


@router.post("/{commune_slug}/parcelles/intersections")
def compute_parcelles_intersections(commune_slug: str, body: IntersectionsRequest):
    """
    Calcule les intersections géométriques entre une parcelle (ou UF contiguë)
    et toutes les couches du catalogue CUA de la commune.
    """
    slug = (commune_slug or "").strip().lower()
    catalogue_path = COMMUNE_CUA_CATALOGUE.get(slug)
    if not catalogue_path or not catalogue_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Catalogue intersections indisponible pour {commune_slug}",
        )

    schema_name, _table_name = _resolve_table_for_commune(slug)
    catalogue = load_catalogue(str(catalogue_path))
    refs = [{"section": r.section.strip(), "numero": r.numero.strip()} for r in body.refs]

    try:
        uf = build_uf(refs, schema=schema_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    rapport = run_intersections(uf, catalogue, schema=schema_name)
    rapport["commune_slug"] = slug
    rapport["computed_at"] = datetime.now(timezone.utc).isoformat()
    rapport["n_couches"] = len(catalogue)
    rapport["n_couches_concernees"] = sum(
        1 for layer in rapport.get("intersections", {}).values() if layer.get("objets")
    )
    return rapport


@router.post("/{commune_slug}/parcelles/carto-context")
def get_parcelles_carto_context(commune_slug: str, body: CartoContextRequest):
    """
    GeoJSON zone d'étude : géométries complètes des entités intersectant
    le buffer (défaut 200 m), avec indicateur intersects_parcel pour filtrage client.
    """
    slug = (commune_slug or "").strip().lower()
    catalogue_path = COMMUNE_CUA_CATALOGUE.get(slug)
    if not catalogue_path or not catalogue_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Catalogue carto-context indisponible pour {commune_slug}",
        )

    schema_name, _table_name = _resolve_table_for_commune(slug)
    catalogue = load_catalogue(str(catalogue_path))
    carto_path = COMMUNE_CARTO_CATALOGUE.get(slug)
    carto_catalogue = load_carto_catalogue(carto_path) if carto_path and carto_path.exists() else None
    refs = [{"section": r.section.strip(), "numero": r.numero.strip()} for r in body.refs]

    try:
        uf = build_uf(refs, schema=schema_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    payload = run_carto_context(
        uf,
        catalogue,
        carto_catalogue,
        context_buffer_m=body.context_buffer_m,
        display_clip_m=body.display_clip_m,
        schema=schema_name,
    )
    payload["commune_slug"] = slug
    payload["parcelles"] = refs
    return payload
