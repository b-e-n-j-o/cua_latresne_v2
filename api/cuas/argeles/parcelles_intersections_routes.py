# -*- coding: utf-8 -*-
"""Intersections à la demande parcelle / UF × catalogue CUA complet."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import os
import re

import requests
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

from api.communes.latresne.parcelles_geojson import _resolve_table_for_commune
from api.cuas.argeles.carto_context import (
    CARTE_CONTEXT_FILENAME,
    DEFAULT_CONTEXT_BUFFER_M,
    DEFAULT_DISPLAY_CLIP_M,
    load_carto_catalogue,
    run_carto_context,
    storage_object_path,
)
from api.cuas.argeles.db import PIPELINES_SCHEMA, SUPABASE_BUCKET, get_supabase
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
    context_buffer_m: float = Field(default=DEFAULT_CONTEXT_BUFFER_M, ge=0, le=200)
    display_clip_m: float = Field(default=DEFAULT_DISPLAY_CLIP_M, ge=50, le=200)


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


_PIPELINE_SLUG_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")


def _resolve_carto_context_filename(pipeline_slug: str) -> str:
    """Nom du HTML gelé : metadata pipeline, listing storage ou fallback historique."""
    try:
        sb = get_supabase()
        row = (
            sb.schema(PIPELINES_SCHEMA)
            .table("pipelines")
            .select("metadata")
            .eq("slug", pipeline_slug)
            .maybe_single()
            .execute()
        )
        meta = (row.data or {}).get("metadata") or {}
        filename = (meta.get("carte_context_filename") or "").strip()
        if filename:
            return filename
    except Exception:
        pass

    try:
        sb = get_supabase()
        entries = sb.storage.from_(SUPABASE_BUCKET).list(pipeline_slug) or []
        for entry in entries:
            name = (entry.get("name") or "").strip()
            if name.startswith("carte_context") and name.endswith(".html"):
                return name
    except Exception:
        pass

    return CARTE_CONTEXT_FILENAME


@router.get("/{commune_slug}/carto/{pipeline_slug}")
def serve_carto_context_html(commune_slug: str, pipeline_slug: str):
    """
    Proxy public : sert la carte HTML gelée depuis Supabase Storage
    sous une URL propre (sans domaine supabase.co dans le CUA).
    """
    slug = (commune_slug or "").strip().lower()
    if slug not in COMMUNE_CUA_CATALOGUE:
        raise HTTPException(status_code=404, detail="Commune introuvable")

    pid = (pipeline_slug or "").strip()
    if not _PIPELINE_SLUG_RE.match(pid):
        raise HTTPException(status_code=404, detail="Carte introuvable")

    supabase_url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    if not supabase_url:
        raise HTTPException(status_code=502, detail="Stockage indisponible")

    remote = storage_object_path(pid, _resolve_carto_context_filename(pid))
    src = f"{supabase_url}/storage/v1/object/public/{SUPABASE_BUCKET}/{remote}"

    try:
        r = requests.get(src, timeout=90)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Stockage indisponible : {exc}") from exc

    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="Carte introuvable")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Erreur stockage ({r.status_code})")

    return Response(
        content=r.content,
        media_type="text/html; charset=utf-8",
        headers={
            "Cache-Control": "public, max-age=300",
            "X-Content-Type-Options": "nosniff",
        },
    )
