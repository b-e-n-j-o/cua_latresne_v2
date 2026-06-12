# -*- coding: utf-8 -*-
"""Exposition lecture seule des catalogues CUA par commune."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/communes", tags=["cua-catalogue"])

_CUAS_DIR = Path(__file__).resolve().parent
CATALOGUE_FILES = {
    "argeles": _CUAS_DIR / "catalogue_cua_argeles.json",
}
CARTO_CATALOGUE_FILES = {
    "argeles": _CUAS_DIR / "catalogue_carto_argeles.json",
}


@router.get("/{commune_slug}/cua/catalogue")
def get_cua_catalogue(commune_slug: str):
    path = CATALOGUE_FILES.get(commune_slug)
    if not path or not path.exists():
        raise HTTPException(status_code=404, detail="Catalogue CUA introuvable pour cette commune")
    return json.loads(path.read_text(encoding="utf-8"))


@router.get("/{commune_slug}/carto/catalogue")
def get_carto_catalogue(commune_slug: str):
    slug = (commune_slug or "").strip().lower()
    path = CARTO_CATALOGUE_FILES.get(slug)
    if not path or not path.exists():
        raise HTTPException(status_code=404, detail="Catalogue carto introuvable pour cette commune")
    return json.loads(path.read_text(encoding="utf-8"))
