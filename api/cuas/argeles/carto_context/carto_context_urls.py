# -*- coding: utf-8 -*-
"""URLs publiques « propres » pour les cartes d'identité d'urbanisme (CUA Argelès)."""

from __future__ import annotations

import os
import re

CARTE_CONTEXT_FILENAME = "carte_context.html"


def _public_api_base() -> str:
    explicit = (
        os.getenv("CUA_PUBLIC_BASE_URL")
        or os.getenv("PUBLIC_API_BASE_URL")
        or os.getenv("RENDER_EXTERNAL_URL")
        or os.getenv("BACKEND_URL")
    )
    if explicit:
        return explicit.strip().rstrip("/")
    # Dev local : ne pas pointer vers api.kerelia.fr tant que le code n'y est pas déployé.
    if not (os.getenv("RENDER") or "").strip():
        return "http://localhost:8000"
    return "https://api.kerelia.fr"


def storage_object_path(pipeline_slug: str, filename: str = CARTE_CONTEXT_FILENAME) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]", "", (pipeline_slug or "").strip())
    if not slug:
        raise ValueError("pipeline_slug invalide")
    return f"{slug}/{filename}"


def friendly_carto_url(commune_slug: str, pipeline_slug: str) -> str:
    """
    URL affichée dans le CUA (sans domaine supabase.co).
    Servie par GET /communes/{commune_slug}/carto/{pipeline_slug}.
    """
    commune = (commune_slug or "").strip().lower()
    slug = re.sub(r"[^a-zA-Z0-9_-]", "", (pipeline_slug or "").strip())
    if not commune or not slug:
        raise ValueError("commune_slug et pipeline_slug requis")
    return f"{_public_api_base()}/communes/{commune}/carto/{slug}"
