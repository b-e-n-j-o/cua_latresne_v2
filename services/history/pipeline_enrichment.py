# -*- coding: utf-8 -*-
"""Enrichissement des pipelines (centroïde manquant pour CUA v2 / historique)."""

from __future__ import annotations

import json
import logging
from typing import Any

from api.cuas.argeles.geo_utils import compute_centroid_from_wkt_l93

logger = logging.getLogger("pipeline_enrichment")

INSEE_TO_CADASTRE_SCHEMA: dict[str, str] = {
    "33234": "latresne",
    "66008": "argeles",
    "33531": "mios",
}

SLUG_TO_CADASTRE_SCHEMA: dict[str, str] = {
    "latresne": "latresne",
    "argeles": "argeles",
    "mios": "mios",
}


def _normalize_centroid(raw: Any) -> dict[str, float] | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if isinstance(raw, dict):
        lon = raw.get("lon")
        lat = raw.get("lat")
        if lon is not None and lat is not None:
            try:
                return {"lon": float(lon), "lat": float(lat)}
            except (TypeError, ValueError):
                return None
    return None


def _parcelle_refs(pipeline: dict[str, Any]) -> list[dict[str, str]]:
    parcelles = pipeline.get("parcelles")
    if isinstance(parcelles, str):
        try:
            parcelles = json.loads(parcelles)
        except json.JSONDecodeError:
            parcelles = None

    if not parcelles:
        cerfa = pipeline.get("cerfa_data")
        if isinstance(cerfa, str):
            try:
                cerfa = json.loads(cerfa)
            except json.JSONDecodeError:
                cerfa = None
        if isinstance(cerfa, dict):
            parcelles = cerfa.get("parcelles")

    if not isinstance(parcelles, list):
        return []

    refs: list[dict[str, str]] = []
    for item in parcelles:
        if not isinstance(item, dict):
            continue
        section = str(item.get("section") or "").strip()
        numero = str(item.get("numero") or "").strip()
        if section and numero:
            refs.append({"section": section, "numero": numero})
    return refs


def _resolve_cadastre_schema(pipeline: dict[str, Any]) -> str | None:
    code_insee = str(pipeline.get("code_insee") or "").strip()
    if code_insee in INSEE_TO_CADASTRE_SCHEMA:
        return INSEE_TO_CADASTRE_SCHEMA[code_insee]

    slug = str(pipeline.get("commune_slug") or pipeline.get("commune") or "").strip().lower()
    return SLUG_TO_CADASTRE_SCHEMA.get(slug)


def enrich_pipeline_centroid(pipeline: dict[str, Any]) -> dict[str, Any]:
    """Complète pipeline.centroid depuis parcelles si absent (CUA v2 Argelès, etc.)."""
    enriched = dict(pipeline)
    existing = _normalize_centroid(enriched.get("centroid"))
    if existing:
        enriched["centroid"] = existing
        return enriched

    refs = _parcelle_refs(enriched)
    schema = _resolve_cadastre_schema(enriched)
    if not refs or not schema:
        return enriched

    try:
        from api.cuas.argeles.uf import build_uf

        uf = build_uf(refs, schema=schema)
        centroid = compute_centroid_from_wkt_l93(uf.wkt)
        if centroid:
            enriched["centroid"] = centroid
            logger.info(
                "Centroïde enrichi pour pipeline %s (%s)",
                enriched.get("slug"),
                schema,
            )
    except Exception as exc:
        logger.warning(
            "Centroïde non enrichi pour %s : %s",
            enriched.get("slug"),
            exc,
        )

    return enriched


def enrich_pipelines_centroids(pipelines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [enrich_pipeline_centroid(p) for p in pipelines]


def _email_local_part(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    local = email.split("@", 1)[0].strip()
    return local or None


def _resolve_creator_labels(user_ids: set[str]) -> dict[str, str]:
    """Préfixe email (avant @) pour chaque user_id créateur de pipeline."""
    if not user_ids:
        return {}

    try:
        from services.auth.commune_access import _get_supabase

        sb = _get_supabase()
    except Exception as exc:
        logger.warning("Impossible de résoudre les créateurs pipeline : %s", exc)
        return {}

    labels: dict[str, str] = {}
    for uid in user_ids:
        try:
            resp = sb.auth.admin.get_user_by_id(uid)
            email = getattr(resp.user, "email", None) if resp and resp.user else None
            local = _email_local_part(email)
            if local:
                labels[uid] = local
        except Exception as exc:
            logger.debug("Email introuvable pour user %s : %s", uid, exc)
    return labels


def enrich_pipeline_creator_label(pipeline: dict[str, Any], labels: dict[str, str]) -> dict[str, Any]:
    enriched = dict(pipeline)
    uid = str(enriched.get("user_id") or "").strip()
    if uid and uid in labels:
        enriched["creator_label"] = labels[uid]
    return enriched


def enrich_pipelines_creator_labels(pipelines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    user_ids = {str(p.get("user_id")).strip() for p in pipelines if p.get("user_id")}
    labels = _resolve_creator_labels(user_ids)
    return [enrich_pipeline_creator_label(p, labels) for p in pipelines]


def enrich_pipelines_for_history(pipelines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Centroïde + libellé créateur (préfixe email) pour l'historique carte / sidebar."""
    return enrich_pipelines_creator_labels(enrich_pipelines_centroids(pipelines))
