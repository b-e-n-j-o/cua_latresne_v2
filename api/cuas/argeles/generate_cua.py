# -*- coding: utf-8 -*-
"""
generate_cua.py — Pipeline métier CUA Argelès (intersections + carto HTML + builder DOCX).

Entrée : liste de références parcellaires (+ dossier optionnel).
Sortie : rapport JSON, carte HTML gelée, DOCX, upload Supabase + enregistrement pipeline.
"""

from __future__ import annotations

import base64
import json
import os
import re
import secrets
import string
import tempfile
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from api.communes.latresne.parcelles_geojson import _resolve_table_for_commune
from api.cuas.argeles.builder import CommuneConfig, build_cua
from api.cuas.argeles.carto_context import (
    DEFAULT_CONTEXT_BUFFER_M,
    DEFAULT_DISPLAY_CLIP_M,
    friendly_carto_url,
    load_carto_catalogue,
    render_carto_context_html,
    run_carto_context,
    storage_object_path,
)
from api.cuas.argeles.db import SUPABASE_BUCKET, get_supabase, logger, persist_cua, upload_file
from api.cuas.argeles.intersections import load_catalogue, run_intersections
from api.cuas.argeles.uf import build_uf
from services.history.project_directory import ensure_project_directory, register_project_file

_CUAS_DIR = Path(__file__).resolve().parent

# Encart identité par défaut (surchargeable via API).
DEFAULT_DOSSIER: dict[str, Any] = {
    "demandeur": "M. Dupont",
    "demandeur_adresse": "12 rue Example, 66700 Argelès-sur-Mer",
    "terrain": "Argelès-sur-Mer",
}

COMMUNE_CUA_CATALOGUE: dict[str, Path] = {
    "argeles": _CUAS_DIR / "catalogue_cua_argeles.json",
}

COMMUNE_CARTO_CATALOGUE: dict[str, Path] = {
    "argeles": _CUAS_DIR / "catalogue_carto_argeles.json",
}

COMMUNE_BUILDER_CONFIG: dict[str, CommuneConfig] = {
    "argeles": CommuneConfig(),
}

COMMUNE_META: dict[str, dict[str, str]] = {
    "argeles": {
        "commune": "argeles",
        "code_insee": "66008",
        "nom": "Argelès-sur-Mer",
    },
}


def _generate_slug(length: int = 26) -> str:
    alphabet = "".join(ch for ch in string.ascii_letters + string.digits if ch not in "O0Il")
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _cua_viewer_url(remote_docx_path: str) -> str:
    payload = {"docx": remote_docx_path}
    token = base64.b64encode(json.dumps(payload).encode()).decode()
    return f"https://kerelia.fr/cua?t={token}"


def _today_fr() -> str:
    return datetime.now().strftime("%d/%m/%Y")


def _merge_dossier(dossier: Optional[dict]) -> dict:
    merged = dict(DEFAULT_DOSSIER)
    if dossier:
        for key, value in dossier.items():
            if value is not None and str(value).strip():
                merged[key] = value
    numero_cu = str(merged.get("numero_cu") or "").strip()
    if not numero_cu:
        raise ValueError(
            "Référence du dossier requise (numero_cu) pour générer le certificat d'urbanisme."
        )
    merged["numero_cu"] = numero_cu
    if not str(merged.get("date_depot") or "").strip():
        merged["date_depot"] = _today_fr()
    return merged


def _sanitize_dossier_ref_for_filename(ref: str, max_len: int = 48) -> str:
    s = unicodedata.normalize("NFKD", (ref or "").strip())
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^\w.-]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-._")
    if not s:
        raise ValueError("Référence du dossier invalide pour nom de fichier.")
    return s[:max_len]


def _cua_docx_filename(numero_cu: str) -> str:
    safe_ref = _sanitize_dossier_ref_for_filename(numero_cu)
    return f"CUA_{safe_ref}.docx"


def _carte_context_filename(numero_cu: str) -> str:
    safe_ref = _sanitize_dossier_ref_for_filename(numero_cu)
    return f"carte_context_{safe_ref}.html"


def generate_cua_for_parcelles(
    refs: list[dict[str, str]],
    *,
    commune_slug: str,
    dossier: Optional[dict] = None,
    persist: bool = True,
    user_id: Optional[str] = None,
    user_email: Optional[str] = None,
) -> dict[str, Any]:
    """
    Construit l'UF, calcule les intersections, génère le DOCX et persiste le pipeline.

    refs : [{"section": "BR", "numero": "273"}, ...]
    """
    slug = (commune_slug or "").strip().lower()
    catalogue_path = COMMUNE_CUA_CATALOGUE.get(slug)
    if not catalogue_path or not catalogue_path.exists():
        raise ValueError(f"Commune non prise en charge pour la génération CUA : {commune_slug}")

    meta = COMMUNE_META.get(slug, {})
    schema_name, _table = _resolve_table_for_commune(slug)
    catalogue = load_catalogue(str(catalogue_path))
    normalized_refs = [
        {"section": r["section"].strip(), "numero": r["numero"].strip()}
        for r in refs
    ]

    uf = build_uf(normalized_refs, schema=schema_name)
    logger.info(
        f"CUA {slug} — {uf.n_parcelles} parcelle(s) | SIG {uf.surface_sig:.2f} m²"
    )

    rapport = run_intersections(uf, catalogue, schema=schema_name)
    rapport["commune_slug"] = slug
    rapport["computed_at"] = datetime.now(timezone.utc).isoformat()
    rapport["n_couches"] = len(catalogue)
    rapport["n_couches_concernees"] = sum(
        1 for layer in rapport.get("intersections", {}).values() if layer.get("objets")
    )

    carto_path = COMMUNE_CARTO_CATALOGUE.get(slug)
    carto_catalogue = (
        load_carto_catalogue(carto_path) if carto_path and carto_path.exists() else None
    )
    carto_payload = run_carto_context(
        uf,
        catalogue,
        carto_catalogue,
        context_buffer_m=DEFAULT_CONTEXT_BUFFER_M,
        display_clip_m=DEFAULT_DISPLAY_CLIP_M,
        schema=schema_name,
    )
    carto_payload["commune_slug"] = slug
    carto_payload["parcelles"] = normalized_refs

    dossier_merged = _merge_dossier(dossier)
    builder_config = COMMUNE_BUILDER_CONFIG.get(slug, CommuneConfig())
    pipeline_slug = _generate_slug()

    carte_context_url: str | None = None
    carte_storage_url: str | None = None

    with tempfile.TemporaryDirectory(prefix="cua_") as tmp:
        tmp_path = Path(tmp)
        numero_cu = dossier_merged["numero_cu"]
        carte_filename = _carte_context_filename(numero_cu)
        docx_filename = _cua_docx_filename(numero_cu)
        html_carto = render_carto_context_html(
            carto_payload,
            commune_nom=meta.get("nom", slug),
            numero_cu=numero_cu,
            carto_catalogue=carto_catalogue,
        )
        html_path = tmp_path / carte_filename
        html_path.write_text(html_carto, encoding="utf-8")
        logger.info(f"Carte contexte HTML ({html_path.stat().st_size} octets)")

        if persist:
            remote_html = storage_object_path(pipeline_slug, carte_filename)
            carte_storage_url = upload_file(
                str(html_path),
                remote_html,
                content_type="text/html; charset=utf-8",
            )
            carte_context_url = friendly_carto_url(slug, pipeline_slug)
            logger.info(f"Carte contexte : {carte_context_url}")

        dossier_merged["carte_context_url"] = carte_context_url
        rapport["carte_context_url"] = carte_context_url

        docx_path = tmp_path / docx_filename
        build_cua(dossier_merged, rapport, str(docx_path), config=builder_config)
        logger.info(f"DOCX CUA généré ({docx_path.stat().st_size} octets)")

        result: dict[str, Any] = {
            "slug": pipeline_slug,
            "commune_slug": slug,
            "commune": meta.get("commune", slug),
            "code_insee": meta.get("code_insee"),
            "parcelles": rapport.get("parcelles", []),
            "n_parcelles": rapport.get("n_parcelles", uf.n_parcelles),
            "surface_m2": rapport.get("surface_m2"),
            "surface_indicative": rapport.get("surface_indicative"),
            "n_couches": rapport["n_couches"],
            "n_couches_concernees": rapport["n_couches_concernees"],
            "computed_at": rapport["computed_at"],
            "dossier": dossier_merged,
            "rapport": rapport,
            "carte_context_url": carte_context_url,
            "carto_context": carto_payload,
        }

        if persist:
            remote_docx = f"{pipeline_slug}/{docx_filename}"
            persisted = persist_cua(
                slug=pipeline_slug,
                docx_path=str(docx_path),
                docx_remote_path=remote_docx,
                refs=rapport.get("parcelles", []),
                surface_cad=rapport.get("surface_indicative") or uf.surface_cadastrale,
                commune=meta.get("commune", slug),
                code_insee=meta.get("code_insee", ""),
                user_id=user_id,
                user_email=user_email,
                wkt=uf.wkt,
                carte_context_url=carte_context_url,
                extra={
                    "n_couches_concernees": rapport["n_couches_concernees"],
                    "dossier": dossier_merged,
                    "carte_context_storage_url": carte_storage_url,
                    "carte_context_filename": carte_filename,
                    "cua_docx_filename": docx_filename,
                },
            )
            result["output_cua"] = persisted["cua_url"]
            result["cua_viewer_url"] = _cua_viewer_url(remote_docx)
            result["bucket_path"] = pipeline_slug

            try:
                sb = get_supabase()
                ensure_project_directory(
                    sb,
                    pipeline_slug,
                    user_id=user_id,
                    created_by=user_id,
                )
                register_project_file(
                    sb,
                    slug=pipeline_slug,
                    file_kind="cua_docx",
                    filename=docx_filename,
                    storage_path=remote_docx,
                    public_url=persisted["cua_url"],
                    storage_bucket=SUPABASE_BUCKET,
                    mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    size_bytes=docx_path.stat().st_size,
                    uploaded_by=user_id,
                    source="cua_generate_v2",
                )
                if carte_context_url and carte_storage_url:
                    register_project_file(
                        sb,
                        slug=pipeline_slug,
                        file_kind="carte_context_html",
                        filename=carte_filename,
                        storage_path=storage_object_path(pipeline_slug, carte_filename),
                        public_url=carte_context_url,
                        storage_bucket=SUPABASE_BUCKET,
                        mime_type="text/html; charset=utf-8",
                        size_bytes=html_path.stat().st_size,
                        uploaded_by=user_id,
                        source="cua_generate_v2",
                    )
            except Exception as exc:
                logger.warning(f"ProjectFile non enregistré pour {pipeline_slug} : {exc}")

    return result
