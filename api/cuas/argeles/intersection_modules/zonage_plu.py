# -*- coding: utf-8 -*-
"""
Module métier dédié : zonage PLU.

À partir des objets intersectés (couche zonage_plu), produit l'intro
réglementaire et les blocs à afficher dans le CUA (seuil de significativité,
dédoublonnage des réglementations, libellés complémentaires).

Pour les UF multi-parcelles, calcule en plus un détail zonage par parcelle.
"""

from __future__ import annotations

from typing import Any, Optional

try:
    from api.cuas.argeles.db import SCHEMA, get_engine
    from api.cuas.argeles.intersection_modules.parcelles_geom import (
        fetch_parcelles_geom,
        format_parcelle_ref,
        intersect_couche_parcelle,
    )
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from db import SCHEMA, get_engine
    from intersection_modules.parcelles_geom import (
        fetch_parcelles_geom,
        format_parcelle_ref,
        intersect_couche_parcelle,
    )

NOM = "Zonage PLU"
MIN_ZONAGE_PCT = 1.0
ZONAGE_TABLE = "zonage_plu"


def _pct_sig(obj: dict) -> float:
    try:
        return float(obj.get("pct_sig") or 0)
    except (TypeError, ValueError):
        return 0.0


def _label_obj(obj: dict, *keys: str) -> str:
    for key in keys:
        val = (obj.get(key) or "").strip()
        if val:
            return val
    return ""


def _reglementation_text(obj: dict) -> Optional[str]:
    regl = obj.get("reglementation")
    if regl and str(regl).strip() and str(regl).strip() != "\\N":
        return str(regl).strip()
    return None


def _texte_objet_fallback(obj: dict) -> Optional[str]:
    fallback = (
        obj.get("libelle")
        or obj.get("libelong")
        or obj.get("legende")
        or obj.get("zonage_reglement")
    )
    return str(fallback).strip() if fallback else None


def _items_avec_pct(
    objets: list,
    min_zonage_pct: float,
    *label_keys: str,
) -> list[tuple[str, float]]:
    """Libellés distincts avec part UF maximale (> seuil)."""
    seen: dict[str, float] = {}
    for obj in objets:
        pct = _pct_sig(obj)
        if pct <= min_zonage_pct:
            continue
        label = _label_obj(obj, *label_keys)
        if not label:
            continue
        seen[label] = max(seen.get(label, 0.0), pct)
    return sorted(seen.items(), key=lambda item: -item[1])


def _zones_agregees(
    objets: list,
    min_zonage_pct: float,
) -> list[tuple[str, float]]:
    """Zones distinctes avec % cumulé (plusieurs entités SIG même zone)."""
    by_zone: dict[str, float] = {}
    for obj in objets:
        zone = _label_obj(obj, "libelle", "zonage_reglement")
        if not zone:
            continue
        by_zone[zone] = by_zone.get(zone, 0.0) + _pct_sig(obj)

    items = [
        (zone, pct)
        for zone, pct in sorted(by_zone.items(), key=lambda item: -item[1])
        if pct > min_zonage_pct
    ]
    if items:
        return items
    return sorted(by_zone.items(), key=lambda item: -item[1])


def _zones_plu_avec_pct(objets: list, min_zonage_pct: float) -> list[tuple[str, float]]:
    return _items_avec_pct(objets, min_zonage_pct, "libelle", "zonage_reglement")


def _format_zones_parts(zones_pct: list[tuple[str, float]]) -> str:
    return ", ".join(f"zone {zone} ({pct:.2f} %)" for zone, pct in zones_pct)


def _format_intro(objets: list, min_zonage_pct: float) -> tuple[list[str], Optional[str]]:
    """Résumé zonage + parts de surface significatives (niveau UF)."""
    items = _zones_plu_avec_pct(objets, min_zonage_pct)
    if items:
        zones = [zone for zone, _ in items]
        if len(items) == 1:
            zone, pct = items[0]
            texte = (
                f"L'unité foncière est située dans la zone {zone} du PLU "
                f"({pct:.2f} % de la surface)."
            )
        else:
            parts = [f"{zone} ({pct:.2f} %)" for zone, pct in items]
            texte = f"L'unité foncière est située dans les zones {', '.join(parts)} du PLU."
        return zones, texte

    zones, seen = [], set()
    for obj in objets:
        zone = _label_obj(obj, "libelle", "zonage_reglement")
        if zone and zone not in seen:
            seen.add(zone)
            zones.append(zone)
    if not zones:
        return [], None
    return zones, f"L'unité foncière est située dans la zone {', '.join(zones)} du PLU."


def _objets_significatifs(objets: list, min_zonage_pct: float) -> list:
    return [obj for obj in objets if _pct_sig(obj) > min_zonage_pct]


def _build_items(
    objets: list,
    zones: list,
    min_zonage_pct: float,
) -> list[dict[str, Any]]:
    """Blocs ordonnés pour le rendu DOCX (réglementation + puces complémentaires)."""
    items: list[dict[str, Any]] = []
    seen_regl: set[str] = set()
    multi_zones = len(zones) > 1

    for obj in _objets_significatifs(objets, min_zonage_pct):
        zone_code = _label_obj(obj, "libelle", "zonage_reglement")
        libelong = (obj.get("libelong") or "").strip()
        regl = _reglementation_text(obj)
        pct = _pct_sig(obj)

        if regl:
            if regl in seen_regl:
                continue
            seen_regl.add(regl)
            item: dict[str, Any] = {
                "kind": "reglementation",
                "zone": zone_code or None,
                "reglementation": regl,
            }
            if multi_zones and zone_code:
                suffix = f" ({pct:.2f} %)" if pct > min_zonage_pct else ""
                item["titre"] = f"Zone {zone_code}{suffix}"
            items.append(item)
            continue

        if libelong and libelong not in zones:
            items.append({"kind": "bullet", "texte": libelong})
            continue

        txt = _texte_objet_fallback(obj)
        if txt and txt not in zones and txt != libelong and txt != zone_code:
            items.append({"kind": "bullet", "texte": txt})

    return items


def _build_detail_parcelles(
    parcelles: list[dict],
    zonage_cfg: dict | None,
    engine,
    schema: str,
    min_zonage_pct: float,
) -> list[dict[str, Any]]:
    """Détail zonage par parcelle (UF multi-parcelles uniquement)."""
    if len(parcelles) <= 1 or not zonage_cfg:
        return []

    parcelle_geoms = fetch_parcelles_geom(parcelles, engine, schema)
    if not parcelle_geoms:
        return []

    details: list[dict[str, Any]] = []
    for parcelle in parcelle_geoms:
        surface = parcelle["surface_sig"]
        if surface <= 0:
            continue
        objets = intersect_couche_parcelle(
            parcelle["wkt"],
            surface,
            ZONAGE_TABLE,
            zonage_cfg,
            engine,
            schema,
        )
        zones_pct = _zones_agregees(objets, min_zonage_pct)
        if not zones_pct:
            continue

        section = str(parcelle["section"])
        numero = str(parcelle["numero"])
        ref = format_parcelle_ref(section, numero)
        if len(zones_pct) == 1:
            zone, pct = zones_pct[0]
            texte = f"{ref} : concernée par la zone {zone} ({pct:.2f} % de la surface parcelle)."
        else:
            texte = f"{ref} : concernée par {_format_zones_parts(zones_pct)} de la surface parcelle."

        details.append(
            {
                "section": section,
                "numero": numero,
                "libelle": ref,
                "zones": [
                    {"zone": zone, "pct": round(pct, 2)}
                    for zone, pct in zones_pct
                ],
                "texte": texte,
            }
        )

    return details


def compute_zonage_plu_reglementation(
    *,
    zonage_objets: list[dict] | None = None,
    parcelles: list[dict] | None = None,
    zonage_cfg: dict | None = None,
    engine=None,
    schema: str = SCHEMA,
    min_zonage_pct: float = MIN_ZONAGE_PCT,
) -> dict[str, Any]:
    """
    Synthétise les intersections zonage PLU pour le CUA.

    Retourne intro (UF), détail par parcelle (si UF multi-parcelles),
    zones significatives et items prêts pour le builder.
    """
    objets = list(zonage_objets or [])
    parcelles = list(parcelles or [])

    detail_parcelles: list[dict[str, Any]] = []
    if len(parcelles) > 1 and zonage_cfg:
        detail_parcelles = _build_detail_parcelles(
            parcelles,
            zonage_cfg,
            engine or get_engine(),
            schema,
            min_zonage_pct,
        )

    if not objets:
        return {
            "status": "non_concernee",
            "diagnostic_metier": "RAS : aucun zonage PLU sur l'UF",
            "nom": NOM,
            "intro": None,
            "zones": [],
            "items": [],
            "detail_parcelles": detail_parcelles,
        }

    zones, intro = _format_intro(objets, min_zonage_pct)
    items = _build_items(objets, zones, min_zonage_pct)
    has_content = bool(intro or items or detail_parcelles)

    diag_parts = []
    if detail_parcelles:
        diag_parts.append(f"{len(detail_parcelles)} parcelle(s) détaillée(s)")
    if zones:
        diag_parts.append(f"{len(zones)} zone(s) UF")
    if items:
        diag_parts.append(f"{len(items)} bloc(s) réglementaire(s)")

    return {
        "status": "concernee" if has_content else "non_concernee",
        "diagnostic_metier": (
            " | ".join(diag_parts)
            if diag_parts
            else "RAS : aucun zonage PLU sur l'UF"
        ),
        "nom": NOM,
        "intro": intro,
        "zones": zones,
        "items": items,
        "detail_parcelles": detail_parcelles,
    }
