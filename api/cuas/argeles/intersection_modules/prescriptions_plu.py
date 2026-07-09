# -*- coding: utf-8 -*-
"""
Module métier dédié : prescriptions PLU (surfaciques, linéaires, ponctuelles).

Produit les blocs réglementaires au niveau UF (sans pourcentages) et, pour les
UF multi-parcelles, un détail indiquant quelles parcelles sont touchées par
quels libellés de prescription.

Les prescriptions surfaciques ne sont retenues que si leur intersection avec
l'UF (ou la parcelle) dépasse MIN_PRESCRIPTION_SURF_PCT — même logique que le
zonage PLU. Cela écarte les entités simplement frontalières (contact en bordure
avec une aire résiduelle > 0,01 m² mais négligeable en % de l'UF).
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

try:
    from api.modules_communs.intersection_partielle import catalogue_affiche_pct_partiel
except ImportError:
    def catalogue_affiche_pct_partiel(cfg: dict | None) -> bool:
        return bool((cfg or {}).get("afficher_pct_sig_partiel"))


COUCHE_SURF = "prescriptions_surf"
COUCHE_LIN = "prescriptions_lineaires"
COUCHE_PONCT = "prescriptions_ponctuelles"

COUCHES = (
    (COUCHE_SURF, "Prescriptions surfaciques (PLU)"),
    (COUCHE_LIN, "Prescriptions linéaires (PLU)"),
    (COUCHE_PONCT, "Prescriptions ponctuelles (PLU)"),
)

# Part minimale de l'UF (ou parcelle) recouverte pour retenir une prescription surfacique.
MIN_PRESCRIPTION_SURF_PCT = 1.0


def _pct_sig(obj: dict) -> float:
    try:
        return float(obj.get("pct_sig") or 0)
    except (TypeError, ValueError):
        return 0.0


def _filtre_surfaciques_significatifs(
    objets: list[dict],
    min_pct: float = MIN_PRESCRIPTION_SURF_PCT,
) -> list[dict]:
    """Exclut les micro-recouvrements frontaliers (≤ seuil % de la surface)."""
    return [obj for obj in objets if _pct_sig(obj) > min_pct]


def _libelle_obj(obj: dict) -> str:
    return (obj.get("libelle") or "").strip()


def _reglementation_text(obj: dict) -> Optional[str]:
    regl = obj.get("reglementation")
    if regl and str(regl).strip() and str(regl).strip() != "\\N":
        return str(regl).strip()
    return None


def _libelles_distincts(objets: list[dict]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for obj in objets:
        lib = _libelle_obj(obj)
        if not lib:
            continue
        key = lib.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(lib)
    return out


def _build_items(objets: list[dict]) -> list[dict[str, Any]]:
    """Blocs UF : libellé + réglementation, sans pourcentage."""
    items: list[dict[str, Any]] = []
    seen_regl: set[str] = set()
    seen_libelles_sans_regl: set[str] = set()

    for obj in objets:
        libelle = _libelle_obj(obj)
        regl = _reglementation_text(obj)

        if regl:
            if regl in seen_regl:
                continue
            seen_regl.add(regl)
            items.append(
                {
                    "kind": "reglementation",
                    "libelle": libelle or None,
                    "typepsc": (obj.get("typepsc") or "").strip() or None,
                    "reglementation": regl,
                    "pct_sig": _pct_sig(obj),
                }
            )
            continue

        if libelle:
            key = libelle.casefold()
            if key in seen_libelles_sans_regl:
                continue
            seen_libelles_sans_regl.add(key)
            items.append({"kind": "bullet", "texte": libelle, "pct_sig": _pct_sig(obj)})

    return items


def _build_couche(
    key: str,
    nom: str,
    objets: list[dict],
    cfg: dict | None = None,
) -> Optional[dict[str, Any]]:
    items = _build_items(objets)
    if not items:
        return None
    return {
        "key": key,
        "nom": nom,
        "items": items,
        "nb_objets": len(objets),
        "afficher_pct_sig_partiel": catalogue_affiche_pct_partiel(cfg),
    }


def _collect_libelles_parcelle(
    parcelle: dict[str, Any],
    configs: dict[str, dict | None],
    engine,
    schema: str,
    min_surf_pct: float = MIN_PRESCRIPTION_SURF_PCT,
) -> dict[str, list[str]]:
    """Libellés distincts intersectés par couche, pour une parcelle."""
    surface = parcelle["surface_sig"]
    if surface <= 0:
        return {}

    par_couche: dict[str, list[str]] = {}
    for key, cfg in configs.items():
        if not cfg:
            continue
        objets = intersect_couche_parcelle(
            parcelle["wkt"],
            surface,
            key,
            cfg,
            engine,
            schema,
        )
        if key == COUCHE_SURF:
            objets = _filtre_surfaciques_significatifs(objets, min_surf_pct)
        libelles = _libelles_distincts(objets)
        if libelles:
            par_couche[key] = libelles
    return par_couche


def _format_detail_texte(ref: str, par_couche: dict[str, list[str]]) -> str:
    libelles = []
    seen: set[str] = set()
    for key in (COUCHE_SURF, COUCHE_LIN, COUCHE_PONCT):
        for lib in par_couche.get(key, []):
            fold = lib.casefold()
            if fold in seen:
                continue
            seen.add(fold)
            libelles.append(lib)
    if not libelles:
        return ""
    if len(libelles) == 1:
        return f"{ref} : concernée par {libelles[0]}."
    return f"{ref} : concernée par {', '.join(libelles)}."


def _build_detail_parcelles(
    parcelles: list[dict],
    configs: dict[str, dict | None],
    engine,
    schema: str,
    min_surf_pct: float = MIN_PRESCRIPTION_SURF_PCT,
) -> list[dict[str, Any]]:
    if len(parcelles) <= 1:
        return []

    parcelle_geoms = fetch_parcelles_geom(parcelles, engine, schema)
    if not parcelle_geoms:
        return []

    details: list[dict[str, Any]] = []
    for parcelle in parcelle_geoms:
        par_couche = _collect_libelles_parcelle(
            parcelle, configs, engine, schema, min_surf_pct
        )
        if not par_couche:
            continue

        section = str(parcelle["section"])
        numero = str(parcelle["numero"])
        ref = format_parcelle_ref(section, numero)
        texte = _format_detail_texte(ref, par_couche)
        if not texte:
            continue

        libelles = []
        seen: set[str] = set()
        for key in (COUCHE_SURF, COUCHE_LIN, COUCHE_PONCT):
            for lib in par_couche.get(key, []):
                fold = lib.casefold()
                if fold in seen:
                    continue
                seen.add(fold)
                libelles.append(lib)

        details.append(
            {
                "section": section,
                "numero": numero,
                "libelle": ref,
                "libelles": libelles,
                "par_couche": par_couche,
                "texte": texte,
            }
        )

    return details


def compute_prescriptions_plu_reglementation(
    *,
    surf_objets: list[dict] | None = None,
    lineaires_objets: list[dict] | None = None,
    ponctuelles_objets: list[dict] | None = None,
    parcelles: list[dict] | None = None,
    surf_cfg: dict | None = None,
    lineaires_cfg: dict | None = None,
    ponctuelles_cfg: dict | None = None,
    engine=None,
    schema: str = SCHEMA,
    min_surf_pct: float = MIN_PRESCRIPTION_SURF_PCT,
) -> dict[str, Any]:
    """
    Synthétise les prescriptions PLU pour le CUA (UF + détail parcelles).
    """
    parcelles = list(parcelles or [])
    objets_par_couche = {
        COUCHE_SURF: _filtre_surfaciques_significatifs(
            list(surf_objets or []), min_surf_pct
        ),
        COUCHE_LIN: list(lineaires_objets or []),
        COUCHE_PONCT: list(ponctuelles_objets or []),
    }
    configs = {
        COUCHE_SURF: surf_cfg,
        COUCHE_LIN: lineaires_cfg,
        COUCHE_PONCT: ponctuelles_cfg,
    }

    couches: list[dict[str, Any]] = []
    for key, nom in COUCHES:
        couche = _build_couche(key, nom, objets_par_couche[key], configs.get(key))
        if couche:
            couches.append(couche)

    detail_parcelles: list[dict[str, Any]] = []
    if len(parcelles) > 1 and any(configs.values()):
        detail_parcelles = _build_detail_parcelles(
            parcelles,
            configs,
            engine or get_engine(),
            schema,
            min_surf_pct,
        )

    n_items = sum(len(c.get("items") or []) for c in couches)
    has_content = bool(couches or detail_parcelles)

    diag_parts = []
    if detail_parcelles:
        diag_parts.append(f"{len(detail_parcelles)} parcelle(s) détaillée(s)")
    if couches:
        diag_parts.append(f"{len(couches)} couche(s) | {n_items} prescription(s)")

    return {
        "status": "concernee" if has_content else "non_concernee",
        "diagnostic_metier": (
            " | ".join(diag_parts)
            if diag_parts
            else "RAS : aucune prescription PLU sur l'UF"
        ),
        "couches": couches,
        "detail_parcelles": detail_parcelles,
    }
