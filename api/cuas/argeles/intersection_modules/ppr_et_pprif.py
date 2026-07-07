# -*- coding: utf-8 -*-
"""
Module métier dédié : PPR inondation et PPRIF.

PPR : attributs réglementaires lus directement sur argeles.ppr (jointure label).
Seuil d'intersection : > 1 % de la surface de l'UF par fragment SIG (comme le zonage PLU).
PPRIF : laius depuis argeles.laius_pprif (jointure label).
"""

from __future__ import annotations

import re
from typing import Any, Optional

from sqlalchemy import text

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


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

PPR_TABLE = "ppr"
PPRIF_TABLE = "pprif"
# Seuil de significativité UF (aligné zonage PLU, prescriptions, enrich_parcelles_resume).
MIN_PPR_PCT = 1.0
MIN_DETAIL_PCT = MIN_PPR_PCT


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


def _table_exists(engine, schema: str, table: str) -> bool:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = :schema AND table_name = :table
                    )
                    """
                ),
                {"schema": schema, "table": table},
            ).scalar()
        )


def _load_laius(engine, schema: str, table: str, key_col: str) -> dict[str, str]:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
    key_col = _safe_ident(key_col)
    sql = text(
        f"""
        SELECT {key_col}, reglementation
        FROM {schema}.{table}
        WHERE {key_col} IS NOT NULL
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    out: dict[str, str] = {}
    for row in rows:
        key = str(row[key_col]).strip()
        if not key:
            continue
        regl = row.get("reglementation")
        out[key.upper()] = (str(regl).strip() if regl else "")
    return out


def _pct_sig(obj: dict) -> float:
    try:
        return float(obj.get("pct_sig") or 0)
    except (TypeError, ValueError):
        return 0.0


def _str_field(obj: dict, key: str) -> Optional[str]:
    v = obj.get(key)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _pick_best_entity(entities: list[dict]) -> dict:
    return max(entities, key=_pct_sig)


def _bloc_from_ppr_entity(entity: dict) -> dict[str, Any]:
    regl = _str_field(entity, "reglementation_generale")
    return {
        "label": _str_field(entity, "label"),
        "degre": _str_field(entity, "degre"),
        "risque": _str_field(entity, "risque"),
        "code_degre": _str_field(entity, "code_degre"),
        "ces": _str_field(entity, "ces"),
        "mise_hors_d_eau": _str_field(entity, "mise_hors_d_eau"),
        "reglementation_generale": regl,
        "reglementation": regl,
        "pct_sig": _pct_sig(entity),
    }


def _ppr_objets_significatifs(
    objets: list[dict],
    min_pct: float = MIN_PPR_PCT,
) -> list[dict]:
    """Exclut les micro-recouvrements frontaliers (≤ seuil % de l'UF)."""
    return [obj for obj in objets if _pct_sig(obj) > min_pct]


def _merge_ppr_fields(entities: list[dict]) -> dict[str, Any]:
    """Fusionne les attributs réglementaires sur tous les fragments d'un même label."""
    bloc = _bloc_from_ppr_entity(_pick_best_entity(entities))
    for key in ("ces", "mise_hors_d_eau", "reglementation_generale", "code_degre"):
        if bloc.get(key):
            continue
        for ent in sorted(entities, key=_pct_sig, reverse=True):
            val = _str_field(ent, key)
            if val:
                bloc[key] = val
                if key == "reglementation_generale":
                    bloc["reglementation"] = val
                break
    return bloc


def _build_ppr_blocs(
    objets: list[dict],
    min_pct: float = MIN_PPR_PCT,
) -> list[dict[str, Any]]:
    """Un bloc par sous-zone PPR intersectée (clé = label), si part UF > seuil."""
    objets = _ppr_objets_significatifs(objets, min_pct)
    if not objets:
        return []

    by_label: dict[str, list[dict]] = {}
    for obj in objets:
        label = _str_field(obj, "label")
        if not label:
            continue
        by_label.setdefault(label.upper(), []).append(obj)

    blocs: list[dict[str, Any]] = []
    for label_key in sorted(by_label):
        blocs.append(_merge_ppr_fields(by_label[label_key]))
    return blocs


def _build_pprif_blocs(
    objets: list[dict],
    laius: dict[str, str],
) -> list[dict[str, Any]]:
    if not objets:
        return []

    by_label: dict[str, list[dict]] = {}
    for obj in objets:
        label = _str_field(obj, "label")
        if not label:
            continue
        by_label.setdefault(label.upper(), []).append(obj)

    blocs: list[dict[str, Any]] = []
    for label_key in sorted(by_label):
        entity = _pick_best_entity(by_label[label_key])
        display_label = _str_field(entity, "label")
        regl = (laius.get(label_key) or "").strip()
        blocs.append(
            {
                "label": display_label,
                "risque": _str_field(entity, "degre"),
                "zone": display_label,
                "reglementation": regl or None,
                "pct_sig": _pct_sig(entity),
            }
        )
    return blocs


def _zone_label(obj: dict) -> str:
    return _str_field(obj, "label") or _str_field(obj, "degre") or ""


def _zones_agregees(
    objets: list[dict],
    min_pct: float,
) -> list[tuple[str, float]]:
    """Sous-zones SIG distinctes avec % cumulé sur la parcelle."""
    by_zone: dict[str, float] = {}
    for obj in objets:
        zone = _zone_label(obj)
        if not zone:
            continue
        by_zone[zone] = by_zone.get(zone, 0.0) + _pct_sig(obj)

    items = [
        (zone, pct)
        for zone, pct in sorted(by_zone.items(), key=lambda item: -item[1])
        if pct > min_pct
    ]
    if items:
        return items
    return sorted(by_zone.items(), key=lambda item: -item[1])


def _format_sous_zones(zones_pct: list[tuple[str, float]]) -> str:
    return ", ".join(f"sous-zone {zone} ({pct:.2f} %)" for zone, pct in zones_pct)


def _format_risque_parcelle(prefix: str, zones_pct: list[tuple[str, float]]) -> str:
    if not zones_pct:
        return ""
    if len(zones_pct) == 1:
        zone, pct = zones_pct[0]
        return f"{prefix} : sous-zone {zone} ({pct:.2f} %)"
    return f"{prefix} : {_format_sous_zones(zones_pct)}"


def _format_parcelles_concernées(parcelles: list[dict]) -> str:
    parts: list[str] = []
    for p in parcelles:
        ref = (p.get("libelle") or "").strip()
        if not ref:
            continue
        try:
            pct = float(p.get("pct") or 0)
        except (TypeError, ValueError):
            pct = 0.0
        if pct > 0:
            parts.append(f"{ref} ({pct:.2f} %)")
        else:
            parts.append(ref)
    return ", ".join(parts)


def _build_parcelles_par_label(
    parcelles: list[dict],
    table: str,
    cfg: dict | None,
    engine,
    schema: str,
    min_pct: float,
) -> dict[str, list[dict[str, Any]]]:
    """Index sous-zone (label) → parcelles UF avec % de surface parcelle."""
    if len(parcelles) <= 1 or not cfg:
        return {}

    parcelle_geoms = fetch_parcelles_geom(parcelles, engine, schema)
    if not parcelle_geoms:
        return {}

    by_label: dict[str, list[dict[str, Any]]] = {}
    for parcelle in parcelle_geoms:
        surface = parcelle["surface_sig"]
        if surface <= 0:
            continue

        objets = intersect_couche_parcelle(
            parcelle["wkt"],
            surface,
            table,
            cfg,
            engine,
            schema,
        )
        zones_pct = _zones_agregees(objets, min_pct)
        if not zones_pct:
            continue

        section = str(parcelle["section"])
        numero = str(parcelle["numero"])
        ref = format_parcelle_ref(section, numero)
        for zone, pct in zones_pct:
            by_label.setdefault(zone.upper(), []).append(
                {
                    "section": section,
                    "numero": numero,
                    "libelle": ref,
                    "pct": round(pct, 2),
                }
            )

    for key in by_label:
        by_label[key] = sorted(by_label[key], key=lambda p: -float(p.get("pct") or 0))
    return by_label


def _attach_parcelles_aux_blocs(
    blocs: list[dict[str, Any]],
    parcelles_par_label: dict[str, list[dict[str, Any]]],
) -> None:
    for bloc in blocs:
        label = (bloc.get("label") or "").strip().upper()
        if label and label in parcelles_par_label:
            bloc["parcelles"] = parcelles_par_label[label]


def _build_detail_parcelles(
    parcelles: list[dict],
    ppr_cfg: dict | None,
    pprif_cfg: dict | None,
    engine,
    schema: str,
    min_pct: float = MIN_DETAIL_PCT,
) -> list[dict[str, Any]]:
    """Détail PPR / PPRIF par parcelle (UF multi-parcelles uniquement)."""
    if len(parcelles) <= 1 or not (ppr_cfg or pprif_cfg):
        return []

    parcelle_geoms = fetch_parcelles_geom(parcelles, engine, schema)
    if not parcelle_geoms:
        return []

    details: list[dict[str, Any]] = []
    for parcelle in parcelle_geoms:
        surface = parcelle["surface_sig"]
        if surface <= 0:
            continue

        ppr_zones: list[tuple[str, float]] = []
        pprif_zones: list[tuple[str, float]] = []

        if ppr_cfg:
            ppr_objets = intersect_couche_parcelle(
                parcelle["wkt"],
                surface,
                PPR_TABLE,
                ppr_cfg,
                engine,
                schema,
            )
            ppr_zones = _zones_agregees(ppr_objets, min_pct)

        if pprif_cfg:
            pprif_objets = intersect_couche_parcelle(
                parcelle["wkt"],
                surface,
                PPRIF_TABLE,
                pprif_cfg,
                engine,
                schema,
            )
            pprif_zones = _zones_agregees(pprif_objets, min_pct)

        if not ppr_zones and not pprif_zones:
            continue

        section = str(parcelle["section"])
        numero = str(parcelle["numero"])
        ref = format_parcelle_ref(section, numero)

        parts: list[str] = []
        if ppr_zones:
            parts.append(_format_risque_parcelle("PPR", ppr_zones))
        if pprif_zones:
            parts.append(_format_risque_parcelle("PPRIF", pprif_zones))

        details.append(
            {
                "section": section,
                "numero": numero,
                "libelle": ref,
                "ppr": {
                    "zones": [
                        {"zone": zone, "pct": round(pct, 2)}
                        for zone, pct in ppr_zones
                    ],
                }
                if ppr_zones
                else None,
                "pprif": {
                    "zones": [
                        {"zone": zone, "pct": round(pct, 2)}
                        for zone, pct in pprif_zones
                    ],
                }
                if pprif_zones
                else None,
                "texte": f"{ref} : {' · '.join(parts)} de la surface parcelle.",
            }
        )

    return details


def compute_ppr_et_pprif_reglementation(
    *,
    ppr_objets: list[dict] | None = None,
    pprif_objets: list[dict] | None = None,
    parcelles: list[dict] | None = None,
    ppr_cfg: dict | None = None,
    pprif_cfg: dict | None = None,
    engine=None,
    schema: str = SCHEMA,
    ppr_laius_table: str = "laius_ppr",
    pprif_laius_table: str = "laius_pprif",
    min_detail_pct: float = MIN_DETAIL_PCT,
) -> dict:
    """
    Enrichit les intersections PPR / PPRIF avec la réglementation applicable.

    PPR : attributs portés par la couche argeles.ppr (label, degre, ces, etc.).
    PPRIF : laius depuis la table laius_pprif.
    """
    _ = ppr_laius_table  # conservé pour signature publique, non utilisé pour le PPR
    ppr_objets = list(ppr_objets or [])
    pprif_objets = list(pprif_objets or [])
    parcelles = list(parcelles or [])

    detail_parcelles: list[dict[str, Any]] = []
    if len(parcelles) > 1 and (ppr_cfg or pprif_cfg):
        detail_parcelles = _build_detail_parcelles(
            parcelles,
            ppr_cfg,
            pprif_cfg,
            engine or get_engine(),
            schema,
            min_detail_pct,
        )

    if not ppr_objets and not pprif_objets:
        has_detail = bool(detail_parcelles)
        return {
            "status": "concernee" if has_detail else "non_concernee",
            "diagnostic_metier": (
                f"{len(detail_parcelles)} parcelle(s) détaillée(s)"
                if has_detail
                else "RAS : aucune contrainte PPR / PPRIF réglementée sur l'UF"
            ),
            "detail_parcelles": detail_parcelles,
            "ppr": {"status": "non_concernee", "blocs": [], "notes": []},
            "pprif": {"status": "non_concernee", "blocs": []},
        }

    engine = engine or get_engine()
    schema = _safe_ident(schema)
    pprif_laius_table = _safe_ident(pprif_laius_table)

    missing: list[str] = []
    if pprif_objets and not _table_exists(engine, schema, pprif_laius_table):
        missing.append(pprif_laius_table)

    if missing:
        return {
            "status": "table_absente",
            "diagnostic_metier": "Module non exécutable : table(s) de laius manquante(s)",
            "tables_manquantes": missing,
            "detail_parcelles": detail_parcelles,
            "ppr": {"status": "table_absente", "blocs": []},
            "pprif": {"status": "table_absente", "blocs": []},
        }

    ppr_blocs = _build_ppr_blocs(ppr_objets, min_pct=min_detail_pct)
    pprif_laius = _load_laius(engine, schema, pprif_laius_table, "label") if pprif_objets else {}
    pprif_blocs = _build_pprif_blocs(pprif_objets, pprif_laius)

    if len(parcelles) > 1:
        ppr_par_label = _build_parcelles_par_label(
            parcelles, PPR_TABLE, ppr_cfg, engine, schema, min_detail_pct,
        )
        pprif_par_label = _build_parcelles_par_label(
            parcelles, PPRIF_TABLE, pprif_cfg, engine, schema, min_detail_pct,
        )
        _attach_parcelles_aux_blocs(ppr_blocs, ppr_par_label)
        _attach_parcelles_aux_blocs(pprif_blocs, pprif_par_label)

    has_content = bool(ppr_blocs or pprif_blocs or detail_parcelles)
    diag_parts = []
    if detail_parcelles:
        diag_parts.append(f"{len(detail_parcelles)} parcelle(s) détaillée(s)")
    if ppr_blocs:
        diag_parts.append(f"PPR : {len(ppr_blocs)} sous-zone(s)")
    if pprif_blocs:
        diag_parts.append(f"PPRIF : {len(pprif_blocs)} bloc(s)")

    return {
        "status": "concernee" if has_content else "non_concernee",
        "diagnostic_metier": (
            " | ".join(diag_parts)
            if diag_parts
            else "RAS : aucune contrainte PPR / PPRIF réglementée sur l'UF"
        ),
        "detail_parcelles": detail_parcelles,
        "ppr": {
            "nom": "PPR (Plan de Prévention des Risques)",
            "status": "concernee" if ppr_blocs else "non_concernee",
            "blocs": ppr_blocs,
            "notes": [],
        },
        "pprif": {
            "nom": "PPRIF (Risque Incendie de Forêt)",
            "status": "concernee" if pprif_blocs else "non_concernee",
            "blocs": pprif_blocs,
        },
    }
