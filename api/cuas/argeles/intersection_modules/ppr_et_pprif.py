# -*- coding: utf-8 -*-
"""
Module métier dédié : PPR inondation et PPRIF.

Enrichit les objets intersectés (couches ppr / pprif) avec les laius
depuis argeles.laius_ppr (jointure code_degre) et argeles.laius_pprif
(jointure label).

Cas particulier PPR : RECUL et CENTRE HISTORIQUE sont des laius
complémentaires — lorsqu'ils sont intersectés, leur texte s'ajoute en
note aux blocs des zones principales (I, II, III).
"""

from __future__ import annotations

import re
from typing import Any, Optional

from sqlalchemy import text

try:
    from api.cuas.argeles.db import SCHEMA, get_engine
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from db import SCHEMA, get_engine


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

MAIN_ZONE_CODES = ("I", "II", "III")
SUPPLEMENTARY_CODES = frozenset({"RECUL", "CENTRE HISTORIQUE"})


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


def _resolve_ppr_code_degre(obj: dict) -> Optional[str]:
    """Déduit le code_degre laius_ppr depuis les attributs intersectés."""
    label = (obj.get("label") or "").strip()
    degre = (obj.get("degre") or "").strip()
    label_lower = label.lower()

    if "centre historique" in label_lower:
        return "CENTRE HISTORIQUE"
    if label_lower.startswith("recul minimum") or label_lower == "recul":
        return "RECUL"

    degre_upper = degre.upper()
    label_upper = label.upper()

    if degre_upper in SUPPLEMENTARY_CODES:
        return degre_upper
    if label_upper in SUPPLEMENTARY_CODES:
        return label_upper

    if degre_upper in MAIN_ZONE_CODES:
        return degre_upper

    compact = label_upper.replace(" ", "")
    if compact.startswith("III"):
        return "III"
    if compact.startswith("II"):
        return "II"
    if compact.startswith("I"):
        return "I"

    return None


def _pick_best_entity(entities: list[dict]) -> dict:
    return max(entities, key=_pct_sig)


def _build_ppr_notes(
    supplementary_codes: set[str],
    laius: dict[str, str],
) -> list[dict[str, str]]:
    notes: list[dict[str, str]] = []
    for code in sorted(supplementary_codes):
        regl = (laius.get(code) or "").strip()
        if regl:
            notes.append({"code": code, "reglementation": regl})
    return notes


def _build_ppr_blocs(
    objets: list[dict],
    laius: dict[str, str],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if not objets:
        return [], []

    by_code: dict[str, list[dict]] = {}
    for obj in objets:
        code = _resolve_ppr_code_degre(obj)
        if not code:
            continue
        by_code.setdefault(code, []).append(obj)

    supplementary_active = {c for c in by_code if c in SUPPLEMENTARY_CODES}
    main_active = {c for c in by_code if c in MAIN_ZONE_CODES}
    notes = _build_ppr_notes(supplementary_active, laius)

    blocs: list[dict[str, Any]] = []

    if main_active:
        for code in sorted(main_active, key=lambda c: MAIN_ZONE_CODES.index(c) if c in MAIN_ZONE_CODES else 99):
            entity = _pick_best_entity(by_code[code])
            regl = (laius.get(code) or "").strip()
            blocs.append(
                {
                    "code_degre": code,
                    "type_risque": (entity.get("risque") or "").strip() or None,
                    "zone": (entity.get("degre") or "").strip() or None,
                    "zone_reglementaire": (entity.get("label") or "").strip() or None,
                    "reglementation": regl or None,
                    "pct_sig": _pct_sig(entity),
                }
            )
        return blocs, notes

    for code in sorted(supplementary_active):
        entity = _pick_best_entity(by_code[code])
        regl = (laius.get(code) or "").strip()
        blocs.append(
            {
                "code_degre": code,
                "type_risque": (entity.get("risque") or "").strip() or None,
                "zone": (entity.get("degre") or "").strip() or None,
                "zone_reglementaire": (entity.get("label") or "").strip() or None,
                "reglementation": regl or None,
                "pct_sig": _pct_sig(entity),
            }
        )
    return blocs, []


def _build_pprif_blocs(
    objets: list[dict],
    laius: dict[str, str],
) -> list[dict[str, Any]]:
    if not objets:
        return []

    by_label: dict[str, list[dict]] = {}
    for obj in objets:
        label = (obj.get("label") or "").strip()
        if not label:
            continue
        by_label.setdefault(label.upper(), []).append(obj)

    blocs: list[dict[str, Any]] = []
    for label_key in sorted(by_label):
        entity = _pick_best_entity(by_label[label_key])
        display_label = (entity.get("label") or "").strip()
        regl = (laius.get(label_key) or "").strip()
        blocs.append(
            {
                "label": display_label,
                "risque": (entity.get("degre") or "").strip() or None,
                "zone": display_label,
                "reglementation": regl or None,
                "pct_sig": _pct_sig(entity),
            }
        )
    return blocs


def compute_ppr_et_pprif_reglementation(
    *,
    ppr_objets: list[dict] | None = None,
    pprif_objets: list[dict] | None = None,
    engine=None,
    schema: str = SCHEMA,
    ppr_laius_table: str = "laius_ppr",
    pprif_laius_table: str = "laius_pprif",
) -> dict:
    """
    Enrichit les intersections PPR / PPRIF avec les laius réglementaires.
    """
    engine = engine or get_engine()
    schema = _safe_ident(schema)
    ppr_laius_table = _safe_ident(ppr_laius_table)
    pprif_laius_table = _safe_ident(pprif_laius_table)

    ppr_objets = list(ppr_objets or [])
    pprif_objets = list(pprif_objets or [])

    missing: list[str] = []
    if ppr_objets and not _table_exists(engine, schema, ppr_laius_table):
        missing.append(ppr_laius_table)
    if pprif_objets and not _table_exists(engine, schema, pprif_laius_table):
        missing.append(pprif_laius_table)

    if missing:
        return {
            "status": "table_absente",
            "diagnostic_metier": "Module non exécutable : table(s) de laius manquante(s)",
            "tables_manquantes": missing,
            "ppr": {"status": "table_absente", "blocs": []},
            "pprif": {"status": "table_absente", "blocs": []},
        }

    ppr_laius = _load_laius(engine, schema, ppr_laius_table, "code_degre") if ppr_objets else {}
    pprif_laius = _load_laius(engine, schema, pprif_laius_table, "label") if pprif_objets else {}

    ppr_blocs, ppr_notes = _build_ppr_blocs(ppr_objets, ppr_laius)
    pprif_blocs = _build_pprif_blocs(pprif_objets, pprif_laius)

    has_content = bool(ppr_blocs or pprif_blocs or ppr_notes)
    return {
        "status": "concernee" if has_content else "non_concernee",
        "diagnostic_metier": (
            f"PPR : {len(ppr_blocs)} bloc(s) | PPRIF : {len(pprif_blocs)} bloc(s)"
            if has_content
            else "RAS : aucune contrainte PPR / PPRIF réglementée sur l'UF"
        ),
        "ppr": {
            "nom": "PPR (Plan de Prévention des Risques)",
            "status": "concernee" if (ppr_blocs or ppr_notes) else "non_concernee",
            "blocs": ppr_blocs,
            "notes": ppr_notes,
        },
        "pprif": {
            "nom": "PPRIF (Risque Incendie de Forêt)",
            "status": "concernee" if pprif_blocs else "non_concernee",
            "blocs": pprif_blocs,
        },
    }
