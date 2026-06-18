# -*- coding: utf-8 -*-
"""
Module métier dédié : logique croisée Natura 2000 / Prairies sensibles.

Réglementations depuis la table natura_2000_et_prairies_reglements (code_regime) :
  - NATURA_2000        → intersection Natura seule ou double
  - BCAE9_NATURA       → double intersection Natura + Prairie sensible
  - BCAE1_HORS_NATURA  → Prairie seule hors Natura
"""

from __future__ import annotations

import re
from typing import Any, Optional

from sqlalchemy import text

try:
    from api.cuas.argeles.db import GEOM_COL, SCHEMA, SRID, get_engine
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from db import GEOM_COL, SCHEMA, SRID, get_engine


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_REGIME_LABELS = {
    "double_intersection_natura_et_prairie": "Natura 2000 et Prairie Sensible)",
    "natura_seule": "Natura 2000",
    "prairie_seule_hors_natura": "Prairie Sensible",
    "hors_natura_et_prairie": "RAS : UF hors contraintes Natura 2000 / Prairies",
}

_REGIMES_PAR_CAS = {
    "double_intersection_natura_et_prairie": ("NATURA_2000", "BCAE9_NATURA"),
    "natura_seule": ("NATURA_2000",),
    "prairie_seule_hors_natura": ("BCAE1_HORS_NATURA",),
}


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


def _load_reglements(engine, schema: str, table: str) -> dict[str, dict[str, Any]]:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
    sql = text(
        f"""
        SELECT code_regime, nom_regime, statut_juridique, reglementation, base_legale
        FROM {schema}.{table}
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return {
        str(row["code_regime"]).strip(): dict(row)
        for row in rows
        if row.get("code_regime")
    }


def _format_reglementation(
    template: Optional[str],
    *,
    c_site: Optional[str] = None,
    n_site: Optional[str] = None,
) -> Optional[str]:
    if not template or not str(template).strip():
        return None
    texte = str(template).strip()
    if c_site is not None or n_site is not None:
        texte = texte.replace("{c_site}", (c_site or "Inconnu").strip())
        texte = texte.replace("{n_site}", (n_site or "Sans nom").strip())
    return texte


def _intersect_natura(
    engine,
    schema: str,
    table: str,
    geom_col: str,
    uf_wkt: str,
) -> Optional[dict[str, Any]]:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
    geom_col = _safe_ident(geom_col)
    sql = text(
        f"""
        SELECT id, c_site, n_site
        FROM {schema}.{table}
        WHERE ST_Intersects(
            ST_MakeValid({geom_col}),
            ST_GeomFromText(:wkt, {SRID})
        )
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"wkt": uf_wkt}).mappings().first()
    return dict(row) if row else None


def _intersect_prairie(
    engine,
    schema: str,
    table: str,
    geom_col: str,
    uf_wkt: str,
) -> Optional[dict[str, Any]]:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
    geom_col = _safe_ident(geom_col)
    sql = text(
        f"""
        SELECT id
        FROM {schema}.{table}
        WHERE ST_Intersects(
            ST_MakeValid({geom_col}),
            ST_GeomFromText(:wkt, {SRID})
        )
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"wkt": uf_wkt}).mappings().first()
    return dict(row) if row else None


def _resolve_regime_code(has_natura: bool, has_prairie: bool) -> str:
    if has_natura and has_prairie:
        return "double_intersection_natura_et_prairie"
    if has_natura:
        return "natura_seule"
    if has_prairie:
        return "prairie_seule_hors_natura"
    return "hors_natura_et_prairie"


def _build_bloc(
    reg: dict[str, Any],
    *,
    c_site: Optional[str] = None,
    n_site: Optional[str] = None,
) -> dict[str, Any]:
    code = str(reg.get("code_regime") or "").strip()
    return {
        "code_regime": code,
        "nom_regime": reg.get("nom_regime"),
        "statut_juridique": reg.get("statut_juridique"),
        "reglementation": _format_reglementation(
            reg.get("reglementation"),
            c_site=c_site,
            n_site=n_site,
        ),
        "base_legale": reg.get("base_legale"),
    }


def compute_prairies_natura_reglementation(
    uf_wkt: str,
    *,
    engine=None,
    schema: str = SCHEMA,
    natura_table: str = "natura_2000",
    prairie_table: str = "prairies_sensibles",
    reglement_table: str = "natura_2000_et_prairies_reglements",
    geom_col: str = GEOM_COL,
) -> dict:
    """
    Retourne le diagnostic métier et les blocs réglementaires associés.
    """
    engine = engine or get_engine()

    schema = _safe_ident(schema)
    natura_table = _safe_ident(natura_table)
    prairie_table = _safe_ident(prairie_table)
    reglement_table = _safe_ident(reglement_table)
    geom_col = _safe_ident(geom_col)

    required_tables = (natura_table, prairie_table, reglement_table)
    missing_tables = [t for t in required_tables if not _table_exists(engine, schema, t)]
    if missing_tables:
        return {
            "status": "table_absente",
            "diagnostic_metier": "Module non exécutable : table(s) manquante(s)",
            "tables_manquantes": missing_tables,
            "blocs": [],
            "natura": None,
            "prairie": None,
            "has_natura": False,
            "has_prairie": False,
        }

    reglements = _load_reglements(engine, schema, reglement_table)
    natura_hit = _intersect_natura(engine, schema, natura_table, geom_col, uf_wkt)
    prairie_hit = _intersect_prairie(engine, schema, prairie_table, geom_col, uf_wkt)

    has_natura = natura_hit is not None
    has_prairie = prairie_hit is not None
    regime_code = _resolve_regime_code(has_natura, has_prairie)
    diagnostic = _REGIME_LABELS[regime_code]

    c_site = natura_hit.get("c_site") if natura_hit else None
    n_site = natura_hit.get("n_site") if natura_hit else None

    blocs: list[dict[str, Any]] = []
    for code in _REGIMES_PAR_CAS.get(regime_code, ()):
        reg = reglements.get(code)
        if not reg:
            continue
        placeholders = code == "NATURA_2000"
        blocs.append(
            _build_bloc(
                reg,
                c_site=c_site if placeholders else None,
                n_site=n_site if placeholders else None,
            )
        )

    natura_block = None
    if has_natura:
        natura_reg = reglements.get("NATURA_2000")
        natura_block = {
            "id": natura_hit.get("id"),
            "code_site": c_site,
            "nom_site": n_site,
            **(
                _build_bloc(natura_reg, c_site=c_site, n_site=n_site)
                if natura_reg
                else {}
            ),
        }

    prairie_block = None
    if has_prairie:
        prairie_code = (
            "BCAE9_NATURA"
            if regime_code == "double_intersection_natura_et_prairie"
            else "BCAE1_HORS_NATURA"
        )
        prairie_reg = reglements.get(prairie_code)
        prairie_block = {
            "id": prairie_hit.get("id"),
            **(_build_bloc(prairie_reg) if prairie_reg else {}),
        }

    return {
        "status": "concernee" if (has_natura or has_prairie) else "non_concernee",
        "regime_code": regime_code,
        "diagnostic_metier": diagnostic,
        "blocs": blocs,
        "natura": natura_block,
        "prairie": prairie_block,
        "has_natura": has_natura,
        "has_prairie": has_prairie,
    }
