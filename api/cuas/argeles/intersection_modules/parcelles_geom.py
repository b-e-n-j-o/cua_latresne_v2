# -*- coding: utf-8 -*-
"""
Helpers géométrie parcelle pour les modules d'intersection métier.

Le rapport d'intersections ne stocke que les références cadastrales
(section / numéro) et l'emprise UF unionnée. Les géométries individuelles
sont relues depuis <schema>.parcelles à la demande.
"""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import text

try:
    from api.cuas.argeles.db import GEOM_COL, SCHEMA
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from db import GEOM_COL, SCHEMA

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


def normalize_section(value: str) -> str:
    return (value or "").strip().upper()


def normalize_numero(value: str) -> str:
    raw = (value or "").strip()
    return raw.zfill(4) if raw else ""


def format_parcelle_ref(section: str, numero: str) -> str:
    return f"Parcelle {normalize_section(section)} n°{(numero or '').strip()}"


def fetch_parcelles_geom(
    parcelles: list[dict],
    engine,
    schema: str = SCHEMA,
) -> list[dict[str, Any]]:
    """Géométrie WKT et surface SIG de chaque parcelle (ordre conservé)."""
    if not parcelles:
        return []

    schema = _safe_ident(schema)
    clauses, params = [], {}
    for i, ref in enumerate(parcelles):
        clauses.append(
            f"(upper(trim(section)) = :s{i} AND lpad(trim(numero), 4, '0') = :n{i})"
        )
        params[f"s{i}"] = normalize_section(ref.get("section", ""))
        params[f"n{i}"] = normalize_numero(ref.get("numero", ""))

    geom_col = _safe_ident(GEOM_COL)
    sql = text(f"""
        SELECT section,
               numero,
               ST_AsText(ST_MakeValid({geom_col})) AS wkt,
               ST_Area(ST_MakeValid({geom_col}))   AS surface_sig
        FROM {schema}.parcelles
        WHERE {" OR ".join(clauses)}
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    by_key = {
        (normalize_section(r["section"]), normalize_numero(r["numero"])): r
        for r in rows
    }

    out: list[dict[str, Any]] = []
    for ref in parcelles:
        key = (
            normalize_section(ref.get("section", "")),
            normalize_numero(ref.get("numero", "")),
        )
        row = by_key.get(key)
        if not row or not row.get("wkt"):
            continue
        out.append(
            {
                "section": row["section"],
                "numero": row["numero"],
                "wkt": row["wkt"],
                "surface_sig": float(row["surface_sig"] or 0),
            }
        )
    return out


def intersect_couche_parcelle(
    wkt: str,
    surface_sig: float,
    table: str,
    cfg: dict,
    engine,
    schema: str = SCHEMA,
) -> list[dict]:
    """Intersection d'une couche catalogue sur une parcelle isolée."""
    try:
        from api.cuas.argeles.intersections import calculate_intersection
    except ImportError:
        from intersections import calculate_intersection

    table = _safe_ident(table)
    layer_cfg = dict(cfg)
    layer_cfg.setdefault("geom_type", "surfacique")
    objets, _, _ = calculate_intersection(
        wkt,
        table,
        layer_cfg,
        surface_sig,
        engine,
        schema,
    )
    return objets
