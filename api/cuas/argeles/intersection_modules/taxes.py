# -*- coding: utf-8 -*-
"""
Module métier dédié : taux de taxe d'aménagement communale.

Intersection SIG avec argeles.taxes : la couche couvre toute la commune ;
on retient le taux de la zone à plus forte couverture sur l'UF.
"""

from __future__ import annotations

import re
from decimal import Decimal
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
TAXES_TABLE = "taxes"
MIN_INTERSECTION_AREA_M2 = 0.01


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


def _normalize_taux(taux: Any) -> float:
    if taux is None:
        raise ValueError("Taux manquant sur la zone fiscale intersectée")
    if isinstance(taux, Decimal):
        val = float(taux)
    else:
        val = float(taux)
    if 0 < val < 1:
        val *= 100
    return val


def _format_taux(taux: float) -> str:
    if taux == int(taux):
        return f"{int(taux)} %"
    txt = f"{taux:.2f}".rstrip("0").rstrip(".")
    return f"{txt.replace('.', ',')} %"


def compute_taxes(
    uf_wkt: str,
    *,
    surface_sig: float = 0.0,
    engine=None,
    schema: str = SCHEMA,
) -> dict[str, Any]:
    """Intersecte argeles.taxes et retourne le taux communale applicable."""
    engine = engine or get_engine()
    schema = _safe_ident(schema)
    table = _safe_ident(TAXES_TABLE)
    geom_col = _safe_ident(GEOM_COL)

    sql = text(
        f"""
        WITH uf AS (
            SELECT ST_GeomFromText(:wkt, {SRID}) AS geom
        ),
        inter_raw AS (
            SELECT t.id,
                   t.libelle,
                   t.taux,
                   ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom) AS inter_geom
            FROM {schema}.{table} t
            CROSS JOIN uf
            WHERE t.{geom_col} IS NOT NULL
              AND ST_Intersects(ST_MakeValid(t.{geom_col}), uf.geom)
              AND ST_Area(ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom))
                  > {MIN_INTERSECTION_AREA_M2}
        )
        SELECT id,
               libelle,
               taux,
               ST_Area(inter_geom) AS surface_inter_m2
        FROM inter_raw
        ORDER BY ST_Area(inter_geom) DESC
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, {"wkt": uf_wkt}).mappings().all()

    if not rows:
        return {"status": "non_concernee", "objets": []}

    objets: list[dict[str, Any]] = []
    for row in rows:
        surface = float(row["surface_inter_m2"] or 0)
        obj: dict[str, Any] = {
            "id": row["id"],
            "libelle": row.get("libelle"),
            "taux": _normalize_taux(row.get("taux")),
            "surface_inter_m2": round(surface, 2),
        }
        if surface_sig > 0:
            obj["pct_sig"] = round(surface / surface_sig * 100, 4)
        objets.append(obj)

    principal = objets[0]
    taux = principal["taux"]
    return {
        "taux_communale": taux,
        "taux_communale_libelle": _format_taux(taux),
        "libelle": principal.get("libelle"),
        "status": "concernee",
        "objets": objets,
    }
