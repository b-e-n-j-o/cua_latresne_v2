# -*- coding: utf-8 -*-
"""
Module métier dédié : taux de taxe d'aménagement communale.

Intersection SIG avec argeles.taxes : si l'UF intersecte une zone, on retient
le taux associé (zone à plus forte couverture). Sinon, taux par défaut 5 %.
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
DEFAULT_TAUX_COMMUNALE = 5.0
MIN_INTERSECTION_AREA_M2 = 0.01


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


def _normalize_taux(taux: Any) -> Optional[float]:
    if taux is None:
        return None
    if isinstance(taux, Decimal):
        val = float(taux)
    else:
        try:
            val = float(taux)
        except (TypeError, ValueError):
            return None
    if 0 < val < 1:
        val *= 100
    return val


def _format_taux(taux: float) -> str:
    if taux == int(taux):
        return f"{int(taux)} %"
    txt = f"{taux:.2f}".rstrip("0").rstrip(".")
    return f"{txt.replace('.', ',')} %"


def _intersect_taxes(
    engine,
    schema: str,
    uf_wkt: str,
    surface_sig: float,
) -> list[dict[str, Any]]:
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
    return objets


def compute_taxes(
    uf_wkt: str,
    *,
    surface_sig: float = 0.0,
    engine=None,
    schema: str = SCHEMA,
    default_taux: float = DEFAULT_TAUX_COMMUNALE,
) -> dict[str, Any]:
    """
    Retourne le taux communale applicable à l'UF.

    Clés principales :
      - taux_communale (float)
      - taux_communale_libelle (str, ex. "5 %")
      - source ("intersection" | "defaut")
      - objets (zones intersectées, triées par couverture décroissante)
    """
    engine = engine or get_engine()

    if not _table_exists(engine, schema, TAXES_TABLE):
        taux = default_taux
        return {
            "taux_communale": taux,
            "taux_communale_libelle": _format_taux(taux),
            "libelle": None,
            "source": "defaut",
            "status": "table_absente",
            "objets": [],
        }

    objets = _intersect_taxes(engine, schema, uf_wkt, surface_sig)
    if not objets:
        taux = default_taux
        return {
            "taux_communale": taux,
            "taux_communale_libelle": _format_taux(taux),
            "libelle": None,
            "source": "defaut",
            "status": "non_concernee",
            "objets": [],
        }

    principal = objets[0]
    taux = principal.get("taux")
    if taux is None:
        taux = default_taux
        source = "defaut"
    else:
        source = "intersection"

    return {
        "taux_communale": taux,
        "taux_communale_libelle": _format_taux(taux),
        "libelle": principal.get("libelle"),
        "source": source,
        "status": "concernee",
        "objets": objets,
    }
