# -*- coding: utf-8 -*-
"""Utilitaires géométriques communs — dual SRID 2154 + 3857."""

from __future__ import annotations

import re

import geopandas as gpd
from sqlalchemy import text
from sqlalchemy.engine import Engine

_GEOM_TYPE_ALIASES = {
    "MULTIPOLYGON": "MultiPolygon",
    "POLYGON": "Polygon",
    "MULTILINESTRING": "MultiLineString",
    "LINESTRING": "LineString",
    "MULTIPOINT": "MultiPoint",
    "POINT": "Point",
    "GEOMETRY": "Geometry",
    "GEOMETRYCOLLECTION": "GeometryCollection",
}


def normalize_pg_geom_type(raw_type: str, srid: int = 3857) -> str:
    """Normalise geometry_columns.type ou geometry(...) en type ALTER TABLE valide."""
    if not raw_type:
        return f"geometry(Geometry, {srid})"
    t = raw_type.strip()
    m = re.match(r"geometry\s*\(\s*([^,]+)\s*,\s*\d+\s*\)", t, re.I)
    if m:
        return f"geometry({m.group(1).strip()}, {srid})"
    geom_type = _GEOM_TYPE_ALIASES.get(t.upper(), t)
    return f"geometry({geom_type}, {srid})"


def add_geom_3857(gdf: gpd.GeoDataFrame, geom_col: str = "geom_2154", normalizer=None):
    """Ajoute geom_3857 dérivée de geom_col (EPSG:2154 → EPSG:3857)."""
    gdf = gdf.copy()
    gdf = gdf.set_geometry(geom_col)
    gdf_3857 = gdf.to_crs(epsg=3857)
    geoms = gdf_3857.geometry
    if normalizer is not None:
        geoms = geoms.apply(normalizer)
    gdf["geom_3857"] = geoms
    return gdf.set_geometry(geom_col)


def _use_st_multi(pg_type: str) -> bool:
    return "Multi" in pg_type


def _geom_3857_from_wkt_sql(pg_type: str) -> str:
    inner = "ST_GeomFromText(:wkt, 3857)"
    if _use_st_multi(pg_type):
        return f"ST_Multi({inner})"
    return inner


def _bare_identifier(name: str) -> str:
    return name.strip().strip('"')


def ensure_geom_3857_column(engine: Engine, table_fqn: str, pg_type: str) -> None:
    """Ajoute geom_3857 + index GIST si la table existe déjà sans cette colonne."""
    table_name = _bare_identifier(table_fqn.split(".")[-1])
    index_name = f"{table_name}_gix_3857"
    with engine.begin() as conn:
        conn.execute(text(f"""
            ALTER TABLE {table_fqn}
            ADD COLUMN IF NOT EXISTS geom_3857 {pg_type};
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table_fqn} USING GIST (geom_3857);
        """))


def backfill_geom_3857(
    engine: Engine,
    table_fqn: str,
    geom_col: str = "geom_2154",
    pg_type: str = "",
) -> int:
    """Remplit geom_3857 depuis geom_2154 pour les lignes où elle est NULL."""
    transform = f"ST_Transform({geom_col}, 3857)"
    expr = f"ST_Multi({transform})" if _use_st_multi(pg_type) else transform
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            UPDATE {table_fqn}
            SET geom_3857 = {expr}
            WHERE geom_3857 IS NULL AND {geom_col} IS NOT NULL
        """))
        return result.rowcount or 0


def refresh_geom_3857(
    engine: Engine,
    table_fqn: str,
    geom_col: str = "geom_2154",
    pg_type: str = "",
) -> int:
    """Recalcule geom_3857 pour toutes les lignes ayant une géométrie source."""
    transform = f"ST_Transform({geom_col}, 3857)"
    expr = f"ST_Multi({transform})" if _use_st_multi(pg_type) else transform
    with engine.begin() as conn:
        result = conn.execute(text(f"""
            UPDATE {table_fqn}
            SET geom_3857 = {expr}
            WHERE {geom_col} IS NOT NULL
        """))
        return result.rowcount or 0
