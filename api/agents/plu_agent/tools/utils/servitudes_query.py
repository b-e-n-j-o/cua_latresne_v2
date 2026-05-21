"""Servitudes d'utilité publique — assiettes surfaciques (sup_assiette_s)."""

from __future__ import annotations

import json

import psycopg2
import psycopg2.extras

SERVITUDES_TABLE = "argeles.sup_assiette_s"
SERVITUDES_MAP_COLOR = "#457B9D"
SERVITUDES_FILL_OPACITY = 0.35


def _db_connect(db_config: dict):
    return psycopg2.connect(**db_config)


def _query(db_config: dict, sql: str, params: tuple) -> list[dict]:
    conn = _db_connect(db_config)
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def _geom_2154_sql(alias: str = "s") -> str:
    """Normalise la colonne geometry vers Lambert-93 pour intersection parcelle."""
    g = f"{alias}.geometry"
    return f"""
        CASE
            WHEN ST_SRID({g}) = 2154 THEN ST_MakeValid({g})
            WHEN ST_SRID({g}) = 0 OR ST_SRID({g}) IS NULL THEN ST_MakeValid(ST_SetSRID({g}, 2154))
            ELSE ST_MakeValid(ST_Transform({g}, 2154))
        END
    """


def _sql_servitudes(with_geojson: bool) -> str:
    geom_2154 = _geom_2154_sql("s")
    geom_sel = (
        f", ST_AsGeoJSON(ST_Transform(ST_Force2D({geom_2154}), 4326)) AS geojson_geom"
        if with_geojson
        else ""
    )
    return f"""
        WITH cible AS (
            SELECT ST_MakeValid(ST_GeomFromEWKB(%s)) AS geom
        ),
        cible_scope AS (
            SELECT ST_MakeValid(
                CASE WHEN %s::float > 0 THEN ST_Buffer(geom, %s::float) ELSE geom END
            ) AS geom
            FROM cible
        )
        SELECT
            s.gid,
            s.idass,
            s.nomass,
            s.suptype,
            s.typeass,
            s.nomsuplitt,
            s.nomreg
            {geom_sel}
        FROM {SERVITUDES_TABLE} s
        CROSS JOIN cible_scope c
        WHERE s.geometry IS NOT NULL
          AND ST_Intersects({geom_2154}, c.geom)
        ORDER BY s.suptype NULLS LAST, s.nomass NULLS LAST, s.gid;
    """


def fetch_servitudes_rows(
    db_config: dict,
    geom_wkb: bytes,
    buffer_m: float = 0.0,
    with_geojson: bool = False,
) -> list[dict]:
    """Assiettes surfaciques de servitudes intersectant l'unité foncière."""
    buf = float(buffer_m or 0)
    sql = _sql_servitudes(with_geojson)
    return _query(db_config, sql, (geom_wkb, buf, buf))


def _parse_geojson(val) -> dict | None:
    if not val:
        return None
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return None


def rows_to_llm_list(rows: list[dict]) -> list[dict]:
    items = []
    for r in rows:
        items.append({
            "gid": r.get("gid"),
            "idass": r.get("idass"),
            "nomass": r.get("nomass"),
            "suptype": r.get("suptype"),
            "typeass": r.get("typeass"),
            "nomsuplitt": r.get("nomsuplitt"),
            "nomreg": r.get("nomreg"),
        })
    return items


def build_llm_payload(rows: list[dict]) -> dict:
    items = rows_to_llm_list(rows)
    return {
        "servitudes": items,
        "count": len(items),
    }


def build_map_servitudes(rows: list[dict]) -> dict:
    """FeatureCollection WGS84 pour MapLibre."""
    features = []
    for r in rows:
        geom = _parse_geojson(r.get("geojson_geom"))
        if not geom:
            continue
        suptype = r.get("suptype")
        typeass = r.get("typeass")
        nomsuplitt = r.get("nomsuplitt")
        label = nomsuplitt or typeass or suptype or r.get("nomass") or r.get("idass")
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "gid": r.get("gid"),
                "idass": r.get("idass"),
                "nomass": r.get("nomass"),
                "suptype": suptype,
                "typeass": typeass,
                "nomsuplitt": nomsuplitt,
                "nomreg": r.get("nomreg"),
                "color": SERVITUDES_MAP_COLOR,
                "label": label,
            },
        })
    return {"type": "FeatureCollection", "features": features}
