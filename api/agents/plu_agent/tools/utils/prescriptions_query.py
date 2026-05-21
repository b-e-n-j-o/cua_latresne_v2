"""Requêtes prescriptions PLU (surfaciques, linéaires, ponctuelles) — intersection unité foncière."""

from __future__ import annotations

import json

import psycopg2
import psycopg2.extras

# kind -> (table, libellé FR, couleur carte, opacité fill ou None)
PRESCRIPTION_CONFIG: dict[str, dict] = {
    "surfaciques": {
        "table": "argeles.prescriptions_surf",
        "kind": "surfacique",
        "color": "#9D4EDD",
        "fill_opacity": 0.4,
    },
    "lineaires": {
        "table": "argeles.prescriptions_lineaires",
        "kind": "lineaire",
        "color": "#E63946",
        "line_width": 3,
    },
    "ponctuelles": {
        "table": "argeles.prescriptions_ponctuelles",
        "kind": "ponctuelle",
        "color": "#FFBE0B",
        "circle_radius": 7,
    },
}


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


def _sql_prescriptions(table: str, with_geojson: bool) -> str:
    geom_sel = (
        ", ST_AsGeoJSON(ST_Transform(ST_Force2D(ST_MakeValid(p.geom_2154)), 4326)) AS geojson_geom"
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
            p.gml_id,
            p.libelle,
            p.txt,
            p.typepsc,
            p.stypepsc
            {geom_sel}
        FROM {table} p
        CROSS JOIN cible_scope c
        WHERE p.geom_2154 IS NOT NULL
          AND ST_Intersects(ST_MakeValid(p.geom_2154), c.geom)
        ORDER BY p.libelle NULLS LAST, p.gml_id;
    """


def fetch_prescriptions_rows(
    db_config: dict,
    geom_wkb: bytes,
    buffer_m: float = 0.0,
    with_geojson: bool = False,
) -> dict[str, list[dict]]:
    """Retourne les prescriptions intersectant l'unité foncière, par catégorie."""
    out: dict[str, list[dict]] = {}
    buf = float(buffer_m or 0)
    for key, cfg in PRESCRIPTION_CONFIG.items():
        sql = _sql_prescriptions(cfg["table"], with_geojson)
        rows = _query(db_config, sql, (geom_wkb, buf, buf))
        out[key] = rows
    return out


def _parse_geojson(val) -> dict | None:
    if not val:
        return None
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return None


def rows_to_llm_list(rows: list[dict], kind: str, txt_max: int = 800) -> list[dict]:
    items = []
    for r in rows:
        txt = (r.get("txt") or "").strip()
        if len(txt) > txt_max:
            txt = txt[:txt_max] + "…"
        items.append({
            "gml_id": r.get("gml_id"),
            "libelle": r.get("libelle"),
            "txt": txt or None,
            "typepsc": r.get("typepsc"),
            "stypepsc": r.get("stypepsc"),
            "kind": kind,
        })
    return items


def build_llm_payload(rows_by_kind: dict[str, list[dict]]) -> dict:
    surf = rows_to_llm_list(rows_by_kind.get("surfaciques") or [], "surfacique")
    lin = rows_to_llm_list(rows_by_kind.get("lineaires") or [], "lineaire")
    pct = rows_to_llm_list(rows_by_kind.get("ponctuelles") or [], "ponctuelle")
    total = len(surf) + len(lin) + len(pct)
    return {
        "surfaciques": surf,
        "lineaires": lin,
        "ponctuelles": pct,
        "count_surfaciques": len(surf),
        "count_lineaires": len(lin),
        "count_ponctuelles": len(pct),
        "count": total,
    }


def _rows_to_features(rows: list[dict], kind_key: str) -> list[dict]:
    cfg = PRESCRIPTION_CONFIG[kind_key]
    features = []
    for r in rows:
        geom = _parse_geojson(r.get("geojson_geom"))
        if not geom:
            continue
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "gml_id": r.get("gml_id"),
                "libelle": r.get("libelle"),
                "typepsc": r.get("typepsc"),
                "stypepsc": r.get("stypepsc"),
                "kind": cfg["kind"],
                "color": cfg["color"],
                "label": r.get("libelle") or r.get("typepsc") or r.get("gml_id"),
            },
        })
    return features


def build_map_prescriptions(rows_by_kind: dict[str, list[dict]]) -> dict:
    """FeatureCollections WGS84 pour MapLibre."""
    return {
        key: {
            "type": "FeatureCollection",
            "features": _rows_to_features(rows_by_kind.get(key) or [], key),
        }
        for key in PRESCRIPTION_CONFIG
    }
