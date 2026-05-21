"""Informations GPU — requêtes, carte GeoJSON, contexte LLM."""

from __future__ import annotations

import json
import logging

import psycopg2

from .db import db_query
from .parcel_geom import resolve_unite_fonciere

logger = logging.getLogger("plu_tools")

INFOS_CONFIG: dict[str, dict] = {
    "surfaciques": {
        "table": "argeles.infos_surf",
        "kind": "surfacique",
        "color": "#2A9D8F",
    },
    "lineaires": {
        "table": "argeles.infos_lin",
        "kind": "lineaire",
        "color": "#1D3557",
    },
    "ponctuelles": {
        "table": "argeles.infos_pct",
        "kind": "ponctuelle",
        "color": "#F4A261",
    },
}


def _sql_infos(table: str, with_geojson: bool) -> str:
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
            p.typeinf,
            p.stypeinf
            {geom_sel}
        FROM {table} p
        CROSS JOIN cible_scope c
        WHERE p.geom_2154 IS NOT NULL
          AND ST_Intersects(ST_MakeValid(p.geom_2154), c.geom)
        ORDER BY p.libelle NULLS LAST, p.gml_id;
    """


def fetch_infos_rows(
    db_config: dict,
    geom_wkb: bytes,
    buffer_m: float = 0.0,
    with_geojson: bool = False,
) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    buf = float(buffer_m or 0)
    for key, cfg in INFOS_CONFIG.items():
        try:
            sql = _sql_infos(cfg["table"], with_geojson)
            out[key] = db_query(db_config, sql, (geom_wkb, buf, buf))
        except psycopg2.Error:
            out[key] = []
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


def _rows_to_llm_list(rows: list[dict], kind: str, txt_max: int = 800) -> list[dict]:
    items = []
    for r in rows:
        txt = (r.get("txt") or "").strip()
        if len(txt) > txt_max:
            txt = txt[:txt_max] + "…"
        items.append({
            "gml_id": r.get("gml_id"),
            "libelle": r.get("libelle"),
            "txt": txt or None,
            "typeinf": r.get("typeinf"),
            "stypeinf": r.get("stypeinf"),
            "kind": kind,
        })
    return items


def build_llm_payload(rows_by_kind: dict[str, list[dict]]) -> dict:
    surf = _rows_to_llm_list(rows_by_kind.get("surfaciques") or [], "surfacique")
    lin = _rows_to_llm_list(rows_by_kind.get("lineaires") or [], "lineaire")
    pct = _rows_to_llm_list(rows_by_kind.get("ponctuelles") or [], "ponctuelle")
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
    cfg = INFOS_CONFIG[kind_key]
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
                "typeinf": r.get("typeinf"),
                "stypeinf": r.get("stypeinf"),
                "kind": cfg["kind"],
                "layer": "information",
                "color": cfg["color"],
                "label": r.get("libelle") or r.get("typeinf") or r.get("gml_id"),
            },
        })
    return features


def build_map_infos(rows_by_kind: dict[str, list[dict]]) -> dict:
    return {
        key: {
            "type": "FeatureCollection",
            "features": _rows_to_features(rows_by_kind.get(key) or [], key),
        }
        for key in INFOS_CONFIG
    }


def get_infos(
    db_config: dict,
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str = None,
    numero: str = None,
    idu: str = None,
    buffer_m: float = 0.0,
) -> dict:
    try:
        resolved = resolve_unite_fonciere(
            db_config,
            parcelles=parcelles,
            idus=idus,
            section=section,
            numero=numero,
            idu=idu,
        )
        if resolved.get("error"):
            logger.warning("get_infos — %s", resolved["error"])
            return {
                "surfaciques": [],
                "lineaires": [],
                "ponctuelles": [],
                "count": 0,
                "error": resolved["error"],
            }

        rows_by_kind = fetch_infos_rows(
            db_config,
            resolved["geom_wkb"],
            buffer_m=buffer_m,
            with_geojson=False,
        )
        payload = build_llm_payload(rows_by_kind)

        logger.info(
            "get_infos — %d surf, %d lin, %d pct",
            payload["count_surfaciques"],
            payload["count_lineaires"],
            payload["count_ponctuelles"],
        )

        return {
            **payload,
            "parcelles": resolved.get("parcelles") or [],
            "nb_parcelles": resolved.get("nb_parcelles"),
            "superficie_unite_m2": resolved.get("superficie_m2"),
            "error": None,
        }

    except Exception as e:
        return {
            "surfaciques": [],
            "lineaires": [],
            "ponctuelles": [],
            "count": 0,
            "error": str(e),
        }
