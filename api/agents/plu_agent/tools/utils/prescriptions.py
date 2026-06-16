"""Prescriptions PLU — requêtes, carte GeoJSON, contexte LLM."""

from __future__ import annotations

import json
import logging

import psycopg2

from ...commune_context import q
from .catalog_bridge import prescription_config
from .db import db_query
from .parcel_geom import resolve_unite_fonciere
from .intersection_metrics import (
    apply_surfacic_metrics_to_item,
    is_surfacic_layer,
    surfacic_metrics_select_sql,
)
from .zonage import strict_parcel_intersection_filter_sql

logger = logging.getLogger("plu_tools")


def _sql_prescriptions(
    table: str,
    with_geojson: bool,
    strict_parcel: bool = True,
    with_area_metrics: bool = False,
) -> str:
    entity_geom = "ST_MakeValid(p.geom_2154)"
    geom_sel = (
        f", ST_AsGeoJSON(ST_Transform(ST_Force2D({entity_geom}), 4326)) AS geojson_geom"
        if with_geojson
        else ""
    )
    metrics_sel = (
        f",{surfacic_metrics_select_sql(entity_geom)}"
        if with_area_metrics
        else ""
    )
    if strict_parcel:
        scope_cte = """
        cible_scope AS (
            SELECT geom FROM cible
        )"""
        intersect_filter = f"""
          AND ST_Intersects({entity_geom}, c.geom)
          AND {strict_parcel_intersection_filter_sql(entity_geom)}"""
    else:
        scope_cte = """
        cible_scope AS (
            SELECT ST_MakeValid(
                CASE WHEN %s::float > 0 THEN ST_Buffer(geom, %s::float) ELSE geom END
            ) AS geom
            FROM cible
        )"""
        intersect_filter = f" AND ST_Intersects({entity_geom}, c.geom)"

    return f"""
        WITH cible AS (
            SELECT ST_MakeValid(ST_GeomFromEWKB(%s)) AS geom
        ),
        {scope_cte}
        SELECT
            p.gml_id,
            p.libelle,
            p.txt,
            p.typepsc,
            p.stypepsc
            {geom_sel}{metrics_sel}
        FROM {q(table)} p
        CROSS JOIN cible_scope c
        WHERE p.geom_2154 IS NOT NULL
          {intersect_filter}
        ORDER BY p.libelle NULLS LAST, p.gml_id;
    """


def fetch_prescriptions_rows(
    db_config: dict,
    geom_wkb: bytes,
    buffer_m: float = 0.0,
    with_geojson: bool = False,
    strict_parcel: bool = True,
) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    buf = float(buffer_m or 0)
    cfg_map = prescription_config()
    for key, cfg in cfg_map.items():
        if not cfg.get("context_carto") and with_geojson:
            continue
        if not cfg.get("context_llm") and not with_geojson:
            continue
        sql = _sql_prescriptions(
            cfg["table"],
            with_geojson,
            strict_parcel=strict_parcel,
            with_area_metrics=is_surfacic_layer(
                kind=cfg.get("kind"),
                subgroup=key,
            ),
        )
        try:
            if strict_parcel:
                out[key] = db_query(db_config, sql, (geom_wkb,))
            else:
                out[key] = db_query(db_config, sql, (geom_wkb, buf, buf))
        except psycopg2.Error as e:
            if cfg.get("optional"):
                logger.warning(
                    "fetch_prescriptions_rows — %s (%s) ignoré : %s",
                    key,
                    cfg["table"],
                    e,
                )
                out[key] = []
            else:
                raise
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
    include_metrics = is_surfacic_layer(kind=kind)
    items = []
    for r in rows:
        txt = (r.get("txt") or "").strip()
        if len(txt) > txt_max:
            txt = txt[:txt_max] + "…"
        item = {
            "gml_id": r.get("gml_id"),
            "libelle": r.get("libelle"),
            "txt": txt or None,
            "typepsc": r.get("typepsc"),
            "stypepsc": r.get("stypepsc"),
            "kind": kind,
        }
        if include_metrics:
            apply_surfacic_metrics_to_item(item, r)
        items.append(item)
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
    cfg = prescription_config().get(kind_key) or {}
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
    keys = set(prescription_config()) | set(rows_by_kind.keys())
    return {
        key: {
            "type": "FeatureCollection",
            "features": _rows_to_features(rows_by_kind.get(key) or [], key),
        }
        for key in keys
    }


def get_prescriptions(
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
            logger.warning("get_prescriptions — %s", resolved["error"])
            return {
                "surfaciques": [],
                "lineaires": [],
                "ponctuelles": [],
                "count": 0,
                "error": resolved["error"],
            }

        if not prescription_config():
            return {
                "surfaciques": [],
                "lineaires": [],
                "ponctuelles": [],
                "count": 0,
                "parcelles": resolved.get("parcelles") or [],
                "nb_parcelles": resolved.get("nb_parcelles"),
                "superficie_unite_m2": resolved.get("superficie_m2"),
                "error": None,
            }

        rows_by_kind = fetch_prescriptions_rows(
            db_config,
            resolved["geom_wkb"],
            buffer_m=buffer_m,
            with_geojson=False,
        )
        payload = build_llm_payload(rows_by_kind)

        logger.info(
            "get_prescriptions — %d surf, %d lin, %d pct",
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
