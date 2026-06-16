"""Servitudes SUP (sup_assiette_s) — requêtes, carte GeoJSON, contexte LLM."""

from __future__ import annotations

import json
import logging

import psycopg2

from ...commune_context import q
from .catalog_bridge import servitudes_spec
from .db import db_query
from .parcel_geom import resolve_unite_fonciere
from .intersection_metrics import (
    apply_surfacic_metrics_to_item,
    surfacic_metrics_select_sql,
)
from .zonage import strict_parcel_intersection_filter_sql

logger = logging.getLogger("plu_tools")


def _nom_servitude_label(row: dict) -> str:
    """Libellé affiché (légende, carte, LLM) — nom_servitude prioritaire sur suptype."""
    nom = (row.get("nom_servitude") or "").strip()
    if nom:
        return nom
    suptype = (row.get("suptype") or "").strip()
    if suptype:
        return suptype
    return (
        (row.get("nomsuplitt") or "").strip()
        or (row.get("typeass") or "").strip()
        or (row.get("nomass") or "").strip()
        or "Servitude"
    )


def _geom_2154_sql(alias: str = "s") -> str:
    g = f"{alias}.geometry"
    return f"""
        CASE
            WHEN ST_SRID({g}) = 2154 THEN ST_MakeValid({g})
            WHEN ST_SRID({g}) = 0 OR ST_SRID({g}) IS NULL THEN ST_MakeValid(ST_SetSRID({g}, 2154))
            ELSE ST_MakeValid(ST_Transform({g}, 2154))
        END
    """


def _sql_servitudes(
    table: str,
    with_geojson: bool,
    strict_parcel: bool = True,
) -> str:
    geom_2154 = _geom_2154_sql("s")
    geom_sel = (
        f", ST_AsGeoJSON(ST_Transform(ST_Force2D({geom_2154}), 4326)) AS geojson_geom"
        if with_geojson
        else ""
    )
    metrics_sel = "" if with_geojson else f",{surfacic_metrics_select_sql(geom_2154)}"
    if strict_parcel:
        scope_cte = """
        cible_scope AS (
            SELECT geom FROM cible
        )"""
        intersect_filter = f"""
          AND ST_Intersects({geom_2154}, c.geom)
          AND {strict_parcel_intersection_filter_sql(geom_2154)}"""
    else:
        scope_cte = """
        cible_scope AS (
            SELECT ST_MakeValid(
                CASE WHEN %s::float > 0 THEN ST_Buffer(geom, %s::float) ELSE geom END
            ) AS geom
            FROM cible
        )"""
        intersect_filter = f" AND ST_Intersects({geom_2154}, c.geom)"

    return f"""
        WITH cible AS (
            SELECT ST_MakeValid(ST_GeomFromEWKB(%s)) AS geom
        ),
        {scope_cte}
        SELECT
            s.gid,
            s.idass,
            s.nomass,
            s.suptype,
            s.nom_servitude,
            s.typeass,
            s.nomsuplitt,
            s.nomreg
            {geom_sel}{metrics_sel}
        FROM {q(table)} s
        CROSS JOIN cible_scope c
        WHERE s.geometry IS NOT NULL
          {intersect_filter}
        ORDER BY s.nom_servitude NULLS LAST, s.nomass NULLS LAST, s.gid;
    """


def fetch_servitudes_rows(
    db_config: dict,
    geom_wkb: bytes,
    buffer_m: float = 0.0,
    with_geojson: bool = False,
    strict_parcel: bool = True,
) -> list[dict]:
    spec = servitudes_spec()
    if not spec:
        return []
    buf = float(buffer_m or 0)
    sql = _sql_servitudes(spec.table, with_geojson, strict_parcel=strict_parcel)
    try:
        if strict_parcel:
            return db_query(db_config, sql, (geom_wkb,))
        return db_query(db_config, sql, (geom_wkb, buf, buf))
    except psycopg2.Error as e:
        if spec.optional:
            logger.warning(
                "fetch_servitudes_rows — %s ignoré : %s",
                spec.table,
                e,
            )
            return []
        raise


def _parse_geojson(val) -> dict | None:
    if not val:
        return None
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return None


def build_llm_payload(rows: list[dict]) -> dict:
    items = []
    for r in rows:
        item = {
            "gid": r.get("gid"),
            "idass": r.get("idass"),
            "nomass": r.get("nomass"),
            "nom_servitude": _nom_servitude_label(r),
            "suptype": r.get("suptype"),
            "typeass": r.get("typeass"),
            "nomsuplitt": r.get("nomsuplitt"),
            "nomreg": r.get("nomreg"),
        }
        apply_surfacic_metrics_to_item(item, r)
        items.append(item)
    return {"servitudes": items, "count": len(items)}


def build_map_servitudes(rows: list[dict]) -> dict:
    spec = servitudes_spec()
    map_color = (spec.color if spec else None) or "#457B9D"
    features = []
    for r in rows:
        geom = _parse_geojson(r.get("geojson_geom"))
        if not geom:
            continue
        nom_servitude = _nom_servitude_label(r)
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "gid": r.get("gid"),
                "idass": r.get("idass"),
                "nomass": r.get("nomass"),
                "nom_servitude": nom_servitude,
                "suptype": r.get("suptype"),
                "typeass": r.get("typeass"),
                "nomsuplitt": r.get("nomsuplitt"),
                "nomreg": r.get("nomreg"),
                "color": map_color,
                "label": nom_servitude,
            },
        })
    return {"type": "FeatureCollection", "features": features}


def get_servitudes(
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
            logger.warning("get_servitudes — %s", resolved["error"])
            return {"servitudes": [], "count": 0, "error": resolved["error"]}

        if not servitudes_spec():
            return {
                "servitudes": [],
                "count": 0,
                "parcelles": resolved.get("parcelles") or [],
                "nb_parcelles": resolved.get("nb_parcelles"),
                "superficie_unite_m2": resolved.get("superficie_m2"),
                "error": None,
            }

        rows = fetch_servitudes_rows(
            db_config,
            resolved["geom_wkb"],
            buffer_m=buffer_m,
            with_geojson=False,
        )
        payload = build_llm_payload(rows)
        logger.info("get_servitudes — %d assiette(s)", payload["count"])

        return {
            **payload,
            "parcelles": resolved.get("parcelles") or [],
            "nb_parcelles": resolved.get("nb_parcelles"),
            "superficie_unite_m2": resolved.get("superficie_m2"),
            "error": None,
        }

    except Exception as e:
        return {"servitudes": [], "count": 0, "error": str(e)}
