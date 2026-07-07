"""Servitudes SUP ({schema}.servitudes) — requêtes, carte GeoJSON, contexte LLM."""

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


def _geom_2154_sql(alias: str = "s", geom_column: str = "geometry") -> str:
    g = f"{alias}.{geom_column}"
    if geom_column == "geom_2154":
        return f"ST_MakeValid({g})"
    return f"""
        CASE
            WHEN ST_SRID({g}) = 2154 THEN ST_MakeValid({g})
            WHEN ST_SRID({g}) = 0 OR ST_SRID({g}) IS NULL THEN ST_MakeValid(ST_SetSRID({g}, 2154))
            ELSE ST_MakeValid(ST_Transform({g}, 2154))
        END
    """


def _attr_select_sql(attributes: tuple[str, ...]) -> str:
    if not attributes:
        return """
            s.id,
            s.suptype,
            s.nomsuplitt,
            s.typeass,
            s.type,
            s.tension,
            s.nom_sup,
            s.nom_captage,
            s.transporteur,
            s.cat_fluide
        """.strip()
    seen: set[str] = set()
    cols: list[str] = []
    for attr in attributes:
        if attr in seen:
            continue
        seen.add(attr)
        cols.append(f"s.{attr}")
    return ",\n            ".join(cols)


def _order_by_sql(attributes: tuple[str, ...]) -> str:
    parts: list[str] = []
    for col in ("nomsuplitt", "nom_servitude", "suptype"):
        if not attributes or col in attributes:
            parts.append(f"s.{col} NULLS LAST")
    if not attributes or "id" in attributes:
        parts.append("s.id")
    elif not attributes or "gid" in attributes:
        parts.append("s.gid")
    return ", ".join(parts) if parts else "s.suptype NULLS LAST"


def _sql_servitudes(
    table: str,
    with_geojson: bool,
    strict_parcel: bool = True,
    *,
    geom_column: str = "geometry",
    attributes: tuple[str, ...] = (),
) -> str:
    geom_2154 = _geom_2154_sql("s", geom_column)
    attr_sql = _attr_select_sql(attributes)
    order_sql = _order_by_sql(attributes)
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
            {attr_sql}
            {geom_sel}{metrics_sel}
        FROM {q(table)} s
        CROSS JOIN cible_scope c
        WHERE s.{geom_column} IS NOT NULL
          {intersect_filter}
        ORDER BY {order_sql};
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
    sql = _sql_servitudes(
        spec.table,
        with_geojson,
        strict_parcel=strict_parcel,
        geom_column=spec.geom_column,
        attributes=spec.attributes,
    )
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
    spec = servitudes_spec()
    attrs = spec.attributes if spec else ()
    items = []
    for r in rows:
        item = {"nom_servitude": _nom_servitude_label(r)}
        keys = attrs or (
            "suptype", "nomsuplitt", "typeass", "type", "tension",
            "nom_sup", "nom_captage", "transporteur", "cat_fluide",
        )
        for key in keys:
            val = r.get(key)
            if val is not None and str(val).strip() != "":
                item[key] = val
        apply_surfacic_metrics_to_item(item, r)
        items.append(item)
    return {"servitudes": items, "count": len(items)}


def build_map_servitudes(rows: list[dict]) -> dict:
    spec = servitudes_spec()
    map_color = (spec.color if spec else None) or "#457B9D"
    prop_keys = spec.attributes if spec and spec.attributes else (
        "suptype", "nomsuplitt", "typeass", "type", "tension", "nom_sup",
    )
    features = []
    for r in rows:
        geom = _parse_geojson(r.get("geojson_geom"))
        if not geom:
            continue
        nom_servitude = _nom_servitude_label(r)
        props = {
            "nom_servitude": nom_servitude,
            "color": map_color,
            "label": nom_servitude,
        }
        for key in prop_keys:
            val = r.get(key)
            if val is not None:
                props[key] = val
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": props,
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
