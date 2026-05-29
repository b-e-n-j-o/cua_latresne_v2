"""Fetch générique d'une couche décrite par LayerSpec (hors socle GPU)."""

from __future__ import annotations

import json
import logging

import psycopg2

from ...commune_context import current_schema, q
from ...layer_catalog import LayerSpec
from .db import db_query
from .servitudes import _geom_2154_sql
from .zonage import strict_parcel_intersection_filter_sql

logger = logging.getLogger("plu_tools")

TXT_MAX_LLM = 800


def _sql_attr(alias: str, column: str) -> str:
    """Colonne SQL (guillemets si casse mixte, ex. Ap, Zone_Regl_)."""
    safe = str(column).replace('"', '""')
    return f'{alias}."{safe}"'


def _entity_geom_sql(alias: str, spec: LayerSpec) -> str:
    if spec.geom_transform == "sup_geometry":
        return _geom_2154_sql(alias)
    col = f"{alias}.{spec.geom_column}"
    return f"ST_MakeValid({col})"


def _clipped_geojson_sql(entity: str, buffer_alias: str = "cb") -> str:
    """Découpe la géométrie au buffer d'affichage (même logique que le zonage carto)."""
    ix = f"ST_Force2D(ST_Intersection({entity}, {buffer_alias}.geom))"
    return f"""
        , ST_AsGeoJSON(
            ST_Transform(
                CASE
                    WHEN ST_GeometryType({ix}) IN ('ST_Polygon', 'ST_MultiPolygon')
                    THEN ST_Multi(ST_CollectionExtract({ix}, 3))
                    WHEN ST_GeometryType({ix}) IN ('ST_LineString', 'ST_MultiLineString')
                    THEN ST_Multi(ST_CollectionExtract({ix}, 2))
                    ELSE {ix}
                END,
                4326
            )
        ) AS geojson_geom"""


def _sql_layer(
    spec: LayerSpec,
    with_geojson: bool,
    *,
    display_buffer_m: float | None = None,
) -> tuple[str, tuple]:
    alias = "p"
    entity = _entity_geom_sql(alias, spec)
    attrs = (
        ", ".join(_sql_attr("p", a) for a in spec.attributes)
        if spec.attributes
        else "p.*"
    )

    clip_carto = with_geojson and display_buffer_m is not None
    buf = float(display_buffer_m or 0)

    if clip_carto:
        geom_sel = _clipped_geojson_sql(entity)
        scope_cte = """
        cible_scope AS (SELECT geom FROM cible),
        cible_buffer AS (
            SELECT ST_MakeValid(
                CASE WHEN %s::float > 0 THEN ST_Buffer(geom, %s::float) ELSE geom END
            ) AS geom
            FROM cible
        )"""
        clip_ix = f"ST_Intersection({entity}, cb.geom)"
        clip_nonempty = f"""
          AND NOT ST_IsEmpty({clip_ix})
          AND (
            ST_Area({clip_ix}) > 0.01
            OR ST_Length({clip_ix}) > 0.01
            OR ST_GeometryType({entity}) IN ('ST_Point', 'ST_MultiPoint')
          )"""
        params = (buf, buf)
        join_scope = "cible_scope c, cible_buffer cb"
        if spec.inclu_buffer:
            intersect = f"""
          AND ST_Intersects({entity}, cb.geom)
          {clip_nonempty}"""
        else:
            intersect = f"""
          AND ST_Intersects({entity}, c.geom)
          AND {strict_parcel_intersection_filter_sql(entity)}
          {clip_nonempty}"""
    else:
        geom_sel = (
            f", ST_AsGeoJSON(ST_Transform(ST_Force2D({entity}), 4326)) AS geojson_geom"
            if with_geojson
            else ""
        )
        strict = spec.strict_parcel
        if strict:
            scope_cte = "cible_scope AS (SELECT geom FROM cible)"
            intersect = f"""
          AND ST_Intersects({entity}, c.geom)
          AND {strict_parcel_intersection_filter_sql(entity)}"""
            params = ()
            join_scope = "cible_scope c"
        else:
            scope_cte = """
        cible_scope AS (
            SELECT ST_MakeValid(
                CASE WHEN %s::float > 0 THEN ST_Buffer(geom, %s::float) ELSE geom END
            ) AS geom FROM cible
        )"""
            intersect = f" AND ST_Intersects({entity}, c.geom)"
            params = (0.0, 0.0)
            join_scope = "cible_scope c"

    null_geom = f"p.{spec.geom_column} IS NOT NULL"

    sql = f"""
        WITH cible AS (
            SELECT ST_MakeValid(ST_GeomFromEWKB(%s)) AS geom
        ),
        {scope_cte}
        SELECT {attrs}{geom_sel}
        FROM {q(spec.table)} {alias}
        CROSS JOIN {join_scope}
        WHERE {null_geom}
          {intersect}
    """
    return sql, params


def fetch_layer_rows(
    db_config: dict,
    geom_wkb: bytes,
    spec: LayerSpec,
    *,
    with_geojson: bool = False,
    display_buffer_m: float | None = None,
) -> list[dict]:
    if not spec.enabled:
        return []
    sql, extra_params = _sql_layer(
        spec, with_geojson, display_buffer_m=display_buffer_m
    )
    try:
        if extra_params:
            return db_query(db_config, sql, (geom_wkb, *extra_params))
        return db_query(db_config, sql, (geom_wkb,))
    except psycopg2.Error as e:
        if spec.optional:
            logger.warning("fetch_layer_rows — %s ignoré : %s", spec.id, e)
            return []
        raise


def _llm_attr_label(spec: LayerSpec, index: int, column: str) -> str:
    labels = spec.attribute_labels
    if labels and index < len(labels):
        return labels[index]
    return column


def _llm_attr_value(column: str, val) -> str | None:
    if val is None:
        return None
    if column in ("txt", "reglementation", "laius_reglement", "legende") and val:
        txt = str(val).strip()
        if len(txt) > TXT_MAX_LLM:
            return txt[:TXT_MAX_LLM] + "…"
        return txt or None
    s = str(val).strip()
    return s if s else None


def rows_to_llm_items(rows: list[dict], spec: LayerSpec) -> list[dict]:
    items = []
    for r in rows:
        item: dict = {"layer_id": spec.id, "group": spec.group}
        if spec.title:
            item["couche"] = spec.title
        if spec.kind:
            item["kind"] = spec.kind
        if spec.subgroup:
            item["subgroup"] = spec.subgroup
        for i, key in enumerate(spec.attributes):
            label = _llm_attr_label(spec, i, key)
            item[label] = _llm_attr_value(key, r.get(key))
        items.append(item)
    return items


def rows_to_map_features(rows: list[dict], spec: LayerSpec) -> list[dict]:
    features = []
    for r in rows:
        raw = r.get("geojson_geom")
        if not raw:
            continue
        geom = raw if isinstance(raw, dict) else json.loads(raw)
        props = {}
        for i, col in enumerate(spec.attributes):
            if col not in r:
                continue
            label = _llm_attr_label(spec, i, col)
            props[label] = r.get(col)
        props["layer_id"] = spec.id
        props["group"] = spec.group
        if spec.title:
            props["couche"] = spec.title
        if spec.kind:
            props["kind"] = spec.kind
        if spec.color:
            props["color"] = spec.color
        label = (
            r.get("libelle")
            or r.get("nom")
            or r.get("nom_servitude")
            or r.get("denom")
            or spec.title
            or spec.id
        )
        props["label"] = label
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": props,
        })
    return features


def fetch_extra_layers_llm(
    db_config: dict,
    geom_wkb: bytes,
    catalog=None,
) -> dict:
    """
    Retourne { "couches_supplementaires": { group: [ items ] }, "count": N }.
    """
    from .catalog_bridge import active_catalog, extra_layers

    cat = catalog or active_catalog()
    sch = current_schema()
    specs = list(extra_layers(cat, context_llm=True))
    groups: dict[str, list[dict]] = {}
    total = 0
    hit_parts: list[str] = []

    for spec in specs:
        rows = fetch_layer_rows(db_config, geom_wkb, spec, with_geojson=False)
        n = len(rows)
        if not n:
            logger.debug(
                "get_extra_layers — %s → 0 [%s.%s]",
                spec.id,
                sch,
                spec.table,
            )
            continue
        items = rows_to_llm_items(rows, spec)
        g = spec.group or "autre"
        groups.setdefault(g, []).extend(items)
        total += len(items)
        hit_parts.append(f"{spec.id}({n})")
        logger.info(
            "get_extra_layers — %s → %d entité(s) [%s.%s]",
            spec.id,
            n,
            sch,
            spec.table,
        )

    if hit_parts:
        logger.info(
            "get_extra_layers — %d entité(s) sur %d couche(s) : %s",
            total,
            len(hit_parts),
            ", ".join(hit_parts),
        )
    else:
        logger.info(
            "get_extra_layers — aucune intersection (%d couche(s) testées, schéma %s)",
            len(specs),
            sch,
        )

    return {"couches_supplementaires": groups, "couches_supplementaires_count": total}


def fetch_extra_layers_carto(
    db_config: dict,
    geom_wkb: bytes,
    catalog=None,
    *,
    buffer_m: float = 100.0,
) -> dict:
    """
    Retourne { layer_id: FeatureCollection } pour la carte.
    Géométries découpées au buffer d'affichage (aligné zonage PLU).
    """
    from .catalog_bridge import active_catalog, extra_layers

    cat = catalog or active_catalog()
    sch = current_schema()
    buf = float(buffer_m)
    out: dict = {}
    hit_parts: list[str] = []
    specs = list(extra_layers(cat, context_carto=True))
    for spec in specs:
        rows = fetch_layer_rows(
            db_config,
            geom_wkb,
            spec,
            with_geojson=True,
            display_buffer_m=buf,
        )
        features = rows_to_map_features(rows, spec)
        if features:
            hit_parts.append(f"{spec.id}({len(features)})")
            logger.info(
                "get_extra_layers_carto — %s → %d feature(s) [%s.%s]",
                spec.id,
                len(features),
                sch,
                spec.table,
            )
        out[spec.id] = {"type": "FeatureCollection", "features": features}
    if hit_parts:
        logger.info(
            "get_extra_layers_carto — %s",
            ", ".join(hit_parts),
        )
    elif specs:
        logger.info(
            "get_extra_layers_carto — aucune intersection (%d couche(s), schéma %s)",
            len(specs),
            sch,
        )
    return out
