# -*- coding: utf-8 -*-
"""
carto_context.py — GeoJSON zone d'étude (UF + buffer) pour affichage carto isolé.

Sélection : entités intersectant le buffer contexte (context_buffer_m, défaut 200 m).
Affichage  : géométries découpées au display_clip_m (défaut 1000 m) autour de l'UF.
Métriques  : toujours calculées sur l'intersection réelle avec la parcelle / UF.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from api.cuas.db import GEOM_COL, SRID, get_engine, logger
from api.cuas.intersections import (
    MIN_INTERSECTION_AREA_M2,
    MIN_INTERSECTION_LENGTH_M,
    _safe_ident,
    _table_exists,
    load_catalogue,
)
from api.cuas.uf import UniteFonciere, build_uf

DEFAULT_CONTEXT_BUFFER_M = 200.0
MAX_CONTEXT_BUFFER_M = 500.0
DEFAULT_DISPLAY_CLIP_M = 1000.0
MAX_DISPLAY_CLIP_M = 2000.0


def _parse_geojson(raw: Any) -> dict | None:
    if not raw:
        return None
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def _parcelle_feature(uf: UniteFonciere, engine) -> dict:
    sql = text(f"""
        SELECT ST_AsGeoJSON(
            ST_Transform(ST_MakeValid(ST_GeomFromText(:wkt, {SRID})), 4326)
        ) AS geojson
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"wkt": uf.wkt}).mappings().one()
    geom = _parse_geojson(row["geojson"])
    props: dict[str, Any] = {
        "nb_parcelles": uf.n_parcelles,
        "surface_m2": round(uf.surface_sig, 2),
        "contenance_m2": round(uf.surface_cadastrale, 2) if uf.surface_cadastrale else None,
        "parcelles": [{"section": s, "numero": n} for s, n in uf.parcelles],
    }
    if uf.n_parcelles == 1:
        s, n = uf.parcelles[0]
        props["section"] = s
        props["numero"] = n
    return {"type": "Feature", "geometry": geom, "properties": props}


def _display_geojson_sql(entity: str, clip_from: str = "uf_disp") -> str:
    """Découpe l'entité au buffer d'affichage (réduit le poids et le bruit visuel)."""
    clip = f"ST_Force2D(ST_Intersection({entity}, {clip_from}.geom))"
    return f"""
        ST_AsGeoJSON(
            ST_Transform(
                CASE
                    WHEN ST_GeometryType({clip}) IN ('ST_Polygon', 'ST_MultiPolygon')
                    THEN ST_Multi(ST_CollectionExtract({clip}, 3))
                    WHEN ST_GeometryType({clip}) IN ('ST_LineString', 'ST_MultiLineString')
                    THEN ST_Multi(ST_CollectionExtract({clip}, 2))
                    WHEN ST_GeometryType({clip}) IN ('ST_Point', 'ST_MultiPoint')
                    THEN {clip}
                    ELSE NULL
                END,
                4326
            )
        ) AS geojson
    """


def _fetch_layer_features(
    uf_wkt: str,
    table: str,
    cfg: dict,
    buffer_m: float,
    display_clip_m: float,
    surface_sig: float,
    engine,
    schema: str,
) -> list[dict]:
    table = _safe_ident(table)
    geom_col = _safe_ident(cfg.get("geom_col", GEOM_COL))
    keep = [_safe_ident(k) for k in cfg.get("keep", [])]
    geom_type = cfg.get("geom_type", "surfacique")

    t_cols = "".join(f"t.{k}, " for k in keep)
    raw_cols = "".join(f"{k}, " for k in keep)

    entity = f"ST_MakeValid(t.{geom_col})"

    if geom_type == "surfacique":
        parcel_ix = f"ST_Intersection({entity}, uf.geom)"
        parcel_metric = f"ST_Area({parcel_ix})"
        parcel_hit = f"{parcel_metric} > {MIN_INTERSECTION_AREA_M2}"
        buf_ix = f"ST_Intersection({entity}, uf_buf.geom)"
        buf_nonempty = f"""
          AND NOT ST_IsEmpty({buf_ix})
          AND ST_Area({buf_ix}) > {MIN_INTERSECTION_AREA_M2}
        """
        metric_sel = f"""
            CASE WHEN {parcel_hit} THEN ROUND(({parcel_metric})::numeric, 2) END AS surface_inter_m2,
            CASE WHEN {parcel_hit} AND {surface_sig} > 0
                 THEN ROUND(({parcel_metric} / {surface_sig} * 100)::numeric, 4) END AS pct_sig,
            NULL::float AS longueur_inter_m
        """
    elif geom_type == "lineaire":
        parcel_ix = f"ST_Intersection({entity}, uf.geom)"
        parcel_metric = f"ST_Length({parcel_ix})"
        parcel_hit = f"{parcel_metric} > {MIN_INTERSECTION_LENGTH_M}"
        buf_ix = f"ST_Intersection({entity}, uf_buf.geom)"
        buf_nonempty = f"""
          AND NOT ST_IsEmpty({buf_ix})
          AND ST_Length({buf_ix}) > {MIN_INTERSECTION_LENGTH_M}
        """
        metric_sel = f"""
            NULL::float AS surface_inter_m2,
            NULL::float AS pct_sig,
            CASE WHEN {parcel_hit} THEN ROUND(({parcel_metric})::numeric, 2) END AS longueur_inter_m
        """
    else:
        parcel_hit = f"ST_Within({entity}, uf.geom)"
        buf_nonempty = f" AND ST_Intersects({entity}, uf_buf.geom)"
        metric_sel = """
            NULL::float AS surface_inter_m2,
            NULL::float AS pct_sig,
            NULL::float AS longueur_inter_m
        """

    display_ix = f"ST_Intersection({entity}, uf_disp.geom)"
    display_nonempty = f"""
          AND NOT ST_IsEmpty({display_ix})
          AND (
            ST_Area({display_ix}) > {MIN_INTERSECTION_AREA_M2}
            OR ST_Length({display_ix}) > {MIN_INTERSECTION_LENGTH_M}
            OR ST_GeometryType({entity}) IN ('ST_Point', 'ST_MultiPoint')
          )
    """
    geojson_sel = _display_geojson_sql(entity)

    sql = text(f"""
        WITH uf AS (
            SELECT ST_GeomFromText(:wkt, {SRID}) AS geom
        ),
        uf_buf AS (
            SELECT ST_MakeValid(ST_Buffer(geom, :buffer_m)) AS geom FROM uf
        ),
        uf_disp AS (
            SELECT ST_MakeValid(ST_Buffer(geom, :display_clip_m)) AS geom FROM uf
        )
        SELECT {raw_cols}
               {geojson_sel},
               ({parcel_hit}) AS intersects_parcel,
               {metric_sel}
        FROM {schema}.{table} t
        CROSS JOIN uf
        CROSS JOIN uf_buf
        CROSS JOIN uf_disp
        WHERE t.{geom_col} IS NOT NULL
          AND ST_Intersects({entity}, uf_buf.geom)
          {buf_nonempty}
          {display_nonempty}
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "wkt": uf_wkt,
                "buffer_m": float(buffer_m),
                "display_clip_m": float(display_clip_m),
            },
        ).mappings().all()

    features = []
    for idx, r in enumerate(rows):
        geom = _parse_geojson(r["geojson"])
        if not geom:
            continue
        props = {k: r[k] for k in keep}
        props["intersects_parcel"] = bool(r["intersects_parcel"])
        if r.get("surface_inter_m2") is not None:
            props["surface_inter_m2"] = float(r["surface_inter_m2"])
        if r.get("pct_sig") is not None:
            props["pct_sig"] = float(r["pct_sig"])
        if r.get("longueur_inter_m") is not None:
            props["longueur_inter_m"] = float(r["longueur_inter_m"])
        props["_fid"] = f"{table}-{idx}"
        features.append({"type": "Feature", "geometry": geom, "properties": props})

    return features


def run_carto_context(
    uf: UniteFonciere,
    catalogue: dict,
    carto_catalogue: dict | None,
    *,
    context_buffer_m: float = DEFAULT_CONTEXT_BUFFER_M,
    display_clip_m: float = DEFAULT_DISPLAY_CLIP_M,
    engine=None,
    schema: str = "argeles",
) -> dict:
    engine = engine or get_engine()
    buffer_m = min(max(0.0, float(context_buffer_m)), MAX_CONTEXT_BUFFER_M)
    clip_m = min(max(0.0, float(display_clip_m)), MAX_DISPLAY_CLIP_M)

    carto_layers = (carto_catalogue or {}).get("layers") or {}
    families = {f["id"]: f["title"] for f in (carto_catalogue or {}).get("families") or []}

    parcelle = _parcelle_feature(uf, engine)
    layers_out: dict[str, dict] = {}

    for table, cfg in catalogue.items():
        carto_meta = carto_layers.get(table) or {}
        if carto_meta.get("src") == "geojson":
            continue

        geom_type_cfg = cfg.get("geom_type", "surfacique")
        family_id = carto_meta.get("family") or "_other"
        title = carto_meta.get("title") or cfg.get("nom") or table

        layer_info: dict[str, Any] = {
            "layer_id": table,
            "title": title,
            "family": family_id,
            "family_title": families.get(family_id, "Autres"),
            "geom_type": geom_type_cfg,
            "tip": carto_meta.get("tip"),
            "group": carto_meta.get("group"),
            "legend": carto_meta.get("legend"),
            "features": {"type": "FeatureCollection", "features": []},
            "status": "pending",
        }

        if not _table_exists(engine, schema, table):
            layer_info["status"] = "table_absente"
            layers_out[table] = layer_info
            continue

        try:
            features = _fetch_layer_features(
                uf.wkt, table, cfg, buffer_m, clip_m, uf.surface_sig, engine, schema
            )
            layer_info["features"]["features"] = features
            layer_info["status"] = "ok" if features else "empty"
            if features:
                logger.info(f"  🗺  {table:<35} {len(features):>3} entité(s)")
        except Exception as exc:
            logger.warning(f"  ⚠  carto {table}: {exc}")
            layer_info["status"] = "erreur"
            layer_info["error"] = str(exc)

        layers_out[table] = layer_info

    n_with_features = sum(
        1 for l in layers_out.values() if l.get("features", {}).get("features")
    )

    return {
        "parcelle": parcelle,
        "parcelles": [{"section": s, "numero": n} for s, n in uf.parcelles],
        "surface_m2": round(uf.surface_sig, 2),
        "context_buffer_m": buffer_m,
        "context_buffer_max_m": buffer_m,
        "display_clip_m": clip_m,
        "n_layers": len(layers_out),
        "n_layers_with_features": n_with_features,
        "layers": layers_out,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }


def load_carto_catalogue(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))
