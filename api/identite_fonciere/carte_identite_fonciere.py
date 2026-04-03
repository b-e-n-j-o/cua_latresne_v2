"""
Génération d'une carte 2D minimale pour l'identité foncière (Folium).

Objectif:
- afficher l'unité foncière analysée
- afficher les couches effectivement intersectées
- fournir une légende simple et lisible
"""

import json
from typing import Any, Dict, List, Optional, Tuple

import folium
from folium import Element
from sqlalchemy import text

from .identite_fonciere import (
    CATALOGUE,
    IDENTITE_DB_SCHEMA,
    _attrs_sans_reglementation,
    _build_parcelle_geom_sql,
    _detect_input_srid,
    _find_geom_column,
    _pg_quote_ident,
    analyser_identite_fonciere,
    engine,
)


DEFAULT_LAYER_COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
]


def _pick_color(idx: int) -> str:
    return DEFAULT_LAYER_COLORS[idx % len(DEFAULT_LAYER_COLORS)]


def _fetch_uf_geojson_4326_and_center(
    geometry: Dict[str, Any],
    srid: Optional[int],
) -> Tuple[Dict[str, Any], float, float]:
    geom_json = json.dumps(geometry, ensure_ascii=False)
    input_srid = _detect_input_srid(geometry, srid)
    parcelle_geom_sql = _build_parcelle_geom_sql(input_srid)

    q = text(
        f"""
        WITH uf AS (
            SELECT {parcelle_geom_sql} AS geom_2154
        )
        SELECT
            ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS uf_geojson,
            ST_Y(ST_Transform(ST_Centroid(geom_2154), 4326)) AS lat,
            ST_X(ST_Transform(ST_Centroid(geom_2154), 4326)) AS lon
        FROM uf
    """
    )
    with engine.connect() as conn:
        row = conn.execute(q, {"geom_json": geom_json}).mappings().first()
    if not row or not row["uf_geojson"]:
        raise ValueError("Impossible de reprojeter la géométrie de l'unité foncière")
    return json.loads(row["uf_geojson"]), float(row["lat"]), float(row["lon"])


def _fetch_layer_features_4326(
    table: str,
    geometry: Dict[str, Any],
    srid: Optional[int],
    max_features: int = 300,
) -> List[Dict[str, Any]]:
    cfg = CATALOGUE.get(table, {})
    keep = cfg.get("keep", [])
    if not isinstance(keep, list):
        keep = []
    keep = [k for k in keep if isinstance(k, str) and k.strip()]

    geom_json = json.dumps(geometry, ensure_ascii=False)
    input_srid = _detect_input_srid(geometry, srid)
    parcelle_geom_sql = _build_parcelle_geom_sql(input_srid)

    with engine.connect() as conn:
        geom_col = _find_geom_column(conn, table, IDENTITE_DB_SCHEMA)
        if not geom_col:
            return []

        cols_rs = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name = :table
            """
            ),
            {"schema": IDENTITE_DB_SCHEMA, "table": table},
        )
        existing = {r[0] for r in cols_rs}
        attrs = [a for a in keep if a in existing]
        output_attrs = _attrs_sans_reglementation(attrs)[:3]

        select_attrs = ""
        if output_attrs:
            # PostgreSQL replie les identifiants non quotés en minuscules : t.Ap → t.ap (invalide si la colonne est "Ap").
            select_attrs = ", " + ", ".join(
                f"t.{_pg_quote_ident(a)} AS {_pg_quote_ident(a)}" for a in output_attrs
            )

        q = text(
            f"""
            WITH uf AS (
                SELECT {parcelle_geom_sql} AS geom_2154
            )
            SELECT
                ST_AsGeoJSON(
                    ST_Transform(
                        ST_Intersection(t.{geom_col}, uf.geom_2154),
                        4326
                    )
                ) AS geom
                {select_attrs}
            FROM {IDENTITE_DB_SCHEMA}.{table} t, uf
            WHERE t.{geom_col} IS NOT NULL
            AND ST_Intersects(t.{geom_col}, uf.geom_2154)
            LIMIT :limit
        """
        )
        rows = conn.execute(q, {"geom_json": geom_json, "limit": max_features}).mappings().all()

    features: List[Dict[str, Any]] = []
    for row in rows:
        geom_txt = row.get("geom")
        if not geom_txt:
            continue
        props: Dict[str, Any] = {}
        for k, v in row.items():
            if k == "geom" or v is None:
                continue
            props[k] = str(v)
        features.append(
            {
                "type": "Feature",
                "geometry": json.loads(geom_txt),
                "properties": props,
            }
        )
    return features


def generate_identite_fonciere_map_html(
    geometry: Dict[str, Any],
    commune: str,
    insee: Optional[str] = None,
    srid: Optional[int] = None,
    intersections: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Génère une carte 2D HTML minimale:
    - UF en rouge pointillé
    - une couche Folium par table intersectée
    - légende fixe avec nom de couche + nb entités
    """
    if intersections is None:
        identite = analyser_identite_fonciere(
            geometry=geometry,
            commune=commune,
            insee=insee,
            srid=srid,
        )
        intersections = identite.get("intersections", [])
        nb_intersections = identite.get("nb_intersections", len(intersections))
    else:
        nb_intersections = len(intersections)

    uf_geojson_4326, lat, lon = _fetch_uf_geojson_4326_and_center(geometry, srid)

    m = folium.Map(
        location=[lat, lon],
        zoom_start=17,
        max_zoom=22,
        tiles="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
        attr="© OpenStreetMap contributors © CARTO",
    )

    folium.GeoJson(
        {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": uf_geojson_4326, "properties": {}}]},
        name="Unité foncière",
        style_function=lambda _x: {
            "color": "#d32f2f",
            "weight": 3,
            "fillOpacity": 0.02,
            "dashArray": "6, 6",
        },
        show=True,
    ).add_to(m)

    legend_rows: List[str] = []
    for idx, layer in enumerate(intersections):
        table = layer.get("table")
        if not table:
            continue
        display_name = layer.get("display_name", table)
        color = _pick_color(idx)
        features = _fetch_layer_features_4326(table, geometry, srid)
        if not features:
            continue

        fg = folium.FeatureGroup(name=display_name, show=False)
        folium.GeoJson(
            {"type": "FeatureCollection", "features": features},
            style_function=lambda _x, c=color: {
                "color": c,
                "weight": 2,
                "fillOpacity": 0.28,
            },
            highlight_function=lambda _x, c=color: {
                "color": c,
                "weight": 4,
                "fillOpacity": 0.55,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=list(features[0]["properties"].keys()) if features and features[0]["properties"] else [],
                aliases=[f"{k} :" for k in (features[0]["properties"].keys() if features and features[0]["properties"] else [])],
                sticky=True,
                labels=True,
            ),
        ).add_to(fg)
        fg.add_to(m)
        legend_rows.append(
            f"<div style='display:flex;align-items:center;gap:8px;margin:4px 0;'>"
            f"<span style='display:inline-block;width:12px;height:12px;border-radius:2px;background:{color};'></span>"
            f"<span>{display_name}</span>"
            f"<span style='margin-left:auto;color:#666;'>×{len(features)}</span>"
            f"</div>"
        )

    folium.LayerControl(collapsed=False, position="topleft").add_to(m)

    legend_content = "".join(legend_rows)
    if not legend_content:
        legend_content = "<div style='color:#666;'>Aucune couche intersectée</div>"

    legend_html = (
        "<div style=\"position:absolute;right:10px;top:10px;z-index:9999;"
        "background:#fff;border:1px solid #d9d9d9;border-radius:8px;padding:10px;"
        "box-shadow:0 2px 8px rgba(0,0,0,0.15);max-width:340px;max-height:60vh;overflow:auto;"
        "font:12px/1.4 Arial,sans-serif;\">"
        "<div style='font-weight:700;font-size:13px;margin-bottom:8px;'>"
        "Couches intersectées"
        "</div>"
        f"{legend_content}"
        "</div>"
    )
    m.get_root().html.add_child(Element(legend_html))

    return {
        "success": True,
        "html": m.get_root().render(),
        "metadata": {
            "commune": commune,
            "insee": insee or "",
            "nb_couches": len(legend_rows),
            "nb_intersections": nb_intersections,
        },
        "intersections": intersections,
    }
