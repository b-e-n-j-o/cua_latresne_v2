# -*- coding: utf-8 -*-
"""
export_geojson_full.py — Génère un GeoJSON complet :
- Unité foncière (WKT)
- Toutes les couches intersectées avec géométrie
"""

import json
from pathlib import Path
from shapely import wkt
from shapely.geometry import mapping
from shapely.ops import transform
import pyproj


def _to_wgs84(geom):
    """Convertit un Shapely geom EPSG:2154 → EPSG:4326."""
    project = pyproj.Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True).transform
    return transform(project, geom)


def export_geojson_unite_fonciere_complete(
    wkt_path: str,
    intersections_json_path: str,
    output_path: str,
    uf_properties: dict | None = None
):
    """
    Crée un GeoJSON contenant :
    - L'unité foncière
    - Toutes les couches intersectées (avec géométrie)
    """

    # ---------------------------------------------
    # 🔹 1) Charge la géométrie de l'unité foncière
    # ---------------------------------------------
    wkt_path = Path(wkt_path)
    uf_geom = wkt.loads(wkt_path.read_text())
    uf_geom_wgs84 = _to_wgs84(uf_geom)

    # Feature UF
    feature_uf = {
        "type": "Feature",
        "geometry": mapping(uf_geom_wgs84),
        "properties": {"layer": "unite_fonciere", **(uf_properties or {})},
    }

    # ---------------------------------------------
    # 🔹 2) Charge les intersections
    # ---------------------------------------------
    intersections = json.loads(Path(intersections_json_path).read_text())

    features = [feature_uf]

    # ---------------------------------------------
    # 🔹 3) Ajoute chaque couche intersectée
    # ---------------------------------------------
    for layer_key, meta in intersections["intersections"].items():

        objets = meta.get("objets", [])
        if not objets:
            continue  # pas d'intersection → ignore

        for obj in objets:
            # Le rapport contient toujours la géométrie via "geom_wkt" (format standardisé)
            wkt_geom = obj.get("geom_wkt")
            if not wkt_geom:
                continue

            geom = wkt.loads(wkt_geom)
            geom_wgs84 = _to_wgs84(geom)

            # Nettoie les propriétés : pas de surface brute
            props = {
                "layer": layer_key,
                "nom": meta.get("nom"),
                "type": meta.get("type"),
                **{k: v for k, v in obj.items() if k not in ("geom_wkt", "geom", "surface_m2", "surface_sig")}
            }

            features.append({
                "type": "Feature",
                "geometry": mapping(geom_wgs84),
                "properties": props,
            })

    # ---------------------------------------------
    # 🔹 4) Écrit le GeoJSON final
    # ---------------------------------------------
    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    Path(output_path).write_text(
        json.dumps(geojson, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    return output_path
