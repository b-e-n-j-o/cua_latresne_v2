import json
import shapely.wkt
import shapely.geometry
from shapely.ops import unary_union
from pathlib import Path

def export_geojson_intersections(wkt_path, intersections_data, output_path):
    """
    Exporte un GeoJSON contenant uniquement les intersections.
    
    Args:
        wkt_path: Chemin vers le fichier WKT de l'unité foncière
        intersections_data: Dict Python contenant les intersections (déjà chargé)
        output_path: Chemin de sortie du GeoJSON
    """
    # Charger UF
    uf_geom = shapely.wkt.loads(Path(wkt_path).read_text())

    # intersections_data est déjà un dict chargé
    intersections = intersections_data

    features = []

    # Si la structure contient "intersections", on l'utilise, sinon on itère directement
    if isinstance(intersections, dict) and "intersections" in intersections:
        intersections_dict = intersections["intersections"]
    else:
        intersections_dict = intersections

    for layer_key, layer_data in intersections_dict.items():
        objets = layer_data.get("objets", []) or layer_data.get("items", [])
        
        for obj in objets:
            # géométrie intersectée
            geom_wkt = obj.get("geom") or obj.get("geom_intersection")
            if not geom_wkt:
                continue

            try:
                geom = shapely.wkt.loads(geom_wkt)
            except:
                continue

            properties = {
                "layer": layer_key,
                "label": obj.get("label"),
                "id": obj.get("id"),
                "surface_m2": obj.get("surface_m2") or obj.get("surface"),
                "type": obj.get("type") or "unknown",
            }

            feature = {
                "type": "Feature",
                "properties": properties,
                "geometry": shapely.geometry.mapping(geom)
            }
            features.append(feature)

    fc = {
        "type": "FeatureCollection",
        "features": features
    }

    Path(output_path).write_text(json.dumps(fc, indent=2), encoding="utf-8")
    return output_path
