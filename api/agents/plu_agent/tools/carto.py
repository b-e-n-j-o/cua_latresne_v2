"""
Catalogue cartographique — GeoJSON pour MapLibre (hors LLM).

Consommé uniquement par GET /session/{id}/map.
Le LLM utilise get_contexte_parcelle pour le texte (zonage + prescriptions + servitudes).
"""

import json

import psycopg2
import psycopg2.extras

from .utils.parcel_geom import resolve_unite_fonciere
from .utils.prescriptions_query import build_map_prescriptions, fetch_prescriptions_rows
from .utils.servitudes_query import build_map_servitudes, fetch_servitudes_rows


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


def _zone_color(typezone: str | None, code_zone: str | None = None) -> str:
    label = (typezone or code_zone or "").upper().strip()
    if not label:
        return "#9CA3AF"
    if label.startswith("AU"):
        return "#E07A7A"
    if label.startswith("N"):
        return "#2D6A4F"
    if label.startswith("A"):
        return "#E9C46A"
    if label == "U":
        return "#C1121F"
    if label.startswith("U"):
        return "#D4574A"
    return "#D4A5A5"


def _geometry_has_area(geom: dict) -> bool:
    if not geom:
        return False
    t = geom.get("type")
    if t in ("Polygon", "MultiPolygon"):
        return bool(geom.get("coordinates"))
    if t == "GeometryCollection":
        return any(_geometry_has_area(g) for g in geom.get("geometries") or [])
    return False


def _parse_geojson(geojson_val) -> dict | None:
    if not geojson_val:
        return None
    if isinstance(geojson_val, dict):
        return geojson_val
    try:
        return json.loads(geojson_val)
    except (json.JSONDecodeError, TypeError):
        return None


def _parcelle_properties(resolved: dict) -> dict:
    parcelles = resolved.get("parcelles") or []
    if len(parcelles) == 1:
        p = parcelles[0]
        return {
            k: p.get(k)
            for k in ("idu", "section", "numero", "contenance")
            if p.get(k) is not None
        }
    contenance_totale = sum(
        int(p["contenance"]) for p in parcelles if p.get("contenance") is not None
    )
    return {
        "parcelles": parcelles,
        "nb_parcelles": len(parcelles),
        "contenance_totale": contenance_totale or None,
        "superficie_m2": resolved.get("superficie_m2"),
    }


def _build_parcelle_layers(resolved: dict) -> dict:
    parcelles = resolved.get("parcelles") or []
    unit_props = _parcelle_properties(resolved)
    union_geom = _parse_geojson(resolved.get("geojson_wgs84"))

    if len(parcelles) <= 1:
        p = parcelles[0] if parcelles else {}
        geom = _parse_geojson(p.get("geojson_wgs84")) or union_geom
        return {
            "parcelle": {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    **{k: p.get(k) for k in ("idu", "section", "numero", "contenance") if p.get(k)},
                    **unit_props,
                    "label": (
                        f"{p.get('section')} {p.get('numero')}"
                        if p.get("section") and p.get("numero")
                        else None
                    ),
                },
            },
            "parcelle_union": None,
        }

    features = []
    for p in parcelles:
        geom = _parse_geojson(p.get("geojson_wgs84"))
        if not geom or not _geometry_has_area(geom):
            continue
        section, numero = p.get("section"), p.get("numero")
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "idu": p.get("idu"),
                "section": section,
                "numero": numero,
                "contenance": p.get("contenance"),
                "label": f"{section} {numero}" if section and numero else p.get("idu"),
                "layer": "feuille",
            },
        })

    out = {
        "parcelle": {"type": "FeatureCollection", "features": features},
        "parcelle_union": None,
    }
    if union_geom and _geometry_has_area(union_geom):
        out["parcelle_union"] = {
            "type": "Feature",
            "geometry": union_geom,
            "properties": {**unit_props, "layer": "unite_fonciere"},
        }
    return out


def build_carto_payload(
    db_config: dict,
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str = None,
    numero: str = None,
    idu: str = None,
    buffer_m: float = 100.0,
) -> dict:
    """
    Payload GeoJSON complet pour la carte : parcelle(s), zonage PLU, prescriptions, servitudes.
    """
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
            return {"error": resolved["error"]}

        geom_wkb = resolved["geom_wkb"]
        parcelle_layers = _build_parcelle_layers(resolved)

        sql_zones = """
            WITH cible AS (
                SELECT ST_MakeValid(ST_GeomFromEWKB(%s)) AS geom
            ),
            cible_buffer AS (
                SELECT ST_MakeValid(ST_Buffer(geom, %s)) AS geom FROM cible
            ),
            zones_hits AS (
                SELECT
                    z.zonage_reglement AS code_zone,
                    z.libelle,
                    z.libelong,
                    z.typezone,
                    z.destdomi,
                    ST_MakeValid(z.geom_2154) AS geom_zone,
                    c.geom AS geom_parcelle,
                    cb.geom AS geom_buffer
                FROM argeles.zonage_plu z
                CROSS JOIN cible c
                CROSS JOIN cible_buffer cb
                WHERE ST_Intersects(ST_MakeValid(z.geom_2154), cb.geom)
            )
            SELECT DISTINCT ON (code_zone)
                code_zone,
                libelle,
                libelong,
                typezone,
                destdomi,
                ST_AsGeoJSON(
                    ST_Transform(
                        ST_Multi(
                            ST_CollectionExtract(
                                ST_Force2D(
                                    ST_Intersection(geom_zone, geom_buffer)
                                ),
                                3
                            )
                        ),
                        4326
                    )
                ) AS geojson_zone,
                ROUND(
                    (ST_Area(ST_Intersection(geom_zone, geom_parcelle))
                        / NULLIF(ST_Area(geom_parcelle), 0) * 100)::numeric,
                    1
                ) AS pct_parcelle_couverte
            FROM zones_hits
            WHERE ST_Area(ST_Intersection(geom_zone, geom_parcelle)) > 0
              AND NOT ST_IsEmpty(
                    ST_Intersection(geom_zone, geom_buffer)
                  )
            ORDER BY code_zone,
                     ST_Area(ST_Intersection(geom_zone, geom_parcelle)) DESC;
        """
        rows_zones = _query(db_config, sql_zones, (geom_wkb, buffer_m))

        zone_features = []
        for z in rows_zones:
            geom_z = z.get("geojson_zone")
            if not geom_z:
                continue
            geom_obj = json.loads(geom_z)
            if not _geometry_has_area(geom_obj):
                continue
            code = z["code_zone"]
            tz = z.get("typezone")
            zone_features.append({
                "type": "Feature",
                "geometry": geom_obj,
                "properties": {
                    "code_zone": code,
                    "libelle": z.get("libelle"),
                    "libelong": z.get("libelong"),
                    "typezone": tz,
                    "destdomi": z.get("destdomi"),
                    "pct_parcelle_couverte": float(z["pct_parcelle_couverte"])
                    if z.get("pct_parcelle_couverte") is not None
                    else None,
                    "color": _zone_color(tz, code),
                },
            })

        presc_rows = fetch_prescriptions_rows(
            db_config, geom_wkb, buffer_m=buffer_m, with_geojson=True
        )
        serv_rows = fetch_servitudes_rows(
            db_config, geom_wkb, buffer_m=buffer_m, with_geojson=True
        )

        return {
            "parcelle": parcelle_layers["parcelle"],
            "parcelle_union": parcelle_layers.get("parcelle_union"),
            "zones": {"type": "FeatureCollection", "features": zone_features},
            "prescriptions": build_map_prescriptions(presc_rows),
            "servitudes": build_map_servitudes(serv_rows),
            "error": None,
        }

    except Exception as e:
        return {"error": str(e)}
