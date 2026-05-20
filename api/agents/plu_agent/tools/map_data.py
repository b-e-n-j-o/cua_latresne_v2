"""Tool get_map_data — GeoJSON parcelle + zones PLU pour MapLibre."""

import json

import psycopg2
import psycopg2.extras
from google.genai import types


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
    """Palette CNIG/GPU — miroir de plu_visuels.py."""
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
    """True si le GeoJSON contient au moins un polygone affichable."""
    if not geom:
        return False
    t = geom.get("type")
    if t == "Polygon":
        return bool(geom.get("coordinates"))
    if t == "MultiPolygon":
        return bool(geom.get("coordinates"))
    if t == "GeometryCollection":
        return any(_geometry_has_area(g) for g in geom.get("geometries") or [])
    return False


def get_map_data(
    db_config: dict,
    section: str = None,
    numero: str = None,
    idu: str = None,
    geojson: str = None,
    buffer_m: float = 100.0,
) -> dict:
    """
    Retourne un GeoJSON Feature (parcelle) + FeatureCollection (zones PLU) en EPSG:4326.

    - Sélection des zones : intersection avec parcelle + buffer (mètres, EPSG:2154).
    - Géométries affichées : partie de chaque zone PLU dans le buffer (contexte carto).
    - pct_parcelle_couverte : part de la parcelle couverte par la zone (hors buffer).
    """
    try:
        if geojson:
            geom_param = geojson if isinstance(geojson, str) else json.dumps(geojson)
            sql_geom = """
                SELECT
                    ST_AsEWKB(
                        ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 2154)
                    ) AS geom_wkb,
                    ST_AsGeoJSON(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)) AS geojson_parcelle
            """
            rows_geom = _query(db_config, sql_geom, (geom_param, geom_param))
            props = {}

        elif idu:
            sql_geom = """
                SELECT
                    ST_AsEWKB(geom_2154) AS geom_wkb,
                    ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geojson_parcelle,
                    idu, section, numero, contenance
                FROM argeles.parcelles
                WHERE idu = %s
                LIMIT 1
            """
            rows_geom = _query(db_config, sql_geom, (idu,))
            props = (
                {k: rows_geom[0].get(k) for k in ("idu", "section", "numero", "contenance")}
                if rows_geom
                else {}
            )

        elif section and numero:
            section_norm = section.upper().strip()
            numero_norm = str(numero).strip()
            sql_geom = """
                SELECT
                    ST_AsEWKB(geom_2154) AS geom_wkb,
                    ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geojson_parcelle,
                    idu, section, numero, contenance
                FROM argeles.parcelles
                WHERE section = %s
                  AND lpad(numero, 4, '0') = lpad(%s, 4, '0')
                LIMIT 1
            """
            rows_geom = _query(db_config, sql_geom, (section_norm, numero_norm))
            props = (
                {k: rows_geom[0].get(k) for k in ("idu", "section", "numero", "contenance")}
                if rows_geom
                else {}
            )

        else:
            return {"error": "Fournir section+numero, idu, ou geojson."}

        if not rows_geom or rows_geom[0]["geom_wkb"] is None:
            return {"error": "Parcelle introuvable."}

        geom_wkb = rows_geom[0]["geom_wkb"]
        geojson_parcelle = rows_geom[0]["geojson_parcelle"]

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

        features = []
        for z in rows_zones:
            geom_z = z.get("geojson_zone")
            if not geom_z:
                continue
            geom_obj = json.loads(geom_z)
            if not _geometry_has_area(geom_obj):
                continue
            code = z["code_zone"]
            tz = z.get("typezone")
            features.append({
                "type": "Feature",
                "geometry": geom_obj,
                "properties": {
                    "code_zone": code,
                    "libelle": z.get("libelle"),
                    "libelong": z.get("libelong"),
                    "typezone": tz,
                    "destdomi": z.get("destdomi"),
                    "pct_parcelle_couverte": float(z["pct_parcelle_couverte"])
                        if z.get("pct_parcelle_couverte") is not None else None,
                    "color": _zone_color(tz, code),
                },
            })

        return {
            "parcelle": {
                "type": "Feature",
                "geometry": json.loads(geojson_parcelle),
                "properties": props,
            },
            "zones": {"type": "FeatureCollection", "features": features},
            "error": None,
        }

    except Exception as e:
        return {"error": str(e)}


DECL_MAP_DATA = types.FunctionDeclaration(
    name="get_map_data",
    description=(
        "Affiche la carte interactive pour l'utilisateur (parcelle + zones PLU colorées). "
        "À appeler quand l'utilisateur demande la carte, la localisation ou une vue du zonage. "
        "Tu reçois un résumé (codes de zone, %, libellés) — pas les coordonnées GeoJSON. "
        "Ne contient pas les textes réglementaires — utiliser get_zonage_et_reglements pour l'analyse."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "section": types.Schema(
                type=types.Type.STRING,
                description="Section cadastrale (ex: 'AC').",
            ),
            "numero": types.Schema(
                type=types.Type.STRING,
                description="Numéro de parcelle (ex: '8770').",
            ),
            "idu": types.Schema(
                type=types.Type.STRING,
                description="IDU complet.",
            ),
            "geojson": types.Schema(
                type=types.Type.STRING,
                description="GeoJSON WGS84 si géométrie ad hoc.",
            ),
            "buffer_m": types.Schema(
                type=types.Type.NUMBER,
                description="Buffer autour de la parcelle en mètres (défaut: 100).",
            ),
        },
    ),
)
