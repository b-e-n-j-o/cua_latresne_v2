"""Tools zonage PLU — get_zonage_et_reglements, get_zones_for_geometry."""

import json
import logging

import psycopg2
import psycopg2.extras
from google.genai import types

logger = logging.getLogger("plu_tools")


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


def get_zonage_et_reglements(
    db_config: dict,
    geojson: str = None,
    idu: str = None,
    section: str = None,
    numero: str = None,
) -> dict:
    try:
        if geojson:
            geom_param = geojson if isinstance(geojson, str) else json.dumps(geojson)
            sql_geom = """
                SELECT ST_AsEWKB(
                    ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 2154)
                ) AS geom_wkb
            """
            rows_geom = _query(db_config, sql_geom, (geom_param,))

        elif idu:
            sql_geom = (
                "SELECT ST_AsEWKB(geom_2154) AS geom_wkb "
                "FROM argeles.parcelles WHERE idu = %s LIMIT 1"
            )
            rows_geom = _query(db_config, sql_geom, (idu,))

        elif section and numero:
            section_norm = section.upper().strip()
            numero_norm = str(numero).strip()
            sql_geom = """
                SELECT ST_AsEWKB(geom_2154) AS geom_wkb, idu, numero
                FROM argeles.parcelles
                WHERE section = %s
                  AND lpad(numero, 4, '0') = lpad(%s, 4, '0')
                LIMIT 1
            """
            logger.info(
                "get_zonage_et_reglements — parcelle section=%r numero=%r",
                section_norm, numero_norm,
            )
            rows_geom = _query(db_config, sql_geom, (section_norm, numero_norm))

        else:
            return {"zones": [], "count": 0, "error": "Fournir geojson, idu, ou section+numero."}

        if not rows_geom or rows_geom[0]["geom_wkb"] is None:
            logger.warning(
                "get_zonage_et_reglements — géométrie introuvable "
                "(geojson=%s, idu=%r, section=%r, numero=%r)",
                bool(geojson), idu, section, numero,
            )
            return {
                "zones": [],
                "count": 0,
                "error": (
                    f"Géométrie introuvable pour les paramètres fournis "
                    f"(section={section!r}, numero={numero!r}, idu={idu!r})."
                ),
            }

        if section and numero and rows_geom[0].get("idu"):
            logger.info(
                "get_zonage_et_reglements — parcelle idu=%s numero_db=%r",
                rows_geom[0]["idu"], rows_geom[0].get("numero"),
            )

        geom_wkb = rows_geom[0]["geom_wkb"]

        sql = """
            WITH cible AS (SELECT %s::geometry AS geom)
            SELECT
                z.zonage_reglement                                            AS code_zone,
                z.libelle, z.libelong, z.typezone, z.destdomi,
                ROUND(ST_Area(ST_Intersection(z.geom_2154, c.geom))::numeric, 1)
                                                                              AS superficie_intersection_m2,
                ROUND((ST_Area(ST_Intersection(z.geom_2154, c.geom))
                       / NULLIF(ST_Area(c.geom), 0) * 100)::numeric, 1)      AS pct_parcelle_couverte,
                r.nom_zone, r.resume_zone, r.reglementation
            FROM argeles.zonage_plu z
            CROSS JOIN cible c
            LEFT JOIN argeles.plu_reglement r ON r.code_zone = z.zonage_reglement
            WHERE ST_Intersects(z.geom_2154, c.geom)
            ORDER BY superficie_intersection_m2 DESC;
        """
        rows = _query(db_config, sql, (geom_wkb,))
        return {"zones": rows, "count": len(rows), "error": None}

    except Exception as e:
        return {"zones": [], "count": 0, "error": str(e)}


def get_zones_for_geometry(db_config: dict, geojson: str) -> dict:
    try:
        geom_dict = json.loads(geojson) if isinstance(geojson, str) else geojson
        geojson_str = json.dumps(geom_dict)
    except (json.JSONDecodeError, TypeError) as e:
        return {"zones": [], "count": 0, "error": f"GeoJSON invalide : {e}"}

    sql = """
        SELECT DISTINCT
            z.zonage_reglement AS code_zone,
            z.libelle, z.libelong, z.typezone, z.destdomi
        FROM argeles.zonage_plu z
        WHERE ST_Intersects(
            z.geom_2154,
            ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 2154)
        )
        ORDER BY z.zonage_reglement;
    """
    try:
        rows = _query(db_config, sql, (geojson_str,))
        return {"zones": rows, "count": len(rows), "error": None}
    except Exception as e:
        return {"zones": [], "count": 0, "error": str(e)}


DECL_ZONAGE = types.FunctionDeclaration(
    name="get_zonage_et_reglements",
    description=(
        "Intersecte une parcelle ou une géométrie avec le zonage PLU d'Argelès-sur-Mer "
        "et retourne pour chaque zone : le code de zone, le libellé, la superficie "
        "d'intersection en m², le pourcentage de couverture de la parcelle, "
        "et le texte réglementaire complet. "
        "Accepte soit un GeoJSON (WGS84), soit une référence cadastrale (section+numero ou idu). "
        "À utiliser pour toute question sur la constructibilité, les usages autorisés, "
        "les règles d'implantation, de hauteur, ou de destination d'une parcelle."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "geojson": types.Schema(
                type=types.Type.STRING,
                description="Géométrie GeoJSON en WGS84 (Point, Polygon, MultiPolygon).",
            ),
            "section": types.Schema(
                type=types.Type.STRING,
                description="Section cadastrale (ex: 'AC').",
            ),
            "numero": types.Schema(
                type=types.Type.STRING,
                description="Numéro de parcelle (ex: '45', '8770'). Tolère les zéros en tête.",
            ),
            "idu": types.Schema(
                type=types.Type.STRING,
                description="IDU de la parcelle.",
            ),
        },
    ),
)

DECL_ZONES_GEOM = types.FunctionDeclaration(
    name="get_zones_for_geometry",
    description=(
        "Identifie rapidement les zones PLU intersectant une géométrie GeoJSON (WGS84), "
        "sans récupérer les textes réglementaires. "
        "Préférer get_zonage_et_reglements pour une analyse réglementaire complète."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "geojson": types.Schema(
                type=types.Type.STRING,
                description=(
                    'GeoJSON WGS84. Ex: {"type":"Point","coordinates":[3.0267,42.5467]}'
                ),
            ),
        },
        required=["geojson"],
    ),
)
