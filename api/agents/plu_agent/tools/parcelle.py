"""Tool get_parcelle — informations cadastrales et géométrie."""

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


def get_parcelle(
    db_config: dict,
    section: str = None,
    numero: str = None,
    idu: str = None,
) -> dict:
    if idu:
        sql = """
            SELECT
                idu, numero, section, contenance, code_insee,
                ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geojson_wgs84,
                ST_Area(geom_2154)                          AS superficie_m2
            FROM argeles.parcelles
            WHERE idu = %s
            LIMIT 1;
        """
        params = (idu,)
    elif section and numero:
        sql = """
            SELECT
                idu, numero, section, contenance, code_insee,
                ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geojson_wgs84,
                ST_Area(geom_2154)                          AS superficie_m2
            FROM argeles.parcelles
            WHERE section = %s
              AND lpad(numero, 4, '0') = lpad(%s, 4, '0')
            LIMIT 1;
        """
        params = (section.upper().strip(), str(numero).strip())
    else:
        return {"error": "Fournir soit idu, soit section + numero.", "parcelle": None}

    try:
        rows = _query(db_config, sql, params)
        if not rows:
            return {"error": f"Parcelle introuvable ({params})", "parcelle": None}
        return {"parcelle": rows[0], "error": None}
    except Exception as e:
        return {"error": str(e), "parcelle": None}


DECL_PARCELLE = types.FunctionDeclaration(
    name="get_parcelle",
    description=(
        "Récupère les informations cadastrales et la géométrie d'une parcelle "
        "d'Argelès-sur-Mer (section+numéro ou IDU). "
        "Retourne superficie, coordonnées et GeoJSON WGS84."
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
                description="Numéro de parcelle (ex: '45', '8770'). Tolère les zéros en tête.",
            ),
            "idu": types.Schema(
                type=types.Type.STRING,
                description="IDU complet (ex: '66008000AC0045').",
            ),
        },
    ),
)
