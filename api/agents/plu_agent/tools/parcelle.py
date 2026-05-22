"""Tool get_parcelle — informations cadastrales et géométrie."""

import json

import psycopg2
import psycopg2.extras
from google.genai import types

from ..commune_context import q
from .utils.parcel_geom import parcel_tool_properties, resolve_unite_fonciere


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
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str = None,
    numero: str = None,
    idu: str = None,
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
            return {"error": resolved["error"], "parcelle": None, "parcelles": []}

        meta = resolved.get("parcelles") or []
        rows = []
        for p in meta:
            sql = f"""
                SELECT
                    idu, numero, section, contenance, code_insee,
                    ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geojson_wgs84,
                    ST_Area(geom_2154) AS superficie_m2
                FROM {q("parcelles")}
                WHERE idu = %s
                LIMIT 1;
            """
            found = _query(db_config, sql, (p["idu"],))
            if found:
                rows.append(found[0])

        unite = {
            "geojson_wgs84": resolved.get("geojson_wgs84"),
            "superficie_m2": resolved.get("superficie_m2"),
            "nb_parcelles": resolved.get("nb_parcelles"),
        }

        if len(rows) == 1:
            return {
                "parcelle": rows[0],
                "parcelles": rows,
                "unite_fonciere": unite,
                "error": None,
            }

        return {
            "parcelle": None,
            "parcelles": rows,
            "unite_fonciere": unite,
            "error": None,
        }

    except Exception as e:
        return {"error": str(e), "parcelle": None, "parcelles": []}


DECL_PARCELLE = types.FunctionDeclaration(
    name="get_parcelle",
    description=(
        "Récupère les informations cadastrales et la géométrie d'une ou plusieurs parcelles "
        "contiguës (unité foncière). Plusieurs parcelles : parcelles[] ou idus[]. "
        "Retourne superficie, GeoJSON WGS84 de l'union et détail par feuille cadastrale."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties=parcel_tool_properties(),
    ),
)
