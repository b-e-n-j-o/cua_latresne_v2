import json
import os

import psycopg2
from fastapi import APIRouter, HTTPException
from psycopg2 import sql

router = APIRouter(prefix="/latresne")
communes_router = APIRouter(prefix="/communes")

SUPABASE_HOST = str(os.getenv("SUPABASE_HOST") or "").strip().strip('"').strip("'")
SUPABASE_PORT = str(os.getenv("SUPABASE_PORT") or "5432").strip().strip('"').strip("'")
if "pooler.supabase.com" in SUPABASE_HOST.lower() and SUPABASE_PORT == "5432":
    SUPABASE_PORT = "6543"

def _load_commune_table_mapping() -> dict[str, tuple[str, str]]:
    """
    Whitelist des communes autorisees.
    Format env optionnel CADASTRE_COMMUNES_TABLES:
    {"latresne":"latresne.parcelles","argeles":"argeles.parcelles","mios":"mios.parcelles"}
    """
    default_mapping = {
        "latresne": ("latresne", "parcelles"),
        "argeles": ("argeles", "parcelles"),
        "mios": ("mios", "parcelles"),
    }
    raw = (os.getenv("CADASTRE_COMMUNES_TABLES") or "").strip()
    if not raw:
        return default_mapping

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return default_mapping

    if not isinstance(parsed, dict):
        return default_mapping

    cleaned: dict[str, tuple[str, str]] = {}
    for slug, value in parsed.items():
        if not isinstance(slug, str):
            continue
        if not isinstance(value, str) or "." not in value:
            continue
        schema, table = value.split(".", 1)
        schema = schema.strip()
        table = table.strip()
        if not schema or not table:
            continue
        cleaned[slug.strip().lower()] = (schema, table)

    return cleaned or default_mapping


CADASTRE_TABLES = _load_commune_table_mapping()


def _resolve_table_for_commune(slug: str) -> tuple[str, str]:
    key = (slug or "").strip().lower()
    if key not in CADASTRE_TABLES:
        raise HTTPException(
            status_code=404,
            detail=f"Commune non supportee: {slug}",
        )
    return CADASTRE_TABLES[key]


def _slug_to_commune_label(slug: str) -> str:
    value = (slug or "").strip().replace("-", " ")
    if not value:
        return ""
    return value.title()


def _build_geojson_payload(schema_name: str, table_name: str, commune_label: str) -> dict:
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=int(SUPABASE_PORT),
    )
    cur = None
    try:
        cur = conn.cursor()
        query = sql.SQL(
            """
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(
                    json_agg(
                        json_build_object(
                            'type', 'Feature',
                            'properties', json_build_object(
                                'section', section,
                                'numero', numero,
                                'commune', %s,
                                'insee', code_insee,
                                'contenance', contenance
                            ),
                            'geometry', ST_AsGeoJSON(ST_Transform(geom_2154, 4326))::json
                        )
                    ),
                    '[]'::json
                )
            )
            FROM {}.{}
            WHERE geom_2154 IS NOT NULL
            """
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
        cur.execute(query, (commune_label,))
        payload = cur.fetchone()[0]
        return payload or {"type": "FeatureCollection", "features": []}
    finally:
        if cur:
            cur.close()
        conn.close()


@router.get("/parcelles/geojson")
def get_all_parcelles():
    schema_name, table_name = _resolve_table_for_commune("latresne")
    return _build_geojson_payload(schema_name, table_name, "Latresne")


@communes_router.get("/{commune_slug}/parcelles/geojson")
def get_all_parcelles_by_commune(commune_slug: str):
    schema_name, table_name = _resolve_table_for_commune(commune_slug)
    commune_label = _slug_to_commune_label(commune_slug)
    return _build_geojson_payload(schema_name, table_name, commune_label)