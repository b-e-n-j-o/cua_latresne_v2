from fastapi import APIRouter
import os
import psycopg2
import json

router = APIRouter(prefix="/latresne")

SUPABASE_HOST = str(os.getenv("SUPABASE_HOST") or "").strip().strip('"').strip("'")
SUPABASE_PORT = str(os.getenv("SUPABASE_PORT") or "5432").strip().strip('"').strip("'")
if "pooler.supabase.com" in SUPABASE_HOST.lower() and SUPABASE_PORT == "5432":
    SUPABASE_PORT = "6543"

@router.get("/parcelles/geojson")
def get_all_parcelles():
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=int(SUPABASE_PORT)
    )
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(
                    json_build_object(
                        'type', 'Feature',
                        'properties', json_build_object('section', section, 'numero', numero, 'commune', nom_com, 'insee', code_insee),
                        'geometry', ST_AsGeoJSON(ST_Transform(geom_2154, 4326))::json
                    )
                )
            )
            FROM latresne.parcelles_latresne
        """)
        return cur.fetchone()[0]
    finally:
        if cur:
            cur.close()
        conn.close()