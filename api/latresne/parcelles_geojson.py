from fastapi import APIRouter
import os
import psycopg2
import json

router = APIRouter(prefix="/latresne")

@router.get("/parcelles/geojson")
def get_all_parcelles():
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=5432
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