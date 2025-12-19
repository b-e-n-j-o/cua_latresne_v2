from fastapi import APIRouter
import psycopg2
import json
import os

router = APIRouter()

def get_db_connection():
    """Crée une nouvelle connexion PostgreSQL."""
    return psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=5432,
    )

@router.get("/communes")
def get_communes():
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT
              insee,
              nom,
              ST_AsGeoJSON(ST_Transform(geom_2154, 4326))
            FROM public.communes
            WHERE geom_2154 IS NOT NULL
        """)

        features = []
        for insee, nom, geom in cur.fetchall():
            features.append({
                "type": "Feature",
                "geometry": json.loads(geom),
                "properties": {
                    "insee": insee,
                    "nom": nom
                }
            })

        return {
            "type": "FeatureCollection",
            "features": features
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
