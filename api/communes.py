from fastapi import APIRouter, Query
import psycopg2
import json
import os

router = APIRouter()

SUPABASE_HOST = str(os.getenv("SUPABASE_HOST") or "").strip().strip('"').strip("'")
SUPABASE_PORT = str(os.getenv("SUPABASE_PORT") or "5432").strip().strip('"').strip("'")
if "pooler.supabase.com" in SUPABASE_HOST.lower() and SUPABASE_PORT == "5432":
    SUPABASE_PORT = "6543"

def get_db_connection():
    """Crée une nouvelle connexion PostgreSQL."""
    return psycopg2.connect(
        host=SUPABASE_HOST,
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=int(SUPABASE_PORT),
    )

@router.get("/communes")
def get_communes(departement: str | None = Query(default=None, min_length=2, max_length=3)):
    """
    Retourne les communes en GeoJSON.
    
    Args:
        departement: Code département (2-3 caractères, requis).
                    Filtre les communes par département.
                    Ex: ?departement=33 pour la Gironde
    
    Returns:
        FeatureCollection GeoJSON des communes
    """
    # Blocage si aucun département n'est fourni (sécurité prod)
    if not departement:
        return {
            "type": "FeatureCollection",
            "features": []
        }
    
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Requête avec filtre département (toujours présent ici)
        sql = """
            SELECT
              insee,
              nom,
              ST_AsGeoJSON(ST_Transform(geom_2154, 4326))
            FROM public.communes
            WHERE geom_2154 IS NOT NULL
            AND insee LIKE %s
        """
        
        cur.execute(sql, (f"{departement}%",))

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
