# backend/routes/geo.py
from fastapi import APIRouter, Depends
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

SUPABASE_HOST = str(os.getenv("SUPABASE_HOST") or "").strip().strip('"').strip("'")
SUPABASE_PORT = str(os.getenv("SUPABASE_PORT") or "5432").strip().strip('"').strip("'")
if "pooler.supabase.com" in SUPABASE_HOST.lower() and SUPABASE_PORT == "5432":
    SUPABASE_PORT = "6543"

# Fonction pour obtenir une connexion (plus propre)
def get_db_conn():
    return psycopg2.connect(
        host=SUPABASE_HOST,
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=int(SUPABASE_PORT),
    )

@router.get("/departements")
def get_departements():
    conn = get_db_conn()
    cur = conn.cursor()
    # On simplifie un peu (0.005 ~500m) pour la fluidité réseau
    cur.execute("""
        SELECT 
          insee, 
          nom, 
          ST_AsGeoJSON(ST_Transform(ST_Simplify(geom_2154, 2000), 4326)) 
        FROM public.departements
    """)
    
    features = []
    for insee, nom, geom in cur.fetchall():
        features.append({
            "type": "Feature",
            "geometry": json.loads(geom),
            "properties": {"insee": insee, "nom": nom}
        })
    conn.close()
    return {"type": "FeatureCollection", "features": features}

@router.get("/communes")
def get_communes():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT insee, nom, ST_AsGeoJSON(ST_Transform(geom_2154, 4326))
        FROM public.communes
        WHERE geom_2154 IS NOT NULL
    """)
    features = []
    for insee, nom, geom in cur.fetchall():
        features.append({
            "type": "Feature",
            "geometry": json.loads(geom),
            "properties": {"insee": insee, "nom": nom}
        })
    conn.close()
    return {"type": "FeatureCollection", "features": features}