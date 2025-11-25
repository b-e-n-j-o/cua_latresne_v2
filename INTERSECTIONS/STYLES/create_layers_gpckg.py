#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
from pathlib import Path
import geopandas as gpd
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

# -------------------------------------------------------------------
# CONSTANTES
# -------------------------------------------------------------------
SCHEMA = "latresne"

# 📌 Localiser correctement le catalogue
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # remonte à cua_latresne_v4
CATALOGUE_PATH = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/catalogues/catalogue_intersections_tagged.json"

print("📂 Catalogue utilisé :", CATALOGUE_PATH)

with open(CATALOGUE_PATH, "r", encoding="utf-8") as f:
    CATALOGUE = json.load(f)

# -------------------------------------------------------------------
# Connexion DB
# -------------------------------------------------------------------
DATABASE_URL = (
    f"postgresql+psycopg2://{os.environ['SUPABASE_USER']}:{os.environ['SUPABASE_PASSWORD']}"
    f"@{os.environ['SUPABASE_HOST']}:{os.environ['SUPABASE_PORT']}/{os.environ['SUPABASE_DB']}"
)
engine = create_engine(DATABASE_URL)

# -------------------------------------------------------------------
# EXPORT FUNCTION
# -------------------------------------------------------------------
def export_layer_to_gpkg(table_name, gpkg_path):
    print(f"→ export {table_name}")

    sql = f"""
        SELECT *, geom_2154 AS geometry
        FROM {SCHEMA}.{table_name}
        WHERE geom_2154 IS NOT NULL
    """

    gdf = gpd.read_postgis(sql, engine, geom_col="geometry")

    if gdf.empty:
        print(f"   (vide) ignoré")
        return

    # Écriture dans le GPKG
    gdf.to_file(gpkg_path, layer=table_name, driver="GPKG")
    print(f"   → OK ({len(gdf)} objets)")

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def generate_gpkg_model(output="modele_styles.gpkg"):

    gpkg_path = Path(output)
    if gpkg_path.exists():
        gpkg_path.unlink()

    print("📦 Création du GeoPackage modèle :", output)

    for table_name in CATALOGUE.keys():
        try:
            export_layer_to_gpkg(table_name, gpkg_path)
        except Exception as e:
            print(f"❌ erreur export {table_name} : {e}")

    print("🎉 GPKG modèle généré avec succès.")

if __name__ == "__main__":
    generate_gpkg_model()
