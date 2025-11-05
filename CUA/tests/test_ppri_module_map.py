# -*- coding: utf-8 -*-
"""
Test unitaire pour ppri_module.py
Permet de vérifier le passage de ppri_table et la requête SQL sous-jacente.
"""

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import folium

from ppri_map_module import ajouter_ppri_a_carte

# ============================================================
# 1️⃣ Connexion à la base
# ============================================================
load_dotenv()
HOST = os.getenv("SUPABASE_HOST")
DB = os.getenv("SUPABASE_DB")
USER = os.getenv("SUPABASE_USER")
PWD = os.getenv("SUPABASE_PASSWORD")
PORT = os.getenv("SUPABASE_PORT", 5432)

engine = create_engine(f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
print("✅ Connexion établie à la base Supabase/PostGIS.")

# ============================================================
# 2️⃣ Création d'une carte Folium vide
# ============================================================
map_folium = folium.Map(location=[44.802, -0.516], zoom_start=16)

# ============================================================
# 3️⃣ Exécution du module PPRI
# ============================================================
section = "AC"
numero = "0496"
code_insee = "33234"
ppri_table = "latresne.pm1_detaillee_gironde"  # ✅ explicite

try:
    meta = ajouter_ppri_a_carte(
        map_folium,
        section=section,
        numero=numero,
        code_insee=code_insee,
        ppri_table=ppri_table,
        engine=engine,
        geom_wkt=None,  # optionnel
        show=True
    )
    print("\n✅ Test terminé avec succès.")
    print("📊 Résumé des métadonnées renvoyées :")
    for k, v in meta.items():
        print(f" - {k}: {v}")
except Exception as e:
    print("\n❌ Erreur lors de l'exécution du module PPRI :")
    print(e)
