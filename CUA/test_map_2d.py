#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_map2d_local.py — Test local du rendu de carte 2D Folium
-------------------------------------------------------------
Ce script permet de générer une carte Folium depuis un WKT
et d'ouvrir automatiquement le fichier HTML pour vérifier le rendu
avant déploiement.
"""

import os
import webbrowser
from map_2d import generate_map_from_wkt

# ============================================================
# 🔧 CONFIGURATION DE TEST
# ============================================================

# Exemple : chemin vers un WKT d'unité foncière déjà généré
WKT_PATH = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/CUA/geom_unite_fonciere.wkt"

# Dossier de sortie (sera créé s’il n’existe pas)
OUTPUT_DIR = "./out_map2d_test"

# Code INSEE de la commune (ex : Latresne = 33234)
CODE_INSEE = "33234"

# Inclure le PPRI (True / False)
INCLURE_PPRI = True

# Nom de la table PPRI
PPRI_TABLE = "pm1_detaillee_gironde"

# ============================================================
# 🚀 GÉNÉRATION
# ============================================================

print("🌍 Génération de la carte 2D de test...")
os.makedirs(OUTPUT_DIR, exist_ok=True)

try:
    html_string, metadata = generate_map_from_wkt(
        wkt_path=WKT_PATH,
        code_insee=CODE_INSEE,
        inclure_ppri=INCLURE_PPRI,
        ppri_table=PPRI_TABLE
    )

    output_file = os.path.join(OUTPUT_DIR, "carte_2d_test.html")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_string)

    print("✅ Carte générée avec succès :")
    print("   ", os.path.abspath(output_file))
    print("\n📊 Métadonnées :")
    for k, v in metadata.items():
        print(f"   - {k}: {v}")

    # Ouvrir automatiquement dans le navigateur
    webbrowser.open("file://" + os.path.abspath(output_file))

except Exception as e:
    print(f"❌ Erreur : {e}")
