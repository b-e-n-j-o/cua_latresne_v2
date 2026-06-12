#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de test unitaire pour le builder du CUA
Permet de tester la génération du CUA DOCX indépendamment du pipeline complet.
"""

import os
import json
import subprocess
from pathlib import Path

# ===============================
# CONFIGURATION DES CHEMINS
# ===============================

BASE_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

# Entrées
cerfa_path = os.path.join(ROOT_DIR, "out_pipeline", "cerfa_result.json")
intersections_path = os.path.join(ROOT_DIR, "out_pipeline", "rapport_unite_fonciere.json")
catalogue_path = os.path.join(BASE_DIR, "catalogue_avec_articles.json")

# Sortie DOCX
output_path = os.path.join(BASE_DIR, "out_pipeline", "CUA_test.docx")

# Logos
logo_latresne_path = os.path.join(ROOT_DIR, "logos", "logo_latresne.png")
logo_kerelia_path = os.path.join(ROOT_DIR, "logos", "logo_kerelia.png")

# Builder (module déplacé sous CUA/docx/)
builder_path = os.path.join(ROOT_DIR, "docx", "cua_builder.py")

# ===============================
# VÉRIFICATION DES ENTRÉES
# ===============================
print("\n🧩 Vérification des fichiers d'entrée...")
for path in [cerfa_path, catalogue_path]:
    if not os.path.exists(path):
        print(f"❌ Fichier manquant : {path}")
        exit(1)
print("✅ Tous les fichiers d'entrée essentiels sont présents.")

# L'intersections_json peut parfois être manquant pour le test :
if not os.path.exists(intersections_path):
    print(f"⚠️ Aucun rapport d'intersections trouvé à : {intersections_path}")
    print("   → Le builder s'exécutera avec un contenu d'intersection vide.")
    # On crée un faux rapport vide minimal
    fake_inters = {"surface_m2": 1600, "intersections": {}}
    with open(intersections_path, "w", encoding="utf-8") as f:
        json.dump(fake_inters, f, indent=2, ensure_ascii=False)

# ===============================
# COMMANDE DE TEST
# ===============================
cmd = [
    "python3", builder_path,
    "--cerfa-json", cerfa_path,
    "--intersections-json", intersections_path,
    "--catalogue-json", catalogue_path,
    "--output", output_path,
    "--logo-first-page", logo_latresne_path,
    "--signature-logo", logo_kerelia_path,
    "--qr-url", "https://kerelia.fr/m/test123",
    "--plu-nom", "PLU de Latresne",
    "--plu-date-appro", "13/02/2017"
]

print("\n🚀 Exécution du builder avec la commande :")
print(" ".join(cmd))

# ===============================
# LANCEMENT DU BUILDER
# ===============================
try:
    subprocess.run(cmd, check=True)
    print(f"\n✅ Test terminé avec succès.")
    print(f"📄 CUA généré : {output_path}")
except subprocess.CalledProcessError as e:
    print(f"\n💥 Échec de génération du CUA : {e}")
