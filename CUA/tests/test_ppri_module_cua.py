#!/usr/bin/env python3
# test_ppri_cua.py

import sys
sys.path.append('./CUA')

from ppri_cua_module import analyser_ppri_corrige

# Test avec votre WKT d'unité foncière
wkt_path = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/out_pipeline/20251104_185314/geom_unite_fonciere.wkt"

with open(wkt_path, 'r') as f:
    geom_wkt = f.read().strip()

print("🧪 Test du module PPRI CUA\n")

resultats = analyser_ppri_corrige(
    geom_wkt=geom_wkt,
    code_insee="33234"
)

print(f"✅ Zones détectées: {len(resultats.get('zones_avec_regles', []))}")
print(f"✅ Zone dominante: {resultats.get('zone_dominante', {}).get('nom')}")
print(f"✅ Multi-zones: {resultats.get('cas_multizone')}\n")

for z in resultats.get('zones_avec_regles', []):
    print(f"📍 {z['nom']}: {z['surface_m2']} m² ({z['pourcentage']}%)")
    print(f"   Texte: {z['texte'][:100]}...")
    print()