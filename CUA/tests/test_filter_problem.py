#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test isolé pour reproduire le bug de filter_intersections()
"""

import json
import sys
from pathlib import Path

# Chemins
INTERSECTIONS_JSON = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/CUA/tests/rapport_test_intersections.json"
CATALOGUE_JSON = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/catalogues/catalogue_intersections_tagged.json"

# Ajouter le dossier CUA au path pour importer cua_utils
sys.path.insert(0, "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/CUA")

from cua_utils import filter_intersections

def test_filter():
    print("=" * 80)
    print("🧪 TEST DU FILTRE filter_intersections()")
    print("=" * 80)
    
    # Chargement des données
    with open(INTERSECTIONS_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    with open(CATALOGUE_JSON, "r", encoding="utf-8") as f:
        catalogue = json.load(f)
    
    intersections_raw = data.get("intersections", {})
    surface_indicative = 1649.0  # Surface CERFA
    
    # Test spécifique PLU
    print("\n📋 AVANT FILTRAGE - PLU Latresne:")
    plu_avant = intersections_raw.get("plu_latresne", {})
    print(f"   • pct_sig: {plu_avant.get('pct_sig')}%")
    print(f"   • Nombre d'objets: {len(plu_avant.get('objets', []))}")
    if plu_avant.get('objets'):
        print(f"   • Premier objet: {plu_avant['objets'][0]}")
    
    # Application du filtre
    print("\n🔧 Application du filtre (min_pct=1.0)...")
    result = filter_intersections(
        intersections_raw,
        catalogue,
        surface_indicative,
        min_pct=1.0
    )
    
    # Vérification après
    print("\n📋 APRÈS FILTRAGE - PLU Latresne:")
    plu_apres = result.get("plu_latresne", {})
    if plu_apres:
        print(f"   • pct_sig: {plu_apres.get('pct_sig')}%")
        print(f"   • pourcentage: {plu_apres.get('pourcentage')}%")
        print(f"   • surface_m2: {plu_apres.get('surface_m2')} m²")
        print(f"   • Nombre d'objets: {len(plu_apres.get('objets', []))}")
        if plu_apres.get('objets'):
            print(f"   • Premier objet: {plu_apres['objets'][0]}")
        else:
            print("   ❌ OBJETS PERDUS !")
    else:
        print("   ❌ Couche PLU supprimée par le filtre !")
    
    # Diagnostic
    print("\n" + "=" * 80)
    if plu_apres and plu_apres.get('objets'):
        print("✅ TEST RÉUSSI : Les objets sont préservés")
    else:
        print("❌ TEST ÉCHOUÉ : Les objets sont perdus")
    print("=" * 80)

if __name__ == "__main__":
    test_filter()