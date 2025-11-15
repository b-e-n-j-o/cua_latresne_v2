#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Met à jour geom_type dans le catalogue d'intersections
"""

import json
from pathlib import Path

# === INPUT / OUTPUT ===
INPUT = "catalogue_intersections.json"
OUTPUT = "catalogue_intersections_tagged.json"

# === LISTES FOURNIES PAR BENJAMIN ===
LINEAIRES = {
    "a4",
    "i4_lignes_electriques_locales",
    "reseaux_hta",
    "i4_cables_haute_tension",
    "haies_bocages_latresne",
    "galerie_cheminement_et_pilier",
    "i6",
    "pt3",
    "reseaux_aep_eu_lin",
    "troncons_et_fosses_latresne",
}

PONCTUELS = {
    "reseaux_aep_eu_pct"
}

def main():
    path = Path(INPUT)
    if not path.exists():
        raise FileNotFoundError(f"❌ Fichier introuvable : {INPUT}")

    with open(path, "r", encoding="utf-8") as f:
        catalogue = json.load(f)

    updated = {}
    for key, item in catalogue.items():
        if key in LINEAIRES:
            item["geom_type"] = "lineaire"
        elif key in PONCTUELS:
            item["geom_type"] = "ponctuel"
        else:
            item["geom_type"] = "surfacique"  # Default
        
        updated[key] = item

    # Sauvegarde
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(updated, f, indent=2, ensure_ascii=False)

    print(f"✅ Catalogue mis à jour et sauvegardé dans : {OUTPUT}")
    print("Résumé :")
    print(f"  - Linéaires : {len(LINEAIRES)}")
    print(f"  - Ponctuels : {len(PONCTUELS)}")
    print(f"  - Surfaciques : {len(updated) - len(LINEAIRES) - len(PONCTUELS)}")

if __name__ == "__main__":
    main()
