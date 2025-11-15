#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
audit_intra_couche.py
----------------------------------------------------
Audit des surfaces *à l'intérieur de chaque couche* :
- Vérifie que la somme des zones = surface de la parcelle
  pour les couches dont le pourcentage est 100%.
"""

import json
import argparse
from pathlib import Path

def audit_intra_couche(rapport_path, tolerance=0.5):
    rapport_path = Path(rapport_path)
    if not rapport_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {rapport_path}")

    with open(rapport_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    surface_parcelle = float(data.get("surface_m2", 0))
    intersections = data.get("intersections", {})

    print("\n===========================")
    print(" AUDIT INTRA-COUCHE (100%) ")
    print("===========================\n")
    print(f"Surface parcelle : {surface_parcelle:.4f} m²\n")

    anomalies = []
    warnings = []

    for key, layer in intersections.items():
        objs = layer.get("objets", [])
        total_surface_layer = sum(float(o.get("surface_inter_m2", 0)) for o in objs)
        pct_layer = float(layer.get("pourcentage", 0))

        # Nous auditons seulement les couches "100%"
        if pct_layer < 99.9:
            continue

        diff = abs(total_surface_layer - surface_parcelle)

        print(f"→ Couche {key}")
        print(f"   Somme objets     : {total_surface_layer:.4f} m²")
        print(f"   % attendu        : 100 % (déclaré : {pct_layer} %)")
        print(f"   Différence       : {diff:.4f} m²")

        if diff > tolerance:
            anomalies.append(
                (key, f"Somme des objets ({total_surface_layer}) ≠ surface parcelle ({surface_parcelle})")
            )
        else:
            warnings.append(
                (key, f"Léger écart dans la tolérance ({diff:.4f} m²)")
            )

        print()

    print("\n---------------------------")
    print(" RÉSULTATS ")
    print("---------------------------\n")

    if anomalies:
        print("❌ Anomalies détectées :")
        for k, msg in anomalies:
            print(f"   - {k}: {msg}")
    else:
        print("Aucune anomalie critique.")

    if warnings:
        print("\n⚠️ Avertissements :")
        for k, msg in warnings:
            print(f"   - {k}: {msg}")
    else:
        print("\nAucun avertissement.")

    print("\n🎉 Audit terminé.\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--intersections-json", required=True)
    ap.add_argument("--tolerance", default=0.5, type=float)
    args = ap.parse_args()

    audit_intra_couche(args.intersections_json, args.tolerance)


if __name__ == "__main__":
    main()
