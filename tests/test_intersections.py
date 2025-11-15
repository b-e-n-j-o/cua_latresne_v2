#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_intersections_full.py
-------------------------------------------------------
Script unique :
  - Récupération WFS IGN (WKT)
  - intersections.py → analyse_parcelle
  - sauvegarde JSON
  - audit global (surfaces/percentages)
  - audit intra-couche (couches 100 %)
"""

import os
import io
import sys
import json
from pathlib import Path
import requests
import geopandas as gpd

# ======================================================
# CONFIGURATION : MODIFIER UNIQUEMENT ICI
# ======================================================
SECTION = "AC"
NUMERO = "0242"
CODE_INSEE = "33234"
OUT_DIR = "./test_output"
TOLERANCE = 0.5  # m²
# ======================================================

BASE = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE / "INTERSECTIONS"))
sys.path.append(str(BASE / "tests"))

# import analyse_parcelle
from intersections import analyse_parcelle


# ======================================================
# 1) WFS IGN → WKT
# ======================================================

ENDPOINT = "https://data.geopf.fr/wfs/ows"
LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
SRS = "EPSG:2154"

def get_parcelle_wkt(section, numero, code_insee):
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": LAYER,
        "outputFormat": "application/json",
        "srsName": SRS,
        "CQL_FILTER": (
            f"section='{section}' AND numero='{numero}' AND code_insee='{code_insee}'"
        )
    }

    print(f"📡 WFS : récupération parcelle {section}-{numero}…")
    r = requests.get(ENDPOINT, params=params, timeout=30)
    r.raise_for_status()

    gdf = gpd.read_file(io.BytesIO(r.content))
    if gdf.empty:
        raise ValueError(f"❌ Aucune parcelle trouvée : {section} {numero}")

    print(f"✅ Parcelle trouvée ({len(gdf)} feature)")
    return gdf.iloc[0].geometry.wkt


# ======================================================
# 2) Audit global surfaces / pourcentages
# ======================================================

def audit_global(data, tolerance=0.5):
    surface_parcelle = float(data.get("surface_m2", 0))
    intersections = data.get("intersections", {})

    print("\n=======================")
    print("  AUDIT GLOBAL COUCHES ")
    print("=======================\n")
    print(f"Surface officielle parcelle : {surface_parcelle:.4f} m²\n")

    total = 0.0

    for k, layer in intersections.items():
        s = float(layer.get("surface_m2") or 0)
        total += s

    diff = abs(total - surface_parcelle)

    print(f"Somme TOTALE surfaces (⚠️ NE DOIT PAS ÊTRE UTILISÉE JURIDIQUEMENT) : {total:.4f} m²")
    print(f"Écart vs surface parcelle : {diff:.4f} m²")

    if diff > tolerance:
        print(f"⚠️ ÉCART SUPÉRIEUR À {tolerance} m² (normal car on NE doit pas sommer les couches)")
    else:
        print("ℹ️ Écart faible — analyse informative uniquement.")

    print("\n(Rappel : les couches ne doivent pas être sommées : ceci est un audit DEBUG.)")


# ======================================================
# 3) Audit INTRA-COUCHE (100 %)
# ======================================================

def audit_intra_couche(data, tolerance=0.5):
    surface_parcelle = float(data.get("surface_m2", 0))
    intersections = data.get("intersections", {})

    print("\n=============================")
    print(" AUDIT INTRA-COUCHE (100 %) ")
    print("=============================\n")

    anomalies = []
    warnings = []

    for key, layer in intersections.items():
        pct = float(layer.get("pourcentage") or 0)

        # On audite uniquement les couches couvrant (en théorie) 100 %
        if pct < 99.9:
            continue

        objs = layer.get("objets") or []
        total_surf = sum(float(o.get("surface_inter_m2") or 0) for o in objs)
        diff = abs(total_surf - surface_parcelle)

        print(f"→ Couche : {key}")
        print(f"   Somme des objets : {total_surf:.4f} m²")
        print(f"   % annoncé        : {pct} %")
        print(f"   Différence       : {diff:.4f} m²")

        if diff > tolerance:
            anomalies.append((key, diff))
        else:
            warnings.append((key, diff))

        print()

    print("\n---------------------------")
    print(" RÉSULTATS INTRA-COUCHE ")
    print("---------------------------")

    if anomalies:
        print("\n❌ Anomalies :")
        for k, d in anomalies:
            print(f" - {k}: écart {d:.4f} m² (> tolérance)")
    else:
        print("\nAucune anomalie critique.")

    if warnings:
        print("\n⚠️ Avertissements (écarts faibles) :")
        for k, d in warnings:
            print(f" - {k}: écart {d:.4f} m²")
    else:
        print("\nAucun avertissement.")

    print("\n🎉 Audit intra-couche terminé.\n")


# ======================================================
# MAIN PIPELINE
# ======================================================

def main():
    print("\n==============================")
    print("   TEST INTERSECTIONS COMPLET ")
    print("==============================\n")

    # 1) WFS
    wkt = get_parcelle_wkt(SECTION, NUMERO, CODE_INSEE)

    # 2) Intersections
    print("\n⚙️  Calcul intersections…")
    rapport = analyse_parcelle(SECTION, NUMERO)

    # 3) Save JSON
    out_dir = Path(OUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / f"intersections_{SECTION}_{NUMERO}.json"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(rapport, f, indent=2, ensure_ascii=False)

    print(f"\n📦 Rapport sauvegardé : {out_json}")

    # 4) Audits
    audit_global(rapport, TOLERANCE)
    audit_intra_couche(rapport, TOLERANCE)

    print("\n🎯 Test complet terminé.\n")


if __name__ == "__main__":
    main()
