#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
test_intersections_json.py
----------------------------------
Produit EXACTEMENT le JSON que génère intersections.py v10
mais en l’exécutant indépendamment du pipeline.
"""

import argparse
import json
import importlib.util
from pathlib import Path
from sqlalchemy import create_engine, text
import os

# ============================================================
# 🔥 Chemin vers ton script opérationnel intersections.py
# ============================================================
SCRIPT_PATH = Path("/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/INTERSECTIONS/intersections.py")

# ============================================================
# 📦 Import dynamique du script opérationnel
# ============================================================
spec = importlib.util.spec_from_file_location("intersections_op", SCRIPT_PATH)
intersections_op = importlib.util.module_from_spec(spec)
spec.loader.exec_module(intersections_op)

CATALOGUE = intersections_op.CATALOGUE

# ============================================================
# 🔎 Fonction
# ============================================================
def compute_full_report(wkt_path: str):
    wkt = Path(wkt_path).read_text().strip()

    # DB init
    SUPABASE_HOST = os.getenv('SUPABASE_HOST')
    SUPABASE_DB = os.getenv('SUPABASE_DB')
    SUPABASE_USER = os.getenv('SUPABASE_USER')
    SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')
    SUPABASE_PORT = os.getenv('SUPABASE_PORT')

    DATABASE_URL = f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
    engine = create_engine(DATABASE_URL)

    # Surface SIG de la parcelle / UF
    with engine.connect() as conn:
        area_parcelle_sig = float(conn.execute(
            text("SELECT ST_Area(ST_GeomFromText(:wkt, 2154))"),
            {"wkt": wkt}
        ).scalar())

    rapport = {
        "parcelle": "UF_TEST",
        "surface_m2": area_parcelle_sig,
        "intersections": {}
    }

    # Boucle exacte
    for table, config in CATALOGUE.items():
        objets, surface_totale_sig, metadata = intersections_op.calculate_intersection(wkt, table)

        if objets:
            pct_sig = round(surface_totale_sig / area_parcelle_sig * 100, 4)

            rapport["intersections"][table] = {
                "nom": config["nom"],
                "type": config["type"],
                "pct_sig": pct_sig,
                "objets": objets
            }
        else:
            rapport["intersections"][table] = {
                "nom": config["nom"],
                "type": config["type"],
                "pct_sig": 0.0,
                "objets": []
            }

    return rapport

# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--wkt", required=True, help="Chemin du fichier WKT")
    parser.add_argument("--out", default="rapport_test_intersections.json")
    args = parser.parse_args()

    rapport = compute_full_report(args.wkt)

    Path(args.out).write_text(json.dumps(rapport, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n🎉 Rapport JSON généré : {args.out}\n")
