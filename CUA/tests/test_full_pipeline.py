#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
test_full_pipeline.py
-----------------------------------
Pipeline complet en local, sans Supabase :

1) Analyse CERFA
2) Vérification unité foncière (UF)
3) Intersections
4) Génération du CUA

Sorties dans : <project_root>/local_test_pipeline/
"""

import os
import json
import sys
from pathlib import Path
from datetime import datetime

# -------------------------------------------------------------
# 🔧 Résolution des chemins / sys.path
# -------------------------------------------------------------

HERE = Path(__file__).resolve()
# tests/  -> parent = CUA/ -> parent = cua_latresne_v4/
PROJECT_ROOT = HERE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

print(f"📂 PROJECT_ROOT = {PROJECT_ROOT}")

# Maintenant on peut importer les modules du projet
from CERFA_ANALYSE.analyse_gemini import analyse_cerfa
from CERFA_ANALYSE.verification_unite_fonciere import verifier_unite_fonciere
from INTERSECTIONS.intersections import calculate_intersection, CATALOGUE
from CUA.cua_builder import run_builder
from sqlalchemy import create_engine, text

# -------------------------------------------------------------
# 📌 PARAMÈTRES
# -------------------------------------------------------------

BASE_DIR = PROJECT_ROOT

CERFA_PDF = BASE_DIR / "CUA/tests/cerfa_CU_13410-2024-07-19.pdf"
CATALOGUE_PATH = BASE_DIR / "catalogues/catalogue_intersections_tagged.json"

OUT_DIR = BASE_DIR / "local_test_pipeline"
OUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"📁 OUT_DIR = {OUT_DIR}")

# -------------------------------------------------------------
# 1️⃣ Analyse CERFA
# -------------------------------------------------------------

cerfa_json_path = OUT_DIR / "cerfa_result.json"

print("\n=== 1️⃣ Analyse du CERFA ===")
print(f"📄 PDF : {CERFA_PDF}")

cerfa_json = analyse_cerfa(
    str(CERFA_PDF),
    out_json=str(cerfa_json_path)
)

print(f"✅ CERFA analysé → {cerfa_json_path}")

# -------------------------------------------------------------
# 2️⃣ Vérification de l’unité foncière
# -------------------------------------------------------------

print("\n=== 2️⃣ Vérification unité foncière ===")

INSEE = cerfa_json["data"].get("commune_insee") or "33234"

# On suit exactement la signature utilisée dans orchestrator_global.py
uf_json = verifier_unite_fonciere(
    str(cerfa_json_path),
    INSEE,
    str(OUT_DIR),
)

uf_json_path = OUT_DIR / "uf_result.json"
uf_json_path.write_text(json.dumps(uf_json, indent=2, ensure_ascii=False), encoding="utf-8")

print(f"✅ UF OK → {uf_json_path}")

WKT_PATH = uf_json.get("geom_wkt_path")
if not WKT_PATH or not Path(WKT_PATH).exists():
    raise RuntimeError(f"❌ Aucun WKT généré par l’UF ! (geom_wkt_path={WKT_PATH})")

print(f"📐 WKT utilisé : {WKT_PATH}")

# -------------------------------------------------------------
# 3️⃣ Intersections SIG (copie de la logique orchestrator)
# -------------------------------------------------------------

print("\n=== 3️⃣ Intersections SIG ===")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")

if not all([SUPABASE_HOST, SUPABASE_DB, SUPABASE_USER, SUPABASE_PASSWORD, SUPABASE_PORT]):
    raise RuntimeError("❌ Variables d'environnement DB (SUPABASE_*) manquantes.")

DATABASE_URL = (
    f"postgresql+psycopg2://{SUPABASE_USER}:"
    f"{SUPABASE_PASSWORD}@{SUPABASE_HOST}:"
    f"{SUPABASE_PORT}/{SUPABASE_DB}"
)

engine = create_engine(DATABASE_URL)

parcelle_wkt = Path(WKT_PATH).read_text(encoding="utf-8").strip()

with engine.connect() as conn:
    area_sig = float(
        conn.execute(
            text("SELECT ST_Area(ST_GeomFromText(:wkt, 2154))"),
            {"wkt": parcelle_wkt},
        ).scalar()
    )

print(f"📏 Surface UF (SIG) : {area_sig:.2f} m²")

rapport_inter = {
    "parcelle": "UF_TEST",
    "surface_m2": area_sig,
    "intersections": {},
}

for table, config in CATALOGUE.items():
    print(f"→ {table}")
    objets, surface_totale_sig, metadata = calculate_intersection(parcelle_wkt, table)

    if area_sig > 0:
        pct = round(surface_totale_sig / area_sig * 100, 4)
    else:
        pct = 0.0

    rapport_inter["intersections"][table] = {
        "nom": config["nom"],
        "type": config["type"],
        "pct_sig": pct,
        "objets": objets,
    }

intersections_path = OUT_DIR / "intersections_result.json"
intersections_path.write_text(json.dumps(rapport_inter, indent=2, ensure_ascii=False), encoding="utf-8")

print(f"✅ Intersections sauvegardées → {intersections_path}")

# -------------------------------------------------------------
# 4️⃣ Génération du CUA final
# -------------------------------------------------------------

print("\n=== 4️⃣ Génération du CUA ===")

output_docx = OUT_DIR / "CUA_test.docx"

run_builder(
    cerfa_json=str(cerfa_json_path),
    intersections_json=str(intersections_path),
    catalogue_json=str(CATALOGUE_PATH),
    output_path=str(output_docx),
    wkt_path=str(WKT_PATH),
    logo_first_page=str(BASE_DIR / "CUA/logos/logo_latresne.png"),
    signature_logo=str(BASE_DIR / "CUA/logos/logo_kerelia.png"),
    qr_url="https://kerelia.fr",  # en local c'est juste cosmétique
    plu_nom="PLU de Latresne",
    plu_date_appro="13/02/2017",
)

print(f"\n🎉 CUA généré → {output_docx}")
