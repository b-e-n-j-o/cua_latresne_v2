#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Audit du pipeline Kerelia :
- Vérifie les fichiers générés dans le OUT_DIR
- Vérifie la cohérence du slug dans sub_orchestrator_result.json
- Vérifie la présence des bons champs dans latresne.pipelines
- Vérifie l’accessibilité des URLs Supabase
"""

import json
from pathlib import Path
import requests
import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def audit_pipeline(out_dir: str):
    out_dir = Path(out_dir)

    print("\n========================")
    print("🔍 AUDIT DU PIPELINE")
    print("========================\n")
    print(f"📁 OUT_DIR = {out_dir}")

    # 1) Vérifier les fichiers essentiels
    files_required = [
        "pipeline_result.json",
        "sub_orchestrator_result.json",
        "CUA_unite_fonciere.docx",
        "intersections.gpkg",
        "carte_2d.html",
        "carte_3d.html",
    ]

    print("\n📌 Vérification fichiers locaux :")
    for f in files_required:
        p = out_dir / f
        print(f"  - {f} : {'✅ OK' if p.exists() else '❌ MANQUANT'}")

    # 2) Lire sub_orchestrator_result pour prendre le slug
    sub_path = out_dir / "sub_orchestrator_result.json"
    if not sub_path.exists():
        print("\n❌ sub_orchestrator_result.json introuvable")
        return

    sub_json = json.loads(sub_path.read_text())
    slug = sub_json.get("slug")

    print(f"\n🎯 SLUG détecté : {slug}")

    # 3) Vérifier la table latresne.pipelines
    print("\n📡 Vérification en base Supabase…")
    response = (
        supabase
        .schema("latresne")
        .table("pipelines")
        .select("*")
        .eq("slug", slug)
        .limit(1)
        .execute()
    )

    rows = response.data or []
    if not rows:
        print("❌ Aucun pipeline trouvé avec ce slug")
        return

    row = rows[0]
    print("✅ Pipeline trouvé en base")

    # 4) Vérifier les URLs Supabase
    urls = [
        ("CUA", row.get("output_cua")),
        ("2D map", row.get("carte_2d_url")),
        ("3D map", row.get("carte_3d_url")),
        ("intersections gpkg", row.get("intersections_gpkg_url")),
        ("pipeline_result.json", row.get("pipeline_result_url")),
    ]

    print("\n🌍 Vérification accessibilité des URLs Supabase :")
    for label, url in urls:
        if not url:
            print(f"  - {label}: ❌ URL manquante")
            continue
        try:
            r = requests.head(url)
            print(f"  - {label}: {'✅ Accessible' if r.status_code == 200 else f'❌ {r.status_code}'}")
        except:
            print(f"  - {label}: ❌ Erreur réseau")

    print("\n✨ AUDIT TERMINÉ ✨")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Audit Kerelia pipeline")
    ap.add_argument("out_dir", help="Dossier OUT_DIR du pipeline")
    args = ap.parse_args()

    audit_pipeline(args.out_dir)
