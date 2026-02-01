#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de test : récupère les pipelines avec centroïdes pour un user_id donné.
Usage: python scripts/test_centroids_by_user.py [user_id]
"""
import os
import sys
import json
from pathlib import Path

# Ajouter la racine du projet au path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

USER_ID = "55c68f76-419b-4951-ba5c-6c9bfa202899"
if len(sys.argv) > 1:
    USER_ID = sys.argv[1]

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ SUPABASE_URL et SERVICE_KEY requis dans .env")
        sys.exit(1)

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print(f"🔍 Recherche pipelines pour user_id = {USER_ID}\n")

    response = (
        supabase
        .schema("latresne")
        .table("pipelines")
        .select("id, slug, created_at, centroid, cerfa_data, user_id, commune")
        .eq("user_id", USER_ID)
        .order("created_at", desc=True)
        .limit(20)
        .execute()
    )

    pipelines = response.data or []
    print(f"📊 Total pipelines trouvés : {len(pipelines)}\n")

    with_centroid = [p for p in pipelines if p.get("centroid") and isinstance(p["centroid"], dict)]
    without_centroid = [p for p in pipelines if not p.get("centroid")]

    print(f"✅ Avec centroid (affichables sur la carte) : {len(with_centroid)}")
    print(f"⚠️ Sans centroid : {len(without_centroid)}\n")

    for i, p in enumerate(pipelines, 1):
        c = p.get("centroid")
        slug = p.get("slug", "?")
        created = p.get("created_at", "?")[:19] if p.get("created_at") else "?"
        has_centroid = c and isinstance(c, dict) and "lon" in c and "lat" in c
        status = "🟢" if has_centroid else "🔴"
        print(f"  {status} [{i}] slug={slug[:20]}... created={created}")
        if has_centroid:
            print(f"      centroid: lon={c['lon']}, lat={c['lat']}")
        elif c:
            print(f"      centroid (invalide): {c}")

    if with_centroid:
        print("\n📋 JSON des centroïdes (pour debug) :")
        for p in with_centroid:
            print(json.dumps({"slug": p.get("slug"), "centroid": p.get("centroid")}, ensure_ascii=False))

if __name__ == "__main__":
    main()
