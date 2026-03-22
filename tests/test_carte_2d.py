#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test autonome carte 2D depuis WKT
"""

import sys
import logging
from pathlib import Path

# ✅ Active les logs de tous les modules (extraction/rendu/etc.)
logging.basicConfig(
    level=logging.INFO,  # mets DEBUG si tu veux encore plus verbeux
    format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)

# 🔧 Fix imports projet
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from CUA.map2d.carte2d.carte2d_rendu import generer_carte_2d_depuis_wkt

WKT_PATH = (
    "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/out_pipeline/20251215_163326/geom_unite_fonciere.wkt"
)

html, metadata = generer_carte_2d_depuis_wkt(
    wkt_path=WKT_PATH,
    code_insee="33234",
)

out = Path("test_carte_2d.html")
out.write_text(html, encoding="utf-8")

print("✅ Carte générée :", out.resolve())
print("📊 Metadata :", metadata)
