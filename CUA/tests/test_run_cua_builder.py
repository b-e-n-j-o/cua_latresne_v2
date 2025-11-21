#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pathlib import Path

# 📌 Ajoute le dossier racine cua_latresne_v4 au PYTHONPATH
ROOT = Path(__file__).resolve().parents[2]   # remonte à .../cua_latresne_v4
sys.path.append(str(ROOT))

# Maintenant on peut importer
from CUA.cua_builder import run_builder

BASE = Path(ROOT)

intersections_json = BASE / "CUA/tests/rapport_test_intersections.json"
cerfa_json = BASE / "CUA/cerfa_result.json"
catalogue_json = BASE / "catalogues/catalogue_intersections_tagged.json"
wkt_path = BASE / "CUA/tests/geom_unite_fonciere.wkt"
output_docx = BASE / "CUA/tests/CUA_test.docx"

logo_first_page = BASE / "CUA/logos/logo_latresne.png"
logo_signature = BASE / "CUA/logos/logo_kerelia.png"

qr_url = "https://kerelia.fr/m/test"

print("🚀 Test local du builder CUA...")
run_builder(
    cerfa_json=str(cerfa_json),
    intersections_json=str(intersections_json),
    catalogue_json=str(catalogue_json),
    output_path=str(output_docx),
    wkt_path=str(wkt_path),
    logo_first_page=str(logo_first_page),
    signature_logo=str(logo_signature),
    qr_url=qr_url,
    plu_nom="PLU de Latresne",
    plu_date_appro="13/02/2017"
)

print("🎉 CUA généré :", output_docx)
