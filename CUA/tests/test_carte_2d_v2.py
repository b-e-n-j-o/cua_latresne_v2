#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pathlib import Path
import os
import logging
import requests
import io
import geopandas as gpd
import tempfile

# ============================================================
# 🔧 Ajouter le dossier racine au PYTHONPATH
# ============================================================
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
print("PYTHONPATH ajouté :", ROOT)

from CUA.map2d.carte2d.carte2d_rendu import generer_carte_2d_depuis_wkt


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_carte2d")


# ===========================================================================
# 🔎 PARAMÈTRES PARCELLE (EN DUR)
# ===========================================================================
INSEE = "33234"     # LATRESNE
SECTION = "AC"
NUMERO = "0690"      # Toujours 4 chiffres côté IGN

ENDPOINT = "https://data.geopf.fr/wfs/ows"
LAYER    = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
SRS      = "EPSG:2154"


# ===========================================================================
# 🔥 Fonction : fetch WKT via WFS IGN
# ===========================================================================
def fetch_parcelle_wkt(insee, section, numero):
    numero = numero.zfill(4)
    section = section.upper()

    cql = f"code_insee='{insee}' AND section='{section}' AND numero='{numero}'"

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": LAYER,
        "srsName": SRS,
        "outputFormat": "application/json",
        "CQL_FILTER": cql,
    }

    url = requests.Request("GET", ENDPOINT, params=params).prepare().url
    logger.info(f"🔎 Requête parcellaire IGN : {url}")

    r = requests.get(url, timeout=20)
    r.raise_for_status()

    gdf = gpd.read_file(io.BytesIO(r.content))
    if gdf.empty:
        raise RuntimeError(f"⚠️ Aucune parcelle trouvée pour {cql}")

    return gdf.iloc[0].geometry.wkt


# ===========================================================================
# 🚀 MAIN
# ===========================================================================
def main():

    logger.info("=" * 60)
    logger.info("🎯 TEST CARTE 2D — DÉBUT")
    logger.info("=" * 60)

    # --- 1. Récupération WKT ---
    logger.info("📡 Récupération WKT de la parcelle IGN…")
    wkt = fetch_parcelle_wkt(INSEE, SECTION, NUMERO)
    logger.info("   ✅ WKT récupéré")

    # --- 2. Création d'un fichier temporaire WKT ---
    with tempfile.NamedTemporaryFile(delete=False, suffix=".wkt", mode="w", encoding="utf-8") as tmp:
        tmp_path = tmp.name
        tmp.write(wkt)

    logger.info(f"📄 Fichier WKT temporaire créé : {tmp_path}")

    # --- 3. Génération carte 2D ---
    html, metadata = generer_carte_2d_depuis_wkt(
        wkt_path=tmp_path,          # ICI : param correct
        code_insee=INSEE,
        inclure_ppri=True,
        ppri_table="pm1_detaillee_gironde",
    )

    # --- 4. Sauvegarde ---
    output_dir = "out_test_2d"
    os.makedirs(output_dir, exist_ok=True)
    output_file = Path(output_dir) / "carte_2d_test.html"

    output_file.write_text(html, encoding="utf-8")

    logger.info(f"✅ Carte générée : {output_file}")
    logger.info("📊 Métadonnées :")
    logger.info(metadata)

    logger.info("=" * 60)
    logger.info("🎉 TEST TERMINÉ AVEC SUCCÈS")
    logger.info("=" * 60)


# ===========================================================================
# Entrée script
# ===========================================================================
if __name__ == "__main__":
    main()
