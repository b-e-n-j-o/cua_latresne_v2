#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_cua_local_from_wfs.py
----------------------------------------------------
Mini pipeline local de test :

1) Récupère la géométrie d'une parcelle via le WFS cadastre IGN (EPSG:2154 direct)
2) Lance les intersections (en utilisant intersections.calculate_intersection)
3) Génère un rapport JSON d'intersections
4) Construit un faux CERFA JSON minimal
5) Génère un CUA DOCX via build_cua_docx

Usage typique :

  python3 test_cua_local_from_wfs.py \
    --section AC \
    --numero 0242 \
    --commune-nom "Latresne" \
    --code-insee 33234 \
    --catalogue-intersections catalogue_intersections.json \
    --catalogue-cua catalogue_avec_articles.json \
    --out-dir ./out_cua_local

Pré-requis :
- Variables SUPABASE_* déjà configurées (comme intersections.py)
"""
import os
import sys
from pathlib import Path
import io

# Récupération du dossier racine du projet (ici cua_latresne_v4)
ROOT = Path(__file__).resolve().parents[1]

# Ajout des chemins nécessaires
INTER_PATH = ROOT / "INTERSECTIONS"
CUA_PATH = ROOT / "CUA"

sys.path.append(str(INTER_PATH))
sys.path.append(str(CUA_PATH))


import json
import logging
import argparse


import requests
import geopandas as gpd
from shapely.geometry import shape
from shapely.ops import transform
import pyproj
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# On réutilise le moteur et la logique d'intersection
from intersections import (
    calculate_intersection,
    CATALOGUE as CATALOGUE_INTER,
    generate_html,
)
from cua_builder import build_cua_docx


# =========================================
# CONFIG / LOGGING
# =========================================

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("test_cua_local")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")

DATABASE_URL = (
    f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}"
    f"@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
)
engine = create_engine(DATABASE_URL)

ENDPOINT = "https://data.geopf.fr/wfs/ows"
LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
SRS = "EPSG:2154"


# =========================================
# 1) Récupération de la géométrie via WFS
# =========================================

def get_parcelle_wkt_from_wfs(section: str, numero: str, code_insee: str) -> str:
    """
    Version alignée sur l’ETL parcelles :
    - WFS data.geopf.fr
    - srsName=EPSG:2154
    - outputFormat=application/json
    - CQL_FILTER section/numero/code_insee
    - Retourne WKT en 2154 sans passer par PostGIS
    """

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": LAYER,
        "outputFormat": "application/json",
        "srsName": SRS,
        "CQL_FILTER": (
            f"section='{section}' AND numero='{numero}' AND code_insee='{code_insee}'"
        ),
    }

    print("📡 Requête WFS IGN en cours…")
    r = requests.get(ENDPOINT, params=params, timeout=30)
    r.raise_for_status()

    print(f"✅ Données reçues ({len(r.content)/1024:.1f} KB)")

    gdf = gpd.read_file(io.BytesIO(r.content))
    if gdf.empty:
        raise ValueError(
            f"Aucune parcelle trouvée pour {section} {numero} ({code_insee})"
        )

    print("📍 1 parcelle trouvée")

    if gdf.crs is None or gdf.crs.to_string() != SRS:
        print("⚠️ CRS inattendu, reprojection en EPSG:2154")
        src_crs = pyproj.CRS.from_user_input(gdf.crs or "EPSG:4326")
        dst_crs = pyproj.CRS.from_user_input(SRS)
        transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)
        geom = transform(transformer.transform, shape(gdf.iloc[0].geometry))
    else:
        geom = shape(gdf.iloc[0].geometry)

    return geom.wkt


# =========================================
# 2) Construction du rapport d'intersections
# =========================================

def build_intersections_report_from_wkt(parcelle_wkt: str, section: str, numero: str) -> dict:
    """
    Reprend la logique d'intersections.py en partant d'un WKT
    (sans aller chercher la parcelle en base).
    """
    logger.info("📐 Calcul de la surface de l'unité foncière…")
    with engine.connect() as conn:
        area_parcelle = float(
            conn.execute(
                text("SELECT ST_Area(ST_GeomFromText(:wkt, 2154))"),
                {"wkt": parcelle_wkt},
            ).scalar()
        )

    rapport = {
        "parcelle": f"{section} {numero}",
        "surface_m2": round(area_parcelle, 2),
        "intersections": {},
    }

    for table, config in CATALOGUE_INTER.items():
        logger.info("→ %s", table)
        objets = calculate_intersection(parcelle_wkt, table)
        surface_totale = sum(obj.get("surface_inter_m2", 0.0) for obj in objets)

        if objets:
            logger.info("  ✅ %d objet(s) | %.2f m²", len(objets), surface_totale)
            rapport["intersections"][table] = {
                "nom": config.get("nom", table),
                "type": config.get("type", "inconnu"),
                "surface_m2": round(surface_totale, 2),
                "pourcentage": round(surface_totale / area_parcelle * 100, 2),
                "objets": objets,
            }
        else:
            logger.info("  ❌ Aucune intersection")
            rapport["intersections"][table] = {
                "nom": config.get("nom", table),
                "type": config.get("type", "inconnu"),
                "surface_m2": 0.0,
                "pourcentage": 0.0,
                "objets": [],
            }

    return rapport


# =========================================
# 3) CERFA JSON minimal de test
# =========================================

def build_fake_cerfa_json(
    section: str,
    numero: str,
    commune_nom: str,
    code_insee: str,
    surface_m2: float,
) -> dict:
    """
    Génère un CERFA JSON minimal pour alimenter build_cua_docx.
    Tu peux l'enrichir au besoin.
    """
    return {
        "data": {
            "commune_nom": commune_nom,
            "commune_insee": code_insee,
            "adresse_terrain": {
                "adresse": "",
                "code_postal": "",
                "ville": commune_nom,
            },
            "references_cadastrales": [
                {"section": section, "numero": numero}
            ],
            "superficie_totale_m2": surface_m2,
            "numero_cu": "CU-TEST-LOCAL",
        }
    }


# =========================================
# 4) MAIN
# =========================================

def main():
    ap = argparse.ArgumentParser(description="Mini pipeline local CUA à partir du WFS cadastre IGN")
    ap.add_argument("--section", required=True, help="Section cadastrale (ex: AC)")
    ap.add_argument("--numero", required=True, help="Numéro de parcelle (ex: 0242)")
    ap.add_argument("--commune-nom", default="Latresne", help="Nom de la commune (ex: Latresne)")
    ap.add_argument("--code-insee", default="33234", help="Code INSEE de la commune")
    ap.add_argument(
        "--catalogue-intersections",
        default="catalogue_intersections.json",
        help="Chemin du catalogue d'intersections (pour info seulement ici, celui d'intersections.py est déjà chargé)",
    )
    ap.add_argument(
        "--catalogue-cua",
        default="catalogue_avec_articles.json",
        help="Catalogue utilisé par le builder CUA (articles, etc.)",
    )
    ap.add_argument(
        "--out-dir",
        default="./out_cua_local",
        help="Dossier de sortie (JSON, HTML, WKT, DOCX)",
    )
    ap.add_argument(
        "--logo-first-page",
        default="",
        help="Logo pour la première page du CUA (optionnel)",
    )
    ap.add_argument(
        "--signature-logo",
        default="",
        help="Logo/signature du maire (optionnel)",
    )
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Géométrie via WFS
    parcelle_wkt = get_parcelle_wkt_from_wfs(
        section=args.section,
        numero=args.numero,
        code_insee=args.code_insee,
    )
    wkt_path = out_dir / "unite_fonciere.wkt"
    wkt_path.write_text(parcelle_wkt, encoding="utf-8")
    logger.info("✏️ WKT unité foncière écrit dans %s", wkt_path)

    # 2) Intersections
    rapport = build_intersections_report_from_wkt(
        parcelle_wkt, args.section, args.numero
    )

    # Sauvegarde JSON + HTML pour inspection rapide
    json_path = out_dir / "rapport_intersections_local.json"
    html_path = out_dir / "rapport_intersections_local.html"

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(rapport, f, indent=2, ensure_ascii=False)

    html = generate_html(rapport)
    html_path.write_text(html, encoding="utf-8")

    logger.info("✅ Rapports d'intersections sauvegardés : %s, %s", json_path, html_path)

    # 3) CERFA JSON minimal
    cerfa_json = build_fake_cerfa_json(
        section=args.section,
        numero=args.numero,
        commune_nom=args.commune_nom,
        code_insee=args.code_insee,
        surface_m2=rapport.get("surface_m2", 0.0),
    )

    # 4) Catalogue CUA
    catalogue_path = (ROOT / args.catalogue_cua).resolve()
    if not catalogue_path.exists():
        raise FileNotFoundError(f"Catalogue CUA introuvable : {catalogue_path}")
    with open(catalogue_path, "r", encoding="utf-8") as f:
        catalogue_cua = json.load(f)

    # 5) Génération du CUA DOCX
    output_docx = out_dir / f"CUA_test_local_{args.section}_{args.numero}.docx"

    build_cua_docx(
        cerfa_json,
        rapport,
        catalogue_cua,
        str(output_docx),
        wkt_path=str(wkt_path),
        logo_first_page=args.logo_first_page or None,
        signature_logo=args.signature_logo or None,
        qr_url="https://www.kerelia.fr/carte",  # tu peux ajuster
        plu_nom="PLU en vigueur",
        plu_date_appro="13/02/2017",
    )

    logger.info("🎉 CUA DOCX généré : %s", output_docx)


if __name__ == "__main__":
    main()
