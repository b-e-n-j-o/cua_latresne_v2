#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline_from_parcelles.py — Pipeline complet depuis une liste de parcelles
----------------------------------------------------------------------------
Lance le pipeline complet (UF → Intersections → Cartes → CUA) 
sans passer par un CERFA, uniquement avec une liste de parcelles.
"""

import os
import json
import sys
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Ajouter le répertoire parent au path
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from CERFA_ANALYSE.verification_unite_fonciere import verifier_unite_fonciere
from INTERSECTIONS.intersections import calculate_intersection, CATALOGUE, fetch_superficie_indicative
from CUA.sub_orchestrator_cua import generer_visualisations_et_cua_depuis_wkt
from CERFA_ANALYSE.auth_utils import is_authorized_for_insee

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("pipeline_from_parcelles")


def create_minimal_cerfa_json(
    parcelles: list,
    code_insee: str,
    commune_nom: str = None,
    departement_code: str = None,
) -> dict:
    """
    Crée un JSON CERFA minimal à partir d'une liste de parcelles.
    
    Args:
        parcelles: Liste de dicts [{"section": "AC", "numero": "0242"}, ...]
        code_insee: Code INSEE de la commune (ex: "33234")
        commune_nom: Nom de la commune (optionnel, sera "Commune" si non fourni)
        departement_code: Code département (optionnel, extrait du code_insee si non fourni)
    
    Returns:
        dict: JSON CERFA minimal compatible avec le pipeline
    """
    # Extraire le département depuis le code INSEE si non fourni
    if not departement_code and code_insee:
        departement_code = code_insee[:2] if len(code_insee) >= 2 else "33"
    
    if not commune_nom:
        commune_nom = "Commune"
    
    return {
        "data": {
            "cerfa_reference": "13410*11",
            "commune_nom": commune_nom,
            "commune_insee": code_insee,
            "departement_code": departement_code,
            "numero_cu": f"{departement_code}-{code_insee}-{datetime.now().strftime('%Y')}-PARCEL",
            "type_cu": "information",
            "date_depot": datetime.now().strftime("%Y-%m-%d"),
            "demandeur": {
                "type": "personne physique",
                "nom": "DEMANDEUR",
                "prenom": "Parcelle"
            },
            "coord_demandeur": {},
            "mandataire": {},
            "adresse_terrain": {
                "numero": None,
                "voie": None,
                "lieu_dit": None,
                "commune": commune_nom,
                "code_postal": None
            },
            "references_cadastrales": parcelles,
            "superficie_totale_m2": None,  # Sera calculée depuis le WKT
            "header_cu": {
                "dept": departement_code,
                "commune_code": code_insee[-3:] if len(code_insee) >= 3 else code_insee,
                "annee": datetime.now().strftime("%y"),
                "numero_dossier": "PARCEL"
            }
        }
    }


def run_pipeline_from_parcelles(
    parcelles: list,
    code_insee: str,
    commune_nom: str = None,
    user_id: str = None,
    user_email: str = None,
    out_dir: str = None,
):
    """
    Pipeline complet depuis une liste de parcelles.
    
    Args:
        parcelles: Liste de dicts [{"section": "AC", "numero": "0242"}, ...]
        code_insee: Code INSEE de la commune
        commune_nom: Nom de la commune (optionnel)
        user_id: ID utilisateur (optionnel)
        user_email: Email utilisateur (optionnel)
        out_dir: Dossier de sortie (optionnel, créé automatiquement si None)
    
    Returns:
        dict: Résultat du pipeline avec slug, URLs, etc.
    """
    # Vérification autorisation utilisateur
    if user_id:
        if not is_authorized_for_insee(user_id, code_insee):
            raise RuntimeError(f"⛔ Utilisateur non autorisé à analyser la commune {code_insee}")
    
    # Création du dossier de sortie
    if out_dir:
        OUT_DIR = Path(out_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        OUT_DIR = Path("./out_pipeline") / timestamp
    
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"📁 Dossier de sortie : {OUT_DIR}")
    
    # ============================================================
    # 1️⃣ Génération JSON CERFA minimal
    # ============================================================
    logger.info("=== Génération JSON CERFA minimal ===")
    cerfa_json = create_minimal_cerfa_json(
        parcelles=parcelles,
        code_insee=code_insee,
        commune_nom=commune_nom,
    )
    
    cerfa_out = OUT_DIR / "cerfa_result.json"
    cerfa_out.write_text(
        json.dumps(cerfa_json, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    logger.info(f"✅ JSON CERFA minimal généré : {cerfa_out}")
    
    # ============================================================
    # 2️⃣ Vérification unité foncière
    # ============================================================
    logger.info("=== Vérification unité foncière ===")
    uf_json = verifier_unite_fonciere(
        str(cerfa_out),
        code_insee,
        str(OUT_DIR)
    )
    
    uf_json_path = OUT_DIR / "rapport_unite_fonciere.json"
    uf_json_path.write_text(
        json.dumps(uf_json, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    
    if not uf_json.get("success"):
        raise RuntimeError(f"❌ Unité foncière non valide : {uf_json.get('message')}")
    
    wkt_path = uf_json.get("geom_wkt_path")
    if not wkt_path or not Path(wkt_path).exists():
        raise RuntimeError("❌ Geom WKT manquant")
    
    logger.info(f"✅ Unité foncière validée : {wkt_path}")
    
    # ============================================================
    # 3️⃣ Intersections
    # ============================================================
    logger.info("=== Analyse des intersections ===")
    from sqlalchemy import create_engine, text
    
    SUPABASE_HOST = os.getenv('SUPABASE_HOST')
    SUPABASE_DB = os.getenv('SUPABASE_DB')
    SUPABASE_USER = os.getenv('SUPABASE_USER')
    SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')
    SUPABASE_PORT = os.getenv('SUPABASE_PORT')
    
    DATABASE_URL = f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
    engine = create_engine(DATABASE_URL)
    
    with open(wkt_path, "r", encoding="utf-8") as f:
        parcelle_wkt = f.read()
    
    # Calcul de la surface SIG
    with engine.connect() as conn:
        area_parcelle_sig = float(conn.execute(
            text("SELECT ST_Area(ST_GeomFromText(:wkt, 2154))"),
            {"wkt": parcelle_wkt}
        ).scalar())
    
    # Mise à jour de la surface dans le JSON CERFA (temporaire, sera remplacée par superficie indicative)
    cerfa_json["data"]["superficie_totale_m2"] = round(area_parcelle_sig, 2)
    cerfa_out.write_text(
        json.dumps(cerfa_json, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    
    # Récupération superficie indicative (contenance IGN)
    superficie_indicative = fetch_superficie_indicative(parcelles, code_insee)
    
    if not superficie_indicative:
        superficie_indicative = round(area_parcelle_sig, 2)
        logger.warning("⚠️ Utilisation surface SIG comme fallback")
    else:
        logger.info(f"✅ Superficie indicative (contenance) : {superficie_indicative} m²")
    
    rapport = {
        "parcelle": "UF",
        "surface_m2": round(area_parcelle_sig, 2),  # Surface SIG calculée
        "surface_indicative": superficie_indicative,  # Surface juridique (contenance)
        "intersections": {}
    }
    
    # Analyse pour chaque table du catalogue
    for table, config in CATALOGUE.items():
        logger.info(f"→ {table}")
        objets, surface_totale_sig, metadata = calculate_intersection(parcelle_wkt, table, area_parcelle_sig)        
        if objets:
            logger.info(f"  ✅ {len(objets)} objet(s) | {surface_totale_sig:.2f} m²")
            pct_sig = 0.0
            if area_parcelle_sig > 0:
                pct_sig = round(surface_totale_sig / area_parcelle_sig * 100, 4)
            
            rapport["intersections"][table] = {
                "nom": config["nom"],
                "type": config["type"],
                "pct_sig": pct_sig,
                "objets": objets
            }
        else:
            logger.info(f"  ❌ Aucune intersection")
            rapport["intersections"][table] = {
                "nom": config["nom"],
                "type": config["type"],
                "pct_sig": 0.0,
                "objets": []
            }
    
    # Sauvegarde des rapports
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    intersections_json_path = OUT_DIR / f"rapport_intersections_{timestamp}.json"
    intersections_json_path.write_text(
        json.dumps(rapport, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    logger.info(f"✅ Rapports d'intersections exportés : {intersections_json_path}")
    
    # ============================================================
    # 4️⃣ Génération cartes + CUA
    # ============================================================
    logger.info("=== Génération cartes + CUA ===")
    
    # Passer les métadonnées CERFA au sous-orchestrateur
    cerfa_meta = cerfa_json.get("data", {})
    cerfa_data = {
        "numero_cu": cerfa_meta.get("numero_cu"),
        "date_depot": cerfa_meta.get("date_depot"),
        "demandeur": cerfa_meta.get("demandeur", {}).get("denomination") or "DEMANDEUR",
        "parcelles": cerfa_meta.get("references_cadastrales"),
        "superficie": cerfa_meta.get("superficie_totale_m2"),
        "adresse_terrain": cerfa_meta.get("adresse_terrain"),
        "commune_nom": cerfa_meta.get("commune_nom"),
        "commune_insee": cerfa_meta.get("commune_insee"),
    }
    
    os.environ["CERFA_DATA_JSON"] = json.dumps(cerfa_data, ensure_ascii=False)
    if user_id:
        os.environ["USER_ID"] = user_id
    if user_email:
        os.environ["USER_EMAIL"] = user_email
    
    # Déterminer le nom de la commune pour le sous-orchestrateur
    commune_name = commune_nom or "latresne"  # Par défaut latresne
    
    cua_result = generer_visualisations_et_cua_depuis_wkt(
        wkt_path=wkt_path,
        out_dir=str(OUT_DIR),
        commune=commune_name.lower(),
        code_insee=code_insee
    )
    
    logger.info("🎉 Pipeline terminé avec succès")
    return cua_result


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Pipeline complet depuis une liste de parcelles"
    )
    parser.add_argument(
        "--parcelles",
        required=True,
        help='Liste de parcelles au format JSON : [{"section":"AC","numero":"0242"},...]'
    )
    parser.add_argument("--code-insee", required=True, help="Code INSEE de la commune")
    parser.add_argument("--commune-nom", default=None, help="Nom de la commune")
    parser.add_argument("--out-dir", default=None, help="Dossier de sortie")
    parser.add_argument("--user-id", default=None, help="ID utilisateur")
    parser.add_argument("--user-email", default=None, help="Email utilisateur")
    
    args = parser.parse_args()
    
    try:
        parcelles = json.loads(args.parcelles)
        if not isinstance(parcelles, list):
            raise ValueError("--parcelles doit être une liste JSON")
        
        result = run_pipeline_from_parcelles(
            parcelles=parcelles,
            code_insee=args.code_insee,
            commune_nom=args.commune_nom,
            user_id=args.user_id,
            user_email=args.user_email,
            out_dir=args.out_dir
        )
        
        print("\n📦 RÉSULTAT FINAL :")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
    except Exception as e:
        logger.error(f"💥 Erreur : {e}")
        sys.exit(1)

