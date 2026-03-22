# -*- coding: utf-8 -*-
"""
sub_orchestrator_cua_test.py — Test unitaire pour génération du CUA uniquement
------------------------------------------------------
Test visant à générer uniquement le CUA DOCX à partir des rapports d'intersections
préexistants, sans passer par toute la suite de génération des cartes et upload.
"""

import os
import json
import subprocess
import logging
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("sub_orchestrator_cua_test")

# Charger les variables d'environnement
load_dotenv()

# Supabase setup (non utilisé dans ce test mais requis dans le pipeline)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = "visualisation"

# Configuration de base
KERELIA_BASE_URL = "https://kerelia.fr/maps"

# ============================================================
# 🧩 Test pour générer uniquement le CUA DOCX
# ============================================================

def generer_cua_depuis_intersections(intersections_path, out_dir, commune="latresne", code_insee="33234"):
    """
    Test unitaire pour générer le CUA DOCX à partir d'un rapport d'intersections existant.
    Ne passe que par la génération du CUA, sans les autres étapes du pipeline (cartes, Supabase, etc.).
    
    Args:
        intersections_path (str): Chemin vers le fichier de rapport d'intersections
        out_dir (str): Dossier de sortie
        commune (str): Nom de la commune
        code_insee (str): Code INSEE
    """
    # S'assurer que le dossier de sortie existe
    os.makedirs(out_dir, exist_ok=True)

    logger.info(f"🚀 Lancement du pipeline de génération CUA à partir du rapport d'intersections : {intersections_path}")
    
    if not os.path.exists(intersections_path):
        raise FileNotFoundError(f"❌ Le rapport d'intersections n'existe pas : {intersections_path}")
    
    # --------------------------------------------------------
    # Étape 1 : Génération du CUA DOCX
    # --------------------------------------------------------

    logger.info("\n📦 Étape 1/1 : Génération du CUA DOCX avec le rapport d'intersections")

    # Construire les chemins de fichiers pour les entrées et sorties
    base_dir = os.path.dirname(__file__)
    cua_dir = os.path.abspath(os.path.join(base_dir, ".."))

    builder_path = os.path.join(cua_dir, "docx", "cua_builder.py")

    # Générer le fichier CUA (sans passer par les autres étapes)
    output_docx_path = os.path.join(out_dir, "CUA_unite_fonciere.docx")
    logo_latresne_path = os.path.join(cua_dir, "logos", "logo_latresne.png")
    logo_kerelia_path = os.path.join(cua_dir, "logos", "logo_kerelia.png")
    
    # Récupérer le JSON du Cerfa depuis un exemple local
    cerfa_path = os.path.join(out_dir, "cerfa_result.json")
    cerfa_json = {
        "data": {
            "commune_insee": "33234",
            "commune_nom": "Latresne"
        }
    }
    with open(cerfa_path, 'w') as f:
        json.dump(cerfa_json, f)

    # Commande pour générer le CUA DOCX avec le rapport d'intersections existant
    cmd = [
        "python3", builder_path,
        "--cerfa-json", cerfa_path,
        "--intersections-json", intersections_path,  # Utilisation du rapport d'intersections existant
        "--catalogue-json", os.path.join(base_dir, "catalogue_avec_articles.json"),
        "--output", output_docx_path,
        "--logo-first-page", logo_latresne_path,
        "--signature-logo", logo_kerelia_path,
        "--plu-nom", "PLU de Latresne",
        "--plu-date-appro", "13/02/2017"
    ]

    logger.info(f"🛠️ Commande exécutée : {' '.join(cmd)}")

    # Exécution de la commande pour générer le CUA
    try:
        subprocess.run(cmd, check=True)
        logger.info("✅ CUA DOCX généré avec succès.")
    except subprocess.CalledProcessError as e:
        logger.error(f"💥 Échec génération CUA DOCX : {e}")

    return output_docx_path

# ============================================================
# 🧩 Test unitaire
# ============================================================

if __name__ == "__main__":
    intersections_path = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/out_pipeline/20251104_192408/rapport_intersections_20251104_192448.json"
    out_dir = Path("/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/CUA/out_test/test_sub_cua")
    
    # Lancer le test pour générer le CUA
    cua_file = generer_cua_depuis_intersections(
        intersections_path=intersections_path,
        out_dir=str(out_dir)
    )
    
    logger.info(f"\n📦 Résultat : CUA généré à l'emplacement suivant : {cua_file}")
