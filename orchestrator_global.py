#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
orchestrator_global.py — Pipeline global KERELIA (phase 2)
-----------------------------------------------------------
1️⃣ Analyse du CERFA via Gemini (analyse_gemini.py)
2️⃣ Vérification unité foncière via WFS IGN (verification_unite_fonciere.py)
3️⃣ Intersections avec couches urbanistiques (intersections.py)
-----------------------------------------------------------
Étapes suivantes prévues :
4️⃣ Génération cartes 2D / 3D
5️⃣ Génération certificat d'urbanisme DOCX
"""

import subprocess
subprocess.run(["pip", "list"], check=True)  # Liste les packages installés
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("orchestrator_global")

CERFA_ANALYSE_SCRIPT = "./CERFA_ANALYSE/analyse_gemini.py"
VERIF_UF_SCRIPT = "./CERFA_ANALYSE/verification_unite_fonciere.py"
INTERSECTIONS_SCRIPT = "./INTERSECTIONS/intersections.py"
SUB_ORCHESTRATOR_CUA = "./CUA/sub_orchestrator_cua.py"

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_DIR = Path("./out_pipeline") / timestamp
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# UTILS
# ============================================================
def run_subprocess(cmd, desc):
    """Exécute une commande subprocess et logge les erreurs proprement."""
    logger.info(f"\n🚀 Étape : {desc}")
    try:
        subprocess.run(cmd, check=True, cwd=Path(__file__).parent)
    except subprocess.CalledProcessError as e:
        logger.error(f"💥 Échec lors de {desc}: {e}")
        sys.exit(1)

# ============================================================
# PIPELINE PRINCIPAL
# ============================================================
def orchestrer_pipeline(pdf_path: str, code_insee: str):
    """
    Orchestration complète du process CERFA → UF → Intersections
    """
    pdf = Path(pdf_path)
    if not pdf.exists():
        logger.error(f"❌ Fichier PDF introuvable : {pdf}")
        sys.exit(1)

    logger.info(f"📄 Analyse du fichier CERFA : {pdf.name}")
    
    cerfa_json_path = OUT_DIR / "cerfa_result.json"
    uf_json_path = OUT_DIR / "rapport_unite_fonciere.json"
    geom_wkt_path = OUT_DIR / "geom_unite_fonciere.wkt"
    intersections_json_path = OUT_DIR / "rapport_intersections.json"

    # -------------------------------
    # ÉTAPE 1 : ANALYSE GEMINI
    # -------------------------------
    run_subprocess([
        "python3", CERFA_ANALYSE_SCRIPT,
        "--pdf", str(pdf),
        "--out-json", str(cerfa_json_path),
        "--out-dir", str(OUT_DIR),
        "--insee-csv", "../CONFIG/v_commune_2025.csv"
    ], "Analyse du CERFA (Gemini)")

    cerfa_data = json.load(open(cerfa_json_path))
    data = cerfa_data.get("data", {})
    insee = data.get("commune_insee") or code_insee
    if not insee:
        logger.error("❌ Code INSEE non trouvé dans l’analyse CERFA.")
        sys.exit(1)

    # -------------------------------
    # ÉTAPE 2 : VALIDATION UNITÉ FONCIÈRE
    # -------------------------------
    run_subprocess([
        "python3", VERIF_UF_SCRIPT,
        "--cerfa-json", str(cerfa_json_path),
        "--code-insee", insee,
        "--out", str(uf_json_path),
        "--out-dir", str(OUT_DIR)
    ], "Vérification unité foncière")

    uf_result = json.load(open(uf_json_path))
    logger.info(f"📊 Résultat UF : {uf_result['message']}")
    if not uf_result.get("success", False):
        logger.warning("❌ Arrêt du pipeline : unité foncière non valide.")
        sys.exit(1)

    # Vérification que la géométrie WKT a bien été générée dans OUT_DIR
    if not geom_wkt_path.exists():
        logger.error(f"❌ Fichier de géométrie d'unité foncière manquant : {geom_wkt_path}")
        sys.exit(1)

    # -------------------------------
    # ÉTAPE 3 : INTERSECTIONS
    # -------------------------------
    run_subprocess([
        "python3", INTERSECTIONS_SCRIPT,
        "--geom-wkt", str(geom_wkt_path),
        "--out-dir", str(OUT_DIR)
    ], "Analyse des intersections")

    # Récupération du rapport généré (le nom dépend du script d'intersections)
    json_candidates = list(OUT_DIR.glob("rapport_intersections_*.json"))
    if json_candidates:
        json_candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        latest_report = json_candidates[0]
        logger.info(f"📑 Rapport d'intersection généré : {latest_report.name}")
    else:
        logger.warning("⚠️ Aucun rapport d'intersection trouvé.")
        latest_report = None

    logger.info("✅ Étape intersections terminée — suite du traitement possible.")
    logger.info("🧩 Étapes suivantes à venir : cartes 2D/3D, CUA...")

    # -------------------------------
    # ÉTAPE 4 : GÉNÉRATION CARTES + CUA
    # -------------------------------
    if latest_report and geom_wkt_path.exists():
        logger.info("\n🗺️  Lancement de la génération des cartes 2D/3D et du CUA...")
        try:
            run_subprocess([
                "python3", SUB_ORCHESTRATOR_CUA,
                "--wkt", str(geom_wkt_path),
                "--code_insee", insee,
                "--commune", "latresne",
                "--out-dir", str(OUT_DIR)  # ✅
            ], "Génération cartes + CUA")
            logger.info("✅ Sous-orchestrateur CUA exécuté avec succès.")
        except Exception as e:
            logger.error(f"💥 Échec du sous-orchestrateur CUA : {e}")
    else:
        logger.warning("⚠️ Impossible de lancer la génération CUA : géométrie ou rapport manquant.")

    # -------------------------------
    # RETOUR GLOBAL
    # -------------------------------
    result = {
        "cerfa_result": str(cerfa_json_path),
        "uf_result": str(uf_json_path),
        "geom_wkt": str(geom_wkt_path),
        "intersections": str(latest_report) if latest_report else None
    }

    # Intégration du résultat global du sous-orchestrateur, s'il a produit un fichier final
    cua_docx = OUT_DIR / "CUA_unite_fonciere.docx"
    if cua_docx.exists():
        result["cua_docx"] = str(cua_docx)

    result_path = OUT_DIR / "pipeline_result.json"
    result_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info(f"\n🎉 PIPELINE TERMINÉ AVEC SUCCÈS 🎉")
    logger.info(f"📦 Résumé enregistré dans : {result_path}")

# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Orchestrator global — KERELIA (phase 2)")
    ap.add_argument("--pdf", required=True, help="Chemin vers le CERFA PDF")
    ap.add_argument("--code-insee", default=None, help="Code INSEE (fallback si non trouvé)")
    args = ap.parse_args()

    orchestrer_pipeline(args.pdf, args.code_insee)
