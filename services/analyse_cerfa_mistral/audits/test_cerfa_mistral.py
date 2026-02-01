#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test orchestrateur CERFA avec monitoring tokens
"""

import sys
import json
import logging
from pathlib import Path

# Ajouter le chemin racine du projet pour les imports de package
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from services.analyse_cerfa_mistral.mistral_cerfa_orchestrator import analyser_cerfa_complet

# ============================================================
# LOGGING DÉTAILLÉ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)

logger = logging.getLogger("test.orchestrator")

# ============================================================
# CONFIG
# ============================================================

PDF_PATH = (
    "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/"
    "cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
)

# ============================================================
# MAIN
# ============================================================

def main():
    if not Path(PDF_PATH).exists():
        logger.error(f"PDF introuvable: {PDF_PATH}")
        return
    
    logger.info("="*70)
    logger.info("🧪 TEST ORCHESTRATEUR CERFA - TOKEN MONITORING")
    logger.info("="*70)
    logger.info(f"PDF: {Path(PDF_PATH).name}")
    logger.info("="*70)
    
    # Analyse complète
    result = analyser_cerfa_complet(PDF_PATH)
    
    logger.info("="*70)
    logger.info("📊 RÉSULTATS")
    logger.info("="*70)
    
    if result["success"]:
        logger.info("✅ Analyse réussie")
        
        # Infos générales
        info = result["data"]["info_generales"]
        logger.info(f"\n📍 Commune: {info.get('commune_nom')} ({info.get('commune_insee')})")
        logger.info(f"   N° CU: {info.get('numero_cu')}")
        logger.info(f"   Type: {info.get('type_cu')}")
        
        # Parcelles
        parcelles = result["data"]["parcelles_detectees"]
        refs = parcelles.get("references_cadastrales", [])
        logger.info(f"\n📦 Parcelles: {len(refs)}")
        logger.info(f"   Superficie totale: {parcelles.get('superficie_totale_m2')} m²")
        
        # Alertes
        alerts = result.get("alerts", [])
        if alerts:
            logger.warning(f"\n⚠️  Alertes ({len(alerts)}):")
            for alert in alerts:
                logger.warning(f"   • {alert}")
        
        # Stats
        stats = result["metadata"]["stats"]
        logger.info(f"\n📈 Stats:")
        logger.info(f"   Parcelles détectées: {stats.get('nb_parcelles')}")
        logger.info(f"   Tokens utilisés: {stats.get('tokens')}")
        
    else:
        logger.error(f"❌ Échec: {result.get('error')}")
        logger.error(f"   Détails: {result.get('details')}")
        output_path = Path("cerfa_orchestrator_error.json")
        output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info(f"💾 Résultat d'erreur sauvegardé dans {output_path}")
        logger.info("="*70)

    # Sauvegarde systématique du résultat brut (succès ou échec)
    output_path = Path("cerfa_orchestrator_result.json")
    output_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"💾 Résultat complet sauvegardé dans {output_path}")
    logger.info("="*70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Interrompu par l'utilisateur")
    except Exception:
        logger.exception("❌ Erreur fatale")