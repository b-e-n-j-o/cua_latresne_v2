#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Orchestrateur CERFA (Mistral)
----------------------------
Analyse préliminaire d’un CERFA 13410*12 :
- extraction des informations générales
- extraction des références cadastrales
- génération d’alertes métier

⚠️ Les données retournées doivent être VALIDÉES par un humain
avant toute génération de certificat d’urbanisme.
"""

import logging
import time
from pathlib import Path

from .mistral_cerfa_info_extractor import extraire_info_cerfa
from .extraire_images_puis_parcelles import extraire_parcelles_depuis_pdf

logger = logging.getLogger("cerfa.orchestrator")


def analyser_cerfa_complet(pdf_path: str) -> dict:
    """
    Orchestrateur principal :
    - appelle l'extracteur d'infos générales
    - appelle l'extracteur de parcelles
    - agrège les résultats + quelques stats (dont tokens)
    """

    t_start = time.time()

    # 1) Infos générales (pages 1–4)
    logger.info("🚀 Début analyse CERFA complète", extra={"pdf_path": pdf_path})
    info_result = extraire_info_cerfa(pdf_path)

    if not info_result.get("success"):
        logger.error("Échec extraction infos générales", extra={"error": info_result.get("error")})
        return {
            "success": False,
            "error": "Erreur lors de l'extraction des informations générales",
            "details": info_result,
        }

    info_data = info_result["data"]
    info_usage = info_result.get("usage", {})
    info_tokens = info_usage.get("total_tokens", 0)

    # 2) Parcelles cadastrales (pages 2 et 4 via pipeline simple)
    parcelles_result = extraire_parcelles_depuis_pdf(pdf_path)

    if not parcelles_result.get("success"):
        logger.error("Échec extraction parcelles", extra={"error": parcelles_result.get("error")})
        return {
            "success": False,
            "error": "Erreur lors de l'extraction des parcelles cadastrales",
            "details": parcelles_result,
        }

    parcelles_data = parcelles_result["data"]
    parcelles_stats = parcelles_result.get("stats", {})
    parcelles_tokens = parcelles_stats.get("tokens", 0)

    # 3) Agrégation
    total_tokens = info_tokens + parcelles_tokens
    duration_s = time.time() - t_start

    logger.info(
        "✅ Analyse CERFA complète terminée",
        extra={
            "total_tokens": total_tokens,
            "info_tokens": info_tokens,
            "parcelles_tokens": parcelles_tokens,
            "duration_s": round(duration_s, 2),
        },
    )

    return {
        "success": True,
        "data": {
            "info_generales": info_data,
            "parcelles_detectees": parcelles_data,
        },
        "alerts": [],  # à remplir plus tard avec des règles métiers
        "metadata": {
            "source_file": Path(pdf_path).name,
            "stats": {
                "nb_parcelles": parcelles_stats.get("nb_parcelles"),
                "somme_surfaces": parcelles_stats.get("somme_surfaces"),
                "ecart_total": parcelles_stats.get("ecart_total"),
                "tokens": total_tokens,
                "info_tokens": info_tokens,
                "parcelles_tokens": parcelles_tokens,
                "duration_s": round(duration_s, 2),
            },
        },
    }



# ============================================================
# CLI de test local
# ============================================================
if __name__ == "__main__":
    pdf = "/path/to/cerfa.pdf"
    res = analyser_cerfa_complet(pdf)

    if not res.get("success"):
        print(f"❌ Erreur : {res.get('error')}")
    else:
        print("✅ Analyse CERFA réussie")
        print(f"Alertes : {len(res.get('alerts', []))}")
        for a in res.get("alerts", []):
            print(f" - {a}")
