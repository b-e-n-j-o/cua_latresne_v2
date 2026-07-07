# -*- coding: utf-8 -*-
"""
Post-traitement du rapport d'intersections : laius servitudes, PLU, PPRMVT.
"""

from __future__ import annotations

import logging

from sqlalchemy.engine import Engine

from .laius_enrichment import enrich_laius_reglementaires
from .servitudes import compute_servitudes_reglementation

logger = logging.getLogger("intersections")


def enrich_intersections_rapport(rapport: dict, parcelle_wkt: str, engine: Engine) -> dict:
    """
    Enrichit le rapport JSON après la boucle catalogue :
    - servitudes_reglementees (latresne.servitudes × public.servitudes_reglements)
    - zonage_plu / pprmvt via plu_laius / pprmvt_laius
  """
    intersections = rapport.setdefault("intersections", {})

    # --- Servitudes unifiées ---
    try:
        special = compute_servitudes_reglementation(parcelle_wkt, engine=engine)
        intersections["servitudes_reglementees"] = {
            "nom": "Servitudes d'utilité publique (réglementation)",
            "type": "servitude",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            **special,
        }
        n = len(special.get("servitudes") or [])
        if n:
            logger.info(f"  ✅ servitudes_reglementees              {n:>3} servitude(s)")
        else:
            logger.info("  ·  servitudes_reglementees                —")
    except Exception as exc:
        logger.warning(f"  ⚠  servitudes_reglementees          {exc}")
        intersections["servitudes_reglementees"] = {
            "nom": "Servitudes d'utilité publique (réglementation)",
            "type": "servitude",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            "status": "erreur",
            "error": str(exc),
            "servitudes": [],
        }

    # --- Laius PLU / PPRMVT ---
    try:
        laius_meta = enrich_laius_reglementaires(rapport, engine)
        rapport["laius_enrichment"] = laius_meta
        rapport["plu_dispositions_generales"] = (
            (laius_meta.get("zonage_plu") or {}).get("dispositions_generales")
        )
        for key, meta in laius_meta.items():
            n = meta.get("enriched", 0)
            if n:
                logger.info(f"  ✅ laius {key:<24} {n:>3} objet(s) enrichi(s)")
    except Exception as exc:
        logger.warning(f"  ⚠  laius_enrichment               {exc}")
        rapport["laius_enrichment"] = {"status": "erreur", "error": str(exc)}

    return rapport
