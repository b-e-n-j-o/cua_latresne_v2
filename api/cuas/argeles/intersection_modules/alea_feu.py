# -*- coding: utf-8 -*-
"""
Module métier dédié : aléa incendie de forêt et de végétation (PAC).

Agrège les intersections avec argeles.alea_feu par libellé distinct
(pourcentage cumulé sur l'UF si plusieurs entités partagent le même libellé).
"""

from __future__ import annotations

from typing import Any

PAC_URL = (
    "https://www.pyrenees-orientales.gouv.fr/Actions-de-l-Etat/"
    "Environnement-eau-risques-naturels-et-technologiques/"
    "Risques-naturels-et-technologiques/Porters-a-connaissance/"
    "Le-risque-Incendie-de-Foret-et-de-Vegetation/Le-PAC"
)
NOM = "Risque d'incendie de forêt et de végétation"
INTRO = (
    "Selon le niveau de risque identifié, un projet de construction ou d'aménagement "
    "peut nécessiter des prescriptions particulières, voire être limité ou refusé afin "
    "de garantir la sécurité des personnes et des biens. Cette information s'inscrit "
    "dans une démarche de prévention et de réduction de la vulnérabilité face au "
    "risque d'incendie."
)


def _pct_sig(obj: dict) -> float:
    try:
        return float(obj.get("pct_sig") or 0)
    except (TypeError, ValueError):
        return 0.0


def compute_alea_feu_reglementation(
    *,
    alea_feu_objets: list[dict] | None = None,
) -> dict[str, Any]:
    """
    Synthétise les intersections PAC par libellé d'aléa distinct.
    """
    objets = list(alea_feu_objets or [])
    if not objets:
        return {
            "status": "non_concernee",
            "diagnostic_metier": (
                "RAS : aucun aléa incendie de forêt et de végétation sur l'UF"
            ),
            "nom": NOM,
            "intro": INTRO,
            "blocs": [],
            "pac_url": PAC_URL,
        }

    by_libelle: dict[str, float] = {}
    for obj in objets:
        libelle = (obj.get("libelle") or "").strip()
        if not libelle:
            continue
        by_libelle[libelle] = by_libelle.get(libelle, 0.0) + _pct_sig(obj)

    blocs = [
        {"libelle": libelle, "pct_sig": round(pct, 2)}
        for libelle, pct in sorted(by_libelle.items(), key=lambda item: -item[1])
        if pct > 0
    ]

    return {
        "status": "concernee" if blocs else "non_concernee",
        "diagnostic_metier": (
            f"{len(blocs)} aléa(s) distinct(s) sur l'UF"
            if blocs
            else "RAS : aucun aléa incendie de forêt et de végétation sur l'UF"
        ),
        "nom": NOM,
        "intro": INTRO,
        "blocs": blocs,
        "pac_url": PAC_URL,
    }
