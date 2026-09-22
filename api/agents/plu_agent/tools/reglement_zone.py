"""Tool get_reglement_zone — texte intégral du règlement PLU par code de zone."""

from __future__ import annotations

import logging

from google.genai import types

from ..corpus import (
    concatener_reglements,
    fetch_textes,
    insee_courant,
    resoudre_codes_zonage,
)

logger = logging.getLogger("plu_tools")


def get_reglement_zone(db_config: dict, code_zone: str) -> dict:
    """
    Récupère le texte complet du règlement d'une zone PLU depuis ``corpus.textes``.
    Les alias spatiaux (ex. UA → UAa, UAb) sont résolus avant lecture.
    """
    if not code_zone or not str(code_zone).strip():
        return {
            "code_zone": None,
            "reglementation": None,
            "texte_id": None,
            "texte_ids": [],
            "found": False,
            "error": "code_zone vide.",
        }

    zone = str(code_zone).strip()
    insee = insee_courant()
    if not insee:
        return {
            "code_zone": zone,
            "reglementation": None,
            "texte_id": None,
            "texte_ids": [],
            "found": False,
            "error": "Commune INSEE inconnue — impossible de lire corpus.textes.",
        }

    try:
        codes = resoudre_codes_zonage(
            db_config, [zone], document_type="PLU", insee=insee
        )
        textes = fetch_textes(
            db_config,
            document_type="PLU",
            codes=codes,
            include_globale=False,
            insee=insee,
        )
    except Exception as e:
        logger.error("get_reglement_zone — lecture corpus échouée : %s", e)
        return {
            "code_zone": zone,
            "reglementation": None,
            "texte_id": None,
            "texte_ids": [],
            "found": False,
            "error": str(e),
        }

    if not textes:
        return {
            "code_zone": zone,
            "codes_resolus": codes,
            "reglementation": None,
            "texte_id": None,
            "texte_ids": [],
            "found": False,
            "error": f"Aucun règlement pour la zone « {zone} ».",
        }

    ids = [t["texte_id"] for t in textes if t.get("texte_id")]
    combined = concatener_reglements(textes)
    return {
        "code_zone": zone,
        "codes_resolus": codes,
        "reglementation": combined,
        "texte_id": ids[0] if ids else None,
        "texte_ids": ids,
        "textes": [
            {
                "texte_id": t.get("texte_id"),
                "zone_code": t.get("zone_code"),
                "titre": t.get("titre"),
                "reglementation": t.get("reglementation"),
            }
            for t in textes
        ],
        "found": bool(combined),
        "error": None,
    }


DECL_REGLEMENT_ZONE = types.FunctionDeclaration(
    name="get_reglement_zone",
    description=(
        "Récupère le texte intégral du règlement écrit d'une zone du PLU communal "
        "(corpus.textes, document PLU), identifiée par son code (ex. UA, N, AU). "
        "get_contexte_parcelle ne contient pas ce texte — l'appeler dès qu'il faut "
        "citer ou analyser le règlement d'une zone. "
        "Utiliser EXACTEMENT le code_zone retourné par get_contexte_parcelle."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "code_zone": types.Schema(
                type=types.Type.STRING,
                description="Code de zone PLU (ex: 'UA', 'N', '1AU').",
            ),
        },
        required=["code_zone"],
    ),
)
