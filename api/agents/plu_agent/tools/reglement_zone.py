"""Tool get_reglement_zone — texte intégral du règlement PLU par code de zone."""

from __future__ import annotations

import logging

from google.genai import types

from ..commune_context import q
from .utils.db import db_query

logger = logging.getLogger("plu_tools")


def get_reglement_zone(db_config: dict, code_zone: str) -> dict:
    """
    Récupère le texte complet du règlement d'une zone PLU depuis `{schema}.plu_reglement`.
    """
    if not code_zone or not str(code_zone).strip():
        return {
            "code_zone": None,
            "reglementation": None,
            "found": False,
            "error": "code_zone vide.",
        }

    zone = str(code_zone).strip()

    sql = f"""
        SELECT code_zone, reglementation
        FROM {q("plu_reglement")}
        WHERE upper(trim(code_zone)) = upper(trim(%s))
        LIMIT 1
    """
    try:
        rows = db_query(db_config, sql, (zone,))
        if not rows:
            return {
                "code_zone": zone,
                "reglementation": None,
                "found": False,
                "error": f"Aucun règlement pour la zone « {zone} ».",
            }
        row = rows[0]
        return {
            "code_zone": row["code_zone"],
            "reglementation": row["reglementation"],
            "found": True,
            "error": None,
        }
    except Exception as e:
        logger.error("get_reglement_zone — SQL échoué : %s", e)
        return {
            "code_zone": zone,
            "reglementation": None,
            "found": False,
            "error": str(e),
        }


DECL_REGLEMENT_ZONE = types.FunctionDeclaration(
    name="get_reglement_zone",
    description=(
        "Récupère le texte intégral du règlement écrit d'une zone du PLU communal "
        "(table plu_reglement), identifiée par son code (ex. UA, N, AU). "
        "À utiliser lorsque get_contexte_parcelle a fourni un code_zone mais que le "
        "règlement n'est pas complet dans la réponse, ou pour approfondir une zone "
        "précise. Utiliser EXACTEMENT le code_zone retourné par get_contexte_parcelle."
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
