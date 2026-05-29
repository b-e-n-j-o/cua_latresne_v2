"""Tool get_reglement_pprmvt — règlement PPRMVT (zones + dispositions générales DG1–DG3)."""

from __future__ import annotations

import logging
import re

from google.genai import types

from ..commune_context import q
from .utils.db import db_query

logger = logging.getLogger("plu_tools")

TABLE_NAME = "pprmvt_reglements"

# Ordre de restitution des 3 parties du titre I (table latresne.pprmvt_reglements).
DG_CODES: tuple[str, ...] = ("DG1", "DG2", "DG3")
DG_LIBELLES: dict[str, str] = {
    "DG1": "Dispositions générales — partie 1 (PPRMVT)",
    "DG2": "Dispositions générales — partie 2 (PPRMVT)",
    "DG3": "Dispositions générales — partie 3 (PPRMVT)",
}


def _norm_code(code: str) -> str:
    return re.sub(r"\s+", "", str(code).strip()).upper()


def _zone_libelle(code: str) -> str:
    return f"Règlement de la zone {code} (PPRMVT)"


def get_reglement_pprmvt(db_config: dict, codes_zone: list[str] | None = None) -> dict:
    """
    Récupère les dispositions générales DG1–DG3 et le règlement des zones demandées
    depuis ``{schema}.pprmvt_reglements``.
    """
    if codes_zone is None:
        requested_raw: list = []
    elif isinstance(codes_zone, str):
        requested_raw = [codes_zone]
    else:
        requested_raw = list(codes_zone)
    requested: list[str] = []
    seen: set[str] = set()
    for raw in requested_raw:
        if raw is None:
            continue
        c = _norm_code(raw)
        if not c or c in seen:
            continue
        if c in DG_CODES:
            continue
        seen.add(c)
        requested.append(c)

    codes_to_fetch = list(DG_CODES) + requested
    if not codes_to_fetch:
        return {
            "dispositions_generales": [],
            "zones": [],
            "dispositions_generales_found": 0,
            "zones_found": 0,
            "zones_requested": [],
            "error": "Aucun code zone fourni et dispositions générales non configurées.",
        }

    placeholders = ", ".join("%s" for _ in codes_to_fetch)
    sql = f"""
        SELECT code_zone, reglementation
        FROM {q(TABLE_NAME)}
        WHERE upper(trim(code_zone)) IN ({placeholders})
    """
    params = tuple(codes_to_fetch)

    try:
        rows = db_query(db_config, sql, params)
    except Exception as e:
        logger.error("get_reglement_pprmvt — SQL échoué : %s", e)
        return {
            "dispositions_generales": [],
            "zones": [],
            "dispositions_generales_found": 0,
            "zones_found": 0,
            "zones_requested": requested,
            "error": str(e),
        }

    by_code: dict[str, str] = {}
    for row in rows:
        key = _norm_code(row["code_zone"])
        by_code[key] = (row.get("reglementation") or "").strip()

    dispositions_generales: list[dict] = []
    for dg in DG_CODES:
        text = by_code.get(dg)
        dispositions_generales.append({
            "code_zone": dg,
            "type": "dispositions_generales",
            "libelle": DG_LIBELLES[dg],
            "reglementation": text,
            "found": bool(text),
            "error": None if text else f"Texte absent en base pour {dg}.",
        })

    zones_out: list[dict] = []
    for code in requested:
        text = by_code.get(code)
        zones_out.append({
            "code_zone": code,
            "type": "zone",
            "libelle": _zone_libelle(code),
            "reglementation": text,
            "found": bool(text),
            "error": None if text else f"Aucun règlement PPRMVT pour la zone « {code} ».",
        })

    dg_found = sum(1 for d in dispositions_generales if d["found"])
    zones_found = sum(1 for z in zones_out if z["found"])

    return {
        "dispositions_generales": dispositions_generales,
        "zones": zones_out,
        "dispositions_generales_found": dg_found,
        "zones_found": zones_found,
        "zones_requested": requested,
        "error": None,
    }


DECL_REGLEMENT_PPRMVT = types.FunctionDeclaration(
    name="get_reglement_pprmvt",
    description=(
        "Récupère le règlement écrit du PPRMVT (plan de prévention des risques "
        "miniers et technologiques) pour Latresne : "
        "toujours les 3 blocs de dispositions générales (DG1, DG2, DG3) "
        "plus le texte intégral de chaque zone demandée (ex. BF, RF, RG). "
        "Chaque entrée est typée (dispositions_generales vs zone) avec un libellé explicite. "
        "À utiliser pour une question sur le PPRMVT / risques miniers, ou lorsque "
        "get_contexte_parcelle indique une zone PPRMVT. "
        "Ne pas confondre avec get_reglement_zone (PLU communal, table plu_reglement). "
        "Passer les codes zone EXACTS (sans préfixe « zone_ »)."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "codes_zone": types.Schema(
                type=types.Type.ARRAY,
                items=types.Schema(type=types.Type.STRING),
                description=(
                    "Liste des codes de zones PPRMVT (ex. ['BF', 'RF']). "
                    "Les dispositions générales DG1, DG2 et DG3 sont toujours incluses "
                    "automatiquement en tête de la réponse."
                ),
            ),
        },
    ),
)
