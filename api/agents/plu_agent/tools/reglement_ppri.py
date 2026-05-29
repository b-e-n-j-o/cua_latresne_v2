"""Tool get_reglement_ppri — règlement PPRI Latresne (dispositions communes + zones)."""

from __future__ import annotations

import logging
import re
from typing import Any

from google.genai import types

from ..commune_context import get_current_profile_optional, q
from .utils.db import db_query

logger = logging.getLogger("plu_tools")

TABLE_NAME = "ppri_reglements"
DEFAULT_CODE_INSEE = "33234"

# zone_code explicites hors zonage couleur (dispositions / règlement commun).
DISPOSITION_ZONE_CODES: frozenset[str] = frozenset({
    "DG",
    "DISPOSITIONS_GENERALES",
    "REGLEMENT_GENERAL",
    "COMMUN",
    "GENERAL",
    "CHAPITRE_A",
    "A",
})

SELECT_COLS = """
    id, commune, code_insee, version_date, zone_code, titre_zone, chapitre,
    source_pdf_page_start, source_pdf_page_end, reglementation
"""


def _norm_zone_code(code: str) -> str:
    c = str(code).strip().upper()
    c = c.replace("É", "E").replace("È", "E").replace("Ê", "E")
    c = re.sub(r"[\s\-]+", "_", c)
    c = re.sub(r"[^A-Z0-9_]", "", c)
    return re.sub(r"_+", "_", c).strip("_")


def _resolve_insee() -> str:
    profile = get_current_profile_optional()
    if profile and profile.insee:
        return str(profile.insee).strip()
    return DEFAULT_CODE_INSEE


def _row_to_bloc(row: dict[str, Any]) -> dict[str, Any]:
    text = (row.get("reglementation") or "").strip()
    zc = (row.get("zone_code") or "").strip() or None
    chapitre = (row.get("chapitre") or "").strip() or None
    titre = (row.get("titre_zone") or "").strip() or None
    libelle_parts = [p for p in (chapitre, titre) if p]
    libelle = " — ".join(libelle_parts) if libelle_parts else (
        f"Dispositions communes (PPRI)" if not zc else f"PPRI — {zc}"
    )
    return {
        "id": row.get("id"),
        "zone_code": zc,
        "chapitre": chapitre,
        "titre_zone": titre,
        "libelle": libelle,
        "reglementation": text,
        "found": bool(text),
        "source_pdf_page_start": row.get("source_pdf_page_start"),
        "source_pdf_page_end": row.get("source_pdf_page_end"),
        "version_date": str(row["version_date"]) if row.get("version_date") else None,
    }


def _is_disposition_row(row: dict[str, Any]) -> bool:
    zc = _norm_zone_code(row.get("zone_code") or "")
    if not zc:
        return True
    if zc in DISPOSITION_ZONE_CODES:
        return True
    chap = (row.get("chapitre") or "").upper()
    if chap.startswith("CHAPITRE A") or chap.startswith("CHAPITRE  A"):
        return True
    if "DISPOSITION" in chap and ("GENERAL" in chap or "COMMUN" in chap):
        return True
    return False


def _aggregate_blocs(blocs: list[dict[str, Any]], *, type_label: str, zone_code: str | None) -> dict[str, Any]:
    found_blocs = [b for b in blocs if b.get("found")]
    texts = [b["reglementation"] for b in found_blocs]
    combined = "\n\n---\n\n".join(texts) if texts else None
    if zone_code:
        libelle = f"Zone {zone_code} (PPRI)"
        if found_blocs and found_blocs[0].get("titre_zone"):
            libelle = f"{found_blocs[0]['titre_zone']} (PPRI)"
    else:
        libelle = "Dispositions communes et règlement général (PPRI)"
    return {
        "type": type_label,
        "zone_code": zone_code,
        "libelle": libelle,
        "reglementation": combined,
        "blocs": blocs,
        "blocs_count": len(blocs),
        "blocs_found": len(found_blocs),
        "found": bool(texts),
        "error": None if texts else "Aucun texte de réglementation en base pour cette entrée.",
    }


def get_reglement_ppri(
    db_config: dict,
    codes_zone: list[str] | None = None,
    *,
    include_dispositions_communes: bool = True,
) -> dict:
    """
    Récupère le règlement PPRI depuis ``{schema}.ppri_reglements`` :
    dispositions communes (chapitre A / zone_code vide) + zones demandées.
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
        c = _norm_zone_code(raw)
        if not c or c in seen:
            continue
        if c in DISPOSITION_ZONE_CODES:
            continue
        seen.add(c)
        requested.append(c)

    code_insee = _resolve_insee()
    sql = f"""
        SELECT {SELECT_COLS}
        FROM {q(TABLE_NAME)}
        WHERE code_insee = %s
        ORDER BY
            CASE WHEN zone_code IS NULL OR trim(zone_code) = '' THEN 0 ELSE 1 END,
            chapitre NULLS LAST,
            zone_code NULLS LAST,
            id
    """

    try:
        rows = db_query(db_config, sql, (code_insee,))
    except Exception as e:
        logger.error("get_reglement_ppri — SQL échoué : %s", e)
        return {
            "dispositions_communes": [],
            "zones": [],
            "dispositions_communes_found": 0,
            "zones_found": 0,
            "zones_requested": requested,
            "code_insee": code_insee,
            "error": str(e),
        }

    disposition_blocs: list[dict[str, Any]] = []
    by_zone: dict[str, list[dict[str, Any]]] = {}

    for row in rows:
        bloc = _row_to_bloc(row)
        if _is_disposition_row(row):
            disposition_blocs.append({**bloc, "type": "dispositions_communes"})
            continue
        zc = _norm_zone_code(row.get("zone_code") or "")
        if not zc:
            continue
        by_zone.setdefault(zc, []).append({**bloc, "type": "zone"})

    dispositions_out: list[dict] = []
    if include_dispositions_communes:
        if disposition_blocs:
            dispositions_out.append(
                _aggregate_blocs(
                    disposition_blocs,
                    type_label="dispositions_communes",
                    zone_code=None,
                )
            )
        else:
            dispositions_out.append({
                "type": "dispositions_communes",
                "zone_code": None,
                "libelle": "Dispositions communes et règlement général (PPRI)",
                "reglementation": None,
                "blocs": [],
                "blocs_count": 0,
                "blocs_found": 0,
                "found": False,
                "error": "Aucune disposition commune trouvée en base.",
            })

    zones_out: list[dict] = []
    for code in requested:
        blocs = by_zone.get(code, [])
        if blocs:
            zones_out.append(
                _aggregate_blocs(blocs, type_label="zone", zone_code=code)
            )
        else:
            zones_out.append({
                "type": "zone",
                "zone_code": code,
                "libelle": f"Zone {code} (PPRI)",
                "reglementation": None,
                "blocs": [],
                "blocs_count": 0,
                "blocs_found": 0,
                "found": False,
                "error": f"Aucun règlement PPRI pour la zone « {code} ».",
            })

    disp_found = sum(1 for d in dispositions_out if d.get("found"))
    zones_found = sum(1 for z in zones_out if z.get("found"))

    return {
        "dispositions_communes": dispositions_out,
        "zones": zones_out,
        "dispositions_communes_found": disp_found,
        "zones_found": zones_found,
        "zones_requested": requested,
        "code_insee": code_insee,
        "zones_available_in_db": sorted(by_zone.keys()),
        "error": None,
    }


DECL_REGLEMENT_PPRI = types.FunctionDeclaration(
    name="get_reglement_ppri",
    description=(
        "Récupère le règlement écrit du PPRI (Plan de Prévention des Risques "
        "d'Inondation) de Latresne depuis ppri_reglements. "
        "Inclut toujours les dispositions communes / règlement général "
        "(chapitre A, hors code couleur de zone) puis le texte des zones "
        "demandées (ex. GRENAT, ROUGE_URBANISEE, BLEUE, BLEU_CLAIR). "
        "Chaque entrée distingue dispositions_communes vs zone ; plusieurs blocs "
        "par zone sont fusionnés avec métadonnées (chapitre, titre_zone, pages PDF). "
        "Ne pas confondre avec get_reglement_zone (PLU) ni get_reglement_pprmvt (PPRMVT). "
        "Utiliser les codes zone EXACTS retournés par get_contexte_parcelle ou la cartographie PPRI."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "codes_zone": types.Schema(
                type=types.Type.ARRAY,
                items=types.Schema(type=types.Type.STRING),
                description=(
                    "Codes des zones PPRI (ex. ['BLEUE', 'ROUGE_URBANISEE']). "
                    "Les dispositions communes (règlement général) sont toujours incluses."
                ),
            ),
            "include_dispositions_communes": types.Schema(
                type=types.Type.BOOLEAN,
                description=(
                    "Inclure le règlement général / chapitre A (défaut : true). "
                    "Mettre false uniquement pour ne charger que des zones."
                ),
            ),
        },
    ),
)
