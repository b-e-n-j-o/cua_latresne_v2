"""Tool get_reglement_ppri — règlement PPRI Latresne (dispositions générales DG + zones)."""

from __future__ import annotations

import logging
import re
from typing import Any

from google.genai import types

from ..corpus import fetch_textes, insee_courant, resoudre_codes_zonage
from ..commune_context import get_current_profile_optional

logger = logging.getLogger("plu_tools")

TABLE_NAME = "ppri_reglements"
DEFAULT_CODE_INSEE = "33234"

# Ligne unique des dispositions générales fusionnées (ingestion PPRI).
DG_ZONE_CODE = "DG"
DG_LIBELLE = "Dispositions générales (PPRI)"

# Codes zone couleur Latresne (table latresne.ppri_reglements) — source de vérité pour le LLM.
PPRI_ZONES_COULEUR: tuple[str, ...] = (
    "BLEUE",
    "BLEUE_CLAIRE",
    "BYZANTINE",
    "GRENAT",
    "ROUGE_CENTRE",
    "ROUGE_INDUS",
    "ROUGE_NON_URBA",
    "ROUGE_URBA",
)

_PPRI_ZONES_COULEUR_HELP = ", ".join(PPRI_ZONES_COULEUR)

# Anciens / synonymes de zone_code pour les dispositions (hors zonage couleur).
DISPOSITION_ZONE_CODES: frozenset[str] = frozenset({
    DG_ZONE_CODE,
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


def _corpus_to_legacy_row(t: dict[str, Any]) -> dict[str, Any]:
    """Aligné sur l'ancien SELECT ppri_reglements pour _row_to_bloc."""
    return {
        "id": t.get("texte_id"),
        "commune": None,
        "code_insee": t.get("commune_insee"),
        "version_date": t.get("date_debut_applicabilite"),
        "zone_code": t.get("zone_code"),
        "titre_zone": t.get("titre"),
        "chapitre": t.get("chapitre"),
        "source_pdf_page_start": t.get("page_start"),
        "source_pdf_page_end": t.get("page_end"),
        "reglementation": t.get("reglementation"),
        "texte_id": t.get("texte_id"),
        "portee": t.get("portee"),
    }


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
    zc_raw = (row.get("zone_code") or "").strip()
    zc = _norm_zone_code(zc_raw) if zc_raw else None
    chapitre = (row.get("chapitre") or "").strip() or None
    titre = (row.get("titre_zone") or "").strip() or None
    libelle_parts = [p for p in (chapitre, titre) if p]
    if libelle_parts:
        libelle = " — ".join(libelle_parts)
    elif zc == DG_ZONE_CODE:
        libelle = DG_LIBELLE
    elif zc:
        libelle = f"PPRI — {zc}"
    else:
        libelle = DG_LIBELLE
    return {
        "id": row.get("id"),
        "texte_id": row.get("texte_id") or (
            str(row["id"]) if row.get("id") else None
        ),
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
    if (row.get("portee") or "") == "globale":
        return True
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


def _parse_requested_codes(codes_zone: list[str] | str | None) -> list[str]:
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
    return requested


def _aggregate_blocs(
    blocs: list[dict[str, Any]],
    *,
    type_label: str,
    zone_code: str | None,
    libelle: str | None = None,
) -> dict[str, Any]:
    found_blocs = [b for b in blocs if b.get("found")]
    texts = [b["reglementation"] for b in found_blocs]
    combined = "\n\n---\n\n".join(texts) if texts else None
    if libelle is None:
        if zone_code and zone_code != DG_ZONE_CODE:
            libelle = f"Zone {zone_code} (PPRI)"
            if found_blocs and found_blocs[0].get("titre_zone"):
                libelle = f"{found_blocs[0]['titre_zone']} (PPRI)"
        else:
            libelle = DG_LIBELLE
    return {
        "type": type_label,
        "zone_code": zone_code,
        "libelle": libelle,
        "reglementation": combined,
        "texte_id": (found_blocs[0].get("texte_id") if found_blocs else None),
        "texte_ids": [b.get("texte_id") for b in found_blocs if b.get("texte_id")],
        "blocs": blocs,
        "blocs_count": len(blocs),
        "blocs_found": len(found_blocs),
        "found": bool(texts),
        "error": None if texts else "Aucun texte de réglementation en base pour cette entrée.",
    }


def _fetch_rows(
    db_config: dict,
    code_insee: str,
    requested: list[str],
    *,
    include_dispositions_generales: bool,
) -> list[dict[str, Any]]:
    codes = resoudre_codes_zonage(
        db_config, requested, document_type="PPRI", insee=code_insee
    )
    textes = fetch_textes(
        db_config,
        document_type="PPRI",
        codes=codes,
        include_globale=include_dispositions_generales,
        insee=code_insee,
    )
    return [_corpus_to_legacy_row(t) for t in textes]


def get_reglement_ppri(
    db_config: dict,
    codes_zone: list[str] | None = None,
    *,
    include_dispositions_communes: bool = True,
) -> dict:
    """
    Récupère le règlement PPRI depuis ``corpus.textes`` :
    dispositions générales (``zone_code`` = DG, texte fusionné DG1–DG3) + zones demandées.
    """
    requested = _parse_requested_codes(codes_zone)
    include_dg = include_dispositions_communes
    code_insee = _resolve_insee()

    if not requested and not include_dg:
        return {
            "dispositions_communes": [],
            "dispositions_generales": [],
            "zones": [],
            "dispositions_communes_found": 0,
            "dispositions_generales_found": 0,
            "zones_found": 0,
            "zones_requested": [],
            "code_insee": code_insee,
            "zones_available_in_db": [],
            "error": "Aucune zone demandée et dispositions générales désactivées.",
        }

    try:
        rows = _fetch_rows(
            db_config,
            code_insee,
            requested,
            include_dispositions_generales=include_dg,
        )
    except Exception as e:
        logger.error("get_reglement_ppri — SQL échoué : %s", e)
        return {
            "dispositions_communes": [],
            "dispositions_generales": [],
            "zones": [],
            "dispositions_communes_found": 0,
            "dispositions_generales_found": 0,
            "zones_found": 0,
            "zones_requested": requested,
            "code_insee": code_insee,
            "zones_available_in_db": [],
            "error": str(e),
        }

    disposition_blocs: list[dict[str, Any]] = []
    by_zone: dict[str, list[dict[str, Any]]] = {}

    for row in rows:
        bloc = _row_to_bloc(row)
        if _is_disposition_row(row):
            disposition_blocs.append({**bloc, "type": "dispositions_generales"})
            continue
        zc = _norm_zone_code(row.get("zone_code") or "")
        if not zc:
            continue
        by_zone.setdefault(zc, []).append({**bloc, "type": "zone"})

    dispositions_out: list[dict] = []
    if include_dg:
        if disposition_blocs:
            dispositions_out.append(
                _aggregate_blocs(
                    disposition_blocs,
                    type_label="dispositions_generales",
                    zone_code=DG_ZONE_CODE,
                    libelle=DG_LIBELLE,
                )
            )
        else:
            empty = {
                "type": "dispositions_generales",
                "zone_code": DG_ZONE_CODE,
                "libelle": DG_LIBELLE,
                "reglementation": None,
                "blocs": [],
                "blocs_count": 0,
                "blocs_found": 0,
                "found": False,
                "error": (
                    f"Aucune disposition générale trouvée en base "
                    f"(attendu zone_code = {DG_ZONE_CODE!r})."
                ),
            }
            dispositions_out.append(empty)

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
        "dispositions_generales": dispositions_out,
        "zones": zones_out,
        "dispositions_communes_found": disp_found,
        "dispositions_generales_found": disp_found,
        "zones_found": zones_found,
        "zones_requested": requested,
        "code_insee": code_insee,
        "zones_available_in_db": sorted(by_zone.keys()),
        "zones_ppri_reference": {
            "dispositions_generales": DG_ZONE_CODE,
            "zones_couleur": list(PPRI_ZONES_COULEUR),
        },
        "error": None,
    }


DECL_REGLEMENT_PPRI = types.FunctionDeclaration(
    name="get_reglement_ppri",
    description=(
        "Récupère le règlement écrit du PPRI (Plan de Prévention des Risques "
        "d'Inondation) de Latresne. "
        "Inclut TOUJOURS automatiquement les dispositions générales (zone_code DG, "
        "texte fusionné DG1–DG3) — ne pas passer DG dans codes_zone. "
        f"Zones couleur valides (orthographe exacte) : {_PPRI_ZONES_COULEUR_HELP}. "
        "Passer dans codes_zone uniquement les zones intersectant la parcelle "
        "(codes issus de get_contexte_parcelle / cartographie PPRI quand disponibles). "
        "Ne pas confondre avec get_reglement_zone (PLU) ni get_reglement_pprmvt (PPRMVT)."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "codes_zone": types.Schema(
                type=types.Type.ARRAY,
                items=types.Schema(type=types.Type.STRING),
                description=(
                    f"Une ou plusieurs zones couleur PPRI parmi : {_PPRI_ZONES_COULEUR_HELP}. "
                    f"Ex. ['BLEUE', 'ROUGE_URBA']. DG ({DG_ZONE_CODE}) est ajouté automatiquement."
                ),
            ),
            "include_dispositions_communes": types.Schema(
                type=types.Type.BOOLEAN,
                description=(
                    f"Inclure les dispositions générales (zone_code {DG_ZONE_CODE}, défaut true). "
                    "Mettre false uniquement pour ne charger que des zones couleur."
                ),
            ),
        },
    ),
)
