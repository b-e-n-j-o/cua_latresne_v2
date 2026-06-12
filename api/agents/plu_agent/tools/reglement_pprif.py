"""Tool get_pprif_reglement — règlement écrit PPRIF Argelès (table reglements_pprif)."""

from __future__ import annotations

import logging
import re
from typing import Any

from google.genai import types

from ..commune_context import get_current_profile_optional, q
from .utils.db import db_query

logger = logging.getLogger("plu_tools")

TABLE_NAME = "reglements_pprif"
DG_ZONE_CODE = "DG"
VALID_ZONE_CODES: frozenset[str] = frozenset({"R", "B1", "B2", "B3", "B4"})
ORDER_ZONE = {"DG": 0, "R": 1, "B1": 2, "B2": 3, "B3": 4, "B4": 5}

PPRIF_ZONES_REFERENCE: dict[str, str] = {
    "R": "zone rouge",
    "B1": "zone bleue B1",
    "B2": "zone bleue B2",
    "B3": "zone bleue B3",
    "B4": "zone blanche B4",
}
_PPRIF_ZONES_HELP = (
    "R (rouge), B1/B2/B3 (bleues), B4 (blanche) — ou ALL pour l'ensemble"
)


def _norm_zone_code(raw: str) -> str | None:
    s = str(raw or "").strip().upper()
    s = s.replace("É", "E").replace("È", "E")
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"[^A-Z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")

    if s in ("DISPOSITIONS_GENERALES", "DISPOSITION_GENERALE", "DG", "GENERAL"):
        return DG_ZONE_CODE
    if s in VALID_ZONE_CODES:
        return s
    if s in ("ROUGE", "ZONE_ROUGE", "ZONE_R", "ZR"):
        return "R"
    if s in ("BLANC", "BLANCHE", "ZONE_BLANCHE", "ZONE_B4", "ZB4"):
        return "B4"
    for code in ("B1", "B2", "B3", "B4"):
        if s == f"ZONE_{code}" or s == f"BLEU_{code}" or s == f"BLEUE_{code}":
            return code
    if s in ("BLEU", "BLEUE", "ZONE_BLEUE"):
        return None
    return None


def _zone_from_carto_label(raw: str) -> str | None:
    """Déduit un zone_code depuis label/degre de la couche spatiale pprif."""
    direct = _norm_zone_code(raw)
    if direct and direct != DG_ZONE_CODE:
        return direct

    s = str(raw or "").strip().upper()
    compact = re.sub(r"[^A-Z0-9]", "", s)
    if compact in VALID_ZONE_CODES:
        return compact

    if "ROUGE" in s or compact == "R":
        return "R"
    if "BLANC" in s:
        return "B4"
    for code in ("B1", "B2", "B3", "B4"):
        if code in compact or code in s.replace(" ", ""):
            return code
    if "BLEU" in s:
        for code in ("B1", "B2", "B3"):
            if code in s:
                return code
    return None


def infer_pprif_reglement_zone_codes(
    *,
    zone_codes: list[str] | str | None = None,
    pprif_intersections: list[dict] | None = None,
) -> list[str]:
    """
    Déduit les zone_code à charger dans reglements_pprif.

  - zone_codes explicite (prioritaire) : R, B1, B2, B3, B4 ou ALL.
    - pprif_intersections : objets de get_contexte_parcelle (label, degre).
    """
    if zone_codes:
        if isinstance(zone_codes, str):
            raw_list = [zone_codes]
        else:
            raw_list = list(zone_codes)
        if len(raw_list) == 1 and str(raw_list[0]).upper() in ("ALL", "TOUT", "ENSEMBLE"):
            return ["R", "B1", "B2", "B3", "B4"]
        out: list[str] = []
        for z in raw_list:
            nz = _norm_zone_code(z)
            if nz and nz != DG_ZONE_CODE and nz not in out:
                out.append(nz)
        return out

    out: list[str] = []
    for item in pprif_intersections or []:
        for key in ("label", "Zone PPRIF", "degre", "Libellé", "Degré", "code_degre"):
            val = item.get(key)
            if not val:
                continue
            nz = _zone_from_carto_label(str(val))
            if nz and nz not in out:
                out.append(nz)
    return out


def _expand_with_dg(codes: list[str], *, include_dg: bool) -> list[str]:
    if not include_dg:
        return codes
    if codes and DG_ZONE_CODE not in codes:
        return [DG_ZONE_CODE, *codes]
    if not codes:
        return [DG_ZONE_CODE]
    return codes


def _fetch_rows(db_config: dict, zone_codes: list[str]) -> list[dict[str, Any]]:
    if not zone_codes:
        return []
    placeholders = ", ".join(["%s"] * len(zone_codes))
    sql = f"""
        SELECT zone_code, type_piece, chapitre, titre_zone, reglementation
        FROM {q(TABLE_NAME)}
        WHERE upper(trim(zone_code)) IN ({placeholders})
    """
    keys = [z.upper() for z in zone_codes]
    rows = db_query(db_config, sql, tuple(keys))
    return sorted(
        rows,
        key=lambda r: ORDER_ZONE.get(_norm_zone_code(r.get("zone_code") or "") or "", 99),
    )


def get_pprif_reglement(
    db_config: dict,
    zone_codes: list[str] | str | None = None,
    *,
    pprif_intersections: list[dict] | None = None,
    include_dispositions_generales: bool = True,
) -> dict:
    """
    Récupère le règlement PPRIF depuis ``{schema}.reglements_pprif``.

    zone_code : DG, R, B1, B2, B3, B4 — ou ALL pour l'ensemble des zones couleur.
    Les dispositions générales (DG) sont incluses automatiquement avec les zones demandées.
    """
    profile = get_current_profile_optional()
    schema = profile.schema if profile else "argeles"

    resolved = infer_pprif_reglement_zone_codes(
        zone_codes=zone_codes,
        pprif_intersections=pprif_intersections,
    )
    if include_dispositions_generales:
        resolved = _expand_with_dg(resolved, include_dg=True)
    else:
        resolved = [c for c in resolved if c != DG_ZONE_CODE]

    resolved = sorted(set(resolved), key=lambda c: ORDER_ZONE.get(c, 99))

    if not resolved:
        return {
            "schema": schema,
            "zone_codes_requested": [],
            "zone_codes_fetched": [],
            "dispositions_generales": None,
            "zones": [],
            "zones_found": 0,
            "zones_pprif_reference": PPRIF_ZONES_REFERENCE,
            "error": "Aucune zone PPRIF à charger.",
        }

    try:
        rows = _fetch_rows(db_config, resolved)
    except Exception as e:
        logger.error("get_pprif_reglement — SQL échoué : %s", e)
        return {
            "schema": schema,
            "zone_codes_requested": resolved,
            "zone_codes_fetched": [],
            "dispositions_generales": None,
            "zones": [],
            "zones_found": 0,
            "zones_pprif_reference": PPRIF_ZONES_REFERENCE,
            "error": str(e),
        }

    by_code: dict[str, dict[str, Any]] = {}
    for row in rows:
        zc = _norm_zone_code(row.get("zone_code") or "") or str(row.get("zone_code") or "")
        by_code[zc] = row

    dg_row = by_code.get(DG_ZONE_CODE)
    dispositions_out = None
    if DG_ZONE_CODE in resolved:
        text = (dg_row or {}).get("reglementation") or ""
        dispositions_out = {
            "zone_code": DG_ZONE_CODE,
            "chapitre": (dg_row or {}).get("chapitre"),
            "reglementation": text.strip() or None,
            "found": bool(text.strip()),
        }

    zones_out: list[dict[str, Any]] = []
    for code in resolved:
        if code == DG_ZONE_CODE:
            continue
        row = by_code.get(code)
        text = (row or {}).get("reglementation") or ""
        zones_out.append(
            {
                "zone_code": code,
                "couleur": PPRIF_ZONES_REFERENCE.get(code),
                "chapitre": (row or {}).get("chapitre") if row else None,
                "reglementation": text.strip() or None,
                "found": bool(text.strip()),
                "error": None if text.strip() else f"Aucun règlement PPRIF pour zone_code={code!r}.",
            }
        )

    zones_found = sum(1 for z in zones_out if z.get("found"))
    if dispositions_out and dispositions_out.get("found"):
        zones_found += 1

    note_parts: list[str] = []
    if len([z for z in zones_out if z.get("found")]) > 1:
        note_parts.append(
            "Plusieurs zones PPRIF intersectent la parcelle : appliquer le règlement "
            "de chaque zone sur la partie correspondante."
        )

    return {
        "schema": schema,
        "table": TABLE_NAME,
        "zone_codes_requested": resolved,
        "zone_codes_fetched": sorted(by_code.keys(), key=lambda c: ORDER_ZONE.get(c, 99)),
        "dispositions_generales": dispositions_out,
        "zones": zones_out,
        "zones_found": zones_found,
        "zones_pprif_reference": PPRIF_ZONES_REFERENCE,
        "guidance": " ".join(note_parts) if note_parts else None,
        "zones_available_in_db": sorted(
            c for c in by_code.keys() if c != DG_ZONE_CODE
        ),
        "error": None,
    }


DECL_REGLEMENT_PPRIF = types.FunctionDeclaration(
    name="get_pprif_reglement",
    description=(
        "Récupère le règlement écrit du PPRIF d'Argelès-sur-Mer (Plan de Prévention des "
        "Risques Incendie de Forêt) depuis reglements_pprif. "
        f"Zones couleur en base : {_PPRIF_ZONES_HELP}. "
        "Les dispositions générales (DG) sont toujours incluses automatiquement avec les zones "
        "demandées — ne pas passer DG dans zone_codes. "
        "Workflow : d'abord get_contexte_parcelle ; si la couche PPRIF intersecte la parcelle, "
        "utiliser le label (ex. R, B1, B2) des éléments dans couches_supplementaires (groupe "
        "pprif) pour déduire les zones, ou passer pprif_intersections. "
        "Ne pas confondre avec get_ppr_reglement (PPR inondation I/II/III) ni get_reglement_zone (PLU)."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "zone_codes": types.Schema(
                type=types.Type.ARRAY,
                items=types.Schema(type=types.Type.STRING),
                description=(
                    "Zones à charger : R (rouge), B1/B2/B3 (bleues), B4 (blanche), ou ALL. "
                    "Prioritaire si renseigné. Les DG sont ajoutées automatiquement."
                ),
            ),
            "pprif_intersections": types.Schema(
                type=types.Type.ARRAY,
                items=types.Schema(type=types.Type.OBJECT),
                description=(
                    "Entrées PPRIF de get_contexte_parcelle (couches_supplementaires, groupe "
                    "pprif) : objets avec label / degre pour déduire R, B1, B2, B3 ou B4."
                ),
            ),
            "include_dispositions_generales": types.Schema(
                type=types.Type.BOOLEAN,
                description=(
                    "Inclure les dispositions générales (DG) avec les zones couleur (défaut true)."
                ),
            ),
        },
    ),
)
