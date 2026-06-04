"""Tool get_ppr_reglement — règlement écrit PPR Argelès (table reglements_ppr)."""

from __future__ import annotations

import logging
import re
from typing import Any

from google.genai import types

from ..commune_context import get_current_profile_optional, q
from .utils.db import db_query

logger = logging.getLogger("plu_tools")

TABLE_NAME = "reglements_ppr"
DG_ZONE_CODE = "DG"
VALID_ZONE_CODES = frozenset({"DG", "I", "II", "III"})
ORDER_ZONE = {"DG": 0, "I": 1, "II": 2, "III": 3}


def _norm_zone_code(raw: str) -> str | None:
    s = str(raw or "").strip().upper()
    s = s.replace("É", "E").replace(" ", "_").replace("-", "_")
    if s in ("DISPOSITIONS_GENERALES", "DISPOSITION_GENERALE", "DG", "GENERAL"):
        return DG_ZONE_CODE
    if s in ("1", "I", "ZONE_I", "ZONE1"):
        return "I"
    if s in ("2", "II", "ZONE_II", "ZONE2"):
        return "II"
    if s in ("3", "III", "ZONE_III", "ZONE3"):
        return "III"
    if s in VALID_ZONE_CODES:
        return s
    return None


def _norm_code_degre(raw: str | int | None) -> str | None:
    if raw is None:
        return None
    return _norm_zone_code(str(raw).strip())


def infer_ppr_reglement_zone_codes(
    *,
    zone_codes: list[str] | str | None = None,
    code_degre: str | int | None = None,
    ppr_intersections: list[dict] | None = None,
    hors_zonage_ppr: bool = False,
    partie_hors_zonage_ppr: bool = False,
) -> list[str]:
    """
    Déduit les zone_code à charger dans reglements_ppr.

    - Pas d'intersection PPR (ou hors_zonage_ppr) → zone III seule (hors zonage 1/2).
    - code_degre 1 → I ; 2 → II (plus DG si include_dg géré à l'appelant).
    - Plusieurs degrés sur la parcelle → I et/ou II selon les intersections.
    - partie_hors_zonage_ppr : parcelle à cheval (ex. zone II + partie sans zonage)
      → ajoute III en plus des degrés 1/2 intersectés.
    """
    if zone_codes:
        if isinstance(zone_codes, str):
            raw_list = [zone_codes]
        else:
            raw_list = list(zone_codes)
        if len(raw_list) == 1 and str(raw_list[0]).upper() in ("ALL", "TOUT", "ENSEMBLE"):
            return ["DG", "I", "II", "III"]
        out: list[str] = []
        for z in raw_list:
            nz = _norm_zone_code(z)
            if nz and nz not in out:
                out.append(nz)
        return out

    # Parcelle entièrement hors cartographie degré 1/2
    if hors_zonage_ppr and not (ppr_intersections or code_degre is not None):
        return ["III"]

    degres: list[str] = []
    if code_degre is not None:
        nz = _norm_code_degre(code_degre)
        if nz and nz in ("I", "II", "III"):
            degres.append(nz)

    for item in ppr_intersections or []:
        cd = item.get("code_degre") or item.get("Degré") or item.get("DEGRE")
        nz = _norm_code_degre(cd)
        if nz and nz in ("I", "II") and nz not in degres:
            degres.append(nz)

    if not degres:
        return ["III"]

    # À cheval : partie en zone I/II cartographiée + partie hors zonage (= zone III)
    if partie_hors_zonage_ppr or (hors_zonage_ppr and degres):
        if "III" not in degres:
            degres.append("III")

    return degres


def _expand_with_dg(codes: list[str], *, include_dg: bool) -> list[str]:
    if not include_dg:
        return codes
    needs_dg = any(c in ("I", "II") for c in codes)
    if needs_dg and DG_ZONE_CODE not in codes:
        return [DG_ZONE_CODE, *codes]
    return codes


def _collect_sous_zone_labels(
    ppr_intersections: list[dict] | None,
    explicit: str | None,
) -> list[str]:
    labels: list[str] = []
    if explicit:
        labels.append(explicit.strip())
    for item in ppr_intersections or []:
        lab = item.get("label") or item.get("Sous-zone PPR") or item.get("LABEL")
        if lab:
            s = str(lab).strip()
            if s and s not in labels:
                labels.append(s)
    return labels


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


def get_ppr_reglement(
    db_config: dict,
    zone_codes: list[str] | str | None = None,
    *,
    code_degre: str | int | None = None,
    hors_zonage_ppr: bool = False,
    partie_hors_zonage_ppr: bool = False,
    ppr_intersections: list[dict] | None = None,
    sous_zone_label: str | None = None,
    include_dispositions_generales: bool = True,
) -> dict:
    """
    Récupère le règlement PPR depuis ``{schema}.reglements_ppr``.

    zone_code : DG, I, II, III — ou ALL pour l'ensemble.
    Les zones I et II sont accompagnées des DG par défaut.
    """
    profile = get_current_profile_optional()
    schema = profile.schema if profile else "argeles"

    resolved = infer_ppr_reglement_zone_codes(
        zone_codes=zone_codes,
        code_degre=code_degre,
        ppr_intersections=ppr_intersections,
        hors_zonage_ppr=hors_zonage_ppr,
        partie_hors_zonage_ppr=partie_hors_zonage_ppr,
    )
    if include_dispositions_generales:
        resolved = _expand_with_dg(resolved, include_dg=True)
    else:
        resolved = [c for c in resolved if c != DG_ZONE_CODE]

    resolved = sorted(set(resolved), key=lambda c: ORDER_ZONE.get(c, 99))
    sous_labels = _collect_sous_zone_labels(ppr_intersections, sous_zone_label)

    if not resolved:
        return {
            "schema": schema,
            "zone_codes_requested": [],
            "zone_codes_fetched": [],
            "sous_zone_labels": sous_labels,
            "dispositions_generales": None,
            "zones": [],
            "zones_found": 0,
            "hors_zonage_ppr": hors_zonage_ppr,
            "partie_hors_zonage_ppr": partie_hors_zonage_ppr,
            "error": "Aucune zone PPR à charger.",
        }

    try:
        rows = _fetch_rows(db_config, resolved)
    except Exception as e:
        logger.error("get_ppr_reglement — SQL échoué : %s", e)
        return {
            "schema": schema,
            "zone_codes_requested": resolved,
            "zone_codes_fetched": [],
            "sous_zone_labels": sous_labels,
            "dispositions_generales": None,
            "zones": [],
            "zones_found": 0,
            "hors_zonage_ppr": hors_zonage_ppr,
            "partie_hors_zonage_ppr": partie_hors_zonage_ppr,
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
                "chapitre": (row or {}).get("chapitre") if row else None,
                "reglementation": text.strip() or None,
                "found": bool(text.strip()),
                "error": None if text.strip() else f"Aucun règlement PPR pour zone_code={code!r}.",
            }
        )

    zones_found = sum(1 for z in zones_out if z.get("found"))
    if dispositions_out and dispositions_out.get("found"):
        zones_found += 1

    note_parts: list[str] = []
    mixte = "III" in resolved and any(c in ("I", "II") for c in resolved)
    if mixte:
        note_parts.append(
            "Parcelle mixte : appliquer le règlement des zones I/II intersectées "
            "sur la partie cartographiée, et la zone III sur la partie hors zonage 1/2."
        )
    elif hors_zonage_ppr or resolved == ["III"]:
        note_parts.append(
            "Parcelle hors zonage PPR degré 1/2 : appliquer le règlement de la zone III."
        )
    if sous_labels:
        note_parts.append(
            "Sous-zones PPR (label) à appliquer dans le texte récupéré : "
            + ", ".join(sous_labels)
        )

    return {
        "schema": schema,
        "table": TABLE_NAME,
        "zone_codes_requested": resolved,
        "zone_codes_fetched": sorted(by_code.keys(), key=lambda c: ORDER_ZONE.get(c, 99)),
        "sous_zone_labels": sous_labels,
        "dispositions_generales": dispositions_out,
        "zones": zones_out,
        "zones_found": zones_found,
        "hors_zonage_ppr": hors_zonage_ppr or resolved == ["III"],
        "partie_hors_zonage_ppr": mixte or partie_hors_zonage_ppr,
        "parcelle_mixte_i_ii_et_iii": mixte,
        "guidance": " ".join(note_parts) if note_parts else None,
        "zones_available_in_db": sorted(by_code.keys()),
        "error": None,
    }


DECL_REGLEMENT_PPR = types.FunctionDeclaration(
    name="get_ppr_reglement",
    description=(
        "Récupère le règlement écrit du PPR d'Argelès-sur-Mer (Plan de Prévention des "
        "Risques inondation / mouvements de terrain) depuis reglements_ppr. "
        "zone_code en base : DG (dispositions générales), I, II, III. "
        "Workflow : d'abord get_contexte_parcelle ; si la couche PPR inondation intersecte "
        "la parcelle, utiliser code_degre (1→zone I, 2→zone II) et label (ex. I-b2) des "
        "éléments retournés dans couches_supplementaires pour cibler la sous-réglementation. "
        "Si la parcelle n'intersecte aucun zonage PPR degré 1 ou 2, passer hors_zonage_ppr=true "
        "ou zone_codes=['III'] (zone 3 = hors cartographie degré 1/2). "
        "Si la parcelle est à cheval (ex. partie zone II + partie sans zonage), passer "
        "ppr_intersections avec les degrés trouvés ET partie_hors_zonage_ppr=true "
        "(ou zone_codes=['II','III']) pour charger II+DG et III. "
        "Pour les zones I ou II, les DG sont incluses automatiquement. "
        "Passer zone_codes=['ALL'] pour charger DG+I+II+III. "
        "Ne pas confondre avec get_reglement_zone (PLU)."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "zone_codes": types.Schema(
                type=types.Type.ARRAY,
                items=types.Schema(type=types.Type.STRING),
                description=(
                    "Zones à charger : DG, I, II, III, ou ALL. "
                    "Prioritaire si renseigné. Ex. ['I'] charge I+DG."
                ),
            ),
            "code_degre": types.Schema(
                type=types.Type.STRING,
                description=(
                    "Degré PPR issu de l'intersection (attribut code_degre / Degré de la couche "
                    "ppr) : '1' ou '2'. Alternative à zone_codes."
                ),
            ),
            "hors_zonage_ppr": types.Schema(
                type=types.Type.BOOLEAN,
                description=(
                    "true si la parcelle est entièrement hors zonage PPR degré 1/2 → zone III "
                    "seule. Ne pas utiliser seul pour un cas mixte (voir partie_hors_zonage_ppr)."
                ),
            ),
            "partie_hors_zonage_ppr": types.Schema(
                type=types.Type.BOOLEAN,
                description=(
                    "true si une partie de la parcelle est en zone I/II (ppr_intersections) "
                    "et une autre partie hors zonage cartographié → ajoute la zone III en plus "
                    "des degrés intersectés (ex. à cheval zone II + zone 3)."
                ),
            ),
            "ppr_intersections": types.Schema(
                type=types.Type.ARRAY,
                items=types.Schema(type=types.Type.OBJECT),
                description=(
                    "Entrées PPR de get_contexte_parcelle (couches_supplementaires, groupe "
                    "pprt) : objets avec code_degre et label pour déduire les zones."
                ),
            ),
            "sous_zone_label": types.Schema(
                type=types.Type.STRING,
                description=(
                    "Label PPR de la sous-zone (ex. I-b2, II-a) pour cibler le passage "
                    "pertinent dans le texte récupéré."
                ),
            ),
            "include_dispositions_generales": types.Schema(
                type=types.Type.BOOLEAN,
                description=(
                    "Inclure les DG avec les zones I/II (défaut true). false pour III seule."
                ),
            ),
        },
    ),
)
