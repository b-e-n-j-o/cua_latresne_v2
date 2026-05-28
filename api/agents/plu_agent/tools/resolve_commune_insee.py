"""Tool resolve_commune_insee — résolution code INSEE depuis un nom de commune."""

from __future__ import annotations

import csv
import logging
import unicodedata
from functools import lru_cache
from pathlib import Path

from google.genai import types

logger = logging.getLogger("plu_tools")

CSV_PATH = Path(__file__).resolve().parents[4] / "config" / "v_commune_2025.csv"


def _normalize_name(value: str) -> str:
    s = (value or "").strip().upper()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    for ch in ("-", "'", "’", "_", "/", ",", ".", ";", ":"):
        s = s.replace(ch, " ")
    return " ".join(s.split())


@lru_cache(maxsize=1)
def _load_communes() -> list[dict]:
    rows: list[dict] = []
    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            insee = (row.get("COM") or "").strip()
            dep = (row.get("DEP") or "").strip()
            nccenr = (row.get("NCCENR") or "").strip()
            libelle = (row.get("LIBELLE") or "").strip()
            ncc = (row.get("NCC") or "").strip()
            if not insee:
                continue
            rows.append(
                {
                    "insee": insee,
                    "departement": dep,
                    "nccenr": nccenr,
                    "libelle": libelle or nccenr or ncc,
                    "norms": {
                        _normalize_name(nccenr),
                        _normalize_name(libelle),
                        _normalize_name(ncc),
                    },
                }
            )
    return rows


def _match_type_score(query_norm: str, candidate_norms: set[str]) -> tuple[str, float] | None:
    if query_norm in candidate_norms:
        return ("exact", 1.0)
    if any(n.startswith(query_norm) for n in candidate_norms if query_norm):
        return ("prefix", 0.85)
    if any(query_norm in n for n in candidate_norms if query_norm):
        return ("contains", 0.65)
    return None


def resolve_commune_insee(
    db_config: dict,  # signature homogène avec les autres tools
    commune: str,
    departement: str | None = None,
    limit: int = 8,
) -> dict:
    _ = db_config
    q = (commune or "").strip()
    if not q:
        return {
            "query": commune,
            "insee": None,
            "status": "not_found",
            "matches": [],
            "count": 0,
            "error": "Nom de commune vide.",
        }

    dep = (departement or "").strip().upper()
    qn = _normalize_name(q)
    try:
        rows = _load_communes()
    except Exception as e:
        logger.error("resolve_commune_insee — chargement CSV impossible: %s", e)
        return {
            "query": q,
            "insee": None,
            "status": "not_found",
            "matches": [],
            "count": 0,
            "error": f"Impossible de charger le référentiel communes ({e}).",
        }

    candidates = []
    for r in rows:
        if dep and r["departement"] != dep:
            continue
        ms = _match_type_score(qn, r["norms"])
        if not ms:
            continue
        match_type, score = ms
        candidates.append(
            {
                "insee": r["insee"],
                "libelle": r["libelle"],
                "departement": r["departement"],
                "match_type": match_type,
                "score": score,
            }
        )

    if not candidates:
        return {
            "query": q,
            "insee": None,
            "status": "not_found",
            "matches": [],
            "count": 0,
            "error": "Aucune commune trouvée.",
        }

    candidates.sort(
        key=lambda x: (x["score"], len(_normalize_name(x["libelle"]))),
        reverse=True,
    )
    top = candidates[: max(1, min(limit, 20))]
    unique_insee = {m["insee"] for m in top if m["match_type"] == "exact"} or {m["insee"] for m in top}

    if len(unique_insee) == 1:
        winner = top[0]
        return {
            "query": q,
            "insee": winner["insee"],
            "commune": winner["libelle"],
            "departement": winner["departement"],
            "status": "ok",
            "matches": top,
            "count": len(top),
            "error": None,
        }

    return {
        "query": q,
        "insee": None,
        "status": "ambiguous",
        "matches": top,
        "count": len(top),
        "error": "Plusieurs communes possibles. Préciser le département.",
    }


DECL_RESOLVE_COMMUNE_INSEE = types.FunctionDeclaration(
    name="resolve_commune_insee",
    description=(
        "Résout le code INSEE d'une commune à partir de son nom "
        "(recherche insensible à la casse/accents/ponctuation), avec option de filtre département."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "commune": types.Schema(
                type=types.Type.STRING,
                description="Nom de commune (ex: 'Argelès-sur-Mer', 'Latresne').",
            ),
            "departement": types.Schema(
                type=types.Type.STRING,
                description="Code département optionnel (ex: '66', '33').",
            ),
            "limit": types.Schema(
                type=types.Type.INTEGER,
                description="Nombre maximum de candidats retournés (défaut 8).",
            ),
        },
        required=["commune"],
    ),
)

