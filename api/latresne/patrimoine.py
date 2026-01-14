# api/latresne/patrimoine.py
import os
import requests
from fastapi import APIRouter, HTTPException
from functools import lru_cache

router = APIRouter(prefix="/latresne", tags=["patrimoine"])

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID")

if not all([AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID]):
    raise RuntimeError("Airtable env vars manquantes")

AIRTABLE_URL = (
    f"https://api.airtable.com/v0/"
    f"{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
)

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}"
}


def normalize_parcelle_id(pid: str) -> str:
    """AE 380 → AE380"""
    return pid.replace(" ", "").upper()


@lru_cache(maxsize=512)
def fetch_airtable_record(normalized_pid: str) -> dict | None:
    """
    Recherche Airtable avec normalisation via SUBSTITUTE.
    Cache LRU pour éviter de spammer Airtable.
    """
    formula = (
        f"SUBSTITUTE({{ID Parcelle(s)}}, ' ', '') = '{normalized_pid}'"
    )

    params = {
        "filterByFormula": formula,
        "maxRecords": 1
    }

    r = requests.get(AIRTABLE_URL, headers=HEADERS, params=params, timeout=10)

    if not r.ok:
        raise HTTPException(
            status_code=502,
            detail="Erreur Airtable"
        )

    records = r.json().get("records", [])
    if not records:
        return None

    return records[0]["fields"]


@router.get("/patrimoine/{parcelle_i}")
def get_patrimoine(parcelle_i: str):
    """
    Retourne les infos patrimoine Airtable pour une parcelle donnée.
    parcelle_i = 'AE380'
    """
    normalized = normalize_parcelle_id(parcelle_i)
    fields = fetch_airtable_record(normalized)

    if not fields:
        raise HTTPException(
            status_code=404,
            detail="Aucune donnée patrimoine trouvée"
        )

    # ⚠️ On ne renvoie PAS tout brut
    return {
        "parcelle_i": normalized,

        # Identification / zonages
        "zone_plu": fields.get("Zone PLU"),
        "zonage_ppri": fields.get("Zonage PPRI"),
        "zonage_pprmvt": fields.get("Zonage PPRMvt"),

        # Réglementaire
        "reglementation_plu": fields.get("Règlementation PLU"),
        "servitudes": fields.get("Servitudes - Prescriptions - Informations"),

        # Environnement / patrimoine
        "patrimoine_naturel": fields.get("Patrimoine naturel"),
        "zaenr": fields.get("ZAEnR"),
        "aoc": fields.get("AOC"),

        # Données descriptives
        "etablissement": fields.get("Etablissement"),
        "surface": fields.get("Surface"),
        "rue": fields.get("Rue"),

        # Projet
        "valable_projet": fields.get("Valable pour un projet"),
        "type_projet": fields.get("Type de projet"),

        # Documents
        "cua": fields.get("CUA"),
    }
