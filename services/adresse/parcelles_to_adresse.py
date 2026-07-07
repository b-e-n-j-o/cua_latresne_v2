"""
Résolution parcelle -> adresse(s) via les WFS BAN-PLUS de la Géoplateforme (IGN).

Chaîne métier
-------------
    1. (section, numero)                          -> IDU cadastral (14 car.)
       INSEE(5) + prefixe(3) + section(2) + numero(4)
       ex: ("BR", 300, "66008") -> "66008000BR0300"

    2. WFS BAN-PLUS:lien_adresse_parcelle  (idu=...)   -> liste d'id_adr
       (une parcelle peut pointer vers plusieurs adresses : nb_adr >= 1)

    3. WFS BAN-PLUS:adresse                (id_adr=...)  -> numero / rep / nom_voie / nom_com

    4. Formatage -> "621 Chemin de la Massane, Argelès-sur-Mer"

Usage
-----
    python ban_parcelle_adresse.py            # lance les tests BT 772 + BR 300
    python ban_parcelle_adresse.py BT 772     # une parcelle à la volée
    python ban_parcelle_adresse.py AB 12 --insee 33234

Dépendance : httpx  (pip install httpx)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)

WFS_URL = "https://data.geopf.fr/wfs/ows"

DEFAULT_INSEE = "66008"    # Argelès-sur-Mer
DEFAULT_PREFIXE = "000"    # préfixe (commune absorbée / arrondissement), 000 par défaut

LAYER_LIEN = "BAN-PLUS:lien_adresse_parcelle"
LAYER_ADRESSE = "BAN-PLUS:adresse"


# --------------------------------------------------------------------------- #
# 1. Construction de l'IDU cadastral
# --------------------------------------------------------------------------- #
def build_idu(
    section: str,
    numero: str | int,
    code_insee: str = DEFAULT_INSEE,
    prefixe: str = DEFAULT_PREFIXE,
) -> str:
    """Construit l'IDU cadastral à 14 caractères.

    INSEE(5) + prefixe(3) + section(2) + numero(4)

    >>> build_idu("BR", 300, "66008")
    '66008000BR0300'
    >>> build_idu("A", 5, "66008")     # section 1 lettre -> paddée '0A'
    '660080000A0005'
    """
    code_insee = str(code_insee).strip().zfill(5)
    prefixe = str(prefixe).strip().zfill(3)
    section = str(section).strip().upper().rjust(2, "0")   # "A" -> "0A", "BR" -> "BR"
    numero = str(numero).strip().rjust(4, "0")             # 300 -> "0300"
    idu = f"{code_insee}{prefixe}{section}{numero}"
    if len(idu) != 14:
        raise ValueError(f"IDU invalide ({len(idu)} caractères) : {idu!r}")
    return idu


# --------------------------------------------------------------------------- #
# Accès WFS générique
# --------------------------------------------------------------------------- #
async def _wfs_features(
    client: httpx.AsyncClient,
    typename: str,
    cql_filter: str,
    count: int = 1000,
) -> list[dict]:
    """GetFeature en GeoJSON, filtré via CQL_FILTER. Renvoie la liste de features."""
    params = {
        "SERVICE": "WFS",
        "VERSION": "2.0.0",
        "REQUEST": "GetFeature",
        "TYPENAMES": typename,          # WFS 2.0 ; si le service renâcle, tester TYPENAME
        "CQL_FILTER": cql_filter,
        "OUTPUTFORMAT": "application/json",
        "SRSNAME": "EPSG:2154",
        "COUNT": str(count),
    }
    resp = await client.get(WFS_URL, params=params, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()
    return data.get("features", [])


# --------------------------------------------------------------------------- #
# 2. lien_adresse_parcelle : idu -> [id_adr]
# --------------------------------------------------------------------------- #
async def fetch_id_adr_for_idu(client: httpx.AsyncClient, idu: str) -> list[str]:
    feats = await _wfs_features(client, LAYER_LIEN, f"idu='{idu}'")
    ids = [
        f.get("properties", {}).get("id_adr")
        for f in feats
        if f.get("properties", {}).get("id_adr")
    ]
    # dédoublonnage en conservant l'ordre
    return list(dict.fromkeys(ids))


# --------------------------------------------------------------------------- #
# 3. adresse : id_adr -> propriétés
# --------------------------------------------------------------------------- #
async def fetch_adresse(client: httpx.AsyncClient, id_adr: str) -> dict | None:
    feats = await _wfs_features(client, LAYER_ADRESSE, f"id_adr='{id_adr}'")
    return feats[0]["properties"] if feats else None


# --------------------------------------------------------------------------- #
# 4. Formatage
# --------------------------------------------------------------------------- #
def format_adresse(props: dict) -> str:
    """'621 Chemin de la Massane, Argelès-sur-Mer' à partir des attributs BAN."""
    numero = props.get("numero")
    rep = (props.get("rep") or "").strip()          # bis / ter / b ...
    voie = (props.get("nom_voie") or "").strip()
    com = (props.get("nom_com") or "").strip()

    num_part = ""
    if numero not in (None, "", 0):
        num_part = f"{numero} {rep}".strip() if rep else str(numero)

    gauche = f"{num_part} {voie}".strip()
    return f"{gauche}, {com}".strip(", ").strip()


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
@dataclass
class ResultatParcelle:
    section: str
    numero: str
    idu: str
    adresses: list[str] = field(default_factory=list)
    id_adrs: list[str] = field(default_factory=list)
    details: list[dict] = field(default_factory=list)
    duree_s: float = 0.0


def _ms(duree_s: float) -> str:
    return f"{duree_s * 1000:.0f} ms"


async def adresses_pour_parcelle(
    section: str,
    numero: str | int,
    code_insee: str = DEFAULT_INSEE,
    prefixe: str = DEFAULT_PREFIXE,
    client: httpx.AsyncClient | None = None,
) -> ResultatParcelle:
    t0 = time.perf_counter()
    parcelle_label = f"{str(section).upper()} {numero}"

    logger.info("── Parcelle %s ──", parcelle_label)

    t_step = time.perf_counter()
    idu = build_idu(section, numero, code_insee, prefixe)
    logger.info(
        "[1/4] IDU cadastral : %s (INSEE=%s, préfixe=%s) — %s",
        idu,
        code_insee,
        prefixe,
        _ms(time.perf_counter() - t_step),
    )

    own_client = client is None
    client = client or httpx.AsyncClient()
    try:
        t_step = time.perf_counter()
        id_adrs = await fetch_id_adr_for_idu(client, idu)
        logger.info(
            "[2/4] WFS %s (idu=%s) → %d id_adr — %s",
            LAYER_LIEN,
            idu,
            len(id_adrs),
            _ms(time.perf_counter() - t_step),
        )
        if id_adrs:
            logger.info("      id_adr : %s", ", ".join(id_adrs))

        t_step = time.perf_counter()
        props_list = await asyncio.gather(
            *(fetch_adresse(client, a) for a in id_adrs)
        )
        details = [p for p in props_list if p]
        logger.info(
            "[3/4] WFS %s → %d adresse(s) récupérée(s) — %s",
            LAYER_ADRESSE,
            len(details),
            _ms(time.perf_counter() - t_step),
        )

        t_step = time.perf_counter()
        adresses = [format_adresse(p) for p in details]
        logger.info(
            "[4/4] Formatage → %d adresse(s) — %s",
            len(adresses),
            _ms(time.perf_counter() - t_step),
        )
        for adr in adresses:
            logger.info("      → %s", adr)
        if not adresses:
            logger.info("      → aucune adresse liée")

        duree_s = time.perf_counter() - t0
        logger.info("⏱  Pipeline parcelle %s : %.2f s", parcelle_label, duree_s)

        return ResultatParcelle(
            section=str(section).upper(),
            numero=str(numero),
            idu=idu,
            adresses=adresses,
            id_adrs=id_adrs,
            details=details,
            duree_s=duree_s,
        )
    finally:
        if own_client:
            await client.aclose()


# --------------------------------------------------------------------------- #
# Test de logique métier
# --------------------------------------------------------------------------- #
async def _run_tests() -> None:
    parcelles = [("BT", 772), ("BR", 300)]
    t_total = time.perf_counter()
    async with httpx.AsyncClient() as client:
        for section, numero in parcelles:
            await adresses_pour_parcelle(section, numero, client=client)
            logger.info("")
    logger.info(
        "⏱  Total (%d parcelle(s)) : %.2f s",
        len(parcelles),
        time.perf_counter() - t_total,
    )


async def _run_single(section: str, numero: str, insee: str, prefixe: str) -> None:
    await adresses_pour_parcelle(section, numero, insee, prefixe)


def main() -> None:
    parser = argparse.ArgumentParser(description="Parcelle -> adresse(s) via WFS BAN-PLUS")
    parser.add_argument("section", nargs="?", help="section cadastrale, ex. BT")
    parser.add_argument("numero", nargs="?", help="numéro de parcelle, ex. 772")
    parser.add_argument("--insee", default=DEFAULT_INSEE, help="code INSEE (défaut 66008)")
    parser.add_argument("--prefixe", default=DEFAULT_PREFIXE, help="préfixe (défaut 000)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )

    if args.section and args.numero:
        asyncio.run(_run_single(args.section, args.numero, args.insee, args.prefixe))
    else:
        asyncio.run(_run_tests())


if __name__ == "__main__":
    main()