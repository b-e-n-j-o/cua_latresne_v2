# -*- coding: utf-8 -*-
"""
Synchronisation RAA : scrape préfecture → diff avec la base → retourne les nouveaux.

Fonction publique : sync_raa(conn, commune_slug, annee=None) -> dict
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Callable

from .raa_config import RaaCommuneConfig, get_raa_config
from .scraper_raa_po import insert_nouveaux_raa, scrape_raa_po

logger = logging.getLogger("raa_sync")

ScrapeFn = Callable[[int], list[dict]]

# Registre des scrapers par slug communal
RAA_SCRAPERS: dict[str, ScrapeFn] = {
    "argeles": scrape_raa_po,
}


def commune_a_scraper(slug: str) -> bool:
    return slug in RAA_SCRAPERS


def sync_raa(
    conn,
    commune_slug: str,
    annee: int | None = None,
) -> dict:
    """
    Scrape la préfecture, insère les recueils absents de la base.

    Retourne :
        commune_slug, annee, nb_scrapes, nb_nouveaux, nouveaux (liste de dict)
    """
    cfg = get_raa_config(commune_slug)
    if not cfg:
        raise ValueError(f"Commune RAA inconnue : {commune_slug}")

    scrape_fn = RAA_SCRAPERS.get(commune_slug)
    if not scrape_fn:
        raise ValueError(
            f"Aucun scraper RAA configuré pour « {commune_slug} »."
        )

    year = annee or date.today().year
    logger.info("Sync RAA %s — année %s", commune_slug, year)

    items = scrape_fn(year)
    nouveaux = insert_nouveaux_raa(conn, cfg, items)

    logger.info(
        "Sync RAA %s — %d trouvé(s) en ligne, %d nouveau(x) inséré(s)",
        commune_slug, len(items), len(nouveaux),
    )

    return {
        "commune_slug": commune_slug,
        "annee": year,
        "nb_scrapes": len(items),
        "nb_nouveaux": len(nouveaux),
        "nouveaux": nouveaux,
    }


def _set_statut(conn, schema: str, raa_id: int, statut: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {schema}.raa SET statut=%s, updated_at=now() WHERE id=%s;",
            (statut, raa_id),
        )
    conn.commit()


def sync_et_analyser(
    conn,
    commune_slug: str,
    annee: int | None = None,
    client=None,
) -> dict:
    """
    Pipeline complet pour cron : scrape → diff → analyse LLM séquentielle des nouveaux.
    Les recueils déjà en base ne sont jamais re-analysés.
    """
    from .service_analyse_raa import analyser_raa, get_client

    result = sync_raa(conn, commune_slug, annee=annee)
    cfg = get_raa_config(commune_slug)
    if not cfg:
        raise ValueError(f"Commune RAA inconnue : {commune_slug}")

    gemini = client or get_client()
    ok = err = 0
    for n in result["nouveaux"]:
        raa_id = n["id"]
        _set_statut(conn, cfg.schema, raa_id, "en_cours")
        res = analyser_raa(conn, raa_id, commune_slug, client=gemini, persist=True)
        if res.get("statut") == "analyse" and not res.get("erreur"):
            ok += 1
            logger.info("Cron RAA %s — #%s analysé (%s)", commune_slug, raa_id, res.get("niveau_alerte"))
        else:
            err += 1
            logger.warning(
                "Cron RAA %s — #%s erreur : %s",
                commune_slug, raa_id, res.get("erreur"),
            )

    result["analyses_ok"] = ok
    result["analyses_err"] = err
    return result


def sync_toutes_communes(conn, annee: int | None = None, client=None) -> list[dict]:
    """Lance sync_et_analyser pour chaque commune avec scraper configuré."""
    results = []
    for slug in sorted(RAA_SCRAPERS):
        try:
            results.append(sync_et_analyser(conn, slug, annee=annee, client=client))
        except Exception as e:
            logger.error("Cron RAA — échec %s : %s", slug, e, exc_info=True)
            results.append({"commune_slug": slug, "erreur": str(e)})
    return results

