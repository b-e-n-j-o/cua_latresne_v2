#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper RAA Gironde (33) — intégré au backend veille RAA.

Particularités de la préfecture de la Gironde :
- 1 page par mois : .../Recueil-…-de-l-annee-{annee}/{Mois}-{annee}
- Design System FR (DSFR) : chaque RAA = une « card » <div class="fr-card">
- Le titre est dans le texte du lien <a> (ex. « RAA 33 SPECIAL N° 2026-181 »)
- La taille et la date sont dans l'attribut `title` du lien
  (ex. "format pdf - 0,64 Mb - 02/07/2026")
- La date de publication est aussi dans <p class="fr-card__detail">

API publique :
    scrape_raa_gironde(annee) -> list[dict]
    insert_nouveaux_raa(conn, cfg, items) -> list[dict]

Usage standalone (scrape + insert en base, sans analyse) :

    # Depuis cua_latresne_v4/ (recommandé)
    python -m api.raa.scraper_raa_gironde --annee 2026
    python -m api.raa.scraper_raa_gironde --annee 2026 --dry-run

    # Depuis api/raa/
    python scraper_raa_gironde.py --annee 2026

Puis lancer l'analyse séparément :
    python -m api.raa.batch_analyser_raa --commune latresne

.env requis à la racine cua_latresne_v4/ : SUPABASE_* (pas de clé Gemini ici).
"""

from __future__ import annotations

import re
from datetime import date, datetime
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

from .raa_config import RaaCommuneConfig

BASE_DOMAIN = "https://www.gironde.gouv.fr"
PAGE_TEMPLATE = (
    BASE_DOMAIN
    + "/Publications/Recueil-des-Actes-Administratifs"
    + "/Recueil-des-Actes-Administratifs-de-l-annee-{annee}/{mois}-{annee}"
)

HEADERS = {"User-Agent": "Mozilla/5.0 (Kerelia veille RAA)"}
TIMEOUT = 30

DEPARTEMENT = "33"
SOURCE = "Gironde"

MOIS_NOMS = [
    "Janvier", "Fevrier", "Mars", "Avril", "Mai", "Juin",
    "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre",
]

_TITLE_ATTR_RE = re.compile(
    r"format\s+pdf\s*[-–]\s*(?P<taille>[\d.,]+)\s*M[bo]\s*[-–]\s*(?P<date>\d{2}/\d{2}/\d{4})",
    re.IGNORECASE,
)
_DATE_RE = re.compile(r"(\d{2}/\d{2}/\d{4})")


def _parse_card_link(a_tag, page_url: str) -> dict | None:
    """Extrait un RAA depuis un lien <a> DSFR pointant vers un PDF."""
    href = a_tag.get("href", "").strip()
    if not href or ".pdf" not in href.lower():
        return None

    pdf_url = urljoin(page_url, href)
    titre = a_tag.get_text(" ", strip=True) or unquote(href.split("/")[-1]).rsplit(".pdf", 1)[0]

    taille_mo: float | None = None
    date_pub: date | None = None

    title_attr = a_tag.get("title", "")
    m = _TITLE_ATTR_RE.search(title_attr)
    if m:
        taille_mo = float(m.group("taille").replace(",", "."))
        try:
            date_pub = datetime.strptime(m.group("date"), "%d/%m/%Y").date()
        except ValueError:
            pass

    if date_pub is None:
        card = a_tag.find_parent("div", class_="fr-card")
        if card:
            detail = card.find("p", class_="fr-card__detail")
            if detail:
                md = _DATE_RE.search(detail.get_text())
                if md:
                    try:
                        date_pub = datetime.strptime(md.group(1), "%d/%m/%Y").date()
                    except ValueError:
                        pass

    return {
        "pdf_url": pdf_url,
        "page_url": page_url,
        "titre": titre,
        "taille_mo": taille_mo,
        "date_publication": date_pub,
    }


def _scrape_mois(annee: int, mois_nom: str) -> list[dict]:
    """Scrape une page mensuelle et retourne les RAA trouvés."""
    page_url = PAGE_TEMPLATE.format(annee=annee, mois=mois_nom)
    try:
        resp = requests.get(page_url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
    except requests.RequestException:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    items: dict[str, dict] = {}

    for card in soup.find_all("div", class_="fr-card"):
        a = card.find("a", href=True)
        if not a:
            continue
        item = _parse_card_link(a, page_url)
        if item:
            cle = unquote(item["pdf_url"]).strip()
            if cle not in items:
                items[cle] = item

    return list(items.values())


def scrape_raa_gironde(annee: int) -> list[dict]:
    """Scrape tous les mois de l'année et retourne les RAA dédupliqués."""
    all_items: dict[str, dict] = {}
    for mois_nom in MOIS_NOMS:
        for item in _scrape_mois(annee, mois_nom):
            cle = unquote(item["pdf_url"]).strip()
            if cle not in all_items:
                all_items[cle] = item

    result = list(all_items.values())
    result.sort(key=lambda x: (x["date_publication"] or date.min), reverse=True)
    return result


def _insert_sql(schema: str) -> str:
    return f"""
    INSERT INTO {schema}.raa
        (departement, source, page_url, pdf_url, titre, date_publication, taille_mo, statut)
    VALUES
        (%(departement)s, %(source)s, %(page_url)s, %(pdf_url)s, %(titre)s,
         %(date_publication)s, %(taille_mo)s, 'detecte')
    ON CONFLICT (pdf_url) DO NOTHING
    RETURNING id, titre, date_publication, pdf_url, statut;
"""


def insert_nouveaux_raa(
    conn,
    cfg: RaaCommuneConfig,
    items: list[dict],
    *,
    departement: str = DEPARTEMENT,
    source: str = SOURCE,
) -> list[dict]:
    """
    Insère uniquement les RAA absents de la base (diff par pdf_url).
    Retourne les lignes réellement créées.
    """
    nouveaux: list[dict] = []
    sql = _insert_sql(cfg.schema)
    with conn.cursor() as cur:
        for it in items:
            cur.execute(
                sql,
                {
                    "departement": departement,
                    "source": source,
                    "page_url": it["page_url"],
                    "pdf_url": it["pdf_url"],
                    "titre": it["titre"],
                    "date_publication": it["date_publication"],
                    "taille_mo": it["taille_mo"],
                },
            )
            row = cur.fetchone()
            if row:
                nouveaux.append({
                    "id": row[0],
                    "titre": row[1],
                    "date_publication": row[2],
                    "pdf_url": row[3],
                    "statut": row[4],
                })
    conn.commit()
    return nouveaux


def _parse_args():
    import argparse
    from datetime import date as _date

    p = argparse.ArgumentParser(
        description="Scrape les RAA Gironde et les insère en base (latresne.raa).",
    )
    p.add_argument(
        "--annee",
        type=int,
        default=_date.today().year,
        help="Année à scraper (défaut : année courante)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape uniquement — affiche les recueils sans écrire en base.",
    )
    return p.parse_args()


def main() -> int:
    import logging
    import sys
    from pathlib import Path

    import psycopg2
    from dotenv import load_dotenv

    _ROOT = Path(__file__).resolve().parents[2]
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))
    load_dotenv(_ROOT / ".env")

    from api._env import DB_CONFIG  # noqa: E402
    from api.raa.raa_config import get_raa_config  # noqa: E402

    args = _parse_args()
    cfg = get_raa_config("latresne")
    if not cfg:
        print("Erreur : config RAA latresne introuvable.", file=sys.stderr)
        return 1

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    logger = logging.getLogger("scraper_raa_gironde")

    logger.info("Scrape RAA Gironde — année %s", args.annee)
    items = scrape_raa_gironde(args.annee)
    logger.info("%d recueil(s) trouvé(s) en ligne", len(items))

    if args.dry_run:
        for i, it in enumerate(items, 1):
            logger.info(
                "[%d/%d] %-10s %6.1f Mo  %s",
                i, len(items),
                it["date_publication"] or "?",
                it["taille_mo"] or 0,
                (it["titre"] or "")[:70],
            )
        logger.info("Dry-run terminé — aucune écriture en base.")
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        nouveaux = insert_nouveaux_raa(conn, cfg, items)
    finally:
        conn.close()

    deja_presents = len(items) - len(nouveaux)
    logger.info(
        "Terminé — %d nouveau(x) inséré(s), %d déjà en base",
        len(nouveaux), deja_presents,
    )
    for n in nouveaux:
        logger.info(
            "  + #%s | %s | %s",
            n["id"], n["date_publication"] or "?", (n["titre"] or "")[:60],
        )
    logger.info(
        "Prochaine étape : python -m api.raa.batch_analyser_raa --commune latresne",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
