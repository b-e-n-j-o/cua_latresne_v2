# -*- coding: utf-8 -*-
"""
Scraper RAA Pyrénées-Orientales (66) — intégré au backend veille RAA.

Particularités de la préfecture des P-O :
- 1 page par année : .../Le-recueil-des-actes-administratifs/Annee-{annee}
- tableau à 12 colonnes + bloc « récents » en haut → dédoublonnage par pdf_url
- titre / date / taille dans le libellé du lien

API publique :
    scrape_raa_po(annee) -> list[dict]
    insert_nouveaux_raa(conn, cfg, items) -> list[dict]   # diff via ON CONFLICT
"""

from __future__ import annotations

import re
from datetime import date, datetime
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup

from .raa_config import RaaCommuneConfig

BASE_DOMAIN = "https://www.pyrenees-orientales.gouv.fr"
PAGE_TEMPLATE = (
    BASE_DOMAIN
    + "/Publications/Le-recueil-des-actes-administratifs/Annee-{annee}"
)

HEADERS = {"User-Agent": "Mozilla/5.0 (Kerelia veille RAA)"}
TIMEOUT = 30

DEPARTEMENT = "66"
SOURCE = "pyrenees-orientales"

_LIBELLE_RE = re.compile(
    r"""^\s*(?:Télécharger\s+)?
        (?P<titre>.*?)
        \s+PDF\s*[-–]\s*
        (?P<taille>[\d.,]+)\s*M[bo]
        \s*[-–]\s*
        (?P<date>\d{2}/\d{2}/\d{4})
        \s*$""",
    re.IGNORECASE | re.VERBOSE,
)
_DATE_ANYWHERE_RE = re.compile(r"(\d{2}/\d{2}/\d{4})")


def _normaliser_url(url: str) -> str:
    return unquote(url).strip()


def _parse_libelle(texte: str) -> tuple[str | None, float | None, date | None]:
    texte = re.sub(r"\s+", " ", texte).strip()
    m = _LIBELLE_RE.match(texte)
    if m:
        titre = m.group("titre").strip()
        taille = float(m.group("taille").replace(",", "."))
        d = datetime.strptime(m.group("date"), "%d/%m/%Y").date()
        return (titre or None), taille, d

    d = None
    md = _DATE_ANYWHERE_RE.search(texte)
    if md:
        try:
            d = datetime.strptime(md.group(1), "%d/%m/%Y").date()
        except ValueError:
            d = None
    titre = texte.replace("Télécharger", "").strip() or None
    return titre, None, d


def _parse_links(html: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    items: dict[str, dict] = {}

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/contenu/telechargement/" not in href or ".pdf" not in href.lower():
            continue

        pdf_url = urljoin(page_url, href)
        cle = _normaliser_url(pdf_url)
        if cle in items:
            continue

        texte = a.get_text(" ", strip=True)
        titre, taille, d = _parse_libelle(texte)
        if not titre:
            titre = unquote(href.split("/")[-1]).rsplit(".pdf", 1)[0]

        items[cle] = {
            "pdf_url": pdf_url,
            "page_url": page_url,
            "titre": titre,
            "taille_mo": taille,
            "date_publication": d,
        }

    return list(items.values())


def scrape_raa_po(annee: int) -> list[dict]:
    """Télécharge la page annuelle P-O et retourne les RAA trouvés (dédupliqués)."""
    page_url = PAGE_TEMPLATE.format(annee=annee)
    resp = requests.get(page_url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    items = _parse_links(resp.text, page_url)
    items.sort(key=lambda x: (x["date_publication"] or date.min), reverse=True)
    return items


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
