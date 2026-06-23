#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyse en batch tous les RAA déjà en base (pipeline scraper).

Usage :

    # Depuis cua_latresne_v4/ (recommandé)
    python -m api.raa.batch_analyser_raa --commune argeles --dry-run

    # Depuis api/raa/ (exécution directe du script)
    python batch_analyser_raa.py --commune argeles --dry-run

.env requis à la racine cua_latresne_v4/ : SUPABASE_* + GEMINI_API_KEY.

Par défaut : ignore les RAA déjà en statut `analyse` (relancer avec --force).
Logs ligne par ligne sur stdout ; bilan coût / tokens en fin de run.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]  # cua_latresne_v4/
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
load_dotenv(_ROOT / ".env")

from api._env import DB_CONFIG, GEMINI_MODEL  # noqa: E402
from api.raa.raa_config import RAA_COMMUNES, get_raa_config  # noqa: E402
from api.raa.service_analyse_raa import analyser_raa, get_client  # noqa: E402

logger = logging.getLogger("batch_raa")

_SQL_LIST = """
    SELECT id, titre, date_publication, statut, taille_mo
    FROM {schema}.raa
    WHERE (%(statuts)s IS NULL OR statut = ANY(%(statuts)s))
    ORDER BY date_publication ASC NULLS LAST, id ASC;
"""


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyse Gemini de tous les RAA en base.")
    p.add_argument(
        "--commune",
        default="argeles",
        choices=sorted(RAA_COMMUNES.keys()),
        help="Slug communal (défaut: argeles)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Ré-analyser aussi les RAA déjà en statut « analyse ».",
    )
    p.add_argument(
        "--only",
        metavar="STATUTS",
        help="Filtrer par statuts CSV (ex. detecte,erreur). Ignoré si --force sans filtre explicite.",
    )
    p.add_argument("--limit", type=int, default=0, help="Nombre max de RAA à traiter (0 = tous).")
    p.add_argument("--pause", type=float, default=1.0, help="Pause en secondes entre chaque RAA.")
    p.add_argument("--dry-run", action="store_true", help="Liste les RAA sans appeler Gemini.")
    return p.parse_args()


def _list_raa(conn, schema: str, statuts: list[str] | None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            _SQL_LIST.format(schema=schema),
            {"statuts": statuts},
        )
        return cur.fetchall()


def main() -> int:
    args = _parse_args()
    cfg = get_raa_config(args.commune)
    if not cfg:
        logger.error("Commune inconnue : %s", args.commune)
        return 1

    statuts: list[str] | None
    if args.only:
        statuts = [s.strip() for s in args.only.split(",") if s.strip()]
    elif args.force:
        statuts = None
    else:
        statuts = ["detecte", "erreur", "en_cours"]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    conn = psycopg2.connect(**DB_CONFIG)
    rows = _list_raa(conn, cfg.schema, statuts)
    if args.limit > 0:
        rows = rows[: args.limit]

    total = len(rows)
    logger.info(
        "Batch RAA — %s (%s) | modèle=%s | %d recueil(s) à traiter",
        cfg.commune_label,
        cfg.schema,
        GEMINI_MODEL,
        total,
    )
    if args.dry_run:
        for i, (rid, titre, dp, statut, mo) in enumerate(rows, 1):
            logger.info(
                "[%d/%d] #%-4s %-10s %6.1f Mo  %s",
                i, total, rid, statut or "?", mo or 0, (titre or "")[:70],
            )
        logger.info("Dry-run terminé — aucun appel Gemini.")
        conn.close()
        return 0

    client = get_client()
    t0 = time.time()
    ok = err = 0
    sum_cost = 0.0
    sum_tin = sum_tout = 0

    for i, (rid, titre, dp, statut, mo) in enumerate(rows, 1):
        label = (titre or f"RAA #{rid}")[:72]
        logger.info(
            "[%d/%d] ▶ #%s | %s | statut=%s | %.1f Mo",
            i, total, rid, dp or "?", statut or "?", mo or 0,
        )
        logger.info("         %s", label)

        res = analyser_raa(conn, rid, args.commune, client=client, persist=True)
        cost = float(res.get("cout_estime") or 0)
        tin = int(res.get("tokens_in") or 0)
        tout = int(res.get("tokens_out") or 0)
        sum_cost += cost
        sum_tin += tin
        sum_tout += tout

        if res.get("statut") == "analyse" and not res.get("erreur"):
            ok += 1
            logger.info(
                "         ✓ %s | %s arrêtés (%s pertinents) | %s→%s tok | $%.4f",
                res.get("niveau_alerte") or "?",
                res.get("nb_arretes_total") or 0,
                res.get("nb_arretes_pertinents") or 0,
                f"{tin:,}",
                f"{tout:,}",
                cost,
            )
        else:
            err += 1
            logger.info("         ✗ ERREUR | $%.4f | %s", cost, res.get("erreur") or "?")

        if i < total and args.pause > 0:
            time.sleep(args.pause)

    elapsed = time.time() - t0
    logger.info("=" * 60)
    logger.info(
        "BILAN — %d traités | %d OK | %d erreur(s) | durée %.0f s (%.1f min)",
        total, ok, err, elapsed, elapsed / 60,
    )
    logger.info(
        "COÛT TOTAL — $%.4f USD | tokens entrée %s | tokens sortie %s",
        sum_cost,
        f"{sum_tin:,}",
        f"{sum_tout:,}",
    )
    logger.info("=" * 60)

    conn.close()
    return 0 if err == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
