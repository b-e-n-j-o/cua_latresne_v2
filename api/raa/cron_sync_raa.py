#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cron quotidien veille RAA — scrape + analyse des nouveaux recueils.

Usage (depuis cua_latresne_v4/) :

    python -m api.raa.cron_sync_raa
    python -m api.raa.cron_sync_raa --commune argeles --annee 2026

Variables .env : SUPABASE_* + GEMINI_API_KEY (ou GOOGLE_API_KEY).

Planification Render : voir scripts/render_cron_sync_raa.sh et render.yaml
Planification locale (ex. 7h Paris = 5h UTC en été) :

    0 5 * * * /path/to/cua_latresne_v4/scripts/render_cron_sync_raa.sh >> /tmp/raa_sync.log 2>&1
"""

from __future__ import annotations

import argparse
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
from api.raa.service_analyse_raa import get_client  # noqa: E402
from api.raa.service_sync_raa import RAA_SCRAPERS, sync_et_analyser, sync_toutes_communes  # noqa: E402

logger = logging.getLogger("cron_raa")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cron sync + analyse RAA (nouveaux recueils uniquement).")
    p.add_argument(
        "--commune",
        choices=sorted(RAA_SCRAPERS.keys()),
        help="Une seule commune (défaut : toutes celles avec scraper)",
    )
    p.add_argument("--annee", type=int, default=None, help="Année à scraper (défaut : année courante)")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    conn = psycopg2.connect(**DB_CONFIG)
    client = get_client()
    try:
        if args.commune:
            results = [sync_et_analyser(conn, args.commune, annee=args.annee, client=client)]
        else:
            results = sync_toutes_communes(conn, annee=args.annee, client=client)
    finally:
        conn.close()

    total_new = sum(r.get("nb_nouveaux", 0) for r in results)
    total_ok = sum(r.get("analyses_ok", 0) for r in results)
    total_err = sum(r.get("analyses_err", 0) for r in results)
    logger.info(
        "CRON RAA terminé — %d commune(s) | %d nouveau(x) | %d analyse(s) OK | %d erreur(s)",
        len(results), total_new, total_ok, total_err,
    )
    return 0 if total_err == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
