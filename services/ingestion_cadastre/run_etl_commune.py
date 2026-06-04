#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_etl_commune.py — CLI ETL commune (parcelles → BAN → enrichissement).

Sur Render : préférer le Cron Job (render.yaml) ou POST /admin/etl/commune.

Usage local :
  cd cua_latresne_v4
  python services/ingestion_cadastre/run_etl_commune.py --schema latresne --insee 33234 --parcelles-mode etalab
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Racine backend pour imports services.*
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from services.ingestion_cadastre.etl_pipeline import (  # noqa: E402
    DEFAULT_BACKEND_URL,
    EtlConfig,
    execute_etl,
    sanitize_insee,
    sanitize_schema,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def parse_args():
    p = argparse.ArgumentParser(description="ETL quotidien commune")
    p.add_argument("--schema", required=True)
    p.add_argument("--insee", required=True)
    p.add_argument(
        "--parcelles-mode",
        choices=("api", "etalab", "schema-only", "skip"),
        default="etalab",
    )
    p.add_argument("--backend-url", default=os.getenv("BACKEND_URL", DEFAULT_BACKEND_URL))
    p.add_argument("--internal-token", default=None)
    p.add_argument("--force-parcelles", action="store_true")
    p.add_argument("--skip-ban", action="store_true")
    p.add_argument("--skip-enrich", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-slack", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = EtlConfig(
        schema=sanitize_schema(args.schema),
        insee=sanitize_insee(args.insee),
        parcelles_mode=args.parcelles_mode,
        backend_url=args.backend_url,
        internal_token=args.internal_token,
        force_parcelles=args.force_parcelles,
        skip_ban=args.skip_ban,
        skip_enrich=args.skip_enrich,
        dry_run=args.dry_run,
        no_slack=args.no_slack,
    )
    try:
        execute_etl(cfg)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
