#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_etl_all_communes.py — enchaîne l'ETL pour toutes les communes listées dans config/etl_communes.json.

Usage (depuis la racine cua_latresne_v4) :
  python services/ingestion_cadastre/run_etl_all_communes.py
  python services/ingestion_cadastre/run_etl_all_communes.py --config path/to/communes.json
  python services/ingestion_cadastre/run_etl_all_communes.py --dry-run

Utilisé par le Cron Render (render_cron_etl_latresne.sh).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from services.ingestion_cadastre.etl_pipeline import EtlConfig, execute_etl  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HERE = Path(__file__).resolve().parent
DEFAULT_CONFIG = HERE / "config" / "etl_communes.json"


def load_communes(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list) or not data:
        raise ValueError(f"{path} doit être un tableau JSON non vide")
    for i, row in enumerate(data):
        if not row.get("schema") or not row.get("insee"):
            raise ValueError(f"Ligne {i}: champs 'schema' et 'insee' requis — {row}")
    return data


def parse_args():
    p = argparse.ArgumentParser(description="ETL toutes les communes du fichier config")
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-slack", action="store_true")
    p.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Arrête au premier échec (défaut : continue les autres communes)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    if not args.config.is_file():
        log.error("Config introuvable : %s", args.config)
        sys.exit(1)

    communes = load_communes(args.config)
    log.info("=== ETL batch : %d commune(s) ===", len(communes))

    errors: list[str] = []
    for i, row in enumerate(communes, 1):
        schema = row["schema"]
        insee = row["insee"]
        label = row.get("label") or schema
        mode = row.get("parcelles_mode", "etalab")
        log.info("--- [%d/%d] %s (%s / %s) ---", i, len(communes), label, schema, insee)

        cfg = EtlConfig(
            schema=schema,
            insee=insee,
            parcelles_mode=mode,
            dry_run=args.dry_run,
            no_slack=args.no_slack,
            skip_ban=row.get("skip_ban", False),
            skip_enrich=row.get("skip_enrich", False),
            force_parcelles=row.get("force_parcelles", False),
        )
        try:
            execute_etl(cfg)
        except Exception as exc:
            msg = f"{label} ({insee}): {exc}"
            log.error("Échec %s", msg)
            errors.append(msg)
            if args.stop_on_error:
                break

    if errors:
        log.error("=== Batch terminé avec %d erreur(s) ===", len(errors))
        for e in errors:
            log.error("  • %s", e)
        sys.exit(1)

    log.info("=== Batch terminé : %d commune(s) OK ===", len(communes))


if __name__ == "__main__":
    main()
