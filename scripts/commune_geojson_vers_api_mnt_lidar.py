#!/usr/bin/env python3
"""
Extrait la géométrie de ``argeles.commune`` (une ligne) en GeoJSON, puis
optionnellement appelle POST ``/admin/mnt-lidar/ingest`` sur votre API déployée.

La géométrie est envoyée en **EPSG:2154** (Lambert 93), identique à ``geom_2154``
en base : pas de reprojection, ce qui évite les dérives sur les bords de tuiles.

Usage (depuis le dossier ``cua_latresne_v4``) :

    cd cua_latresne_v4
    PYTHONPATH=. python scripts/commune_geojson_vers_api_mnt_lidar.py --save /tmp/argeles_commune.json

    # Même run : lecture base + POST ingest dès que --api-base ou MNT_LIDAR_API_BASE est défini
    PYTHONPATH=. python scripts/commune_geojson_vers_api_mnt_lidar.py \\
        --api-base https://votre-api.example.com --storage-prefix 66008

    # Ou définir MNT_LIDAR_API_BASE dans .env puis (même run : base + POST) :
    PYTHONPATH=. python scripts/commune_geojson_vers_api_mnt_lidar.py

    # Avec MNT_LIDAR_API_BASE dans .env mais affichage GeoJSON seul pour ce run :
    PYTHONPATH=. python scripts/commune_geojson_vers_api_mnt_lidar.py --geojson-only

    # dry-run sur l'API (liste des dalles sans télécharger)
    PYTHONPATH=. python scripts/commune_geojson_vers_api_mnt_lidar.py \\
        --api-base http://127.0.0.1:8000 --dry-run --sync

    # job en arrière-plan + attente fin
    PYTHONPATH=. python scripts/commune_geojson_vers_api_mnt_lidar.py \\
        --api-base https://votre-api.example.com --wait

Variables DB : ``SUPABASE_*``. Pour l'ingest HTTP : ``--api-base`` ou variable d'environnement ``MNT_LIDAR_API_BASE``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Racine du package backend (parent de ``scripts/``)
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from shapely.geometry import mapping  # noqa: E402

from services.ingestion_mnt_lidar.telecharger_mnt_ou_lidar import (  # noqa: E402
    fetch_commune_geometry,
)


def build_feature(geom, insee: str, nom: str) -> dict[str, Any]:
    return {
        "type": "Feature",
        "properties": {"insee": str(insee), "nom": nom},
        "geometry": mapping(geom),
    }


def main() -> None:
    p = argparse.ArgumentParser(
        description="GeoJSON depuis argeles.commune + appel optionnel POST /admin/mnt-lidar/ingest"
    )
    p.add_argument("--schema", default="argeles")
    p.add_argument("--table", default="commune")
    p.add_argument(
        "--save",
        metavar="FICHIER.json",
        help="Écrit un GeoJSON Feature (geometry + insee/nom) dans ce fichier",
    )
    p.add_argument(
        "--api-base",
        help="Origine de l'API (sans slash final). Si présent (ou MNT_LIDAR_API_BASE dans .env), "
        "la géométrie extraite est envoyée au POST /admin/mnt-lidar/ingest dans le même run.",
    )
    p.add_argument(
        "--geojson-only",
        action="store_true",
        help="N'appelle pas l'API (utile si MNT_LIDAR_API_BASE est dans .env mais vous voulez seulement afficher / --save).",
    )
    p.add_argument(
        "--storage-prefix",
        help="Dossier dans les buckets (défaut : code INSEE lu en base)",
    )
    p.add_argument("--no-lidar", action="store_true", help="include_lidar=false")
    p.add_argument("--no-mnt", action="store_true", help="include_mnt=false")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Côté API : ne télécharge pas, retourne la liste des dalles",
    )
    p.add_argument(
        "--sync",
        action="store_true",
        help="Côté API : background=false (réponse bloquante, risque timeout sur gros volumes)",
    )
    p.add_argument(
        "--wait",
        action="store_true",
        help="Si la réponse est 202, interroge GET .../jobs/{id} jusqu'à fin ou timeout",
    )
    p.add_argument("--poll-interval", type=float, default=5.0, help="Secondes entre deux polls si --wait")
    p.add_argument("--max-wait", type=float, default=7200.0, help="Timeout total si --wait (secondes)")
    args = p.parse_args()
    load_dotenv()
    api_base = (args.api_base or os.getenv("MNT_LIDAR_API_BASE") or "").strip().rstrip("/")
    if args.geojson_only:
        api_base = ""
    # Un seul run : la géométrie lue en base est envoyée au POST si une base d'API est connue.
    do_ingest = bool(api_base)

    geom, nom, insee_val = fetch_commune_geometry(args.schema, args.table)
    feature = build_feature(geom, insee_val, nom or "")

    if args.save:
        Path(args.save).parent.mkdir(parents=True, exist_ok=True)
        Path(args.save).write_text(json.dumps(feature, indent=2), encoding="utf-8")
        print(f"GeoJSON écrit : {args.save}")

    if not do_ingest:
        if not args.save:
            print(json.dumps(feature, indent=2, ensure_ascii=False))
        return

    import requests

    base = args.api_base.rstrip("/")
    prefix = (args.storage_prefix or str(insee_val)).strip().strip("/")
    body = {
        "geometry": feature,
        "input_crs": "EPSG:2154",
        "storage_prefix": prefix,
        "include_lidar": not args.no_lidar,
        "include_mnt": not args.no_mnt,
        "dry_run": args.dry_run,
        "download_timeout": 600,
        "background": not args.sync,
    }

    url = f"{base}/admin/mnt-lidar/ingest"
    r = requests.post(url, json=body, timeout=120)
    print(f"HTTP {r.status_code} {url}")
    try:
        data = r.json()
    except Exception:
        print(r.text[:2000])
        r.raise_for_status()
        return

    print(json.dumps(data, indent=2, ensure_ascii=False))

    if r.status_code not in (200, 202):
        r.raise_for_status()

    if args.wait and r.status_code == 202 and isinstance(data, dict):
        job_id = data.get("job_id")
        poll_path = data.get("poll_path") or f"/admin/mnt-lidar/jobs/{job_id}"
        if not job_id:
            print("--wait ignoré : pas de job_id dans la réponse")
            return
        job_url = f"{base}{poll_path}" if poll_path.startswith("/") else f"{base}/{poll_path}"
        t0 = time.monotonic()
        while time.monotonic() - t0 < args.max_wait:
            time.sleep(args.poll_interval)
            jr = requests.get(job_url, timeout=60)
            jr.raise_for_status()
            j = jr.json()
            st = j.get("status")
            print(f"[poll] {st}")
            if st in ("completed", "failed"):
                print(json.dumps(j, indent=2, ensure_ascii=False))
                if st == "failed":
                    raise SystemExit(1)
                return
        print("Timeout --max-wait atteint sans fin de job.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
