#!/usr/bin/env python3
"""
PostGIS -> PMTiles (ogr2ogr) -> Supabase Storage.

Pré-requis : GDAL >= 3.8 (driver PMTiles) | pip install supabase python-dotenv psycopg2-binary

.env attendu :
  SUPABASE_HOST, SUPABASE_PORT, SUPABASE_USER, SUPABASE_PASSWORD, SUPABASE_DB
  SUPABASE_URL          # URL API (client Storage)
  SERVICE_KEY           # service_role (écriture Storage)
  PMTILES_BUCKET=pmtiles   # optionnel

Exemples :
  python pmtiles.py ma_commune zonage_plu ma_commune_zonage.pmtiles
  python pmtiles.py ma_commune parcelles parcelles.pmtiles -l parcelles
  python pmtiles.py ma_commune zonage_plu zonage.pmtiles --prefix ma_commune

Batch (catalogue) :
  python pmtiles_batch.py --schema argeles
  python pmtiles_batch.py --schema argeles --dry-run
"""

from __future__ import annotations

import argparse
import sys

from pmtiles_core import process_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Génère des PMTiles depuis une table PostGIS et les uploade sur Supabase Storage.",
        epilog="Exemple : python pmtiles.py ma_commune zonage_plu ma_commune_zonage.pmtiles",
    )
    parser.add_argument("schema", help="Schéma PostgreSQL source")
    parser.add_argument("table", help="Table PostGIS source (colonne geom_3857 requise)")
    parser.add_argument("filename", help="Nom du fichier PMTiles dans le bucket")
    parser.add_argument("--prefix", help="Dossier dans le bucket (défaut : nom du schéma)")
    parser.add_argument(
        "-l",
        "--layer-name",
        help='Nom source-layer MapLibre (défaut : nom de table)',
    )
    parser.add_argument(
        "--geom-type",
        choices=("surfacique", "lineaire", "ponctuel"),
        default="surfacique",
        help="Type de géométrie pour le SQL de transformation",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"→ {args.schema}.{args.table} (zoom 12–16)")
    result = process_table(
        args.schema,
        args.table,
        args.filename,
        layer_name=args.layer_name,
        prefix=args.prefix,
        geom_type=args.geom_type,
    )

    if result.status == "ok":
        print(f"  {result.message}")
        if result.layer_name:
            print(f'  source-layer MapLibre : "{result.layer_name}"')
        print("✅ terminé")
        return

    print(f"❌ {result.message}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
