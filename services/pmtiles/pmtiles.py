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
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

MIN_ZOOM = 12
MAX_ZOOM = 16
GEOM_COL = "geom_3857"
IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

PG_CONN = (
    f"host={os.environ['SUPABASE_HOST']} "
    f"port={os.environ['SUPABASE_PORT']} "
    f"dbname={os.environ['SUPABASE_DB']} "
    f"user={os.environ['SUPABASE_USER']} "
    f"password='{os.environ['SUPABASE_PASSWORD']}'"
)


def _validate_identifier(name: str, label: str) -> str:
    if not IDENT_RE.match(name):
        sys.exit(f"Erreur : {label} invalide ({name!r}), attendu [a-zA-Z_][a-zA-Z0-9_]*")
    return name


def _pg_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ["SUPABASE_HOST"],
        port=os.environ["SUPABASE_PORT"],
        dbname=os.environ["SUPABASE_DB"],
        user=os.environ["SUPABASE_USER"],
        password=os.environ["SUPABASE_PASSWORD"],
    )


def fetch_geometry_columns(schema: str, table: str) -> set[str]:
    """Toutes les colonnes geometry de la table (geom_2154, geom_3857, …)."""
    with _pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT f_geometry_column
            FROM geometry_columns
            WHERE f_table_schema = %s AND f_table_name = %s
            """,
            (schema, table),
        )
        geom_cols = {r[0] for r in cur.fetchall()}

    if not geom_cols:
        with _pg_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND udt_name = 'geometry'
                """,
                (schema, table),
            )
            geom_cols = {r[0] for r in cur.fetchall()}

    return geom_cols


def fetch_attribute_columns(schema: str, table: str) -> list[str]:
    """Colonnes attributaires uniquement (aucune colonne geometry)."""
    geom_cols = fetch_geometry_columns(schema, table)

    if GEOM_COL not in geom_cols:
        sys.exit(f"Erreur : colonne {GEOM_COL} absente sur {schema}.{table}")

    with _pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        rows = [r[0] for r in cur.fetchall()]

    if not rows:
        sys.exit(f"Erreur : table introuvable {schema}.{table}")

    return [c for c in rows if c not in geom_cols]


def preflight_check(schema: str, table: str, sql: str) -> None:
    """Vérifie qu'il reste des entités exploitables avant ogr2ogr."""
    with _pg_connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM ({sql}) AS _src")
        count = cur.fetchone()[0]
        if count == 0:
            sys.exit("Erreur : 0 entité après filtrage géométrique — PMTiles impossible.")

        cur.execute(
            f"""
            SELECT ST_XMin(ext), ST_YMin(ext), ST_XMax(ext), ST_YMax(ext)
            FROM (
                SELECT ST_Extent({GEOM_COL}) AS ext FROM ({sql}) AS _src
            ) AS _b
            """
        )
        bounds = cur.fetchone()

    if bounds is None or any(v is None for v in bounds):
        sys.exit("Erreur : emprise (bounds) invalide — géométries corrompues ou vides.")

    print(f"  entités : {count:,} | emprise 3857 : {bounds}")


def build_sql(schema: str, table: str, attributes: list[str]) -> str:
    # Une seule geom en sortie ; MakeValid + CCW pour MapLibre fill
    geom = f"ST_ForcePolygonCCW(ST_MakeValid({GEOM_COL})) AS {GEOM_COL}"
    cols = ", ".join([*attributes, geom]) if attributes else geom
    return (
        f"SELECT {cols} FROM {schema}.{table} "
        f"WHERE {GEOM_COL} IS NOT NULL AND NOT ST_IsEmpty({GEOM_COL})"
    )


def build_pmtiles(sql: str, layer_name: str, out: Path) -> None:
    """PostGIS -> PMTiles via ogr2ogr (reprojection EPSG:4326)."""
    subprocess.run(
        [
            "ogr2ogr",
            "-f",
            "PMTiles",
            str(out),
            f"PG:{PG_CONN}",
            "-sql",
            sql,
            "-nln",
            layer_name,
            "-t_srs",
            "EPSG:4326",
            "-dsco",
            f"MINZOOM={MIN_ZOOM}",
            "-dsco",
            f"MAXZOOM={MAX_ZOOM}",
            "-skipfailures",
        ],
        check=True,
    )


def normalize_filename(name: str) -> str:
    if not name.endswith(".pmtiles"):
        name = f"{name}.pmtiles"
    return name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Génère des PMTiles depuis une table PostGIS et les uploade sur Supabase Storage.",
        epilog="Exemple : python pmtiles.py ma_commune zonage_plu ma_commune_zonage.pmtiles",
    )
    parser.add_argument(
        "schema",
        help="Schéma PostgreSQL source",
    )
    parser.add_argument(
        "table",
        help="Table PostGIS source (colonne geom_3857 requise)",
    )
    parser.add_argument(
        "filename",
        help="Nom du fichier PMTiles dans le bucket (ex. ma_commune_zonage.pmtiles)",
    )
    parser.add_argument(
        "--prefix",
        help="Dossier dans le bucket (défaut : nom du schéma passé en argument)",
    )
    parser.add_argument(
        "-l",
        "--layer-name",
        help='Nom de la couche dans le tile / "source-layer" MapLibre (défaut : nom de table)',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    schema = _validate_identifier(args.schema, "schéma")
    table = _validate_identifier(args.table, "table")
    layer_name = args.layer_name or table
    _validate_identifier(layer_name, "layer-name")

    filename = normalize_filename(args.filename)
    prefix = (args.prefix or schema).strip("/")
    remote = f"{prefix}/{filename}"

    attributes = fetch_attribute_columns(schema, table)
    sql = build_sql(schema, table, attributes)

    print(f"→ {schema}.{table} → {layer_name!r} (zoom {MIN_ZOOM}–{MAX_ZOOM})")
    print(f"  attributs : {', '.join(attributes) or '(aucun)'}")
    print(f"  SQL : {sql}")
    preflight_check(schema, table, sql)

    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SERVICE_KEY"])
    bucket = os.environ.get("PMTILES_BUCKET", "pmtiles")

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / filename
        build_pmtiles(sql, layer_name, out)
        size_mb = out.stat().st_size / 1_048_576
        print(f"  {filename} : {size_mb:.2f} Mo")

        with open(out, "rb") as f:
            sb.storage.from_(bucket).upload(
                remote,
                f.read(),
                {
                    "content-type": "application/octet-stream",
                    "cache-control": "public, max-age=86400",
                    "upsert": "true",
                },
            )
        print(f"  uploadé → {bucket}/{remote}")
        print(f'  source-layer MapLibre : "{layer_name}"')

    print("✅ terminé")


if __name__ == "__main__":
    main()
