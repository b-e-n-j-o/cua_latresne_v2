#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crée et remplit geom_3857 pour chaque couche du catalogue carto.

Parcourt catalogue_carto_argeles.json : pour chaque table (sauf src=geojson),
si geom_3857 est absente ou partiellement vide, la crée et la remplit depuis
geom_2154 (ou geometry en repli) via ST_Transform(..., 3857).

Exemples :
  python3 backfill_geom_3857_carto.py
  python3 backfill_geom_3857_carto.py --dry-run
  python3 backfill_geom_3857_carto.py --only zonage_plu,prescriptions_surf
  python3 backfill_geom_3857_carto.py --schema argeles --table zonage_plu
  python3 backfill_geom_3857_carto.py --schema mon_schema --table ma_table --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

sys.path.insert(0, str(Path(__file__).resolve().parent))
from geom_utils import backfill_geom_3857, ensure_geom_3857_column, normalize_pg_geom_type

DEFAULT_CATALOGUE = (
    Path(__file__).resolve().parents[2]
    / "BACKEND_PRINCIPAL"
    / "LATRESNE"
    / "cua_latresne_v4"
    / "api"
    / "cuas"
    / "catalogue_carto_argeles.json"
)

GEOM_HINT_3857 = {
    "surf": "geometry(MultiPolygon, 3857)",
    "lin": "geometry(MultiLineString, 3857)",
    "pct": "geometry(MultiPoint, 3857)",
}

SOURCE_GEOM_CANDIDATES = ("geom_2154", "geometry")


def get_engine():
    load_dotenv()
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        host=os.getenv("SUPABASE_HOST"),
        port=int(os.getenv("SUPABASE_PORT", "5432")),
        database=os.getenv("SUPABASE_DB"),
    )
    return create_engine(url, pool_pre_ping=True)


def load_catalogue(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def table_exists(engine, schema: str, table: str) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = :schema AND table_name = :table
                    )
                    """
                ),
                {"schema": schema, "table": table},
            ).scalar()
        )


def column_exists(engine, schema: str, table: str, column: str) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = :schema
                          AND table_name = :table
                          AND column_name = :column
                    )
                    """
                ),
                {"schema": schema, "table": table, "column": column},
            ).scalar()
        )


def count_rows_to_backfill(
    engine, schema: str, table: str, source_col: str, *, has_geom_3857: bool
) -> int:
    """Lignes à traiter : geom_3857 NULL ou colonne absente."""
    q = f'"{schema}"."{table}"'
    c = f'"{source_col}"'
    if has_geom_3857:
        sql = f"""
            SELECT COUNT(*) FROM {q}
            WHERE geom_3857 IS NULL
              AND {c} IS NOT NULL
              AND NOT ST_IsEmpty({c})
        """
    else:
        sql = f"""
            SELECT COUNT(*) FROM {q}
            WHERE {c} IS NOT NULL
              AND NOT ST_IsEmpty({c})
        """
    with engine.connect() as conn:
        return int(conn.execute(text(sql)).scalar() or 0)


def resolve_source_geom(engine, schema: str, table: str) -> str | None:
    for col in SOURCE_GEOM_CANDIDATES:
        if column_exists(engine, schema, table, col):
            return col

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT f_geometry_column
                FROM geometry_columns
                WHERE f_table_schema = :schema
                  AND f_table_name = :table
                  AND f_geometry_column <> 'geom_3857'
                ORDER BY f_geometry_column
                LIMIT 1
                """
            ),
            {"schema": schema, "table": table},
        ).fetchone()
    return row[0] if row else None


def resolve_geom_3857_type(engine, schema: str, table: str, source_col: str, geom_hint: str) -> str:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT type
                FROM geometry_columns
                WHERE f_table_schema = :schema
                  AND f_table_name = :table
                  AND f_geometry_column = :col
                """
            ),
            {"schema": schema, "table": table, "col": source_col},
        ).fetchone()

    if row and row[0]:
        return normalize_pg_geom_type(str(row[0]), 3857)

    return GEOM_HINT_3857.get(geom_hint, "geometry(Geometry, 3857)")


def process_layer(
    engine,
    schema: str,
    table: str,
    meta: dict,
    *,
    dry_run: bool,
) -> str:
    if meta.get("src") == "geojson":
        return "skip (geojson)"

    if not table_exists(engine, schema, table):
        return "skip (table absente)"

    source_col = resolve_source_geom(engine, schema, table)
    if not source_col:
        return "skip (pas de geom_2154 ni geometry)"

    geom_hint = meta.get("geom", "surf")
    pg_type = resolve_geom_3857_type(engine, schema, table, source_col, geom_hint)
    table_fqn = f'"{schema}"."{table}"'
    had_col = column_exists(engine, schema, table, "geom_3857")
    pending = count_rows_to_backfill(engine, schema, table, source_col, has_geom_3857=had_col)

    if pending == 0:
        return "ok (déjà rempli)" if had_col else "skip (0 entité source)"

    if dry_run:
        action = "créer + remplir" if not had_col else "remplir"
        return f"dry-run: {action} {pending} ligne(s) depuis {source_col} → {pg_type}"

    if not had_col:
        ensure_geom_3857_column(engine, table_fqn, pg_type)

    filled = backfill_geom_3857(engine, table_fqn, source_col, pg_type)
    return f"{'colonne créée, ' if not had_col else ''}{filled} ligne(s) remplie(s)"


def resolve_layers(args) -> tuple[str, dict]:
    """Retourne (schema, {table: meta}) selon le mode CLI."""
    if args.table:
        schema = args.schema or "argeles"
        meta: dict = {"geom": "surf"}
        if args.catalogue.is_file():
            catalogue = load_catalogue(args.catalogue)
            meta = catalogue.get("layers", {}).get(args.table, meta)
        return schema, {args.table: meta}

    if not args.catalogue.is_file():
        sys.exit(f"Catalogue introuvable : {args.catalogue}")

    catalogue = load_catalogue(args.catalogue)
    schema = args.schema or catalogue.get("schema", "argeles")
    layers: dict = catalogue.get("layers") or {}

    if args.only:
        wanted = {t.strip() for t in args.only.split(",") if t.strip()}
        layers = {k: v for k, v in layers.items() if k in wanted}

    return schema, layers


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill geom_3857 depuis le catalogue carto")
    ap.add_argument("--catalogue", type=Path, default=DEFAULT_CATALOGUE)
    ap.add_argument(
        "--schema",
        metavar="SCHEMA",
        help="Schéma PostgreSQL (défaut : catalogue ou 'argeles' avec --table)",
    )
    ap.add_argument(
        "--table",
        metavar="TABLE",
        help="Une seule table (sans parcourir tout le catalogue)",
    )
    ap.add_argument("--only", help="Tables comma-separées (mode catalogue)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.table and args.only:
        sys.exit("Utiliser --table OU --only, pas les deux.")

    schema, layers = resolve_layers(args)

    engine = get_engine()

    print("=" * 72)
    print(f"Backfill geom_3857 — schéma {schema!r} — {len(layers)} couche(s)")
    if args.table:
        print(f"Cible : {schema}.{args.table}")
    else:
        print(f"Catalogue : {args.catalogue}")
    if args.dry_run:
        print("Mode : DRY-RUN")
    print("=" * 72)

    for table in sorted(layers):
        meta = layers[table]
        title = meta.get("title", table)
        print(f"\n→ {table} ({title})")
        try:
            msg = process_layer(engine, schema, table, meta, dry_run=args.dry_run)
            print(f"  {msg}")
        except Exception as exc:
            print(f"  ❌ {exc}")

    print("\n🏁 FIN")


if __name__ == "__main__":
    main()
