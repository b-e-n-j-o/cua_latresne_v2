#!/usr/bin/env python3
"""Logique partagée PostGIS → PMTiles → Supabase Storage."""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import psycopg2
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

MIN_ZOOM = 12
MAX_ZOOM = 16
GEOM_COL = "geom_3857"
IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
GeomType = Literal["surfacique", "lineaire", "ponctuel"]

PG_CONN = (
    f"host={os.environ['SUPABASE_HOST']} "
    f"port={os.environ['SUPABASE_PORT']} "
    f"dbname={os.environ['SUPABASE_DB']} "
    f"user={os.environ['SUPABASE_USER']} "
    f"password='{os.environ['SUPABASE_PASSWORD']}'"
)


@dataclass
class PmtilesResult:
    schema: str
    table: str
    status: str  # ok | skipped | error
    message: str = ""
    remote: str = ""
    layer_name: str = ""
    count: int = 0
    size_mb: float = 0.0


def validate_identifier(name: str, label: str) -> str:
    if not IDENT_RE.match(name):
        raise ValueError(f"{label} invalide ({name!r}), attendu [a-zA-Z_][a-zA-Z0-9_]*")
    return name


def pg_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.environ["SUPABASE_HOST"],
        port=os.environ["SUPABASE_PORT"],
        dbname=os.environ["SUPABASE_DB"],
        user=os.environ["SUPABASE_USER"],
        password=os.environ["SUPABASE_PASSWORD"],
    )


def table_exists(schema: str, table: str) -> bool:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (schema, table),
        )
        return bool(cur.fetchone()[0])


def fetch_geometry_columns(schema: str, table: str) -> set[str]:
    with pg_connect() as conn, conn.cursor() as cur:
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
        with pg_connect() as conn, conn.cursor() as cur:
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
    geom_cols = fetch_geometry_columns(schema, table)
    if GEOM_COL not in geom_cols:
        raise ValueError(f"colonne {GEOM_COL} absente sur {schema}.{table}")

    with pg_connect() as conn, conn.cursor() as cur:
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
        raise ValueError(f"table introuvable {schema}.{table}")

    return [c for c in rows if c not in geom_cols]


def build_sql(schema: str, table: str, attributes: list[str], geom_type: GeomType = "surfacique") -> str:
    if geom_type == "surfacique":
        geom_expr = f"ST_ForcePolygonCCW(ST_MakeValid({GEOM_COL}))"
    elif geom_type == "lineaire":
        geom_expr = f"ST_MakeValid({GEOM_COL})"
    else:
        geom_expr = GEOM_COL

    geom = f"{geom_expr} AS {GEOM_COL}"
    cols = ", ".join([*attributes, geom]) if attributes else geom
    return (
        f"SELECT {cols} FROM {schema}.{table} "
        f"WHERE {GEOM_COL} IS NOT NULL AND NOT ST_IsEmpty({GEOM_COL})"
    )


def preflight_check(schema: str, table: str, sql: str) -> tuple[int, tuple | None]:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM ({sql}) AS _src")
        count = int(cur.fetchone()[0])
        if count == 0:
            return 0, None

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
        raise ValueError("emprise (bounds) invalide")

    return count, bounds


def build_pmtiles_file(sql: str, layer_name: str, out: Path) -> None:
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


def upload_pmtiles(local_path: Path, remote: str) -> None:
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SERVICE_KEY"])
    bucket = os.environ.get("PMTILES_BUCKET", "pmtiles")
    with open(local_path, "rb") as f:
        sb.storage.from_(bucket).upload(
            remote,
            f.read(),
            {
                "content-type": "application/octet-stream",
                "cache-control": "public, max-age=86400",
                "upsert": "true",
            },
        )


def process_table(
    schema: str,
    table: str,
    filename: str,
    *,
    layer_name: str | None = None,
    prefix: str | None = None,
    geom_type: GeomType = "surfacique",
    upload: bool = True,
    dry_run: bool = False,
) -> PmtilesResult:
    """Génère et uploade un PMTiles pour une table. Ne lève pas — retourne un PmtilesResult."""
    try:
        schema = validate_identifier(schema, "schéma")
        table = validate_identifier(table, "table")
        layer = layer_name or table
        validate_identifier(layer, "layer-name")

        if not table_exists(schema, table):
            return PmtilesResult(schema, table, "skipped", "table absente en base")

        filename = normalize_filename(filename)
        bucket_prefix = (prefix or schema).strip("/")
        remote = f"{bucket_prefix}/{filename}"

        attributes = fetch_attribute_columns(schema, table)
        sql = build_sql(schema, table, attributes, geom_type)
        count, bounds = preflight_check(schema, table, sql)

        if count == 0:
            return PmtilesResult(schema, table, "skipped", "table vide")

        if dry_run:
            return PmtilesResult(
                schema,
                table,
                "ok",
                f"dry-run: {count:,} entités, bounds={bounds}",
                remote=remote,
                layer_name=layer,
                count=count,
            )

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / filename
            build_pmtiles_file(sql, layer, out)
            size_mb = out.stat().st_size / 1_048_576

            if upload:
                upload_pmtiles(out, remote)

        return PmtilesResult(
            schema,
            table,
            "ok",
            f"uploadé → {remote}",
            remote=remote,
            layer_name=layer,
            count=count,
            size_mb=size_mb,
        )
    except Exception as exc:
        return PmtilesResult(schema, table, "error", str(exc))
