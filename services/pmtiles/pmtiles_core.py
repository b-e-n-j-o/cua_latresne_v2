#!/usr/bin/env python3
"""Logique partagée PostGIS → PMTiles → Supabase Storage."""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

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
    audit: dict[str, Any] = field(default_factory=dict)


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


def storage_client():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SERVICE_KEY"])


def storage_bucket() -> str:
    return os.environ.get("PMTILES_BUCKET", "pmtiles")


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download_remote_pmtiles(remote: str, dest: Path) -> bool:
    """Télécharge le PMTiles déjà publié. False si absent / erreur Storage."""
    try:
        data = storage_client().storage.from_(storage_bucket()).download(remote)
    except Exception:
        return False
    if not data:
        return False
    dest.write_bytes(data)
    return dest.is_file() and dest.stat().st_size > 0


def ogr_feature_count(path: Path) -> int | None:
    try:
        proc = subprocess.run(
            ["ogrinfo", "-so", "-al", str(path)],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None
    for line in (proc.stdout or "").splitlines():
        if "Feature Count" in line:
            try:
                return int(line.split(":")[-1].strip())
            except ValueError:
                return None
    return None


def fetch_table_audit(schema: str, table: str, attributes: list[str]) -> dict[str, Any]:
    """Snapshot léger de la table source (compte, emprise, répartition d'un champ clé)."""
    group_field = next(
        (c for c in ("label", "libelle", "code_degre", "degre", "type", "nom") if c in attributes),
        None,
    )
    if group_field:
        group_field = validate_identifier(group_field, "champ audit")
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              count(*)::int,
              COALESCE(round(sum(ST_Area({GEOM_COL}))::numeric, 2), 0),
              ST_XMin(ST_Extent({GEOM_COL})),
              ST_YMin(ST_Extent({GEOM_COL})),
              ST_XMax(ST_Extent({GEOM_COL})),
              ST_YMax(ST_Extent({GEOM_COL}))
            FROM {schema}.{table}
            WHERE {GEOM_COL} IS NOT NULL AND NOT ST_IsEmpty({GEOM_COL})
            """
        )
        n, area, xmin, ymin, xmax, ymax = cur.fetchone()
        breakdown: list[tuple[str, int]] = []
        if group_field:
            cur.execute(
                f"""
                SELECT coalesce({group_field}::text, '') AS k, count(*)::int
                FROM {schema}.{table}
                WHERE {GEOM_COL} IS NOT NULL AND NOT ST_IsEmpty({GEOM_COL})
                GROUP BY 1
                ORDER BY 2 DESC, 1
                LIMIT 40
                """
            )
            breakdown = [(r[0], r[1]) for r in cur.fetchall()]

    return {
        "count": int(n or 0),
        "area_m2": float(area or 0),
        "bounds_3857": [xmin, ymin, xmax, ymax],
        "group_field": group_field,
        "group_counts": breakdown,
    }


def format_audit_log(
    *,
    table_audit: dict[str, Any],
    old_size: int | None,
    old_md5: str | None,
    old_count: int | None,
    new_size: int,
    new_md5: str,
    new_count: int | None,
    remote_existed: bool,
) -> str:
    lines = ["📋 Audit PMTiles"]
    lines.append(
        f"  Table source : {table_audit['count']:,} entités"
        f" | aire ≈ {table_audit['area_m2']:,.0f} m²"
    )
    field = table_audit.get("group_field")
    if field and table_audit.get("group_counts"):
        parts = [f"{k or '(vide)'}={c}" for k, c in table_audit["group_counts"]]
        lines.append(f"  Répartition {field} : " + " ; ".join(parts))

    if not remote_existed:
        lines.append("  Distant Storage : aucun fichier (première publication)")
    else:
        old_mb = (old_size or 0) / 1_048_576
        lines.append(
            f"  Distant (avant) : {old_mb:.2f} Mo"
            + (f" | md5={old_md5}" if old_md5 else "")
            + (f" | features ogr={old_count:,}" if old_count is not None else "")
        )

    new_mb = new_size / 1_048_576
    lines.append(
        f"  Nouveau fichier : {new_mb:.2f} Mo | md5={new_md5}"
        + (f" | features ogr={new_count:,}" if new_count is not None else "")
    )

    if remote_existed and old_md5:
        if old_md5 == new_md5:
            lines.append("  Δ binaire : identique — le PMTiles publié ne change pas")
        else:
            d_size = new_size - (old_size or 0)
            sign = "+" if d_size >= 0 else ""
            lines.append(f"  Δ binaire : fichiers différents | taille {sign}{d_size:,} octets")
            if old_count is not None and new_count is not None:
                d = new_count - old_count
                sign_c = "+" if d >= 0 else ""
                lines.append(f"  Δ features (ogr) : {sign_c}{d}")
            table_n = table_audit.get("count")
            if new_count is not None and table_n is not None and new_count != table_n:
                lines.append(
                    f"  ⚠️ ogr features ({new_count}) ≠ table ({table_n}) "
                    "(zoom / skipfailures / géoms invalides possibles)"
                )
    return "\n".join(lines)


def upload_pmtiles(local_path: Path, remote: str) -> None:
    with open(local_path, "rb") as f:
        storage_client().storage.from_(storage_bucket()).upload(
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
    audit: bool = True,
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

        table_audit = fetch_table_audit(schema, table, attributes) if audit else {}

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            out = tmp_dir / filename
            old_path = tmp_dir / f"old_{filename}"

            old_size = old_md5 = None
            old_count = None
            remote_existed = False
            if audit and upload:
                remote_existed = download_remote_pmtiles(remote, old_path)
                if remote_existed:
                    old_size = old_path.stat().st_size
                    old_md5 = file_md5(old_path)
                    old_count = ogr_feature_count(old_path)

            build_pmtiles_file(sql, layer, out)
            size_mb = out.stat().st_size / 1_048_576
            new_md5 = file_md5(out)
            new_count = ogr_feature_count(out) if audit else None

            audit_payload = {
                "table": table_audit,
                "old_md5": old_md5,
                "new_md5": new_md5,
                "old_count": old_count,
                "new_count": new_count,
                "identical": bool(old_md5 and old_md5 == new_md5),
            }
            if audit:
                print(
                    format_audit_log(
                        table_audit=table_audit,
                        old_size=old_size,
                        old_md5=old_md5,
                        old_count=old_count,
                        new_size=out.stat().st_size,
                        new_md5=new_md5,
                        new_count=new_count,
                        remote_existed=remote_existed,
                    )
                )

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
            audit=audit_payload,
        )
    except Exception as exc:
        return PmtilesResult(schema, table, "error", str(exc))
