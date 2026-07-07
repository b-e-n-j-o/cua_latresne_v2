#!/usr/bin/env python3
"""
Ingère des laïus (.md plats, un fichier = une zone) dans des tables Postgres.

Schéma cible (ex. latresne) — même forme que plu_reglement :

    CREATE TABLE latresne.plu_laius (
      code_zone text NOT NULL PRIMARY KEY,
      reglementation text NOT NULL
    );

Usage (depuis cua_latresne_v4) :

    cd cua_latresne_v4

    # Un dossier → une table
    PYTHONPATH=. python scripts/ingest_laius_md.py \\
        --schema latresne \\
        --table plu_laius \\
        --folder /chemin/vers/plu-laius-latresne

    # Les 3 dossiers Latresne d'un coup
    PYTHONPATH=. python scripts/ingest_laius_md.py \\
        --schema latresne \\
        --ingest-latresne-laius \\
        --base-dir /chemin/vers/LAIUS

    PYTHONPATH=. python scripts/ingest_laius_md.py \\
        --schema latresne --table ppri_laius \\
        --folder ../REGLEMENTS_PLU_PAR_LLM/latresne/LAIUS/ppri-latresne-laius \\
        --dry-run

Variables DB : SUPABASE_* ou SUPABASE_DIRECT_URL / DATABASE_URL.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from psycopg2 import extras, sql
from psycopg2.extensions import connection as PgConnection

_SCRIPT_DIR = Path(__file__).resolve().parent
_ROOT = _SCRIPT_DIR.parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / ".env")

_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

# Dossiers produits par le batch laïus (nom fichier .md = code_zone)
LATRESNE_LAIUS_FOLDERS: dict[str, str] = {
    "plu-laius-latresne": "plu_laius",
    "ppri-latresne-laius": "ppri_laius",
    "pprmvt-latresne-laius": "pprmvt_laius",
}


def _database_dsn() -> str:
    direct = (os.getenv("SUPABASE_DIRECT_URL") or os.getenv("DATABASE_URL") or "").strip()
    if direct:
        return direct.replace("postgresql+psycopg2://", "postgresql://").replace(
            "postgresql+psycopg://", "postgresql://"
        )

    host = (os.getenv("SUPABASE_HOST") or "").strip().strip('"').strip("'")
    db = (os.getenv("SUPABASE_DB") or "").strip().strip('"').strip("'")
    user = (os.getenv("SUPABASE_USER") or "").strip().strip('"').strip("'")
    password = (os.getenv("SUPABASE_PASSWORD") or "").strip().strip('"').strip("'")
    port = str(os.getenv("SUPABASE_PORT") or "5432").strip().strip('"').strip("'")

    if host and "pooler.supabase.com" in host and port == "5432":
        port = "6543"

    if not all([host, db, user, password]):
        raise RuntimeError(
            "Connexion DB manquante : SUPABASE_DIRECT_URL ou SUPABASE_HOST / "
            "SUPABASE_DB / SUPABASE_USER / SUPABASE_PASSWORD."
        )
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def _connect() -> PgConnection:
    import psycopg2

    return psycopg2.connect(_database_dsn(), sslmode="require")


def _sanitize_ident(name: str, label: str) -> str:
    s = (name or "").strip().lower()
    if not _IDENT_RE.fullmatch(s):
        raise ValueError(f"{label} SQL invalide : {name!r}")
    return s


def _is_real_md_file(path: Path) -> bool:
    name = path.name
    if name.startswith(".") or name.startswith("._"):
        return False
    return path.suffix.lower() in {".md", ".markdown"}


def _read_md_text(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _zone_from_filename(path: Path) -> str:
    name = path.name
    lower = name.lower()
    if lower.endswith(".markdown"):
        return name[:-9]
    if lower.endswith(".md"):
        return name[:-3]
    return path.stem


def collect_laius_rows(folder: Path) -> list[dict[str, str]]:
    if not folder.is_dir():
        raise FileNotFoundError(f"Dossier introuvable : {folder}")

    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for md_path in sorted(folder.glob("*.md")):
        if not md_path.is_file() or not _is_real_md_file(md_path):
            continue
        code_zone = _zone_from_filename(md_path).strip()
        if not code_zone:
            print(f"  ⚠️  Ignoré (nom vide) : {md_path.name}")
            continue
        if code_zone in seen:
            print(f"  ⚠️  Doublon ignoré : {code_zone} ({md_path.name})")
            continue

        content = _read_md_text(md_path).strip()
        if not content:
            print(f"  ⚠️  Fichier vide : {md_path.name}")
            continue

        seen.add(code_zone)
        rows.append({"code_zone": code_zone, "reglementation": content})
        print(f"  ✓ {md_path.name} → code_zone={code_zone!r} ({len(content):,} car.)")

    return rows


def ensure_laius_table(conn: PgConnection, schema: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    code_zone text NOT NULL PRIMARY KEY,
                    reglementation text NOT NULL
                )
                """
            ).format(sql.Identifier(schema), sql.Identifier(table))
        )


def ingest_rows(
    rows: list[dict[str, str]],
    *,
    schema: str,
    table: str,
    replace: bool,
    dry_run: bool,
) -> None:
    full = f"{schema}.{table}"

    if dry_run:
        print(f"\n[DRY-RUN] Cible : {full} ({len(rows)} ligne(s))")
        if replace:
            print(f"[DRY-RUN] TRUNCATE {schema}.{table}")
        for r in rows:
            print(f"  → {r['code_zone']!r} ({len(r['reglementation']):,} car.)")
        return

    conn = _connect()
    try:
        ensure_laius_table(conn, schema, table)
        with conn:
            with conn.cursor() as cur:
                if replace:
                    cur.execute(
                        sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY").format(
                            sql.Identifier(schema),
                            sql.Identifier(table),
                        )
                    )

                insert_sql = f"""
                    INSERT INTO "{schema}"."{table}" (code_zone, reglementation)
                    VALUES (%(code_zone)s, %(reglementation)s)
                    ON CONFLICT (code_zone) DO UPDATE SET
                        reglementation = EXCLUDED.reglementation
                """
                extras.execute_batch(cur, insert_sql, rows)

        print(f"\n✅ {len(rows)} laïus ingéré(s) dans {full}")
    finally:
        conn.close()


def ingest_folder(
    folder: Path,
    *,
    schema: str,
    table: str,
    replace: bool,
    dry_run: bool,
) -> int:
    print(f"\n📂 {folder}")
    print(f"🎯 {schema}.{table}\n")
    rows = collect_laius_rows(folder)
    if not rows:
        print("  ❌ Aucun .md valide")
        return 0
    ingest_rows(rows, schema=schema, table=table, replace=replace, dry_run=dry_run)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingère des laïus Markdown (fichiers plats) en base.",
    )
    parser.add_argument("--schema", default="latresne", help="Schéma Postgres (défaut : latresne)")
    parser.add_argument("--table", help="Table cible (ex. plu_laius)")
    parser.add_argument("--folder", type=Path, help="Dossier contenant les .md")
    parser.add_argument(
        "--ingest-latresne-laius",
        action="store_true",
        help="Ingère les 3 dossiers Latresne (plu / ppri / pprmvt)",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        help="Répertoire parent contenant plu-laius-latresne, ppri-latresne-laius, etc.",
    )
    parser.add_argument(
        "--no-replace",
        action="store_true",
        help="Ne pas TRUNCATE avant insert (upsert par code_zone)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Simulation sans écriture")
    args = parser.parse_args()

    schema = _sanitize_ident(args.schema, "Schéma")
    replace = not args.no_replace

    if args.ingest_latresne_laius:
        base = (args.base_dir or Path.cwd()).expanduser().resolve()
        if not base.is_dir():
            raise SystemExit(f"Dossier introuvable : {base}")

        total = 0
        for folder_name, table_name in LATRESNE_LAIUS_FOLDERS.items():
            folder = base / folder_name
            if not folder.is_dir():
                print(f"\n⏭️  {folder_name} — dossier absent, ignoré")
                continue
            total += ingest_folder(
                folder,
                schema=schema,
                table=table_name,
                replace=replace,
                dry_run=args.dry_run,
            )
        print(f"\n═══ Total : {total} laïus traités ═══")
        return

    if not args.table or not args.folder:
        raise SystemExit("Précisez --table et --folder, ou --ingest-latresne-laius --base-dir")

    table = _sanitize_ident(args.table, "Table")
    folder = args.folder.expanduser().resolve()
    count = ingest_folder(
        folder,
        schema=schema,
        table=table,
        replace=replace,
        dry_run=args.dry_run,
    )
    if count == 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
