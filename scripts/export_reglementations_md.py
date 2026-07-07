#!/usr/bin/env python3
"""
Exporte la colonne ``reglementation`` d'une table Postgres vers des fichiers .md
(un fichier par ligne, nommé d'après la clé primaire / identifiant zone).

Prévu pour alimenter l'outil batch laïus (/markdown → Résumé / laïus).

Usage (depuis ``cua_latresne_v4``) :

    cd cua_latresne_v4
    PYTHONPATH=. python scripts/export_reglementations_md.py \\
        --table latresne.plu_reglement \\
        --id-column code_zone \\
        --output ./exports/latresne_plu

    # Ou schéma + table séparés :
    PYTHONPATH=. python scripts/export_reglementations_md.py \\
        --schema latresne \\
        --table plu_reglement \\
        --output ./exports/latresne_plu

    # Autre table (ex. laius source zonage) :
    PYTHONPATH=. python scripts/export_reglementations_md.py \\
        --schema argeles \\
        --table laius_ppr \\
        --id-column code_degre \\
        --text-column reglementation \\
        --output ./exports/argeles_laius_ppr

Variables DB : ``SUPABASE_HOST``, ``SUPABASE_DB``, ``SUPABASE_USER``, ``SUPABASE_PASSWORD``
(ou ``SUPABASE_DIRECT_URL`` / ``DATABASE_URL``).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / ".env")

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_UNSAFE_FS = re.compile(r'[<>:"/\\|?*\x00]')


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
            "Connexion DB manquante : définir SUPABASE_DIRECT_URL ou SUPABASE_HOST / "
            "SUPABASE_DB / SUPABASE_USER / SUPABASE_PASSWORD."
        )
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def _connect() -> PgConnection:
    import psycopg2

    return psycopg2.connect(_database_dsn(), sslmode="require")


def _parse_table_ref(table_arg: str, schema_arg: str | None) -> tuple[str, str]:
    raw = (table_arg or "").strip()
    if not raw:
        raise ValueError("Table requise (--table).")

    if "." in raw:
        parts = raw.split(".", 1)
        schema, table = parts[0].strip(), parts[1].strip()
    elif schema_arg:
        schema, table = schema_arg.strip(), raw
    else:
        raise ValueError(
            "Précisez le schéma (--schema) ou une table qualifiée (ex. latresne.plu_reglement)."
        )

    for label, ident in (("schéma", schema), ("table", table)):
        if not _IDENT_RE.match(ident):
            raise ValueError(f"{label} SQL invalide : {ident!r}")

    return schema, table


def _safe_filename(value: str) -> str:
    name = str(value).strip()
    if not name:
        return "sans_identifiant"
    name = _UNSAFE_FS.sub("_", name)
    return name[:200]


def export_reglementations(
    *,
    schema: str,
    table: str,
    output_dir: Path,
    id_column: str,
    text_column: str = "reglementation",
    dry_run: bool = False,
) -> dict[str, int]:
    if not _IDENT_RE.match(id_column):
        raise ValueError(f"Colonne id invalide : {id_column!r}")
    if not _IDENT_RE.match(text_column):
        raise ValueError(f"Colonne texte invalide : {text_column!r}")

    output_dir.mkdir(parents=True, exist_ok=True)

    query = sql.SQL(
        """
        SELECT {id_col}, {text_col}
        FROM {schema}.{table}
        WHERE {text_col} IS NOT NULL
          AND btrim({text_col}::text) <> ''
        ORDER BY {id_col}
        """
    ).format(
        id_col=sql.Identifier(id_column),
        text_col=sql.Identifier(text_column),
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
    )

    stats = {"rows": 0, "written": 0, "skipped_empty_id": 0, "skipped_duplicate": 0}

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        stats["rows"] = len(rows)
        seen_names: set[str] = set()

        for row_id, text in rows:
            if row_id is None or str(row_id).strip() == "":
                stats["skipped_empty_id"] += 1
                continue

            basename = f"{_safe_filename(str(row_id))}.md"
            if basename in seen_names:
                stats["skipped_duplicate"] += 1
                print(f"⚠ Doublon ignoré : {basename} (id={row_id!r})", file=sys.stderr)
                continue
            seen_names.add(basename)

            body = str(text).strip()
            if not body:
                continue

            out_path = output_dir / basename
            if dry_run:
                print(f"[dry-run] {out_path} ({len(body)} car.)")
            else:
                out_path.write_text(body + "\n", encoding="utf-8")
            stats["written"] += 1

    finally:
        conn.close()

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export reglementation → fichiers .md (un fichier par zone / clé)."
    )
    parser.add_argument(
        "--schema",
        help="Schéma Postgres (ex. latresne). Optionnel si --table est qualifiée (schema.table).",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Table cible (ex. plu_reglement ou latresne.plu_reglement).",
    )
    parser.add_argument(
        "--id-column",
        default="code_zone",
        help="Colonne identifiant → nom du fichier .md (défaut : code_zone).",
    )
    parser.add_argument(
        "--text-column",
        default="reglementation",
        help="Colonne texte long à exporter (défaut : reglementation).",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Dossier de sortie pour les fichiers .md.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche les fichiers qui seraient écrits sans écrire sur disque.",
    )
    args = parser.parse_args()

    schema, table = _parse_table_ref(args.table, args.schema)
    output_dir = args.output.expanduser().resolve()

    print(f"Source : {schema}.{table} ({args.id_column} → {args.text_column})")
    print(f"Sortie : {output_dir}")

    try:
        stats = export_reglementations(
            schema=schema,
            table=table,
            output_dir=output_dir,
            id_column=args.id_column,
            text_column=args.text_column,
            dry_run=args.dry_run,
        )
    except Exception as e:
        print(f"Erreur : {e}", file=sys.stderr)
        sys.exit(1)

    print(
        f"Terminé — {stats['written']} fichier(s) .md "
        f"({stats['rows']} ligne(s) lues, "
        f"{stats['skipped_empty_id']} id vide(s), "
        f"{stats['skipped_duplicate']} doublon(s))."
    )


if __name__ == "__main__":
    main()
