#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ingère les laius réglementaires (notes de synthèse zones PLU) dans zonage_plu.reglementation.

Parse le markdown « Notes de synthèses zones PLU Argelès.md » : chaque bloc ### CODE : titre
est associé aux lignes de zonage_plu où libelle = CODE.

Les zones sans correspondance réglementaire (3AU, 6AU, A1, Nca, Nl, Nx1, Nxl1) sont ignorées.

Exemples :
  python ingest_zonage_reglementation.py --dry-run
  python ingest_zonage_reglementation.py
  python ingest_zonage_reglementation.py --schema argeles --markdown "Notes de synthèses zones PLU Argelès.md"
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from pathlib import Path

import psycopg
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
DEFAULT_MARKDOWN = HERE / "Notes de synthèses zones PLU Argelès.md"
DEFAULT_SCHEMA = os.getenv("ARGELES_SCHEMA", "argeles")

# Zones présentes au zonage mais absentes du règlement écrit — non ingérées.
ZONES_EXCLUES = frozenset({"3AU", "6AU", "A1", "Nca", "Nl", "Nx1", "Nxl1"})

SECTION_SPLIT_RE = re.compile(r"^###\s+", re.MULTILINE)
HEADER_CODE_RE = re.compile(r"^([^:]+?)\s*:")
STOP_LINE_RE = re.compile(r"^#{1,2}\s|^###\s|^---\s*$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def load_env() -> None:
    env_backend = HERE.parents[1] / ".env"
    if env_backend.is_file():
        load_dotenv(env_backend, override=True)
    else:
        load_dotenv()


def connect_supabase():
    load_env()
    host = os.getenv("SUPABASE_HOST")
    dbname = os.getenv("SUPABASE_DB")
    user = os.getenv("SUPABASE_USER")
    password = os.getenv("SUPABASE_PASSWORD")
    port = os.getenv("SUPABASE_PORT", "5432")
    sslmode = os.getenv("SUPABASE_SSLMODE", "require")

    if not all([host, dbname, user, password]):
        raise RuntimeError(
            "Variables SUPABASE_* manquantes (SUPABASE_HOST, SUPABASE_DB, "
            "SUPABASE_USER, SUPABASE_PASSWORD)."
        )

    return psycopg.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        autocommit=False,
    )


def parse_zones_markdown(md_path: Path) -> dict[str, str]:
    """Extrait {code_zone: texte} depuis les titres ### CODE : …"""
    text = md_path.read_text(encoding="utf-8")

    marker = "# Zones présentes au zonage mais non réglementées"
    if marker in text:
        text = text.split(marker, 1)[0]

    zones: dict[str, str] = {}
    chunks = SECTION_SPLIT_RE.split(text)
    for chunk in chunks[1:]:
        lines = chunk.splitlines()
        if not lines:
            continue

        header = lines[0].strip()
        m = HEADER_CODE_RE.match(header)
        if not m:
            log.warning("En-tête ### non reconnu, ignoré : %r", header)
            continue

        code = m.group(1).strip()
        if code in ZONES_EXCLUES:
            log.info("Zone exclue (non réglementée) : %s", code)
            continue

        body_lines: list[str] = []
        for line in lines[1:]:
            if STOP_LINE_RE.match(line):
                break
            body_lines.append(line.rstrip())

        body = "\n".join(body_lines).strip()
        if not body:
            log.warning("Bloc vide pour la zone %s", code)
            continue

        if code in zones:
            log.warning("Doublon markdown pour %s — dernière occurrence conservée", code)
        zones[code] = body

    return zones


def ensure_column(conn, schema: str) -> None:
    sql = f"""
        ALTER TABLE {schema}.zonage_plu
        ADD COLUMN IF NOT EXISTS reglementation TEXT
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def fetch_libelles(conn, schema: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT libelle FROM {schema}.zonage_plu WHERE libelle IS NOT NULL"
        )
        return {row[0] for row in cur.fetchall()}


def update_reglementation(
    conn,
    schema: str,
    zones: dict[str, str],
    dry_run: bool = False,
) -> tuple[int, list[str], list[str], list[str]]:
    """
    Retourne (nb_lignes_maj, libelles_db_sans_md, codes_md_sans_db, codes_exclus_en_db).
    """
    libelles_db = fetch_libelles(conn, schema)
    codes_md = set(zones.keys())

    libelles_sans_md = sorted(libelles_db - codes_md - ZONES_EXCLUES)
    codes_sans_db = sorted(codes_md - libelles_db)
    exclus_en_db = sorted(libelles_db & ZONES_EXCLUES)

    if dry_run:
        for code, body in sorted(zones.items()):
            if code in libelles_db:
                log.info("[dry-run] %s → %d caractères", code, len(body))
        return 0, libelles_sans_md, codes_sans_db, exclus_en_db

    updated = 0
    with conn.cursor() as cur:
        for code, body in zones.items():
            cur.execute(
                f"""
                UPDATE {schema}.zonage_plu
                SET reglementation = %s
                WHERE libelle = %s
                """,
                (body, code),
            )
            updated += cur.rowcount

    return updated, libelles_sans_md, codes_sans_db, exclus_en_db


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingère les laius PLU Argelès dans zonage_plu.reglementation"
    )
    parser.add_argument(
        "--markdown",
        type=Path,
        default=DEFAULT_MARKDOWN,
        help="Chemin vers le fichier markdown des notes de synthèse",
    )
    parser.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help="Schéma PostGIS (défaut : ARGELES_SCHEMA ou argeles)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse et affiche sans écrire en base",
    )
    args = parser.parse_args()

    if not args.markdown.is_file():
        log.error("Fichier markdown introuvable : %s", args.markdown)
        return 1

    schema = args.schema.strip().lower()
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", schema):
        log.error("Schéma invalide : %r", schema)
        return 1

    zones = parse_zones_markdown(args.markdown)
    log.info("%d zone(s) extraite(s) du markdown", len(zones))

    conn = connect_supabase()
    try:
        if not args.dry_run:
            ensure_column(conn, schema)
            log.info("Colonne reglementation vérifiée sur %s.zonage_plu", schema)

        updated, sans_md, sans_db, exclus_db = update_reglementation(
            conn, schema, zones, dry_run=args.dry_run
        )

        if not args.dry_run:
            conn.commit()
            log.info("%d ligne(s) mises à jour", updated)

        if sans_db:
            log.warning(
                "Codes markdown sans libelle en base (%d) : %s",
                len(sans_db),
                ", ".join(sans_db),
            )
        if sans_md:
            log.warning(
                "Libellés en base sans texte markdown (%d) : %s",
                len(sans_md),
                ", ".join(sans_md),
            )
        if exclus_db:
            log.info(
                "Libellés exclus laissés vides (%d) : %s",
                len(exclus_db),
                ", ".join(exclus_db),
            )

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
