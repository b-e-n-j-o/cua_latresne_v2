#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
enrichir_parcelles_adresses.py
------------------------------
Enrichit <schema>.parcelles avec les adresses BAN reliées et les autres
parcelles partageant au moins une même adresse (via ban_lien_adresse_parcelle).

Prérequis (même schéma) :
  - parcelles (idu unique)
  - ban_adresse, ban_lien_adresse_parcelle (ingest_ban_adresse_et_lien_parcelles.py)

Colonnes ajoutées sur parcelles :
  - adresse_principale   : libellé court (priorité lien type_lien = GEO)
  - adresses_liees       : jsonb — liste des adresses de la parcelle
  - nb_adresses          : nombre d'id_adr distincts
  - parcelles_liees      : jsonb — autres parcelles (même adresse BAN)
  - nb_parcelles_liees   : nombre d'autres idu (hors self)
  - ban_enrichi_at       : horodatage du dernier enrichissement

Usage :
  python enrichir_parcelles_adresses.py --schema latresne
  python enrichir_parcelles_adresses.py --schema latresne --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from pathlib import Path

import psycopg

try:
    from services.ingestion_cadastre.env_loader import ENV_BACKEND, load_project_env
except ImportError:
    from env_loader import ENV_BACKEND, load_project_env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

ENRICH_SQL = """
WITH adresse_labels AS (
    SELECT
        l.idu,
        l.id_adr,
        l.type_lien,
        a.id AS ban_id,
        a.numero,
        a.rep,
        a.nom_voie,
        a.nom_com,
        a.position,
        TRIM(BOTH FROM CONCAT_WS(
            ' ',
            NULLIF(a.numero::text, ''),
            NULLIF(a.rep, ''),
            NULLIF(a.nom_voie, ''),
            NULLIF(a.nom_com, '')
        )) AS label
    FROM {schema}.ban_lien_adresse_parcelle l
    INNER JOIN {schema}.ban_adresse a ON a.id_adr = l.id_adr
    WHERE l.idu IS NOT NULL AND l.id_adr IS NOT NULL
),
agg_adresses AS (
    SELECT
        idu,
        jsonb_agg(
            jsonb_build_object(
                'id_adr', id_adr,
                'ban_id', ban_id,
                'numero', numero,
                'rep', rep,
                'nom_voie', nom_voie,
                'nom_com', nom_com,
                'position', position,
                'type_lien', type_lien,
                'label', label
            )
            ORDER BY
                CASE WHEN type_lien = 'GEO' THEN 0 ELSE 1 END,
                label
        ) AS adresses_liees,
        COUNT(DISTINCT id_adr)::int AS nb_adresses,
        (ARRAY_AGG(
            label
            ORDER BY
                CASE WHEN type_lien = 'GEO' THEN 0 ELSE 1 END,
                label
        ))[1] AS adresse_principale
    FROM adresse_labels
    GROUP BY idu
),
parcelle_pairs AS (
    SELECT DISTINCT ON (l1.idu, p2.idu, l1.id_adr)
        l1.idu AS idu_source,
        p2.idu,
        p2.section,
        p2.numero AS numero_parcelle,
        p2.contenance,
        p2.code_insee,
        l1.id_adr,
        al.label AS adresse_partagee
    FROM {schema}.ban_lien_adresse_parcelle l1
    INNER JOIN {schema}.ban_lien_adresse_parcelle l2
        ON l1.id_adr = l2.id_adr
       AND l1.idu IS DISTINCT FROM l2.idu
    INNER JOIN {schema}.parcelles p2 ON p2.idu = l2.idu
    LEFT JOIN adresse_labels al
        ON al.idu = l1.idu AND al.id_adr = l1.id_adr
    WHERE l1.idu IS NOT NULL
),
agg_parcelles_liees AS (
    SELECT
        idu_source AS idu,
        jsonb_agg(
            jsonb_build_object(
                'idu', idu,
                'section', section,
                'numero', numero_parcelle,
                'contenance', contenance,
                'code_insee', code_insee,
                'id_adr', id_adr,
                'adresse_partagee', adresse_partagee
            )
            ORDER BY idu
        ) AS parcelles_liees,
        COUNT(*)::int AS nb_parcelles_liees
    FROM parcelle_pairs
    GROUP BY idu_source
)
UPDATE {schema}.parcelles p
SET
    adresse_principale = sub.adresse_principale,
    adresses_liees = COALESCE(sub.adresses_liees, '[]'::jsonb),
    nb_adresses = COALESCE(sub.nb_adresses, 0),
    parcelles_liees = COALESCE(sub.parcelles_liees, '[]'::jsonb),
    nb_parcelles_liees = COALESCE(sub.nb_parcelles_liees, 0),
    ban_enrichi_at = NOW()
FROM (
    SELECT
        p0.idu,
        aa.adresse_principale,
        aa.adresses_liees,
        aa.nb_adresses,
        ap.parcelles_liees,
        ap.nb_parcelles_liees
    FROM {schema}.parcelles p0
    LEFT JOIN agg_adresses aa ON aa.idu = p0.idu
    LEFT JOIN agg_parcelles_liees ap ON ap.idu = p0.idu
) AS sub
WHERE p.idu = sub.idu;
"""

ALTER_PARCELLES_SQL = """
ALTER TABLE {schema}.parcelles
    ADD COLUMN IF NOT EXISTS adresse_principale text,
    ADD COLUMN IF NOT EXISTS adresses_liees jsonb DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS nb_adresses integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS parcelles_liees jsonb DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS nb_parcelles_liees integer DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ban_enrichi_at timestamptz;
"""

STATS_SQL = """
SELECT
    COUNT(*)::int AS total_parcelles,
    COUNT(*) FILTER (WHERE nb_adresses > 0)::int AS avec_adresse,
    COUNT(*) FILTER (WHERE nb_parcelles_liees > 0)::int AS avec_parcelles_liees
FROM {schema}.parcelles;
"""

PREVIEW_SQL = """
SELECT
    COUNT(DISTINCT l.idu)::int AS parcelles_avec_lien_ban,
    COUNT(DISTINCT l.id_adr)::int AS adresses_distinctes
FROM {schema}.ban_lien_adresse_parcelle l
WHERE l.idu IS NOT NULL;
"""


def connect_supabase(db_url: str | None = None):
    if db_url:
        return psycopg.connect(db_url, autocommit=False)

    load_project_env()
    host = os.getenv("SUPABASE_HOST")
    dbname = os.getenv("SUPABASE_DB")
    user = os.getenv("SUPABASE_USER")
    password = os.getenv("SUPABASE_PASSWORD")
    port = os.getenv("SUPABASE_PORT", "5432")
    sslmode = os.getenv("SUPABASE_SSLMODE", "require")

    if not all([host, dbname, user, password]):
        raise RuntimeError(
            f"Variables SUPABASE_* manquantes ({ENV_BACKEND})"
        )

    log.info("Connexion Supabase : %s@%s:%s/%s", user, host, port, dbname)
    return psycopg.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        autocommit=False,
    )


def sanitize_schema(schema: str) -> str:
    s = schema.strip().lower()
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", s):
        raise ValueError("Schéma invalide (attendu : [a-z0-9_], commence par lettre/_).")
    return s


def ensure_prerequisites(conn, schema: str) -> None:
    required = ("parcelles", "ban_adresse", "ban_lien_adresse_parcelle")
    with conn.cursor() as cur:
        for table in required:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            if cur.fetchone() is None:
                raise RuntimeError(f"Table manquante : {schema}.{table}")


def ensure_columns(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(ALTER_PARCELLES_SQL.format(schema=schema))
    conn.commit()
    log.info("Colonnes d'enrichissement vérifiées sur %s.parcelles", schema)


def run_enrichment(conn, schema: str) -> int:
    with conn.cursor() as cur:
        cur.execute(ENRICH_SQL.format(schema=schema))
        updated = cur.rowcount
    conn.commit()
    return updated


def print_stats(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(STATS_SQL.format(schema=schema))
        total, avec_adr, avec_liees = cur.fetchone()
    log.info("Parcelles totales        : %d", total)
    log.info("Avec au moins 1 adresse  : %d", avec_adr)
    log.info("Avec parcelles liées     : %d", avec_liees)


def parse_args():
    p = argparse.ArgumentParser(
        description="Enrichit parcelles avec adresses BAN et parcelles associées"
    )
    p.add_argument(
        "--schema",
        required=True,
        help="Schéma PostgreSQL (= commune), ex: latresne",
    )
    p.add_argument("--db-url", default=None, help="DSN PostgreSQL (optionnel)")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Vérifie les prérequis et affiche des stats sans modifier parcelles",
    )
    return p.parse_args()


def main():
    args = parse_args()
    schema = sanitize_schema(args.schema)

    log.info("=== Enrichissement parcelles ↔ BAN ===")
    log.info("Schéma : %s", schema)

    conn = connect_supabase(args.db_url)
    try:
        ensure_prerequisites(conn, schema)

        with conn.cursor() as cur:
            cur.execute(PREVIEW_SQL.format(schema=schema))
            parcelles_lien, adresses = cur.fetchone()
        log.info(
            "Liens BAN : %d parcelles (idu) reliées, %d adresses distinctes",
            parcelles_lien,
            adresses,
        )

        if args.dry_run:
            log.info("Mode dry-run : aucune mise à jour.")
            return

        ensure_columns(conn, schema)
        n = run_enrichment(conn, schema)
        log.info("%d lignes parcelles mises à jour", n)
        print_stats(conn, schema)
    finally:
        conn.close()

    log.info("=== Terminé ===")


if __name__ == "__main__":
    main()
