#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_parcelles_vers_schema.py
-----------------------------
Copie / upsert des parcelles depuis la table nationale parcelles.parcelles
vers <schema>.parcelles (ex. latresne.parcelles).

À lancer après router_sync_parcelles (POST /admin/parcelles/sync) ou en parallèle
si la table nationale est déjà à jour.

Usage :
  python sync_parcelles_vers_schema.py --schema latresne --insee 33234
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

NATIONAL_TABLE = "parcelles.parcelles"

UPSERT_SQL = """
INSERT INTO {schema}.parcelles (
    idu, numero, feuille, section, code_dep, code_com, com_abs, code_arr,
    contenance, code_insee, geom_2154, geom_3857
)
SELECT
    p.idu,
    p.numero,
    p.feuille,
    p.section,
    p.code_dep,
    SUBSTRING(p.idu FROM 3 FOR 3) AS code_com,
    p.com_abs,
    '000' AS code_arr,
    p.contenance,
    p.code_insee,
    p.geom_2154,
    ST_Transform(p.geom_2154, 3857)
FROM {national} p
WHERE p.code_insee = %s
  AND p.geom_2154 IS NOT NULL
ON CONFLICT (idu) DO UPDATE SET
    numero = EXCLUDED.numero,
    feuille = EXCLUDED.feuille,
    section = EXCLUDED.section,
    code_dep = EXCLUDED.code_dep,
    code_com = EXCLUDED.code_com,
    com_abs = EXCLUDED.com_abs,
    contenance = EXCLUDED.contenance,
    code_insee = EXCLUDED.code_insee,
    geom_2154 = EXCLUDED.geom_2154,
    geom_3857 = EXCLUDED.geom_3857;
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
        raise ValueError("Schéma invalide.")
    return s


def sanitize_insee(code: str) -> str:
    c = code.strip().upper()
    if not re.fullmatch(r"[0-9A-Z]{5}", c):
        raise ValueError("Code INSEE invalide (5 caractères).")
    return c


def run_sync(conn, schema: str, insee: str) -> int:
    sql = UPSERT_SQL.format(schema=schema, national=NATIONAL_TABLE)
    with conn.cursor() as cur:
        cur.execute(sql, (insee,))
        n = cur.rowcount
    conn.commit()
    return n


def parse_args():
    p = argparse.ArgumentParser(description="Sync parcelles.parcelles → schema.parcelles")
    p.add_argument("--schema", required=True, help="Schéma cible (ex: latresne)")
    p.add_argument("--insee", required=True, help="Code INSEE commune (ex: 33234)")
    p.add_argument("--db-url", default=None)
    return p.parse_args()


def main():
    args = parse_args()
    schema = sanitize_schema(args.schema)
    insee = sanitize_insee(args.insee)

    log.info("Sync %s → %s.parcelles (insee=%s)", NATIONAL_TABLE, schema, insee)
    conn = connect_supabase(args.db_url)
    try:
        n = run_sync(conn, schema, insee)
        log.info("%d parcelles upsertées", n)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
