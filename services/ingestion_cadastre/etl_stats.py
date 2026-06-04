"""Lecture des stats post-ETL en base."""

from __future__ import annotations

import psycopg

from services.ingestion_cadastre.enrichir_parcelles_adresses import (
    PREVIEW_SQL,
    STATS_SQL,
    connect_supabase,
)


def fetch_ban_stats(conn, schema: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*)::int FROM {schema}.ban_adresse")
        nb_adresses = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*)::int FROM {schema}.ban_lien_adresse_parcelle")
        nb_liens = cur.fetchone()[0]
    return {"nb_adresses": nb_adresses, "nb_liens": nb_liens}


def fetch_enrich_stats(conn, schema: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(STATS_SQL.format(schema=schema))
        total, avec_adr, avec_liees = cur.fetchone()
        cur.execute(PREVIEW_SQL.format(schema=schema))
        parcelles_lien, adresses = cur.fetchone()
    return {
        "total_parcelles": total,
        "avec_adresse": avec_adr,
        "avec_parcelles_liees": avec_liees,
        "parcelles_lien_ban": parcelles_lien,
        "adresses_ban": adresses,
    }


def fetch_post_etl_stats(
    schema: str,
    *,
    include_ban: bool = True,
    include_enrich: bool = True,
) -> tuple[dict | None, dict | None]:
    conn = connect_supabase()
    try:
        ban = fetch_ban_stats(conn, schema) if include_ban else None
        enrich = fetch_enrich_stats(conn, schema) if include_enrich else None
        return ban, enrich
    finally:
        conn.close()
