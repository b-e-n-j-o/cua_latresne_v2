"""Connexion PostGIS partagée par les modules utils."""

from __future__ import annotations

import psycopg2
import psycopg2.extras


def db_query(db_config: dict, sql: str, params: tuple) -> list[dict]:
    conn = psycopg2.connect(**db_config)
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows
