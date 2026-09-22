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


def existing_columns(db_config: dict, schema: str, table: str) -> set[str]:
    """Colonnes réelles d'une table (vide si table absente)."""
    rows = db_query(
        db_config,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {str(r["column_name"]) for r in rows}


def sql_select_existing(
    alias: str,
    wanted: list[str] | tuple[str, ...],
    available: set[str],
) -> str:
    """SELECT des colonnes demandées ; absentes → NULL AS nom (schéma GPU variable)."""
    parts: list[str] = []
    for col in wanted:
        name = str(col).strip()
        if not name or name.startswith("-"):
            continue
        safe = name.replace('"', '""')
        if name in available:
            parts.append(f'{alias}."{safe}"')
        else:
            parts.append(f'NULL AS "{safe}"')
    return ", ".join(parts) if parts else f"{alias}.*"
