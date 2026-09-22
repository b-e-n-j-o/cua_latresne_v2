#!/usr/bin/env python3
"""Applique les SQL du lot 0 puis vérifie DG / alias / métriques."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")
SQL_DIR = ROOT / "sql" / "corpus"


def _conn():
    return psycopg2.connect(
        host=os.environ["SUPABASE_HOST"],
        port=int(os.environ.get("SUPABASE_PORT", 5432)),
        dbname=os.environ["SUPABASE_DB"],
        user=os.environ["SUPABASE_USER"],
        password=os.environ["SUPABASE_PASSWORD"],
        sslmode="require",
        connect_timeout=15,
    )


def _run_file(cur, path: Path) -> None:
    print(f"— {path.name}")
    cur.execute(path.read_text(encoding="utf-8"))


def main() -> int:
    files = [
        SQL_DIR / "003_dispositions_generales.sql",
        SQL_DIR / "004_alias_zonage.sql",
        SQL_DIR / "005_metriques_tour.sql",
    ]
    conn = _conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for path in files:
                    _run_file(cur, path)

                cur.execute(
                    """
                    SELECT document_type, zone_code, portee
                    FROM corpus.textes
                    WHERE zone_code ~ '^DG[0-9]*$'
                    ORDER BY 1, 2
                    """
                )
                print("DG portee:", [dict(r) for r in cur.fetchall()])

                cur.execute(
                    """
                    SELECT zone_code, portee
                    FROM corpus.textes_pour_zonages('33234', ARRAY['UB'], current_date)
                    WHERE portee = 'globale' OR zone_code ~ '^DG'
                    ORDER BY document_type, zone_code
                    """
                )
                dgs = [dict(r) for r in cur.fetchall()]
                print("textes_pour_zonages(UB) DG:", dgs)
                if not dgs:
                    print("ERREUR: aucune DG retournée pour UB Latresne", file=sys.stderr)
                    return 1

                cur.execute(
                    "SELECT corpus.resoudre_codes_zonage('66008', 'PLU', ARRAY['UA']) AS codes"
                )
                ua = list(cur.fetchone()["codes"] or [])
                print("alias UA:", ua)
                if sorted(ua) != ["UAa", "UAb"]:
                    print("ERREUR: UA doit résoudre vers UAa et UAb", file=sys.stderr)
                    return 1

                cur.execute(
                    "SELECT corpus.resoudre_codes_zonage('66008', 'PPR', ARRAY['I-b2']) AS codes"
                )
                ib2 = list(cur.fetchone()["codes"] or [])
                print("alias I-b2:", ib2)
                if ib2 != ["I"]:
                    print("ERREUR: I-b2 doit résoudre vers I uniquement", file=sys.stderr)
                    return 1

                cur.execute(
                    """
                    SELECT 'latresne' AS s, count(*) FILTER (WHERE role='model' AND metriques_tour IS NOT NULL) AS n
                    FROM latresne.plu_messages
                    UNION ALL
                    SELECT 'argeles', count(*) FILTER (WHERE role='model' AND metriques_tour IS NOT NULL)
                    FROM argeles.plu_messages
                    """
                )
                print("metriques_tour renseignées:", [dict(r) for r in cur.fetchall()])
        print("Lot 0 SQL OK")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
