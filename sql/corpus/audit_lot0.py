#!/usr/bin/env python3
"""Diagnostic 0.3 — codes cartographiés sans texte corpus (après alias)."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")
OUT = ROOT / "sql" / "corpus" / "audit_zonages_orphelins.md"


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


def main() -> None:
    conn = _conn()
    lines = ["# Audit zonages cartographiés sans texte (lot 0.3)\n"]
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT DISTINCT '66008'::text AS commune_insee, 'PLU'::text AS document_type,
                   z.zonage_reglement AS code_spatial, z.libelle
            FROM argeles.zonage_plu z
            WHERE NOT EXISTS (
              SELECT 1 FROM corpus.textes t
              WHERE t.document_type IN ('PLU', 'PLUI')
                AND (t.commune_insee = '66008' OR t.commune_insee IS NULL)
                AND upper(coalesce(t.zone_code, '')) = upper(z.zonage_reglement)
            )
            AND NOT EXISTS (
              SELECT 1 FROM corpus.alias_zonage a
              JOIN corpus.textes t
                ON t.document_type IN ('PLU','PLUI')
               AND t.commune_insee = '66008'
               AND t.zone_code = a.code_texte
              WHERE a.commune_insee = '66008' AND a.document_type = 'PLU'
                AND upper(a.code_spatial) = upper(z.zonage_reglement)
            )
            ORDER BY 3, 4
            """
        )
        argeles = [dict(r) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT DISTINCT '33234'::text AS commune_insee, 'PLU'::text AS document_type,
                   z.zonage_reglement AS code_spatial, z.libelle
            FROM latresne.zonage_plu z
            WHERE NOT EXISTS (
              SELECT 1 FROM corpus.textes t
              WHERE t.document_type IN ('PLU', 'PLUI')
                AND (t.commune_insee = '33234' OR t.commune_insee IS NULL)
                AND upper(coalesce(t.zone_code, '')) = upper(z.zonage_reglement)
            )
            ORDER BY 3, 4
            """
        )
        latresne = [dict(r) for r in cur.fetchall()]
        lines.append("## PLU Argelès (après alias)\n")
        if not argeles:
            lines.append("Aucun orphelin.\n")
        for r in argeles:
            lines.append(f"- `{r['code_spatial']}` ({r['libelle']}) — **orphelin assumé**\n")
        lines.append("\n## PLU Latresne\n")
        if not latresne:
            lines.append("Aucun orphelin.\n")
        for r in latresne:
            lines.append(f"- `{r['code_spatial']}` ({r['libelle']})\n")
        lines.append(
            "\n3AU et 6AU n'ont pas de ligne dans `corpus.textes` : liste assumée, "
            "hors `couverture_declaree` (lot 1).\n"
        )
        OUT.write_text("".join(lines), encoding="utf-8")
        print("".join(lines))
        print(f"Écrit : {OUT}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
