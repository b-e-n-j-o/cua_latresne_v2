#!/usr/bin/env python3
"""
Lot 0.6 — replay lectures commune vs corpus.

Compare les tables source (inchangées) et corpus.textes, plus 15 questions
réelles (messages user) pour expliquer les écarts.
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

ROOT = Path(__file__).resolve().parents[4]
load_dotenv(ROOT / ".env")
OUT = ROOT / "sql" / "corpus" / "replay_lot0.md"


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


def _fp(text: str | None) -> str:
    raw = (text or "").strip().encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:12]


def main() -> int:
    conn = _conn()
    lines: list[str] = ["# Replay lot 0.6 — lectures corpus vs tables source\n"]
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            """
            SELECT schema_name, table_name, commune_insee, document_type,
                   col_zone, col_texte
            FROM corpus.mapping_sources
            WHERE actif
            ORDER BY id
            """
        )
        mappings = [dict(r) for r in cur.fetchall()]
        lines.append("## Correspondance table source ↔ corpus\n")
        for m in mappings:
            if not m["commune_insee"] and m["document_type"] == "SUP":
                continue
            schema, table = m["schema_name"], m["table_name"]
            col_z, col_t = m["col_zone"], m["col_texte"]
            insee = m["commune_insee"]
            dtype = m["document_type"]
            sql = f"""
                SELECT s.{col_z} AS code,
                       length(btrim(s.{col_t})) AS n_src,
                       length(btrim(t.contenu_markdown)) AS n_corpus,
                       t.id IS NOT NULL AS dans_corpus
                FROM {schema}.{table} s
                LEFT JOIN corpus.textes t
                  ON t.document_type = %s
                 AND (t.commune_insee = %s OR (%s IS NULL AND t.commune_insee IS NULL))
                 AND coalesce(t.zone_code, '') = coalesce(s.{col_z}::text, '')
                ORDER BY 1
            """
            try:
                cur.execute(sql, (dtype, insee, insee))
                rows = [dict(r) for r in cur.fetchall()]
            except Exception as e:
                conn.rollback()
                lines.append(f"- **{schema}.{table}** : erreur {e}\n")
                continue
            mismatches = [
                r for r in rows
                if not r["dans_corpus"] or (r["n_src"] or 0) != (r["n_corpus"] or 0)
            ]
            lines.append(
                f"- `{schema}.{table}` ({dtype}) : {len(rows)} lignes source, "
                f"{len(mismatches)} écart(s) de présence/longueur.\n"
            )
            for r in mismatches[:12]:
                lines.append(
                    f"  - `{r['code']}` src={r['n_src']} corpus={r['n_corpus']} "
                    f"dans_corpus={r['dans_corpus']}\n"
                )

        lines.append("\n## Alias UA / I-b2\n")
        cur.execute("SELECT corpus.resoudre_codes_zonage('66008','PLU',ARRAY['UA']) AS c")
        lines.append(f"- UA → `{list(cur.fetchone()['c'])}`\n")
        cur.execute("SELECT corpus.resoudre_codes_zonage('66008','PPR',ARRAY['I-b2']) AS c")
        lines.append(f"- I-b2 → `{list(cur.fetchone()['c'])}`\n")

        cur.execute(
            """
            SELECT zone_code, length(contenu_markdown) AS n
            FROM corpus.textes
            WHERE commune_insee='66008' AND document_type='PLU'
              AND zone_code IN ('UAa','UAb')
            ORDER BY 1
            """
        )
        lines.append("- Textes UAa/UAb : " + ", ".join(
            f"{r['zone_code']} ({r['n']} car.)" for r in cur.fetchall()
        ) + "\n")

        lines.append("\n## 15 questions réelles (messages user les plus récents)\n")
        cur.execute(
            """
            SELECT 'latresne' AS commune, id::text, left(content, 180) AS content, created_at
            FROM latresne.plu_messages
            WHERE role='user' AND content IS NOT NULL AND btrim(content) <> ''
            UNION ALL
            SELECT 'argeles', id::text, left(content, 180), created_at
            FROM argeles.plu_messages
            WHERE role='user' AND content IS NOT NULL AND btrim(content) <> ''
            ORDER BY created_at DESC
            LIMIT 15
            """
        )
        questions = [dict(r) for r in cur.fetchall()]
        for i, q in enumerate(questions, 1):
            snippet = re.sub(r"\s+", " ", q["content"] or "").strip()
            lines.append(
                f"{i}. [{q['commune']}] {snippet}\n"
            )

        lines.append(
            "\nLes tables `argeles.*` / `latresne.*` restent la source d'ingestion. "
            "Les écarts de longueur ci-dessus sont ceux à expliquer avant de "
            "considérer la bascule lot 0.6 comme validée.\n"
        )
        OUT.write_text("".join(lines), encoding="utf-8")
        print(f"Rapport écrit : {OUT}")
        print("".join(lines))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
