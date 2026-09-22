#!/usr/bin/env python3
"""
Ingère les extractions LLM du CES dans argeles.plu_ces.
Usage : python ingestion_ces_argeles.py ces_plu_argeles.json

Connexion : .env du dossier (SUPABASE_DIRECT_URL / SUPABASE_PASSWORD),
sinon DATABASE_URL.
"""

import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

import psycopg  # psycopg 3
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")
for parent in HERE.parents:
    backend_env = parent / ".env"
    if (parent / "main.py").is_file() and backend_env.is_file():
        load_dotenv(backend_env, override=False)
        break


def _connect_kwargs(*, host: str, port: int, dbname: str, user: str, password: str) -> dict:
    kwargs: dict = {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
        "sslmode": "require",
        "connect_timeout": 15,
    }
    if "pooler.supabase.com" in host or port == 6543:
        kwargs["prepare_threshold"] = None
    return kwargs


def connect_supabase():
    password = (os.getenv("SUPABASE_PASSWORD") or "").strip()
    host = (os.getenv("SUPABASE_HOST") or "").strip()
    user = (os.getenv("SUPABASE_USER") or "").strip()
    dbname = (os.getenv("SUPABASE_DB") or "postgres").strip()
    port_raw = (os.getenv("SUPABASE_PORT") or "6543").strip()

    if host and user and password:
        port = int(port_raw or "6543")
        if "pooler.supabase.com" in host and port == 5432:
            port = 6543
        return psycopg.connect(
            **_connect_kwargs(
                host=host, port=port, dbname=dbname, user=user, password=password
            )
        )

    dsn = (
        os.getenv("SUPABASE_DIRECT_URL")
        or os.getenv("DATABASE_URL")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError(
            f"SUPABASE_DIRECT_URL / DATABASE_URL ou SUPABASE_HOST manquant "
            f"({HERE / '.env'})"
        )

    parsed = urlparse(dsn)
    if not parsed.hostname:
        raise RuntimeError("DSN Supabase invalide (hôte manquant)")
    return psycopg.connect(
        **_connect_kwargs(
            host=parsed.hostname,
            port=parsed.port or 5432,
            dbname=(parsed.path or "/postgres").lstrip("/") or "postgres",
            user=unquote(parsed.username or "postgres"),
            password=password or unquote(parsed.password or ""),
        )
    )

# Ordre des zones tel qu'elles ont été passées au LLM.
# À vérifier avec ton pipeline avant de lancer.
INDEX_TO_ZONE = {
    1: "1AU", 2: "2AU", 3: "2AUL", 4: "2AUX",
    5: "A", 6: "N",
    7: "UAa", 8: "UAb",
    9: "UC", 10: "UD",
    11: "UE", 12: "UL",
    13: "UP", 14: "UT",
    15: "UX", 16: "UB",
}

EXTRACTED_BY = "mistral-large-2411"

PARTIELLE_KEYWORDS = re.compile(
    r"\b(uniquement|seulement|applicable\s+aux|seuls?\s+secteurs?|"
    r"extension|extensions|dans\s+le\s+seul|pour\s+le\s+secteur|"
    r"sauf|autres\s+secteurs|commerces?)\b",
    re.IGNORECASE,
)


def infer_applicabilite(type_regle: str, commentaire: str | None) -> str:
    """
    - non_reglemente / renvoi_graphique / voir_reglementation → non_precise
    - ratio ou absolu sans commentaire restrictif → toute_la_zone
    - ratio ou absolu avec mot-clé restrictif → partielle
    """
    if type_regle in ("non_reglemente", "renvoi_graphique", "voir_reglementation"):
        return "non_precise"
    if commentaire and PARTIELLE_KEYWORDS.search(commentaire):
        return "partielle"
    return "toute_la_zone"


def load_extractions(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    rows = []
    for item in data:
        idx = item["index"]
        code_zone = INDEX_TO_ZONE.get(idx)
        if not code_zone:
            print(f"⚠ index {idx} sans mapping, ignoré", file=sys.stderr)
            continue
        out = item["output"]
        rows.append({
            "code_zone": code_zone,
            "type_regle": out["type_regle"],
            "ces_max_pct": out.get("ces_max_pct"),
            "emprise_max_m2": out.get("emprise_max_m2"),
            "citation": out.get("citation"),
            "article": out.get("article"),
            "commentaire": out.get("commentaire"),
            "applicabilite": infer_applicabilite(
                out["type_regle"], out.get("commentaire")
            ),
        })
    return rows


UPSERT_SQL = """
INSERT INTO argeles.plu_ces
  (code_zone, type_regle, ces_max_pct, emprise_max_m2, applicabilite,
   citation, article, commentaire, extracted_by)
VALUES
  (%(code_zone)s, %(type_regle)s, %(ces_max_pct)s, %(emprise_max_m2)s,
   %(applicabilite)s, %(citation)s, %(article)s, %(commentaire)s,
   %(extracted_by)s)
ON CONFLICT (code_zone) DO UPDATE SET
  type_regle     = EXCLUDED.type_regle,
  ces_max_pct    = EXCLUDED.ces_max_pct,
  emprise_max_m2 = EXCLUDED.emprise_max_m2,
  applicabilite  = EXCLUDED.applicabilite,
  citation       = EXCLUDED.citation,
  article        = EXCLUDED.article,
  commentaire    = EXCLUDED.commentaire,
  extracted_by   = EXCLUDED.extracted_by,
  extracted_at   = now(),
  statut         = 'a_valider',
  validated_by   = NULL,
  validated_at   = NULL
WHERE argeles.plu_ces.statut <> 'valide';   -- on ne réécrit pas ce qui a été validé
"""


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: ingestion_ces_argeles.py <extractions.json>", file=sys.stderr)
        sys.exit(1)

    rows = load_extractions(Path(sys.argv[1]))
    rows = [{**r, "extracted_by": EXTRACTED_BY} for r in rows]

    with connect_supabase() as conn, conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
        conn.commit()

    print(f"✓ {len(rows)} zones ingérées.")
    for r in rows:
        chiffre = (
            f"{r['ces_max_pct']} %" if r["ces_max_pct"]
            else f"{r['emprise_max_m2']} m²" if r["emprise_max_m2"]
            else "—"
        )
        print(f"  {r['code_zone']:<6} {r['type_regle']:<22} "
              f"{r['applicabilite']:<14} {chiffre}")


if __name__ == "__main__":
    main()