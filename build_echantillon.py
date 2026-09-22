# -*- coding: utf-8 -*-
"""
build_echantillon.py — Fige un échantillon d'UF pour les tests de batch.

À lancer ponctuellement (pas à chaque test) : récupère les N derniers CUA
générés avec succès par Argelès dans public.pipelines et écrit leurs UF dans
tests/echantillon_uf.json. Ce fichier est ensuite l'entrée de batch.py.

Lecture seule sur pipelines. Ne touche ni au Storage ni à la table.

Usage :
  python build_echantillon.py            # 30 derniers → echantillon_uf.json
  python build_echantillon.py -n 50
  python build_echantillon.py --out autre.json
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARGELES_DIR = HERE.parent
if str(ARGELES_DIR) not in sys.path:
    sys.path.insert(0, str(ARGELES_DIR))

from sqlalchemy import text  # noqa: E402

try:
    from api.cuas.argeles.db import get_engine
except ImportError:
    from db import get_engine

OUT = HERE / "echantillon_uf.json"


def _norm_refs(parcelles) -> list[dict]:
    """[{'section','numero'}] nettoyé ; supporte le jsonb déjà décodé ou une chaîne."""
    if isinstance(parcelles, str):
        parcelles = json.loads(parcelles)
    out = []
    for p in parcelles or []:
        section = str(p.get("section", "")).strip()
        numero = str(p.get("numero", "")).strip()
        if section and numero:
            out.append({"section": section, "numero": numero})
    return out


def fetch(n: int) -> list[dict]:
    sql = text("""
        SELECT slug, parcelles, created_at
        FROM public.pipelines
        WHERE commune_slug = 'argeles'
          AND status = 'success'
          AND parcelles IS NOT NULL
          AND jsonb_array_length(parcelles) > 0
        ORDER BY created_at DESC
        LIMIT :n
    """)
    ufs, vus = [], set()
    with get_engine().connect() as conn:
        for row in conn.execute(sql, {"n": n * 2}).mappings():  # marge pour les doublons
            refs = _norm_refs(row["parcelles"])
            if not refs:
                continue
            cle = tuple(sorted((r["section"], r["numero"]) for r in refs))  # dédoublonne les UF identiques
            if cle in vus:
                continue
            vus.add(cle)
            ufs.append({
                "id": row["slug"],
                "refs": refs,
                "cree_le": row["created_at"].isoformat() if row["created_at"] else None,
            })
            if len(ufs) >= n:
                break
    return ufs


def main() -> int:
    ap = argparse.ArgumentParser(description="Fige l'échantillon d'UF Argelès pour les tests")
    ap.add_argument("-n", type=int, default=30, help="nombre d'UF (défaut 30)")
    ap.add_argument("--out", default=str(OUT))
    args = ap.parse_args()

    ufs = fetch(args.n)
    if not ufs:
        print("Aucune UF trouvée dans public.pipelines pour argeles.")
        return 1

    payload = {
        "commune": "argeles",
        "genere_le": datetime.now().isoformat(timespec="seconds"),
        "n": len(ufs),
        "ufs": ufs,
    }
    Path(args.out).write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    mono = sum(1 for u in ufs if len(u["refs"]) == 1)
    print(f"✅ {len(ufs)} UF écrites → {args.out}  ({mono} mono-parcelle, {len(ufs)-mono} multi)")
    return 0


if __name__ == "__main__":
    sys.exit(main())