#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit servitudes I4 — détecte doublons / variantes sur une UF (table unifiée servitudes).

Usage (depuis cua_latresne_v4, venv + .env Supabase) :

    python api/cuas/argeles/tests/audit_servitudes_i4.py --refs "BC:1374"
    python api/cuas/argeles/tests/audit_servitudes_i4.py --refs "BC:1374" --json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

TESTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TESTS_DIR.parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from api.cuas.argeles.db import SCHEMA, get_engine  # noqa: E402
from api.cuas.argeles.intersection_modules.servitudes import (  # noqa: E402
    ARGELES_SERVITUDES_CONFIG,
    compute_servitudes_reglementation,
)
from api.cuas.argeles.uf import build_uf  # noqa: E402
from api.modules_communs.servitudes import (  # noqa: E402
    _intersect_servitudes,
    _load_i4_variantes,
    _load_reglements,
    _match_i4_variante,
    _parse_tension,
)


def _parse_refs(raw: str) -> list[dict]:
    refs = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        section, _, numero = tok.partition(":")
        refs.append({"section": section.strip(), "numero": numero.strip()})
    return refs


def _reglementation_preview(text: str | None, n: int = 120) -> str:
    if not text:
        return ""
    one_line = " ".join(text.split())
    return one_line[:n] + ("…" if len(one_line) > n else "")


def audit_i4(uf_wkt: str, schema: str = SCHEMA) -> dict:
    engine = get_engine()
    config = ARGELES_SERVITUDES_CONFIG
    if config.geo_schema != schema:
        config = ARGELES_SERVITUDES_CONFIG.__class__(
            geo_schema=schema,
            excluded_suptypes=ARGELES_SERVITUDES_CONFIG.excluded_suptypes,
        )

    reglements = _load_reglements(engine)
    i4_rows = _load_i4_variantes(engine)
    base_i4 = reglements.get("I4") or reglements.get("i4") or {}

    entities = _intersect_servitudes(engine, uf_wkt, config)
    entites_brutes: list[dict] = []
    for entity in entities:
        if (entity.get("suptype") or "").upper() != "I4":
            continue
        tension = _parse_tension(entity.get("tension"))
        variante = _match_i4_variante(entity.get("type"), tension, i4_rows)
        entites_brutes.append({
            "entity_id": entity.get("id"),
            "suptype": entity.get("suptype"),
            "type": entity.get("type"),
            "tension": entity.get("tension"),
            "tension_parsee": tension,
            "i4_matche": variante is not None,
            "i4_libelle_var": (variante or {}).get("libelle_var"),
            "i4_complement_preview": _reglementation_preview(
                (variante or {}).get("complement"), 80
            ),
            "metric_m2": entity.get("metric"),
        })

    pipeline = compute_servitudes_reglementation(uf_wkt, engine=engine, schema=schema)
    pipeline_i4 = [
        s for s in (pipeline.get("servitudes") or [])
        if (s.get("suptype") or "").upper() == "I4"
    ]

    doublons_probables = []
    if pipeline_i4 and any(s.get("i4_non_resolu") for s in pipeline_i4):
        doublons_probables.append({
            "type": "i4_non_resolu_dans_pipeline",
            "message": "Des fragments I4 n'ont pas de variante servitudes_reglements_i4.",
        })

    return {
        "schema": schema,
        "table": f"{schema}.servitudes",
        "base_i4_variable": bool(base_i4.get("variable")),
        "base_i4_libelle": base_i4.get("libelle"),
        "base_i4_reglementation_len": len((base_i4.get("reglementation") or "").strip()),
        "lignes_i4_table": [
            {
                "gen_type": r.get("gen_type"),
                "tension_min": r.get("tension_min"),
                "tension_max": r.get("tension_max"),
                "libelle_var": r.get("libelle_var"),
                "complement_len": len((r.get("complement") or "")),
            }
            for r in i4_rows
        ],
        "entites_i4_intersectees": entites_brutes,
        "pipeline_servitudes_count": len(pipeline.get("servitudes") or []),
        "pipeline_i4": pipeline_i4,
        "doublons_probables": doublons_probables,
    }


def main():
    ap = argparse.ArgumentParser(description="Audit servitudes I4 (table unifiée servitudes)")
    ap.add_argument("--refs", required=True, help='Ex: "BC:1374" ou "BC:1374,BC:1375"')
    ap.add_argument("--schema", default=SCHEMA)
    ap.add_argument("--json", action="store_true", help="Sortie JSON brute")
    args = ap.parse_args()

    refs = _parse_refs(args.refs)
    uf = build_uf(refs, schema=args.schema)
    rapport = audit_i4(uf.wkt, schema=args.schema)

    if args.json:
        print(json.dumps(rapport, indent=2, ensure_ascii=False, default=str))
        return

    print(f"\n=== Audit servitudes I4 — {args.refs} ({args.schema}) ===\n")
    print(f"Table : {rapport['table']}")
    print(
        f"Base I4 (servitudes_reglements) : variable={rapport['base_i4_variable']}, "
        f"len={rapport['base_i4_reglementation_len']} car."
    )

    print("\n--- Lignes servitudes_reglements_i4 ---")
    for row in rapport["lignes_i4_table"]:
        print(
            f"  {row['gen_type']} | {row['tension_min']}-{row['tension_max']} kV | "
            f"{row['libelle_var']}"
        )

    print("\n--- Entités I4 intersectées (brut) ---")
    for e in rapport["entites_i4_intersectees"]:
        print(
            f"  #{e.get('entity_id')} | type={e.get('type')!r} tension={e.get('tension')!r} "
            f"→ parsed={e.get('tension_parsee')} | "
            f"i4={'OK ' + str(e.get('i4_libelle_var')) if e.get('i4_matche') else 'NON RÉSOLU'}"
        )

    print("\n--- Pipeline I4 agrégé ---")
    for s in rapport["pipeline_i4"]:
        n_var = len(s.get("variantes") or [])
        print(
            f"  {s.get('libelle')} | variantes={n_var} | "
            f"non_résolus={'oui' if s.get('i4_non_resolu') else 'non'}"
        )

    print("\n--- Doublons probables ---")
    if not rapport["doublons_probables"]:
        print("  (aucun pattern détecté)")
    for d in rapport["doublons_probables"]:
        print(f"  ⚠  {d['type']}: {d['message']}")

    print(f"\nPipeline final : {rapport['pipeline_servitudes_count']} servitude(s)\n")


if __name__ == "__main__":
    main()
