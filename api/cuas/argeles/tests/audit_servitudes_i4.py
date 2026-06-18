#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit servitudes I4 — détecte doublons base / complément i4 sur une UF.

Usage (depuis cua_latresne_v4, venv + .env Supabase) :

    python api/cuas/argeles/tests/audit_servitudes_i4.py --refs "BC:1374"
    python api/cuas/argeles/tests/audit_servitudes_i4.py --refs "BC:1374" --json

Signale :
  - entités I4 intersectées par couche source ;
  - résolution i4 (gen_type, gen_tension → ligne servitudes_reglements_i4) ;
  - clé de dédup actuelle vs doublons visuels probables (base seule + base+complément).
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

from api.cuas.argeles.intersection_modules.servitudes import (  # noqa: E402
    SUP_SOURCE_TABLES,
    _build_reglementation_text,
    _filter_servitudes_redundant_generic,
    _intersect_sup_table,
    _load_reglements,
    _load_reglements_i4,
    _match_i4_reglement,
    _parse_tension_kv,
    _resolve_servitude_entry,
    _servitude_dedup_key,
    _table_exists,
    compute_servitudes_reglementation,
)
from api.cuas.argeles.db import SCHEMA, get_engine  # noqa: E402
from api.cuas.argeles.uf import build_uf  # noqa: E402


def _parse_refs(raw: str) -> list[dict]:
    refs = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        section, _, numero = tok.partition(":")
        refs.append({"section": section.strip(), "numero": numero.strip()})
    return refs


def _dedup_key(entry: dict) -> tuple:
    return _servitude_dedup_key(entry)


def _reglementation_preview(text: str | None, n: int = 120) -> str:
    if not text:
        return ""
    one_line = " ".join(text.split())
    return one_line[:n] + ("…" if len(one_line) > n else "")


def audit_i4(uf_wkt: str, schema: str = SCHEMA) -> dict:
    engine = get_engine()
    reglements = _load_reglements(engine, schema, "servitudes_reglements")
    i4_rows = _load_reglements_i4(engine, schema, "servitudes_reglements_i4")
    base_i4 = reglements.get("i4") or {}

    entites_brutes: list[dict] = []
    entites_resolues: list[dict] = []

    for source in SUP_SOURCE_TABLES:
        table = source["table"]
        if not _table_exists(engine, schema, table):
            continue
        try:
            entities = _intersect_sup_table(engine, schema, uf_wkt, source)
        except Exception as exc:
            entites_brutes.append({
                "source_table": table,
                "erreur": str(exc),
            })
            continue

        for entity in entities:
            tension = _parse_tension_kv(entity.get("gen_tension"))
            i4_row = None
            if base_i4.get("variable"):
                i4_row = _match_i4_reglement(entity.get("gen_type"), tension, i4_rows)

            brut = {
                "source_table": table,
                "entity_id": entity.get(source.get("entity_id", "gid")) or entity.get("id"),
                "suptype": entity.get("suptype"),
                "gen_type": entity.get("gen_type"),
                "gen_tension": entity.get("gen_tension"),
                "tension_kv_parsee": tension,
                "i4_matche": i4_row is not None,
                "i4_id": i4_row.get("id") if i4_row else None,
                "i4_libelle_var": i4_row.get("libelle_var") if i4_row else None,
                "i4_complement_preview": _reglementation_preview(
                    (i4_row or {}).get("complement"), 80
                ),
            }
            entites_brutes.append(brut)

            entry = _resolve_servitude_entry(entity, source, reglements, i4_rows)
            if entry:
                entites_resolues.append({
                    "source_table": entry["source_table"],
                    "entity_id": entry["entity_id"],
                    "libelle": entry["libelle"],
                    "suptype": entry["suptype"],
                    "variable": entry.get("variable"),
                    "i4": entry.get("i4"),
                    "i4_non_resolu": entry.get("i4_non_resolu", False),
                    "dedup_key": _dedup_key(entry),
                    "reglementation_len": len(entry.get("reglementation") or ""),
                    "reglementation_preview": _reglementation_preview(entry.get("reglementation")),
                    "contient_assiette_i4": "ASSIETTE" in (entry.get("reglementation") or "").upper(),
                })

    # Doublons : plusieurs entrées i4 dont une sans résolution i4
    i4_entries = [e for e in entites_resolues if (e.get("suptype") or "").lower() == "i4"]
    i4_avec_detail = [e for e in i4_entries if e.get("i4")]
    i4_sans_detail = [e for e in i4_entries if not e.get("i4")]

    doublons_probables = []
    if i4_avec_detail and i4_sans_detail:
        doublons_probables.append({
            "type": "base_generique_plus_i4_detaille",
            "message": (
                f"{len(i4_sans_detail)} entrée(s) I4 avec réglementation générique seule "
                f"et {len(i4_avec_detail)} entrée(s) I4 enrichie(s) i4 — "
                "le DOCX affiche les deux (clés de dédup différentes)."
            ),
            "sans_i4": i4_sans_detail,
            "avec_i4": i4_avec_detail,
        })

    base_text = (base_i4.get("reglementation") or "").strip()
    for enriched in i4_avec_detail:
        regl = enriched.get("reglementation_preview") or ""
        if base_text and regl.startswith(_reglementation_preview(base_text, len(regl))[:80]):
            doublons_probables.append({
                "type": "texte_base_recopie_dans_entree_enrichie",
                "message": (
                    "L'entrée enrichie concatène servitudes_reglements.reglementation "
                    "+ servitudes_reglements_i4.complement (_build_reglementation_text)."
                ),
                "entity_id": enriched.get("entity_id"),
                "source_table": enriched.get("source_table"),
            })

    pipeline = compute_servitudes_reglementation(uf_wkt, engine=engine, schema=schema)
    resolved_filtered = _filter_servitudes_redundant_generic(entites_resolues)

    return {
        "schema": schema,
        "base_i4_variable": bool(base_i4.get("variable")),
        "base_i4_libelle": base_i4.get("libelle"),
        "base_i4_reglementation_len": len(base_text),
        "lignes_i4_table": [
            {
                "id": r.get("id"),
                "gen_type": r.get("gen_type"),
                "tension_min": r.get("tension_min"),
                "tension_max": r.get("tension_max"),
                "libelle_var": r.get("libelle_var"),
                "complement_len": len((r.get("complement") or "")),
            }
            for r in i4_rows
        ],
        "entites_intersectees": entites_brutes,
        "entites_resolues": entites_resolues,
        "entites_resolues_apres_filtre": resolved_filtered,
        "doublons_probables": doublons_probables,
        "pipeline_servitudes_count": len(pipeline.get("servitudes") or []),
        "pipeline_servitudes": pipeline.get("servitudes") or [],
    }


def main():
    ap = argparse.ArgumentParser(description="Audit servitudes I4 (doublons base / complément)")
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
    print(f"Base I4 (servitudes_reglements) : variable={rapport['base_i4_variable']}, "
          f"len={rapport['base_i4_reglementation_len']} car.")

    print("\n--- Lignes servitudes_reglements_i4 ---")
    for row in rapport["lignes_i4_table"]:
        print(f"  id={row['id']} | {row['gen_type']} | "
              f"{row['tension_min']}-{row['tension_max']} kV | {row['libelle_var']}")

    print("\n--- Entités SUP intersectées (I4 uniquement) ---")
    for e in rapport["entites_intersectees"]:
        if (e.get("suptype") or "").lower() != "i4":
            continue
        print(
            f"  {e.get('source_table')} #{e.get('entity_id')} | "
            f"gen_type={e.get('gen_type')!r} tension={e.get('gen_tension')!r} "
            f"→ parsed={e.get('tension_kv_parsee')} | "
            f"i4={'OK id='+str(e.get('i4_id')) if e.get('i4_matche') else 'NON RÉSOLU'}"
        )

    print("\n--- Entrées finales (après _resolve_servitude_entry) ---")
    for e in rapport["entites_resolues"]:
        if (e.get("suptype") or "").lower() != "i4":
            continue
        flag = "ENRICHIE" if e.get("i4") else ("NON RÉSOLU" if e.get("i4_non_resolu") else "BASE")
        print(f"  [{flag}] {e.get('source_table')} #{e.get('entity_id')} | {e.get('libelle')}")
        print(f"         dedup_key i4_id={e['dedup_key'][1]} | len={e['reglementation_len']} | "
              f"assiette={'oui' if e['contient_assiette_i4'] else 'non'}")

    print("\n--- Doublons probables ---")
    if not rapport["doublons_probables"]:
        print("  (aucun pattern détecté)")
    for d in rapport["doublons_probables"]:
        print(f"  ⚠  {d['type']}: {d['message']}")

    print(f"\nPipeline final : {rapport['pipeline_servitudes_count']} servitude(s) dans le rapport")
    print(f"Après filtre anti-doublon I4 : {len(rapport.get('entites_resolues_apres_filtre') or [])} entrée(s)\n")


if __name__ == "__main__":
    main()
