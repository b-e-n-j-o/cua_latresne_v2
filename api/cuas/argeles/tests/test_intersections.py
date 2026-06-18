#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test manuel du pipeline intersections + builder CUA (Argelès).

Usage :
    cd cua_latresne_v4
    python api/cuas/tests/test_intersections.py

    # ou depuis api/cuas/tests/ :
    python test_intersections.py

Modifier PARCELLES (et éventuellement DOSSIER) ci-dessous, puis relancer.
Produit dans tests/output/ : rapport JSON + DOCX.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from dotenv import load_dotenv

# ── À modifier ────────────────────────────────────────────────
PARCELLES: list[tuple[str, str]] = [
    ("BC", "1374"),
    # ("BR", "274"),  # plusieurs parcelles → UF si contiguës
]

SCHEMA = "argeles"
OUTPUT: Path | None = None       # None = auto dans tests/output/
DOSSIER: dict = {}               # encart identité (vide → cadastre/superficie depuis le rapport)
BUILD_DOCX: bool = True          # False = JSON seulement
# ──────────────────────────────────────────────────────────────

TESTS_DIR = Path(__file__).resolve().parent   # …/api/cuas/argeles/tests
CUAS_DIR = TESTS_DIR.parent                   # …/api/cuas/argeles
PROJECT_ROOT = TESTS_DIR.parents[3]           # …/cua_latresne_v4
INTERSECTIONS_SCRIPT = CUAS_DIR / "intersections.py"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if not INTERSECTIONS_SCRIPT.is_file():
    raise SystemExit(f"Script intersections introuvable : {INTERSECTIONS_SCRIPT}")

load_dotenv(PROJECT_ROOT / ".env")

from api.cuas.argeles.builder import build_cua
from api.cuas.argeles.db import logger
from api.cuas.argeles.intersections import load_catalogue, run_intersections
from api.cuas.argeles.uf import build_uf

CATALOGUE_PATH = CUAS_DIR / "catalogue_cua_argeles.json"
OUTPUT_DIR = TESTS_DIR / "output"


def main() -> None:
    refs = [{"section": s, "numero": n} for s, n in PARCELLES]
    if not refs:
        raise SystemExit("PARCELLES est vide — ajoute au moins une référence.")

    catalogue = load_catalogue(str(CATALOGUE_PATH))

    labels = [f"{r['section']}:{r['numero']}" for r in refs]
    logger.info(f"Parcelles : {', '.join(labels)}")
    uf = build_uf(refs, schema=SCHEMA)

    logger.info(f"UF — {uf.n_parcelles} parcelle(s) | surface SIG {uf.surface_sig:.2f} m²")
    logger.info(f"Intersection sur {len(catalogue)} couche(s)…")

    rapport = run_intersections(uf, catalogue, schema=SCHEMA)

    n_touch = sum(1 for v in rapport["intersections"].values() if v.get("objets"))
    logger.info(f"Résultat : {n_touch}/{len(catalogue)} couche(s) intersectée(s).")

    label = "_".join(f"{r['section']}{r['numero']}" for r in refs)
    out_path = OUTPUT or (OUTPUT_DIR / f"rapport_intersections_{label}.json")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(rapport, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    logger.info(f"Rapport écrit : {out_path}")

    if BUILD_DOCX:
        docx_path = out_path.parent / f"CUA_{label}.docx"
        build_cua(DOSSIER, rapport, str(docx_path))
        logger.info(f"CUA généré : {docx_path}")


if __name__ == "__main__":
    main()
