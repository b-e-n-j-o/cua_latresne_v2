# -*- coding: utf-8 -*-
"""
test_unitaire_batch.py — Regénère les CUA de l'échantillon en local pour relecture.

Lit tests/echantillon_uf.json (figé une fois par build_echantillon.py),
regénère chaque CUA dans tests/out/<timestamp>/. Ne touche jamais au
Storage ni à la table pipelines (n'appelle pas persist_cua).

Les indices --index sont 1-based (la 1re UF du JSON = 1, pas 0).

Usage (depuis cua_latresne_v4) :
  python api/cuas/argeles/tests/test_unitaire_batch.py
  python api/cuas/argeles/tests/test_unitaire_batch.py --list
  python api/cuas/argeles/tests/test_unitaire_batch.py --index 17
  python api/cuas/argeles/tests/test_unitaire_batch.py --index 3,17,22 --pdf
  python api/cuas/argeles/tests/test_unitaire_batch.py --limit 5 --pdf
  python api/cuas/argeles/tests/test_unitaire_batch.py --refs AB:123,AB:124 --pdf

À la fin : ouvrir tests/out/<timestamp>/pdf/_tous.pdf et faire défiler.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
ARGELES_DIR = HERE.parent
ROOT = HERE.parents[3]  # cua_latresne_v4 (pour `from api.cuas.argeles...`)
for _p in (str(ARGELES_DIR), str(ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from api.cuas.argeles.uf import build_uf
    from api.cuas.argeles.intersections import run_intersections
    from api.cuas.argeles.builder import build_cua
except ImportError:
    from uf import build_uf
    from intersections import run_intersections
    from builder import build_cua

CATALOGUE = ARGELES_DIR / "catalogue_cua_argeles.json"
FIXTURE = HERE / "echantillon_uf.json"


def charger_echantillon(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(
            f"Échantillon absent : {path.name}. Lancer d'abord build_echantillon.py."
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    return data["ufs"]


def dossier_test(slug: str, refs: list[dict]) -> dict:
    return {
        "demandeur": f"TEST — {slug} — ne pas diffuser",
        "demandeur_adresse": "—",
        "terrain": ", ".join(f"{r['section']} n°{r['numero']}" for r in refs),
        "date_depot": datetime.now().strftime("%d/%m/%Y"),
        "numero_cu": f"TEST-{slug[:20]}",
    }


def regenere(slug: str, refs: list[dict], catalogue: dict, out_dir: Path) -> Path:
    uf = build_uf(refs)
    rapport = run_intersections(uf, catalogue)
    docx_path = out_dir / f"{slug}.docx"
    build_cua(dossier_test(slug, refs), rapport, str(docx_path))
    return docx_path


def convertir_pdf(docx_paths: list[Path], out_dir: Path) -> None:
    soffice = shutil.which("soffice") or shutil.which("libreoffice")
    if not soffice:
        print("⏭  PDF : LibreOffice introuvable (installer libreoffice-core)")
        return
    if not docx_paths:
        return
    pdf_dir = out_dir / "pdf"
    pdf_dir.mkdir(exist_ok=True)
    subprocess.run(
        [soffice, "--headless", "--convert-to", "pdf",
         "--outdir", str(pdf_dir), *(str(p) for p in docx_paths)],
        capture_output=True,
        timeout=60 + 15 * len(docx_paths),
    )
    pdfs = sorted(pdf_dir.glob("*.pdf"))
    print(f"📄 {len(pdfs)} PDF → {pdf_dir}")
    if pdfs and shutil.which("pdfunite"):
        merged = pdf_dir / "_tous.pdf"
        subprocess.run(["pdfunite", *(str(p) for p in pdfs), str(merged)], check=False)
        print(f"📄 fusion → {merged}")


def parse_refs(raw: str) -> list[dict]:
    out = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        s, _, n = tok.partition(":")
        if not n:
            raise ValueError(f"ref invalide '{tok}' (attendu SECTION:NUMERO)")
        out.append({"section": s.strip(), "numero": n.strip()})
    return out


def _fmt_refs(refs: list[dict]) -> str:
    return ", ".join(f"{r['section']} n°{r['numero']}" for r in refs)


def parse_indices(raw: str, n: int) -> list[int]:
    """Indices 1-based. Ex. '17' ou '3,17,22'."""
    out: list[int] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            idx = int(tok)
        except ValueError as exc:
            raise ValueError(f"indice invalide '{tok}' (entier attendu)") from exc
        if idx < 1 or idx > n:
            raise ValueError(f"indice {idx} hors plage (échantillon : 1 à {n})")
        if idx not in out:
            out.append(idx)
    if not out:
        raise ValueError("aucun indice fourni")
    return out


def lister_echantillon(ufs: list[dict]) -> None:
    print(f"{len(ufs)} UF dans l'échantillon :")
    for i, uf in enumerate(ufs, 1):
        refs = uf.get("refs") or []
        print(f"  {i:3d}  {uf.get('id', '?'):<28}  {len(refs):2d} parc.  {_fmt_refs(refs)}")


def selectionner_ufs(args, ufs: list[dict]) -> list[tuple[int, dict]]:
    """Retourne [(indice_1based_dans_l_echantillon, uf), ...]."""
    numbered = list(enumerate(ufs, 1))
    if args.refs:
        return [(1, {"id": "adhoc", "refs": parse_refs(args.refs)})]
    if args.index:
        by_i = {i: uf for i, uf in numbered}
        return [(i, by_i[i]) for i in parse_indices(args.index, len(ufs))]
    if args.limit:
        return numbered[: args.limit]
    return numbered


def main() -> int:
    ap = argparse.ArgumentParser(description="Batch de test CUA Argelès (hors prod)")
    ap.add_argument(
        "--fixture",
        default=str(FIXTURE),
        help="JSON d'échantillon (défaut : tests/echantillon_uf.json)",
    )
    ap.add_argument("--list", action="store_true", help="lister les UF avec leur indice, sans générer")
    ap.add_argument(
        "--index",
        help="UF à générer, indice 1-based dans l'échantillon (ex: 17 ou 3,17,22)",
    )
    ap.add_argument("--limit", type=int, default=0, help="limiter aux N premières UF")
    ap.add_argument("--refs", help="ex: 'AB:123,AB:124' — une UF précise, hors échantillon")
    ap.add_argument("--pdf", action="store_true", help="convertir les docx en PDF")
    args = ap.parse_args()

    if args.refs and args.index:
        ap.error("--refs et --index sont exclusifs")
    if args.limit and args.index:
        ap.error("--limit et --index sont exclusifs")
    if args.refs and args.limit:
        ap.error("--refs et --limit sont exclusifs")

    fixture = Path(args.fixture)
    if args.refs:
        ufs = []
    else:
        ufs = charger_echantillon(fixture)
        print(f"Échantillon : {fixture}  ({len(ufs)} UF)")

    if args.list:
        if args.refs:
            ap.error("--list lit l'échantillon, pas --refs")
        lister_echantillon(ufs)
        return 0

    try:
        selection = selectionner_ufs(args, ufs)
    except ValueError as exc:
        print(f"Erreur : {exc}", file=sys.stderr)
        return 2

    catalogue = json.loads(CATALOGUE.read_text(encoding="utf-8"))
    out_dir = HERE / "out" / datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True)

    n_sel, n_total = len(selection), len(ufs) or len(selection)
    docx_paths, echecs = [], 0
    for i, uf in selection:
        slug, refs = uf["id"], uf["refs"]
        t0 = time.perf_counter()
        try:
            path = regenere(slug, refs, catalogue, out_dir)
            docx_paths.append(path)
            print(
                f"OK [{i}/{n_total}] {slug:<40} {len(refs)} parc. "
                f"{time.perf_counter()-t0:.1f}s  {_fmt_refs(refs)}"
            )
        except Exception as exc:
            echecs += 1
            (out_dir / f"{slug}.error.txt").write_text(traceback.format_exc(), encoding="utf-8")
            print(f"KO [{i}/{n_total}] {slug:<40} {type(exc).__name__}: {exc}")

    if args.pdf:
        convertir_pdf(docx_paths, out_dir)
    print(f"\nDossier : {out_dir}  ({len(docx_paths)} ok, {echecs} echec(s), {n_sel} demandé(s))")
    return 1 if echecs else 0


if __name__ == "__main__":
    sys.exit(main())