#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Premier passage pour repérer des modules Python potentiellement non référencés.

Pour chaque fichier *.py d'un dossier, cherche son nom (sans .py) avec ripgrep
dans un périmètre de recherche. Si le nom n'apparaît nulle part ailleurs, le
fichier est listé comme candidat « code mort » à vérifier manuellement.

Limites connues (volontairement simple) :
  - ne détecte pas les imports dynamiques ;
  - un nom générique (utils, main…) peut donner des faux positifs/négatifs ;
  - un script lancé en CLI sans import statique peut être listé à tort.

Usage :
    cd BACKEND_PRINCIPAL/LATRESNE/cua_latresne_v4
    python find_unreferenced_modules.py .
    python find_unreferenced_modules.py api --search-root .
    python find_unreferenced_modules.py . --json
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

# Noms trop génériques : signaler, ne pas exclure automatiquement.
AMBIGUOUS_STEMS = frozenset(
    {
        "__init__",
        "main",
        "utils",
        "config",
        "models",
        "schemas",
        "router",
        "types",
        "constants",
        "helpers",
        "common",
        "base",
        "test",
        "tests",
    }
)


@dataclass(frozen=True)
class ModuleScanResult:
    path: str
    stem: str
    reference_count: int
    referenced_by: list[str]
    ambiguous_name: bool
    skipped: bool
    skip_reason: str | None = None


def _ensure_rg() -> str:
    rg = shutil.which("rg")
    if not rg:
        sys.exit("Erreur : ripgrep (rg) est requis mais introuvable dans le PATH.")
    return rg


def _is_hidden(path: Path) -> bool:
    """Fichier ou dossier dont un segment commence par '.' (ex. .git, ._foo.py)."""
    return any(part.startswith(".") for part in path.parts)


def _collect_py_files(root: Path) -> list[Path]:
    return sorted(
        p for p in root.rglob("*.py") if p.is_file() and not _is_hidden(p)
    )


def _rg_referencing_files(
    rg: str,
    pattern: str,
    search_root: Path,
    *,
    py_only: bool,
) -> list[Path]:
    cmd = [rg, "-l", "-w", "--glob", "!.git", pattern, str(search_root)]
    if py_only:
        cmd[1:1] = ["--type", "py"]

    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode not in (0, 1):
        sys.exit(f"Erreur rg pour « {pattern} » :\n{proc.stderr.strip()}")

    if not proc.stdout.strip():
        return []

    return [Path(line) for line in proc.stdout.splitlines() if line.strip()]


def _relative_path(path: Path, base: Path) -> str:
    try:
        return str(path.resolve().relative_to(base.resolve()))
    except ValueError:
        return str(path)


def scan_module(
    py_file: Path,
    *,
    input_root: Path,
    search_root: Path,
    rg: str,
    py_only: bool,
    skip_init: bool,
) -> ModuleScanResult:
    rel = _relative_path(py_file, input_root)
    stem = py_file.stem

    if skip_init and stem == "__init__":
        return ModuleScanResult(
            path=rel,
            stem=stem,
            reference_count=0,
            referenced_by=[],
            ambiguous_name=False,
            skipped=True,
            skip_reason="__init__.py ignoré (nom non discriminant)",
        )

    refs = _rg_referencing_files(rg, re.escape(stem), search_root, py_only=py_only)
    refs = [
        p
        for p in refs
        if p.resolve() != py_file.resolve() and not _is_hidden(p)
    ]

    return ModuleScanResult(
        path=rel,
        stem=stem,
        reference_count=len(refs),
        referenced_by=sorted(_relative_path(p, search_root) for p in refs),
        ambiguous_name=stem in AMBIGUOUS_STEMS,
        skipped=False,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Liste les fichiers .py dont le nom de module ne ressort pas dans rg.",
    )
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Dossier contenant les .py à analyser (parcours récursif).",
    )
    parser.add_argument(
        "--search-root",
        type=Path,
        default=None,
        help="Racine de recherche rg (défaut : input_dir).",
    )
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Chercher dans tous les fichiers, pas seulement les .py.",
    )
    parser.add_argument(
        "--include-init",
        action="store_true",
        help="Inclure les __init__.py (déconseillé).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Sortie JSON (sinon texte lisible).",
    )
    parser.add_argument(
        "--show-referenced",
        action="store_true",
        help="Afficher aussi les modules référencés ailleurs.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    input_dir = args.input_dir.resolve()
    search_root = (args.search_root or args.input_dir).resolve()

    if not input_dir.is_dir():
        sys.exit(f"Erreur : dossier introuvable : {input_dir}")
    if not search_root.is_dir():
        sys.exit(f"Erreur : search-root introuvable : {search_root}")

    rg = _ensure_rg()
    py_files = _collect_py_files(input_dir)

    results = [
        scan_module(
            py_file,
            input_root=input_dir,
            search_root=search_root,
            rg=rg,
            py_only=not args.all_files,
            skip_init=not args.include_init,
        )
        for py_file in py_files
    ]

    unreferenced = [r for r in results if not r.skipped and r.reference_count == 0]
    ambiguous_unreferenced = [r for r in unreferenced if r.ambiguous_name]

    if args.json:
        payload = {
            "input_dir": str(input_dir),
            "search_root": str(search_root),
            "scanned": len(py_files),
            "unreferenced": [asdict(r) for r in unreferenced],
            "ambiguous_unreferenced": [r.path for r in ambiguous_unreferenced],
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    print(f"Scanné : {len(py_files)} fichier(s) .py dans {input_dir}")
    print(f"Recherche rg : « <nom_module> » dans {search_root}")
    print()

    if not unreferenced:
        print("Aucun candidat sans référence externe.")
    else:
        print(f"Candidats sans référence externe ({len(unreferenced)}) :")
        for r in unreferenced:
            flag = " [nom ambigu]" if r.ambiguous_name else ""
            print(f"  - {r.path}{flag}")

    if ambiguous_unreferenced:
        print()
        print(
            f"Note : {len(ambiguous_unreferenced)} candidat(s) ont un nom générique "
            f"({', '.join(sorted(AMBIGUOUS_STEMS))}) — à vérifier avec prudence."
        )

    skipped = [r for r in results if r.skipped]
    if skipped:
        print()
        print(f"Ignorés : {len(skipped)} (__init__.py par défaut)")

    if args.show_referenced:
        referenced = [r for r in results if not r.skipped and r.reference_count > 0]
        print()
        print(f"Référencés ailleurs ({len(referenced)}) :")
        for r in referenced:
            print(f"  - {r.path} ({r.reference_count} fichier(s))")

    print()
    print("À valider manuellement (imports dynamiques, CLI, Docker, cron…).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
