#!/usr/bin/env python3
"""
Génère des PMTiles pour toutes les couches d'un catalogue JSON (ex. catalogue_cua_argeles.json).

Parcourt chaque entrée du catalogue, vérifie la table PostGIS, génère {table}.pmtiles
(source-layer = nom de table) et uploade sur Supabase Storage.

Pré-requis : identiques à pmtiles.py (GDAL ≥ 3.8, .env SUPABASE_*).

Exemples :
  python pmtiles_batch.py --schema argeles
  python pmtiles_batch.py --schema argeles --dry-run
  python pmtiles_batch.py --schema argeles --only zonage_plu,prescriptions_surf
  python pmtiles_batch.py --schema argeles --catalogue /chemin/catalogue.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pmtiles_core import GeomType, PmtilesResult, process_table

# Couches carto hors catalogue CUA intersections
EXTRA_LAYERS: dict[str, dict] = {
    "sup_generateur_s": {"nom": "Générateurs SUP surfaciques", "geom_type": "surfacique"},
    "sup_generateur_l": {"nom": "Générateurs SUP linéaires", "geom_type": "lineaire"},
    "sup_generateur_p": {"nom": "Générateurs SUP ponctuels", "geom_type": "ponctuel"},
    "sup_assiette_l": {"nom": "Assiettes SUP linéaires", "geom_type": "lineaire"},
    "sup_assiette_p": {"nom": "Assiettes SUP ponctuelles", "geom_type": "ponctuel"},
    "parcelles": {"nom": "Parcelles cadastrales", "geom_type": "surfacique"},
    "batiments": {"nom": "Bâtiments", "geom_type": "surfacique"},
}

DEFAULT_CATALOGUE = (
    Path(__file__).resolve().parents[2]
    / "api"
    / "cuas"
    / "catalogue_cua_argeles.json"
)


def load_catalogue(path: Path) -> dict[str, dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Catalogue invalide (dict attendu) : {path}")
    return data


def resolve_geom_type(entry: dict) -> GeomType:
    raw = str(entry.get("geom_type") or "surfacique").lower()
    if raw in ("surfacique", "polygon", "multipolygon"):
        return "surfacique"
    if raw in ("lineaire", "line", "multilinestring"):
        return "lineaire"
    if raw in ("ponctuel", "point", "multipoint"):
        return "ponctuel"
    return "surfacique"


def filename_for_table(table: str) -> str:
    return f"{table}.pmtiles"


def merge_layers(catalogue: dict[str, dict], include_extra: bool) -> dict[str, dict]:
    merged = dict(catalogue)
    if include_extra:
        for table, meta in EXTRA_LAYERS.items():
            merged.setdefault(table, meta)
    return merged


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch PMTiles depuis un catalogue de couches")
    parser.add_argument("--schema", required=True, help="Schéma PostgreSQL (ex: argeles)")
    parser.add_argument(
        "--catalogue",
        type=Path,
        default=DEFAULT_CATALOGUE,
        help=f"Chemin catalogue JSON (défaut: {DEFAULT_CATALOGUE.name})",
    )
    parser.add_argument(
        "--only",
        help="Tables comma-separées (ex: zonage_plu,prescriptions_surf)",
    )
    parser.add_argument(
        "--include-extra",
        action="store_true",
        help="Ajoute sup_generateur_*, sup_assiette_l/p, parcelles, batiments",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preflight uniquement, pas de génération")
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Arrête au premier échec (défaut: continue)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.catalogue.is_file():
        sys.exit(f"Catalogue introuvable : {args.catalogue}")

    catalogue = load_catalogue(args.catalogue)
    layers = merge_layers(catalogue, args.include_extra)

    if args.only:
        wanted = {t.strip() for t in args.only.split(",") if t.strip()}
        layers = {k: v for k, v in layers.items() if k in wanted}
        unknown = wanted - set(layers)
        if unknown:
            print(f"⚠️  Tables inconnues dans le catalogue : {', '.join(sorted(unknown))}")

    if not layers:
        sys.exit("Aucune couche à traiter.")

    print("=" * 72)
    print(f"BATCH PMTiles — schéma {args.schema!r} — {len(layers)} couche(s)")
    print(f"Catalogue : {args.catalogue}")
    if args.dry_run:
        print("Mode : DRY-RUN (preflight seulement)")
    print("=" * 72)

    results: list[PmtilesResult] = []

    for table in sorted(layers):
        entry = layers[table]
        geom_type = resolve_geom_type(entry)
        filename = filename_for_table(table)
        nom = entry.get("nom", table)

        print(f"\n→ {table} ({nom}) [{geom_type}] → {filename}")

        result = process_table(
            args.schema,
            table,
            filename,
            geom_type=geom_type,
            upload=not args.dry_run,
            dry_run=args.dry_run,
        )
        results.append(result)

        icon = {"ok": "✅", "skipped": "⏭️", "error": "❌"}.get(result.status, "?")
        detail = result.message
        if result.count:
            detail = f"{result.count:,} entités — {detail}"
        if result.size_mb:
            detail += f" — {result.size_mb:.2f} Mo"
        if result.layer_name:
            detail += f' — source-layer="{result.layer_name}"'
        print(f"  {icon} {detail}")

        if result.status == "error" and args.stop_on_error:
            break

    ok = sum(1 for r in results if r.status == "ok")
    skipped = sum(1 for r in results if r.status == "skipped")
    errors = sum(1 for r in results if r.status == "error")

    print("\n" + "=" * 72)
    print(f"Résumé : {ok} ok | {skipped} ignorées | {errors} erreurs")
    print("=" * 72)

    if errors:
        print("\nErreurs :")
        for r in results:
            if r.status == "error":
                print(f"  • {r.table}: {r.message}")

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
