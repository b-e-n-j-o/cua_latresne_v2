#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnostic intersection parcelle / UF × couche (Latresne).

Compare les entités brutes (ST_Intersects) vs le résultat filtré (seuils
intersections.py) et indique si la couche serait rendue dans le CUA.

Usage (depuis cua_latresne_v4, venv + .env chargé) :

  # Une parcelle
  python api/communes/latresne/cuas/INTERSECTIONS/tests/test_couche_parcelle.py \\
    --section AN --numero 0474 --couche preemption

  # Unité foncière (plusieurs parcelles → ST_Union)
  python api/communes/latresne/cuas/INTERSECTIONS/tests/test_couche_parcelle.py \\
    --refs "AN:0474,AN:0476" --couche preemption --json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

TESTS_DIR = Path(__file__).resolve().parent
_REF_RE = re.compile(r"^\s*([A-Za-z]+)\s*[:\s]\s*(\d+)\s*$")


def _find_project_root() -> Path:
    """Remonte jusqu'à cua_latresne_v4 (répertoire contenant main.py + package api/)."""
    for candidate in (TESTS_DIR, *TESTS_DIR.parents):
        if (candidate / "main.py").is_file() and (candidate / "api").is_dir():
            return candidate
    raise RuntimeError(
        "Impossible de localiser la racine cua_latresne_v4 (main.py + api/). "
        "Lancez le script depuis le dépôt ou vérifiez l'arborescence."
    )


PROJECT_ROOT = _find_project_root()

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from api.communes.latresne.cuas.INTERSECTIONS.intersections import (
    CATALOGUE,
    DEFAULT_MIN_PCT_SIG,
    GEOM_COL,
    MIN_INTERSECTION_AREA_M2,
    MIN_INTERSECTION_LENGTH_M,
    SCHEMA,
    SRID,
    _normalize_geom_type,
    _safe_ident,
    calculate_intersection,
    engine,
    format_intersection_layer,
    get_parcelle_geometry,
    resolve_min_pct_sig,
)


def parse_refs(refs: str) -> list[tuple[str, str]]:
    """
    Parse 'AN:0474,AN:0476' ou 'AN 0474, AN 0476' → [('AN','0474'), ('AN','0476')].
    """
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for chunk in refs.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        m = _REF_RE.match(chunk)
        if not m:
            raise SystemExit(
                f"Référence invalide : {chunk!r}. Format attendu : SECTION:NUMERO (ex. AN:0474)"
            )
        section = m.group(1).upper()
        numero = m.group(2).zfill(4)
        key = (section, numero)
        if key not in seen:
            seen.add(key)
            out.append(key)
    if not out:
        raise SystemExit("Aucune référence cadastrale fournie.")
    return out


def build_uf_wkt(parcelles: list[tuple[str, str]]) -> str:
    """Union des géométries parcelles (équivalent unary_union du pipeline UF)."""
    values_sql = ", ".join([f"(:s{i}, :n{i})" for i in range(len(parcelles))])
    params: dict = {}
    for i, (section, numero) in enumerate(parcelles):
        params[f"s{i}"] = section
        params[f"n{i}"] = numero

    q = text(
        f"""
        WITH requested(section, numero) AS (
            VALUES {values_sql}
        ),
        geoms AS (
            SELECT ST_MakeValid(p.geom_2154) AS g
            FROM requested r
            JOIN {SCHEMA}.parcelles p
              ON UPPER(TRIM(p.section)) = r.section
             AND LPAD(TRIM(p.numero), 4, '0') = r.numero
        )
        SELECT ST_AsText(ST_Union(g)) AS uf_wkt
        FROM geoms
        """
    )
    with engine.connect() as conn:
        uf_wkt = conn.execute(q, params).scalar()
    if not uf_wkt:
        refs = ", ".join(f"{s} {n}" for s, n in parcelles)
        raise SystemExit(f"Impossible de construire l'UF : parcelle(s) introuvable(s) ? ({refs})")
    return uf_wkt


def _geom_area_m2(geom_wkt: str) -> float:
    with engine.connect() as conn:
        return float(
            conn.execute(
                text(f"SELECT ST_Area(ST_GeomFromText(:wkt, {SRID}))"),
                {"wkt": geom_wkt},
            ).scalar()
        )


def _audit_raw_entities(
    geom_wkt: str,
    table: str,
    config: dict,
    area_sig: float,
) -> list[dict]:
    """Entités ST_Intersects sans filtre métier (diagnostic)."""
    geom_type = _normalize_geom_type(config.get("geom_type"))
    table_sql = _safe_ident(table)
    geom_col = _safe_ident(config.get("geom_col", GEOM_COL))
    keep_cols = [_safe_ident(c) for c in (config.get("keep") or [])]
    if not keep_cols:
        return []

    select_cols = ", ".join(f"t.{c}" for c in keep_cols)

    if geom_type == "ponctuel":
        metric_sql = "NULL::float AS metric"
        where_extra = f"AND ST_Within(t.{geom_col}, p.g)"
    elif geom_type == "lineaire":
        metric_sql = (
            f"ROUND(CAST(ST_Length(ST_Intersection(ST_MakeValid(t.{geom_col}), p.g)) "
            f"AS numeric), 4) AS metric"
        )
        where_extra = ""
    else:
        metric_sql = (
            f"ROUND(CAST(ST_Area(ST_Intersection(ST_MakeValid(t.{geom_col}), p.g)) "
            f"AS numeric), 4) AS metric"
        )
        where_extra = ""

    q = f"""
        WITH p AS (
            SELECT ST_MakeValid(ST_GeomFromText(:wkt, {SRID})) AS g
        )
        SELECT
            {select_cols},
            {metric_sql}
        FROM {SCHEMA}.{table_sql} t, p
        WHERE t.{geom_col} IS NOT NULL
          AND ST_Intersects(ST_MakeValid(t.{geom_col}), p.g)
          {where_extra}
        ORDER BY metric DESC NULLS LAST
    """

    with engine.connect() as conn:
        rows = conn.execute(text(q), {"wkt": geom_wkt}).mappings().all()

    out = []
    for row in rows:
        item = {k: row[k] for k in keep_cols}
        metric = row.get("metric")
        if metric is not None:
            metric = float(metric)
        item["metric"] = metric
        if geom_type == "surfacique" and metric is not None and area_sig > 0:
            item["pct_uf"] = round(metric / area_sig * 100, 4)
        elif geom_type == "lineaire":
            item["longueur_inter_m"] = metric
        out.append(item)
    return out


def _passes_filter(geom_type: str, metric: float | None, pct_uf: float | None, min_pct_sig: float) -> bool:
    if geom_type == "ponctuel":
        return True
    if geom_type == "lineaire":
        return (metric or 0) > MIN_INTERSECTION_LENGTH_M
    area = metric or 0
    if area <= MIN_INTERSECTION_AREA_M2:
        return False
    if min_pct_sig <= 0:
        return True
    return (pct_uf or 0) > min_pct_sig


def _would_render_in_cua(layer_key: str, layer: dict, catalogue: dict) -> bool:
    """Reproduit la logique builder actuelle : objets requis."""
    objets = layer.get("objets") or []
    if not objets:
        return False
    geom_type = layer.get("geom_type") or _normalize_geom_type(
        catalogue.get(layer_key, {}).get("geom_type")
    )
    return bool(objets) if geom_type else bool(objets)


def _diagnose_geom(
    geom_wkt: str,
    couche: str,
    *,
    label: str,
    parcelles: list[tuple[str, str]] | None = None,
) -> dict:
    if couche not in CATALOGUE:
        known = ", ".join(sorted(CATALOGUE.keys())[:8])
        raise SystemExit(f"Couche inconnue : {couche!r}. Exemples : {known}, …")

    config = CATALOGUE[couche]
    geom_type = _normalize_geom_type(config.get("geom_type"))
    min_pct_sig = resolve_min_pct_sig(config)
    area_sig = _geom_area_m2(geom_wkt)

    raw_entities = _audit_raw_entities(geom_wkt, couche, config, area_sig)
    objets, total_metric, metadata = calculate_intersection(geom_wkt, couche, area_sig)
    layer = format_intersection_layer(config, objets, total_metric, area_sig)

    raw_audit = []
    for ent in raw_entities:
        metric = ent.get("metric")
        pct = ent.get("pct_uf")
        raw_audit.append({
            "attributs": {
                k: v for k, v in ent.items()
                if k not in ("metric", "pct_uf", "longueur_inter_m")
            },
            "metric": metric,
            "pct_uf": pct,
            "passe_filtre": _passes_filter(geom_type, metric, pct, min_pct_sig),
        })

    in_cua = _would_render_in_cua(couche, layer, CATALOGUE)

    out = {
        "label": label,
        "parcelles": [f"{s} {n}" for s, n in (parcelles or [])],
        "couche": couche,
        "nom": config.get("nom"),
        "article": config.get("article"),
        "geom_type": geom_type,
        "surface_m2": round(area_sig, 2),
        "seuils": {
            "min_area_m2": MIN_INTERSECTION_AREA_M2,
            "min_length_m": MIN_INTERSECTION_LENGTH_M,
            "min_pct_sig": min_pct_sig,
            "default_min_pct_sig": DEFAULT_MIN_PCT_SIG,
        },
        "brut": {
            "nb_entites_st_intersects": len(raw_entities),
            "entites": raw_audit,
        },
        "filtre": {
            "nb_objets": len(objets),
            "total_metric": round(total_metric, 4),
            "pct_sig": layer.get("pct_sig", 0),
            "objets": objets,
            "metadata": metadata,
        },
        "verdict": {
            "intersection_comptabilisee": bool(objets),
            "paragraphe_cua": in_cua,
            "message": (
                "Couche retenue → paragraphe CUA"
                if in_cua
                else "Couche ignorée → pas de paragraphe CUA"
            ),
        },
    }
    return out


def diagnose_single(section: str, numero: str, couche: str) -> dict:
    section = section.upper().strip()
    numero = str(numero).strip().zfill(4)
    parcelles = [(section, numero)]
    geom_wkt = get_parcelle_geometry(section, numero)
    report = _diagnose_geom(
        geom_wkt,
        couche,
        label=f"{section} {numero}",
        parcelles=parcelles,
    )
    report["mode"] = "parcelle"
    report["parcelle"] = f"{section} {numero}"
    return report


def diagnose_uf(parcelles: list[tuple[str, str]], couche: str, *, compare: bool) -> dict:
    uf_wkt = build_uf_wkt(parcelles)
    uf_report = _diagnose_geom(
        uf_wkt,
        couche,
        label="UF",
        parcelles=parcelles,
    )
    uf_report["mode"] = "unite_fonciere"
    uf_report["unite_fonciere"] = {
        "parcelles": [f"{s} {n}" for s, n in parcelles],
        "surface_m2": uf_report["surface_m2"],
    }
    uf_report["parcelle"] = "UF (" + ", ".join(uf_report["unite_fonciere"]["parcelles"]) + ")"

    if compare and len(parcelles) > 1:
        uf_report["comparaison_parcelles"] = []
        for section, numero in parcelles:
            wkt = get_parcelle_geometry(section, numero)
            uf_report["comparaison_parcelles"].append(
                _diagnose_geom(
                    wkt,
                    couche,
                    label=f"{section} {numero}",
                    parcelles=[(section, numero)],
                )
            )
    return uf_report


def _print_block(report: dict, title: str | None = None) -> None:
    if title:
        print(title)
    print(f"  Emprise       : {report.get('label', report.get('parcelle', '—'))}")
    print(f"  Surface       : {report['surface_m2']} m²")
    print(f"  Brut          : {report['brut']['nb_entites_st_intersects']} entité(s)")
    for i, ent in enumerate(report["brut"]["entites"], 1):
        attrs = ent["attributs"]
        label = next((str(v) for v in attrs.values() if v), f"entité #{i}")
        flag = "✓" if ent["passe_filtre"] else "✗"
        if report["geom_type"] == "surfacique":
            print(
                f"    {flag} [{i}] {str(label)[:50]} | "
                f"{ent['metric']} m² ({ent.get('pct_uf', 0):.4f} %)"
            )
        elif report["geom_type"] == "lineaire":
            print(f"    {flag} [{i}] {str(label)[:50]} | {ent['metric']} m")
        else:
            print(f"    {flag} [{i}] {str(label)[:50]}")
    filtre = report["filtre"]
    print(
        f"  Après filtre  : {filtre['nb_objets']} objet(s), "
        f"pct_sig={filtre['pct_sig']:.4f} %"
    )
    v = report["verdict"]
    print(f"  CUA           : {'OUI' if v['paragraphe_cua'] else 'NON'} — {v['message']}")


def _print_human(report: dict) -> None:
    print("=" * 72)
    print(f"Mode          : {report['mode']}")
    print(f"Couche        : {report['couche']} ({report['nom']})")
    print(f"Article CUA   : {report['article']}")
    print(f"Géométrie     : {report['geom_type']}")
    seuils = report["seuils"]
    print(
        f"Seuils        : area>{seuils['min_area_m2']} m², "
        f"length>{seuils['min_length_m']} m, "
        f"pct>{seuils['min_pct_sig']} %"
    )
    print("-" * 72)
    _print_block(report)
    for sub in report.get("comparaison_parcelles") or []:
        print("-" * 72)
        _print_block(sub, title=f"Parcelle isolée : {sub['label']}")
    print("=" * 72)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnostic intersection parcelle / UF × couche (Latresne)"
    )
    parser.add_argument(
        "--refs",
        help='Unité foncière : "AN:0474,AN:0476" ou "AN 0474, AN 0476"',
    )
    parser.add_argument("--section", help="Section cadastrale — parcelle unique (ex. AN)")
    parser.add_argument("--numero", help="Numéro de parcelle — parcelle unique (ex. 0474)")
    parser.add_argument("--couche", required=True, help="Clé catalogue / table SQL (ex. preemption)")
    parser.add_argument(
        "--compare-parcelles",
        action="store_true",
        help="Avec --refs : affiche aussi le diagnostic parcelle par parcelle",
    )
    parser.add_argument("--json", action="store_true", help="Sortie JSON complète")
    args = parser.parse_args()

    if args.refs:
        parcelles = parse_refs(args.refs)
        report = diagnose_uf(parcelles, args.couche, compare=args.compare_parcelles)
    else:
        if not args.section or not args.numero:
            parser.error("Fournir --refs ou bien --section et --numero")
        report = diagnose_single(args.section, args.numero, args.couche)

    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    else:
        _print_human(report)


if __name__ == "__main__":
    main()
