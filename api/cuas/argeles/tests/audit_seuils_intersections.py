#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit des seuils d'intersection surfacique (calibrage min_pct_sig).

Mode batch (défaut) : 1 requête SQL par couche × toutes les parcelles du lot
(modèle enrich_parcelles_resume) — adapté à 100 parcelles ou à la commune entière.

Mode legacy (--legacy) : 1 parcelle × N couches (lent, debug uniquement).

Usage (depuis cua_latresne_v4, venv + .env) :

  # 100 parcelles aléatoires (défaut, batch)
  python api/cuas/argeles/tests/audit_seuils_intersections.py

  # Commune entière (~12k parcelles, quelques minutes)
  python api/cuas/argeles/tests/audit_seuils_intersections.py --all

  # Échantillon reproductible
  python api/cuas/argeles/tests/audit_seuils_intersections.py --sample 500 --seed 42

  # Seuil production (catalogue 1 %)
  python api/cuas/argeles/tests/audit_seuils_intersections.py --all --use-catalogue-threshold
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import text

TESTS_DIR = Path(__file__).resolve().parent
CUAS_DIR = TESTS_DIR.parent
PROJECT_ROOT = TESTS_DIR.parents[3]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from api.cuas.argeles.db import GEOM_COL, SCHEMA, get_engine, logger
from api.cuas.argeles.intersections import (
    MIN_INTERSECTION_AREA_M2,
    _safe_ident,
    _table_exists,
    calculate_intersection,
    load_catalogue,
    resolve_min_pct_sig,
)
from api.cuas.argeles.uf import build_uf
from api.modules_communs.servitudes import ARGELES_SERVITUDES_CONFIG

DEFAULT_CATALOGUE = CUAS_DIR / "catalogue_cua_argeles.json"
OUTPUT_DIR = TESTS_DIR / "output"
DEFAULT_OUT = OUTPUT_DIR / "audit_seuils_distribution_100.json"
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

BUCKETS = ("micro_le_1", "entre_1_et_5", "gt_5")


def _bucket_pct(pct: float) -> str | None:
    if pct <= 0:
        return None
    if pct <= 1.0:
        return "micro_le_1"
    if pct <= 5.0:
        return "entre_1_et_5"
    return "gt_5"


def _obj_label(obj: dict, cfg: dict) -> str:
    for key in cfg.get("keep") or []:
        val = obj.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()[:120]
    pct = obj.get("pct_sig")
    if pct is not None:
        return f"(sans libellé, {pct:.4f} %)"
    return "(sans libellé)"


def _prepare_catalogue(catalogue: dict, *, audit_raw: bool) -> dict:
    out = deepcopy(catalogue)
    if not audit_raw:
        return out
    for cfg in out.values():
        if cfg.get("geom_type", "surfacique") == "surfacique":
            cfg["min_pct_sig"] = 0.0
    return out


def _cache_existing_tables(engine, schema: str, catalogue: dict) -> set[str]:
    tables: set[str] = set()
    for table, cfg in catalogue.items():
        if cfg.get("handler") == "servitudes":
            tables.add("servitudes")
        elif table != "reseaux_enedis_lineaires":
            tables.add(table)
    return {t for t in tables if _table_exists(engine, schema, t)}


def _parcelles_batch_cte(
    schema: str,
    *,
    sample: int | None,
    seed: str | None,
) -> tuple[str, dict[str, Any]]:
    schema_id = _safe_ident(schema)
    geom_col = _safe_ident(GEOM_COL)
    params: dict[str, Any] = {}

    if seed is not None:
        order_sql = (
            "ORDER BY md5(upper(trim(section)) || lpad(trim(numero), 4, '0') || :seed)"
        )
        params["seed"] = str(seed)
    else:
        order_sql = "ORDER BY section, numero"

    limit_sql = ""
    if sample is not None:
        limit_sql = "LIMIT :sample"
        params["sample"] = int(sample)

    cte = f"""
        parcelles_src AS (
            SELECT section,
                   numero,
                   ST_MakeValid({geom_col}) AS geom,
                   GREATEST(ST_Area(ST_MakeValid({geom_col})), 0.01) AS surface_sig
            FROM {schema_id}.parcelles
            WHERE {geom_col} IS NOT NULL
              AND NOT ST_IsEmpty({geom_col})
              AND ST_Area(ST_MakeValid({geom_col})) > 0
        ),
        parcelles_batch AS (
            SELECT section, numero, geom, surface_sig
            FROM parcelles_src
            {order_sql}
            {limit_sql}
        )
    """
    return cte, params


def _count_parcelles_batch(conn, batch_cte: str, params: dict) -> int:
    sql = text(f"WITH {batch_cte} SELECT COUNT(*) FROM parcelles_batch")
    return int(conn.execute(sql, params).scalar() or 0)


@dataclass
class LayerStats:
    nom: str = ""
    geom_type: str = ""
    total_hits: int = 0
    buckets: dict[str, int] = field(default_factory=lambda: {b: 0 for b in BUCKETS})
    parcelles_avec_hit: set[str] = field(default_factory=set)
    parcelles_avec_1_5: set[str] = field(default_factory=set)
    pct_min: float | None = None
    pct_max: float | None = None

    def register(self, parcel_ref: str, pct: float | None) -> None:
        if pct is None or pct <= 0:
            return
        bucket = _bucket_pct(pct)
        if not bucket:
            return
        self.total_hits += 1
        self.buckets[bucket] += 1
        self.parcelles_avec_hit.add(parcel_ref)
        if bucket == "entre_1_et_5":
            self.parcelles_avec_1_5.add(parcel_ref)
        self.pct_min = pct if self.pct_min is None else min(self.pct_min, pct)
        self.pct_max = pct if self.pct_max is None else max(self.pct_max, pct)


def _register_hit(
    layer_stats: dict[str, LayerStats],
    verbose_hits: list[dict],
    *,
    table: str,
    cfg: dict,
    section: str,
    numero: str,
    pct_sig: float,
    libelle: str | None = None,
) -> None:
    parcel_ref = f"{section}:{numero}"
    geom_type = cfg.get("geom_type", "surfacique")
    stats = layer_stats.setdefault(
        table,
        LayerStats(
            nom=cfg.get("nom") or table,
            geom_type=geom_type,
        ),
    )
    stats.register(parcel_ref, pct_sig)
    if _bucket_pct(pct_sig) == "entre_1_et_5":
        verbose_hits.append(
            {
                "parcelle": parcel_ref,
                "couche": table,
                "nom_couche": stats.nom,
                "pct_sig": round(pct_sig, 4),
                "libelle": libelle or "",
            }
        )


def _batch_catalogue_layer(
    conn,
    schema: str,
    table: str,
    cfg: dict,
    batch_cte: str,
    params: dict,
    min_pct_sig: float,
) -> list[dict]:
    table_id = _safe_ident(table)
    geom_col = _safe_ident(cfg.get("geom_col", GEOM_COL))
    schema_id = _safe_ident(schema)
    label_col = None
    for key in cfg.get("keep") or []:
        if _IDENT_RE.match(key):
            label_col = _safe_ident(key)
            break
    label_sql = f"z.{label_col}" if label_col else "NULL::text"

    sql = text(
        f"""
        WITH {batch_cte},
        inter AS (
            SELECT p.section,
                   p.numero,
                   p.surface_sig,
                   {label_sql} AS label,
                   ST_Area(
                       ST_Intersection(p.geom, ST_MakeValid(z.{geom_col}))
                   ) AS inter_area
            FROM parcelles_batch p
            INNER JOIN {schema_id}.{table_id} z
                ON z.{geom_col} IS NOT NULL
               AND p.geom && z.{geom_col}
               AND ST_Intersects(p.geom, z.{geom_col})
        )
        SELECT section,
               numero,
               label,
               ROUND((inter_area / surface_sig * 100)::numeric, 4) AS pct_sig
        FROM inter
        WHERE inter_area > {MIN_INTERSECTION_AREA_M2}
          AND (
              :min_pct_sig <= 0
              OR surface_sig <= 0
              OR (inter_area / surface_sig * 100) > :min_pct_sig
          )
        """
    )
    qparams = {**params, "min_pct_sig": float(min_pct_sig)}
    return [dict(r) for r in conn.execute(sql, qparams).mappings().all()]


def _batch_servitudes_layer(
    conn,
    schema: str,
    batch_cte: str,
    params: dict,
    min_pct_sig: float,
) -> list[dict]:
    config = ARGELES_SERVITUDES_CONFIG
    schema_id = _safe_ident(schema)
    table_id = _safe_ident(config.servitudes_table)
    geom_col = _safe_ident(config.geom_column)
    excluded = config.excluded_suptypes
    excluded_sql = ""
    if excluded:
        quoted = ", ".join(f"'{_safe_ident(s)}'" for s in sorted(excluded))
        excluded_sql = f"AND UPPER(TRIM(z.suptype)) NOT IN ({quoted})"

    sql = text(
        f"""
        WITH {batch_cte},
        inter AS (
            SELECT p.section,
                   p.numero,
                   p.surface_sig,
                   z.suptype AS label,
                   ST_Area(
                       ST_Intersection(p.geom, ST_MakeValid(z.{geom_col}))
                   ) AS inter_area
            FROM parcelles_batch p
            INNER JOIN {schema_id}.{table_id} z
                ON z.{geom_col} IS NOT NULL
               AND z.suptype IS NOT NULL
               AND p.geom && z.{geom_col}
               AND ST_Intersects(p.geom, z.{geom_col})
               {excluded_sql}
        )
        SELECT section,
               numero,
               label,
               ROUND((inter_area / surface_sig * 100)::numeric, 4) AS pct_sig
        FROM inter
        WHERE inter_area > {MIN_INTERSECTION_AREA_M2}
          AND (
              :min_pct_sig <= 0
              OR surface_sig <= 0
              OR (inter_area / surface_sig * 100) > :min_pct_sig
          )
        """
    )
    qparams = {**params, "min_pct_sig": float(min_pct_sig)}
    return [dict(r) for r in conn.execute(sql, qparams).mappings().all()]


def run_batch_audit(
    engine,
    schema: str,
    catalogue: dict,
    audit_catalogue: dict,
    *,
    sample: int | None,
    seed: str | None,
    include_lineaire: bool,
) -> tuple[int, dict[str, LayerStats], list[dict], list[dict]]:
    layer_stats: dict[str, LayerStats] = {}
    verbose_hits: list[dict] = []
    errors: list[dict] = []
    existing = _cache_existing_tables(engine, schema, catalogue)

    batch_cte, params = _parcelles_batch_cte(schema, sample=sample, seed=seed)

    with engine.connect() as conn:
        n_parcelles = _count_parcelles_batch(conn, batch_cte, params)
        logger.info(f"Lot batch : {n_parcelles} parcelle(s)")

        for table, cfg in audit_catalogue.items():
            geom_type = cfg.get("geom_type", "surfacique")
            if table == "reseaux_enedis_lineaires":
                continue
            if geom_type != "surfacique" and not include_lineaire:
                continue

            handler = cfg.get("handler")
            if handler == "servitudes":
                if "servitudes" not in existing:
                    logger.warning(f"  ⏭  {table:<35} table absente")
                    continue
                t0 = time.perf_counter()
                try:
                    rows = _batch_servitudes_layer(
                        conn, schema, batch_cte, params,
                        resolve_min_pct_sig(cfg),
                    )
                    for row in rows:
                        _register_hit(
                            layer_stats,
                            verbose_hits,
                            table=table,
                            cfg=catalogue.get(table, cfg),
                            section=str(row["section"]),
                            numero=str(row["numero"]),
                            pct_sig=float(row["pct_sig"]),
                            libelle=str(row.get("label") or ""),
                        )
                    elapsed = time.perf_counter() - t0
                    logger.info(
                        f"  ✅ {table:<35} {len(rows):>6} hit(s) | {elapsed:.1f}s"
                    )
                except Exception as exc:
                    logger.warning(f"  ⚠  {table:<35} {exc}")
                    errors.append({"couche": table, "error": str(exc)})
                continue

            if table not in existing:
                logger.warning(f"  ⏭  {table:<35} table absente")
                continue

            t0 = time.perf_counter()
            try:
                rows = _batch_catalogue_layer(
                    conn,
                    schema,
                    table,
                    cfg,
                    batch_cte,
                    params,
                    resolve_min_pct_sig(cfg),
                )
                for row in rows:
                    _register_hit(
                        layer_stats,
                        verbose_hits,
                        table=table,
                        cfg=catalogue.get(table, cfg),
                        section=str(row["section"]),
                        numero=str(row["numero"]),
                        pct_sig=float(row["pct_sig"]),
                        libelle=str(row.get("label") or "") or None,
                    )
                elapsed = time.perf_counter() - t0
                logger.info(f"  ✅ {table:<35} {len(rows):>6} hit(s) | {elapsed:.1f}s")
            except Exception as exc:
                logger.warning(f"  ⚠  {table:<35} {exc}")
                errors.append({"couche": table, "error": str(exc)})

    return n_parcelles, layer_stats, verbose_hits, errors


def _run_legacy_audit(
    engine,
    schema: str,
    catalogue: dict,
    audit_catalogue: dict,
    refs: list[dict[str, str]],
    *,
    include_lineaire: bool,
) -> tuple[int, dict[str, LayerStats], list[dict], list[dict]]:
    layer_stats: dict[str, LayerStats] = {}
    verbose_hits: list[dict] = []
    errors: list[dict] = []
    existing = _cache_existing_tables(engine, schema, catalogue)

    for i, ref in enumerate(refs, 1):
        parcel_ref = f"{ref['section']}:{ref['numero']}"
        logger.info(f"[{i}/{len(refs)}] {parcel_ref}")
        try:
            uf = build_uf([ref], engine=engine, schema=schema)
            for table, cfg in audit_catalogue.items():
                geom_type = cfg.get("geom_type", "surfacique")
                if table == "reseaux_enedis_lineaires":
                    continue
                if geom_type != "surfacique" and not include_lineaire:
                    continue

                if cfg.get("handler") == "servitudes":
                    if "servitudes" not in existing:
                        continue
                    from api.cuas.argeles.intersection_modules.servitudes import (
                        compute_servitudes_reglementation,
                    )
                    special = compute_servitudes_reglementation(
                        uf.wkt,
                        engine=engine,
                        schema=schema,
                        surface_sig=uf.surface_sig,
                        min_pct_sig=resolve_min_pct_sig(cfg),
                    )
                    for ent in special.get("servitudes") or []:
                        pct = ent.get("pct_sig")
                        if pct is None and ent.get("metric") and uf.surface_sig > 0:
                            pct = float(ent["metric"]) / uf.surface_sig * 100
                        _register_hit(
                            layer_stats,
                            verbose_hits,
                            table=table,
                            cfg=catalogue.get(table, cfg),
                            section=ref["section"],
                            numero=ref["numero"],
                            pct_sig=float(pct or 0),
                            libelle=str(ent.get("libelle") or ent.get("suptype") or ""),
                        )
                    continue

                if table not in existing:
                    continue
                objets, _, geom_type = calculate_intersection(
                    uf.wkt, table, cfg, uf.surface_sig, engine, schema,
                )
                for obj in objets:
                    if geom_type != "surfacique":
                        continue
                    pct = obj.get("pct_sig")
                    if pct is None:
                        continue
                    _register_hit(
                        layer_stats,
                        verbose_hits,
                        table=table,
                        cfg=catalogue.get(table, cfg),
                        section=ref["section"],
                        numero=ref["numero"],
                        pct_sig=float(pct),
                        libelle=_obj_label(obj, cfg),
                    )
        except Exception as exc:
            logger.warning(f"  ⚠  {parcel_ref} : {exc}")
            errors.append({"parcelle": parcel_ref, "error": str(exc)})

    return len(refs), layer_stats, verbose_hits, errors


def _print_summary(
    layer_stats: dict[str, LayerStats],
    *,
    n_parcelles: int,
    audit_raw: bool,
    elapsed_s: float,
) -> None:
    mode = "audit brut (min_pct_sig=0)" if audit_raw else "seuil catalogue"
    logger.info("")
    logger.info(f"{'═' * 88}")
    logger.info(
        f"  AUDIT SEUILS — {n_parcelles} parcelle(s) — {mode} — {elapsed_s:.1f}s"
    )
    logger.info(f"{'═' * 88}")
    logger.info(
        f"{'Couche':<32} {'Type':<11} {'Hits':>5} {'≤1%':>5} {'1-5%':>5} {'>5%':>5} "
        f"{'Parc.1-5%':>9} {'pct min':>8} {'pct max':>8}"
    )
    logger.info(f"{'─' * 88}")

    rows = sorted(
        layer_stats.items(),
        key=lambda item: (-item[1].buckets["entre_1_et_5"], -item[1].total_hits, item[0]),
    )
    for table, st in rows:
        if st.geom_type != "surfacique":
            continue
        parc_1_5 = f"{len(st.parcelles_avec_1_5)}/{n_parcelles}"
        pct_min = f"{st.pct_min:.2f}" if st.pct_min is not None else "—"
        pct_max = f"{st.pct_max:.2f}" if st.pct_max is not None else "—"
        logger.info(
            f"{table:<32} {st.geom_type:<11} {st.total_hits:>5} "
            f"{st.buckets['micro_le_1']:>5} {st.buckets['entre_1_et_5']:>5} "
            f"{st.buckets['gt_5']:>5} {parc_1_5:>9} {pct_min:>8} {pct_max:>8}"
        )

    total_1_5 = sum(st.buckets["entre_1_et_5"] for st in layer_stats.values())
    total_micro = sum(st.buckets["micro_le_1"] for st in layer_stats.values())
    logger.info(f"{'─' * 88}")
    logger.info(
        f"  Total hits ≤1 % : {total_micro}  |  Total hits 1-5 % : {total_1_5}"
    )
    logger.info(f"{'═' * 88}")


def _build_report(
    *,
    schema: str,
    catalogue_path: Path,
    n_parcelles: int,
    audit_raw: bool,
    seed: str | None,
    mode: str,
    elapsed_s: float,
    layer_stats: dict[str, LayerStats],
    verbose_hits: list[dict],
    errors: list[dict],
) -> dict[str, Any]:
    summary_layers = {}
    for table, st in sorted(layer_stats.items()):
        summary_layers[table] = {
            "nom": st.nom,
            "geom_type": st.geom_type,
            "total_hits": st.total_hits,
            "buckets": dict(st.buckets),
            "parcelles_avec_hit": len(st.parcelles_avec_hit),
            "parcelles_avec_hit_1_5_pct": len(st.parcelles_avec_1_5),
            "pct_sig_min": round(st.pct_min, 4) if st.pct_min is not None else None,
            "pct_sig_max": round(st.pct_max, 4) if st.pct_max is not None else None,
            "part_parcels_1_5_pct": (
                round(len(st.parcelles_avec_1_5) / n_parcelles * 100, 1)
                if n_parcelles > 0
                else 0.0
            ),
        }

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema": schema,
            "catalogue": str(catalogue_path),
            "n_parcelles": n_parcelles,
            "mode": mode,
            "audit_mode": "raw_min_pct_sig_0" if audit_raw else "catalogue_threshold",
            "seed": seed,
            "elapsed_s": round(elapsed_s, 2),
            "buckets": {
                "micro_le_1": "> 0 % et ≤ 1 %",
                "entre_1_et_5": "> 1 % et ≤ 5 %",
                "gt_5": "> 5 %",
            },
        },
        "summary_by_layer": summary_layers,
        "hits_entre_1_et_5_pct": verbose_hits,
        "errors": errors,
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Audit distribution pct_sig (mode batch par défaut).",
    )
    ap.add_argument("--schema", default=SCHEMA)
    ap.add_argument("--catalogue", default=str(DEFAULT_CATALOGUE))
    ap.add_argument("--sample", type=int, default=100, help="Parcelles (défaut 100).")
    ap.add_argument("--all", action="store_true", help="Toutes les parcelles valides.")
    ap.add_argument("--seed", default=None, help="Graine reproductible (ex. 42).")
    ap.add_argument("--use-catalogue-threshold", action="store_true")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--legacy", action="store_true", help="Mode lent parcelle×couche.")
    ap.add_argument("--include-lineaire", action="store_true")
    args = ap.parse_args()

    t0 = time.perf_counter()
    engine = get_engine()
    catalogue = load_catalogue(args.catalogue)
    audit_raw = not args.use_catalogue_threshold
    audit_catalogue = _prepare_catalogue(catalogue, audit_raw=audit_raw)

    sample = None if args.all else args.sample
    if args.all:
        logger.info("Audit batch — commune entière")

    if args.legacy:
        batch_cte, params = _parcelles_batch_cte(args.schema, sample=sample, seed=args.seed)
        sql = text(f"WITH {batch_cte} SELECT section, numero FROM parcelles_batch")
        with engine.connect() as conn:
            refs = [
                {"section": str(r["section"]), "numero": str(r["numero"])}
                for r in conn.execute(sql, params).mappings().all()
            ]
        logger.info(f"Mode legacy — {len(refs)} parcelle(s)")
        n_parcelles, layer_stats, verbose_hits, errors = _run_legacy_audit(
            engine,
            args.schema,
            catalogue,
            audit_catalogue,
            refs,
            include_lineaire=args.include_lineaire,
        )
        mode = "legacy"
    else:
        n_parcelles, layer_stats, verbose_hits, errors = run_batch_audit(
            engine,
            args.schema,
            catalogue,
            audit_catalogue,
            sample=sample,
            seed=args.seed,
            include_lineaire=args.include_lineaire,
        )
        mode = "batch"

    elapsed = time.perf_counter() - t0
    _print_summary(
        layer_stats,
        n_parcelles=n_parcelles,
        audit_raw=audit_raw,
        elapsed_s=elapsed,
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    report = _build_report(
        schema=args.schema,
        catalogue_path=Path(args.catalogue),
        n_parcelles=n_parcelles,
        audit_raw=audit_raw,
        seed=args.seed,
        mode=mode,
        elapsed_s=elapsed,
        layer_stats=layer_stats,
        verbose_hits=verbose_hits,
        errors=errors,
    )
    out_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    logger.info(f"Synthèse écrite : {out_path}")


if __name__ == "__main__":
    main()
