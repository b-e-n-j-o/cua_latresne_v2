#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit des seuils d'intersection surfacique (calibrage min_pct_sig).

Deux modes de comptage pour une couche (--couche) :

  Par parcelle (défaut avec --distribution fine) :
    ST_Union des intersections → 1 % par parcelle.
    Répond à : « la couche recouvre-t-elle bien le cadastre ? »
    Sorties : graphique fine, GeoJSON par parcelle.

  Par entité (--hits) :
    1 ligne = 1 intersection parcelle × objet SIG (polygone/zonage).
    Répond à : « les entités découpent-elles les parcelles en morceaux ? »
    Le log affiche hits ET parcelles uniques touchées.

Mode batch (défaut) : 1 requête SQL par couche × toutes les parcelles du lot
(modèle enrich_parcelles_resume) — adapté à 100 parcelles ou à la commune entière.

Mode legacy (--legacy) : 1 parcelle × N couches (lent, debug uniquement).

Usage (depuis cua_latresne_v4, venv + .env) :

  # Couverture cadastre — une couche, commune entière
  python api/cuas/argeles/tests/audit_seuils_intersections.py --all \
    --couche hauteurs --distribution fine \
    --out api/cuas/argeles/tests/output/audit_hauteurs_all.json

  # Découpage entités SIG sur le cadastre (hits, pas agrégation)
  python api/cuas/argeles/tests/audit_seuils_intersections.py --all \
    --couche zonage_plu --hits \
    --out api/cuas/argeles/tests/output/audit_zonage_plu_hits.json

  # DPU (infos_surf filtrée) — commune entière + courbe de distribution
  # NB : 2 libellés DPU en base ; --filtre-mode contains aligné sur builder.py
  python api/cuas/argeles/tests/audit_seuils_intersections.py --all \
    --couche infos_surf --filtre libelle --value préemption --filtre-mode contains \
    --out api/cuas/argeles/tests/output/audit_dpu_all.json
  # → génère aussi audit_dpu_all.png et audit_dpu_all.geojson (parcelles intersectées)
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

from api.cuas.argeles.db import GEOM_COL, SCHEMA, SRID, get_engine, logger
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

# Distribution fine (par parcelle) — activée avec --couche ou --distribution fine
FINE_BUCKETS: tuple[tuple[str, float, float | None, str], ...] = (
    ("zero", 0.0, 0.0, "0 %"),
    ("gt_0_le_1", 0.0, 1.0, "0-1 %"),
    ("entre_1_et_5", 1.0, 5.0, "1-5 %"),
    ("entre_5_et_10", 5.0, 10.0, "5-10 %"),
    ("entre_10_et_20", 10.0, 20.0, "10-20 %"),
    ("entre_20_et_50", 20.0, 50.0, "20-50 %"),
    ("entre_50_et_60", 50.0, 60.0, "50-60 %"),
    ("entre_60_et_70", 60.0, 70.0, "60-70 %"),
    ("entre_70_et_80", 70.0, 80.0, "70-80 %"),
    ("entre_80_et_90", 80.0, 90.0, "80-90 %"),
    ("entre_90_et_100", 90.0, 100.0, "90-100 %"),
    ("gt_100", 100.0, None, "> 100 %"),
)
FINE_BUCKET_KEYS = tuple(b[0] for b in FINE_BUCKETS)


def _bucket_pct(pct: float, *, fine: bool = False) -> str | None:
    if fine:
        return _bucket_pct_fine(pct)
    if pct <= 0:
        return None
    if pct <= 1.0:
        return "micro_le_1"
    if pct <= 5.0:
        return "entre_1_et_5"
    return "gt_5"


def _bucket_pct_fine(pct: float) -> str:
    if pct <= 0:
        return "zero"
    for key, low, high, _ in FINE_BUCKETS:
        if key == "zero":
            continue
        if high is None:
            if pct > low:
                return key
        elif pct > low and pct <= high:
            return key
    return "gt_100"


def _bucket_labels(*, fine: bool) -> dict[str, str]:
    if fine:
        return {key: label for key, _, _, label in FINE_BUCKETS}
    return {
        "micro_le_1": "> 0 % et ≤ 1 %",
        "entre_1_et_5": "> 1 % et ≤ 5 %",
        "gt_5": "> 5 %",
    }


def _empty_buckets(*, fine: bool) -> dict[str, int]:
    keys = FINE_BUCKET_KEYS if fine else BUCKETS
    return {k: 0 for k in keys}


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
    fine_buckets: bool = False
    total_hits: int = 0
    buckets: dict[str, int] = field(default_factory=dict)
    parcelles_avec_hit: set[str] = field(default_factory=set)
    parcelles_avec_1_5: set[str] = field(default_factory=set)
    parcelle_pct: dict[str, float] = field(default_factory=dict)
    pct_min: float | None = None
    pct_max: float | None = None

    def __post_init__(self) -> None:
        if not self.buckets:
            self.buckets = _empty_buckets(fine=self.fine_buckets)

    def register(
        self,
        parcel_ref: str,
        pct: float | None,
        *,
        per_parcel: bool = False,
    ) -> None:
        if self.fine_buckets and per_parcel:
            pct_val = float(pct or 0.0)
            self.parcelle_pct[parcel_ref] = pct_val
            bucket = _bucket_pct_fine(pct_val)
            self.buckets[bucket] = self.buckets.get(bucket, 0) + 1
            if pct_val > 0:
                self.parcelles_avec_hit.add(parcel_ref)
                self.pct_min = pct_val if self.pct_min is None else min(self.pct_min, pct_val)
                self.pct_max = pct_val if self.pct_max is None else max(self.pct_max, pct_val)
            if bucket == "entre_1_et_5":
                self.parcelles_avec_1_5.add(parcel_ref)
            return

        if pct is None or pct <= 0:
            return
        bucket = _bucket_pct(pct, fine=self.fine_buckets)
        if not bucket:
            return
        self.total_hits += 1
        self.buckets[bucket] += 1
        self.parcelles_avec_hit.add(parcel_ref)
        if bucket == "entre_1_et_5":
            self.parcelles_avec_1_5.add(parcel_ref)
        self.pct_min = pct if self.pct_min is None else min(self.pct_min, pct)
        self.pct_max = pct if self.pct_max is None else max(self.pct_max, pct)

    def finalize_parcel_distribution(self, all_parcel_refs: set[str]) -> None:
        """Complète la distribution fine avec les parcelles sans intersection (0 %)."""
        if not self.fine_buckets:
            return
        missing = all_parcel_refs - set(self.parcelle_pct)
        for parcel_ref in missing:
            self.parcelle_pct[parcel_ref] = 0.0
            self.buckets["zero"] = self.buckets.get("zero", 0) + 1


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
    fine_buckets: bool = False,
    per_parcel: bool = False,
) -> None:
    parcel_ref = f"{section}:{numero}"
    geom_type = cfg.get("geom_type", "surfacique")
    stats = layer_stats.setdefault(
        table,
        LayerStats(
            nom=cfg.get("nom") or table,
            geom_type=geom_type,
            fine_buckets=fine_buckets,
        ),
    )
    stats.register(parcel_ref, pct_sig, per_parcel=per_parcel)
    bucket = _bucket_pct_fine(pct_sig) if fine_buckets else _bucket_pct(pct_sig)
    if not fine_buckets and bucket == "entre_1_et_5":
        verbose_hits.append(
            {
                "parcelle": parcel_ref,
                "couche": table,
                "nom_couche": stats.nom,
                "pct_sig": round(float(pct_sig or 0), 4),
                "libelle": libelle or "",
                "bucket": bucket,
            }
        )
    elif fine_buckets and bucket in {
        "gt_0_le_1", "entre_1_et_5", "entre_5_et_10", "entre_10_et_20"
    }:
        verbose_hits.append(
            {
                "parcelle": parcel_ref,
                "couche": table,
                "nom_couche": stats.nom,
                "pct_sig": round(float(pct_sig or 0), 4),
                "libelle": libelle or "",
                "bucket": bucket,
            }
        )


def _build_filter_sql(
    filtre_col: str,
    filtre_value: str,
    filtre_mode: str,
) -> tuple[str, dict[str, Any]]:
    filter_col_id = _safe_ident(filtre_col)
    params: dict[str, Any] = {}
    mode = (filtre_mode or "exact").lower()

    if mode == "exact":
        params["filtre_value"] = str(filtre_value)
        return f"AND TRIM(z.{filter_col_id}) = :filtre_value", params

    if mode == "contains":
        needle = str(filtre_value).lower()
        params["filtre_pattern"] = f"%{needle}%"
        return (
            f"AND LOWER(TRIM(z.{filter_col_id})) LIKE :filtre_pattern",
            params,
        )

    if mode == "ilike":
        params["filtre_pattern"] = f"%{filtre_value}%"
        return f"AND TRIM(z.{filter_col_id}) ILIKE :filtre_pattern", params

    if mode == "in":
        values = [v.strip() for v in str(filtre_value).split("|") if v.strip()]
        if not values:
            raise ValueError("--value vide pour --filtre-mode in")
        placeholders = []
        for i, val in enumerate(values):
            key = f"filtre_in_{i}"
            placeholders.append(f":{key}")
            params[key] = val
        return f"AND TRIM(z.{filter_col_id}) IN ({', '.join(placeholders)})", params

    raise ValueError(f"Mode filtre inconnu : {filtre_mode!r}")


def _batch_catalogue_layer(
    conn,
    schema: str,
    table: str,
    cfg: dict,
    batch_cte: str,
    params: dict,
    min_pct_sig: float,
    *,
    filtre_col: str | None = None,
    filtre_value: str | None = None,
    filtre_mode: str = "exact",
    per_parcel: bool = False,
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

    filter_sql = ""
    qparams = {**params, "min_pct_sig": float(min_pct_sig)}
    if filtre_col and filtre_value is not None:
        filter_sql, filter_params = _build_filter_sql(
            filtre_col, filtre_value, filtre_mode,
        )
        qparams.update(filter_params)

    if per_parcel:
        sql = text(
            f"""
            WITH {batch_cte},
            inter AS (
                SELECT p.section,
                       p.numero,
                       p.surface_sig,
                       ST_Intersection(p.geom, ST_MakeValid(z.{geom_col})) AS inter_geom
                FROM parcelles_batch p
                INNER JOIN {schema_id}.{table_id} z
                    ON z.{geom_col} IS NOT NULL
                   AND p.geom && z.{geom_col}
                   AND ST_Intersects(p.geom, z.{geom_col})
                   {filter_sql}
            ),
            inter_valid AS (
                SELECT section,
                       numero,
                       surface_sig,
                       inter_geom
                FROM inter
                WHERE ST_Area(inter_geom) > {MIN_INTERSECTION_AREA_M2}
            ),
            agg AS (
                SELECT section,
                       numero,
                       surface_sig,
                       ST_Area(ST_Union(inter_geom)) AS inter_area
                FROM inter_valid
                GROUP BY section, numero, surface_sig
            )
            SELECT section,
                   numero,
                   NULL::text AS label,
                   ROUND((inter_area / surface_sig * 100)::numeric, 4) AS pct_sig
            FROM agg
            WHERE (
                :min_pct_sig <= 0
                OR surface_sig <= 0
                OR (inter_area / surface_sig * 100) > :min_pct_sig
            )
            """
        )
        return [dict(r) for r in conn.execute(sql, qparams).mappings().all()]

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
               {filter_sql}
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


def _filter_catalogue(
    catalogue: dict,
    *,
    couche: str | None,
) -> dict:
    if not couche:
        return catalogue
    if couche not in catalogue:
        raise SystemExit(f"Couche inconnue dans le catalogue : {couche!r}")
    return {couche: catalogue[couche]}


def _fetch_parcel_refs(
    conn,
    batch_cte: str,
    params: dict,
) -> set[str]:
    sql = text(f"WITH {batch_cte} SELECT section, numero FROM parcelles_batch")
    refs = {
        f"{r['section']}:{r['numero']}"
        for r in conn.execute(sql, params).mappings().all()
    }
    return refs


def run_batch_audit(
    engine,
    schema: str,
    catalogue: dict,
    audit_catalogue: dict,
    *,
    sample: int | None,
    seed: str | None,
    include_lineaire: bool,
    couche: str | None = None,
    filtre_col: str | None = None,
    filtre_value: str | None = None,
    filtre_mode: str = "exact",
    fine_buckets: bool = False,
    per_parcel: bool = False,
) -> tuple[int, dict[str, LayerStats], list[dict], list[dict]]:
    layer_stats: dict[str, LayerStats] = {}
    verbose_hits: list[dict] = []
    errors: list[dict] = []
    existing = _cache_existing_tables(engine, schema, catalogue)
    audit_catalogue = _filter_catalogue(audit_catalogue, couche=couche)

    if filtre_col and filtre_value is None:
        raise ValueError("--filtre requiert --value")
    if filtre_value is not None and not filtre_col:
        raise ValueError("--value requiert --filtre")
    if per_parcel and not couche:
        raise ValueError("Mode par parcelle requiert --couche")

    batch_cte, params = _parcelles_batch_cte(schema, sample=sample, seed=seed)

    with engine.connect() as conn:
        n_parcelles = _count_parcelles_batch(conn, batch_cte, params)
        logger.info(f"Lot batch : {n_parcelles} parcelle(s)")
        all_parcel_refs = _fetch_parcel_refs(conn, batch_cte, params)

        if filtre_col and filtre_value is not None:
            logger.info(
                f"Filtre couche : {filtre_col} {filtre_mode} {filtre_value!r}"
            )
        elif per_parcel:
            logger.info(
                "Mode comptage : par parcelle (ST_Union — couverture cadastre globale)"
            )
        elif couche:
            logger.info(
                "Mode comptage : par entité SIG (1 hit = 1 intersection parcelle×objet)"
            )

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
                    filtre_col=filtre_col,
                    filtre_value=filtre_value,
                    filtre_mode=filtre_mode,
                    per_parcel=per_parcel,
                )
                hit_rows: dict[str, float] = {}
                for row in rows:
                    parcel_ref = f"{row['section']}:{row['numero']}"
                    pct = float(row["pct_sig"])
                    if per_parcel:
                        hit_rows[parcel_ref] = max(hit_rows.get(parcel_ref, 0.0), pct)
                    else:
                        _register_hit(
                            layer_stats,
                            verbose_hits,
                            table=table,
                            cfg=catalogue.get(table, cfg),
                            section=str(row["section"]),
                            numero=str(row["numero"]),
                            pct_sig=pct,
                            libelle=str(row.get("label") or "") or None,
                            fine_buckets=fine_buckets,
                        )
                if per_parcel:
                    layer_cfg = catalogue.get(table, cfg)
                    lib = str(
                        filtre_value
                        or layer_cfg.get("nom")
                        or table
                    )
                    for parcel_ref, pct in hit_rows.items():
                        section, numero = parcel_ref.split(":", 1)
                        _register_hit(
                            layer_stats,
                            verbose_hits,
                            table=table,
                            cfg=layer_cfg,
                            section=section,
                            numero=numero,
                            pct_sig=pct,
                            libelle=lib,
                            fine_buckets=fine_buckets,
                            per_parcel=True,
                        )
                    st = layer_stats.get(table)
                    if st is not None:
                        st.finalize_parcel_distribution(all_parcel_refs)
                elapsed = time.perf_counter() - t0
                n_reported = len(hit_rows) if per_parcel else len(rows)
                unit = "parcelle(s)" if per_parcel else "hit(s)"
                logger.info(
                    f"  ✅ {table:<35} {n_reported:>6} {unit} | {elapsed:.1f}s"
                )
                if not per_parcel and rows:
                    n_unique = len({
                        f"{row['section']}:{row['numero']}" for row in rows
                    })
                    logger.info(
                        f"      ↳ {n_unique:>6} parcelle(s) unique(s) touchée(s)"
                    )
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


def _fine_distribution_payload(
    table: str,
    st: LayerStats,
    *,
    n_parcelles: int,
    comptage: str,
) -> dict[str, Any]:
    labels = _bucket_labels(fine=True)
    distribution: dict[str, dict[str, Any]] = {}
    for key in FINE_BUCKET_KEYS:
        count = st.buckets.get(key, 0)
        if count <= 0:
            continue
        distribution[labels[key]] = {
            "parcelles": count,
            "part_du_lot_pct": (
                round(count / n_parcelles * 100, 2) if n_parcelles > 0 else 0.0
            ),
        }
    return {
        "couche": table,
        "nom": st.nom,
        "comptage": comptage,
        "parcelles_lot": n_parcelles,
        "parcelles_concernees": len(st.parcelles_avec_hit),
        "pct_sig_min": round(st.pct_min, 4) if st.pct_min is not None else None,
        "pct_sig_max": round(st.pct_max, 4) if st.pct_max is not None else None,
        "distribution": distribution,
    }


def _log_json_block(payload: dict[str, Any], *, title: str | None = None) -> None:
    if title:
        logger.info(title)
    for line in json.dumps(payload, indent=2, ensure_ascii=False).splitlines():
        logger.info(line)


def _print_summary(
    layer_stats: dict[str, LayerStats],
    *,
    n_parcelles: int,
    audit_raw: bool,
    elapsed_s: float,
    fine_buckets: bool = False,
) -> None:
    mode = "audit brut (min_pct_sig=0)" if audit_raw else "seuil catalogue"
    logger.info("")
    logger.info(f"{'═' * 88}")
    logger.info(
        f"  AUDIT SEUILS — {n_parcelles} parcelle(s) — {mode} — {elapsed_s:.1f}s"
    )
    logger.info(f"{'═' * 88}")

    if fine_buckets:
        comptage = (
            "par parcelle"
            if any(st.parcelle_pct for st in layer_stats.values())
            else "par entité SIG"
        )
        logger.info(f"  Distribution fine ({comptage}) :")
        logger.info("")
        for table, st in sorted(layer_stats.items()):
            payload = _fine_distribution_payload(
                table, st, n_parcelles=n_parcelles, comptage=comptage,
            )
            _log_json_block(payload, title=f"  ── {table} ──")
            logger.info("")
        logger.info(f"{'═' * 88}")
        return

    logger.info(
        f"{'Couche':<32} {'Type':<11} {'Hits':>5} {'≤1%':>5} {'1-5%':>5} {'>5%':>5} "
        f"{'Parc.1-5%':>9} {'pct min':>8} {'pct max':>8}"
    )
    logger.info(f"{'─' * 88}")

    rows = sorted(
        layer_stats.items(),
        key=lambda item: (-item[1].buckets.get("entre_1_et_5", 0), -item[1].total_hits, item[0]),
    )
    for table, st in rows:
        if st.geom_type != "surfacique":
            continue
        parc_1_5 = f"{len(st.parcelles_avec_1_5)}/{n_parcelles}"
        pct_min = f"{st.pct_min:.2f}" if st.pct_min is not None else "—"
        pct_max = f"{st.pct_max:.2f}" if st.pct_max is not None else "—"
        logger.info(
            f"{table:<32} {st.geom_type:<11} {st.total_hits:>5} "
            f"{st.buckets.get('micro_le_1', 0):>5} {st.buckets.get('entre_1_et_5', 0):>5} "
            f"{st.buckets.get('gt_5', 0):>5} {parc_1_5:>9} {pct_min:>8} {pct_max:>8}"
        )

    total_1_5 = sum(st.buckets.get("entre_1_et_5", 0) for st in layer_stats.values())
    total_micro = sum(st.buckets.get("micro_le_1", 0) for st in layer_stats.values())
    logger.info(f"{'─' * 88}")
    logger.info(
        f"  Total hits ≤1 % : {total_micro}  |  Total hits 1-5 % : {total_1_5}"
    )
    logger.info(f"{'═' * 88}")


def _plot_distribution(
    layer_stats: dict[str, LayerStats],
    *,
    out_path: Path,
    n_parcelles: int,
    title: str,
) -> Path | None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("matplotlib indisponible — courbe non générée")
        return None

    if not layer_stats:
        return None

    table, st = next(iter(layer_stats.items()))
    labels_map = _bucket_labels(fine=True)
    keys = list(FINE_BUCKET_KEYS)
    counts = [st.buckets.get(k, 0) for k in keys]
    x_labels = [labels_map[k] for k in keys]
    cumulative = []
    running = 0
    for c in counts:
        running += c
        cumulative.append(running / n_parcelles * 100 if n_parcelles else 0.0)

    fig, ax1 = plt.subplots(figsize=(max(14, len(keys) * 1.1), 6))
    bar_colors = []
    high_keys = {
        "entre_50_et_60", "entre_60_et_70", "entre_70_et_80",
        "entre_80_et_90", "entre_90_et_100", "gt_100",
    }
    for k in keys:
        if k == "zero":
            bar_colors.append("#94A3B8")
        elif k in high_keys:
            bar_colors.append("#7C3AED")
        else:
            bar_colors.append("#4F46E5")
    bars = ax1.bar(range(len(keys)), counts, color=bar_colors, alpha=0.9, label="Parcelles")
    ax1.set_xticks(range(len(keys)))
    ax1.set_xticklabels(x_labels, rotation=35, ha="right", fontsize=8)
    ax1.set_ylabel("Nombre de parcelles")
    ax1.set_title(title, fontsize=12, fontweight="bold")
    ax1.grid(axis="y", alpha=0.25)

    ax2 = ax1.twinx()
    ax2.plot(
        range(len(keys)),
        cumulative,
        color="#DC2626",
        marker="o",
        linewidth=2,
        label="Cumul % parcelles",
    )
    ax2.set_ylabel("Cumul (% du lot)")
    ax2.set_ylim(0, 105)

    for i, (c, cum) in enumerate(zip(counts, cumulative)):
        if c > 0:
            ax1.text(i, c, str(c), ha="center", va="bottom", fontsize=7)
        if c > 0 or i == 0:
            ax2.text(i, cum, f"{cum:.1f}%", ha="center", va="bottom", fontsize=6, color="#DC2626")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Courbe écrite : {out_path} ({table})")
    return out_path


def _split_parcel_ref(ref: str) -> tuple[str, str]:
    section, _, numero = ref.partition(":")
    return section, numero


def _export_intersected_parcelles_geojson(
    engine,
    schema: str,
    layer_stats: dict[str, LayerStats],
    *,
    out_path: Path,
    couche: str | None = None,
    filtre_col: str | None = None,
    filtre_value: str | None = None,
    filtre_mode: str | None = None,
) -> Path | None:
    """Exporte les parcelles avec intersection > 0 % (y compris 0-1 %)."""
    if not layer_stats:
        return None

    table, st = next(iter(layer_stats.items()))
    hits = {
        ref: pct for ref, pct in st.parcelle_pct.items() if float(pct) > 0
    }
    if not hits:
        logger.warning("Aucune parcelle intersectée — GeoJSON non généré")
        return None

    schema_id = _safe_ident(schema)
    geom_col = _safe_ident(GEOM_COL)
    labels = _bucket_labels(fine=True)
    sections = []
    numeros = []
    pcts = []
    for ref, pct in sorted(hits.items()):
        section, numero = _split_parcel_ref(ref)
        sections.append(section)
        numeros.append(numero)
        pcts.append(float(pct))

    sql = text(
        f"""
        WITH hits AS (
            SELECT h.section,
                   h.numero,
                   h.pct_sig
            FROM unnest(
                CAST(:sections AS text[]),
                CAST(:numeros AS text[]),
                CAST(:pcts AS double precision[])
            ) AS h(section, numero, pct_sig)
        )
        SELECT p.section,
               p.numero,
               h.pct_sig,
               ST_AsGeoJSON(ST_MakeValid(p.{geom_col}))::json AS geometry
        FROM {schema_id}.parcelles p
        INNER JOIN hits h
            ON trim(p.section) = trim(h.section)
           AND trim(p.numero) = trim(h.numero)
        WHERE p.{geom_col} IS NOT NULL
        """
    )

    with engine.connect() as conn:
        rows = list(
            conn.execute(
                sql,
                {"sections": sections, "numeros": numeros, "pcts": pcts},
            ).mappings().all()
        )

    features: list[dict[str, Any]] = []
    for row in rows:
        pct = float(row["pct_sig"])
        bucket = _bucket_pct_fine(pct)
        section = str(row["section"]).strip()
        numero = str(row["numero"]).strip()
        features.append(
            {
                "type": "Feature",
                "geometry": row["geometry"],
                "properties": {
                    "parcelle": f"{section}:{numero}",
                    "section": section,
                    "numero": numero,
                    "pct_sig": round(pct, 4),
                    "fourchette_pct": labels.get(bucket, bucket),
                    "bucket": bucket,
                    "couche": couche or table,
                },
            }
        )

    collection: dict[str, Any] = {
        "type": "FeatureCollection",
        "name": out_path.stem,
        "crs": {
            "type": "name",
            "properties": {"name": f"EPSG:{SRID}"},
        },
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema": schema,
            "couche": couche or table,
            "n_features": len(features),
            "filtre": (
                {"colonne": filtre_col, "value": filtre_value, "mode": filtre_mode}
                if filtre_col
                else None
            ),
        },
        "features": features,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(collection, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"GeoJSON écrit : {out_path} ({len(features)} parcelle(s))")
    return out_path


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
    couche: str | None = None,
    filtre_col: str | None = None,
    filtre_value: str | None = None,
    filtre_mode: str = "exact",
    fine_buckets: bool = False,
    per_parcel_mode: bool = False,
    plot_path: str | None = None,
    geojson_path: str | None = None,
) -> dict[str, Any]:
    bucket_labels = _bucket_labels(fine=fine_buckets)
    summary_layers = {}
    for table, st in sorted(layer_stats.items()):
        entry: dict[str, Any] = {
            "nom": st.nom,
            "geom_type": st.geom_type,
            "total_hits": st.total_hits,
            "buckets": dict(st.buckets),
            "bucket_labels": {k: bucket_labels[k] for k in st.buckets},
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
        if fine_buckets and st.parcelle_pct:
            entry["parcelles_sans_intersection"] = st.buckets.get("zero", 0)
            entry["part_parcels_concernees"] = (
                round(len(st.parcelles_avec_hit) / n_parcelles * 100, 1)
                if n_parcelles > 0
                else 0.0
            )
        summary_layers[table] = entry

    meta: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema": schema,
        "catalogue": str(catalogue_path),
        "n_parcelles": n_parcelles,
        "mode": mode,
        "audit_mode": "raw_min_pct_sig_0" if audit_raw else "catalogue_threshold",
        "seed": seed,
        "elapsed_s": round(elapsed_s, 2),
        "distribution": "fine" if fine_buckets else "standard",
        "comptage": "parcelle" if per_parcel_mode else ("entite" if couche else "hit"),
        "buckets": bucket_labels,
    }
    if couche:
        meta["couche"] = couche
    if filtre_col:
        meta["filtre"] = {
            "colonne": filtre_col,
            "value": filtre_value,
            "mode": filtre_mode,
        }
    if plot_path:
        meta["plot"] = plot_path
    if geojson_path:
        meta["geojson"] = geojson_path

    hits_key = "hits_detail" if fine_buckets else "hits_entre_1_et_5_pct"

    return {
        "meta": meta,
        "summary_by_layer": summary_layers,
        hits_key: verbose_hits,
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
    ap.add_argument(
        "--couche",
        default=None,
        help="Une seule couche catalogue (ex. infos_surf, zonage_plu).",
    )
    ap.add_argument(
        "--filtre",
        default=None,
        help="Colonne SQL de filtre sur la couche (ex. libelle). Requiert --value.",
    )
    ap.add_argument(
        "--value",
        default=None,
        help="Valeur du filtre (exacte, sous-chaîne, ou A|B pour --filtre-mode in).",
    )
    ap.add_argument(
        "--filtre-mode",
        choices=("exact", "contains", "ilike", "in"),
        default="exact",
        help="exact (défaut), contains (sous-chaîne insensible à la casse), "
        "ilike (motif SQL %%), in (valeurs séparées par |).",
    )
    ap.add_argument(
        "--distribution",
        choices=("standard", "fine"),
        default=None,
        help="Buckets de distribution (fine auto en mode par parcelle).",
    )
    ap.add_argument(
        "--per-parcel",
        action="store_true",
        help="Force l'agrégation ST_Union par parcelle (couverture cadastre).",
    )
    ap.add_argument(
        "--hits",
        action="store_true",
        help="Mode entités : 1 comptage par intersection parcelle×objet SIG "
        "(détecte les découpages de zonage). Exclut l'agrégation par parcelle.",
    )
    ap.add_argument(
        "--plot",
        nargs="?",
        const="auto",
        default=None,
        help="PNG de la courbe (chemin ou auto à côté de --out).",
    )
    ap.add_argument("--no-plot", action="store_true", help="Désactive le graphique.")
    ap.add_argument(
        "--geojson",
        nargs="?",
        const="auto",
        default=None,
        help="GeoJSON des parcelles intersectées (> 0 %), chemin ou auto à côté de --out.",
    )
    ap.add_argument("--no-geojson", action="store_true", help="Désactive l'export GeoJSON.")
    args = ap.parse_args()

    if args.filtre and args.value is None:
        ap.error("--filtre requiert --value")
    if args.value is not None and not args.filtre:
        ap.error("--value requiert --filtre")
    if (args.filtre or args.value) and not args.couche:
        ap.error("--filtre/--value requiert --couche")
    if args.per_parcel and not args.couche:
        ap.error("--per-parcel requiert --couche")
    if args.hits and args.per_parcel:
        ap.error("--hits et --per-parcel sont incompatibles")

    t0 = time.perf_counter()
    engine = get_engine()
    catalogue = load_catalogue(args.catalogue)
    audit_raw = not args.use_catalogue_threshold
    audit_catalogue = _prepare_catalogue(catalogue, audit_raw=audit_raw)

    per_parcel_filter = bool(args.filtre and args.value is not None)
    per_parcel_mode = (
        not args.hits
        and (
            args.per_parcel
            or per_parcel_filter
            or (args.couche and args.distribution == "fine")
            or (args.couche and args.geojson is not None)
        )
    )
    fine_buckets = args.distribution == "fine" or per_parcel_mode

    if args.hits and args.distribution == "fine":
        logger.warning(
            "Mode --hits : la distribution fine compte des entités SIG, "
            "pas des parcelles uniques — préférer le mode standard ou par parcelle."
        )
    if args.hits and args.geojson is not None:
        logger.warning("--hits : GeoJSON ignoré (nécessite l'agrégation par parcelle).")

    if per_parcel_filter and args.use_catalogue_threshold:
        logger.warning(
            "Mode filtré : seuil catalogue ignoré (distribution brute min_pct_sig=0)."
        )
        audit_raw = True
        audit_catalogue = _prepare_catalogue(catalogue, audit_raw=True)

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
            couche=args.couche,
            filtre_col=args.filtre,
            filtre_value=args.value,
            filtre_mode=args.filtre_mode,
            fine_buckets=fine_buckets,
            per_parcel=per_parcel_mode,
        )
        mode = "batch"

    elapsed = time.perf_counter() - t0
    _print_summary(
        layer_stats,
        n_parcelles=n_parcelles,
        audit_raw=audit_raw,
        elapsed_s=elapsed,
        fine_buckets=fine_buckets,
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    plot_path: Path | None = None
    want_plot = fine_buckets and not args.no_plot and (
        args.plot is not None or per_parcel_mode
    )
    if want_plot:
        if args.plot and args.plot != "auto":
            plot_path = Path(args.plot)
        else:
            plot_path = out_path.with_suffix(".png")
        title_parts = ["Distribution couverture surfacique"]
        if args.couche:
            title_parts.append(args.couche)
        if args.filtre and args.value:
            title_parts.append(f"{args.filtre}={args.value}")
        title_parts.append(f"({n_parcelles} parcelles)")
        _plot_distribution(
            layer_stats,
            out_path=plot_path,
            n_parcelles=n_parcelles,
            title=" — ".join(title_parts),
        )

    geojson_path: Path | None = None
    want_geojson = (
        per_parcel_mode
        and not args.no_geojson
        and (args.geojson is not None or args.couche)
    )
    if want_geojson:
        if args.geojson and args.geojson != "auto":
            geojson_path = Path(args.geojson)
        else:
            geojson_path = out_path.with_suffix(".geojson")
        _export_intersected_parcelles_geojson(
            engine,
            args.schema,
            layer_stats,
            out_path=geojson_path,
            couche=args.couche,
            filtre_col=args.filtre,
            filtre_value=args.value,
            filtre_mode=args.filtre_mode,
        )

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
        couche=args.couche,
        filtre_col=args.filtre,
        filtre_value=args.value,
        filtre_mode=args.filtre_mode,
        fine_buckets=fine_buckets,
        per_parcel_mode=per_parcel_mode,
        plot_path=str(plot_path) if plot_path else None,
        geojson_path=str(geojson_path) if geojson_path else None,
    )
    out_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    logger.info(f"Synthèse écrite : {out_path}")


if __name__ == "__main__":
    main()
