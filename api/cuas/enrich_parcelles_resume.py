#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichissement batch des parcelles — couches « résumé » (statiques).

1 requête SQL set-based par couche du catalogue (zonage, hauteurs, servitudes,
PPR, PPRIF) → une colonne JSONB par couche (ex. sig_hauteurs, sig_zonage_plu).

Chaque couche run écrit toutes les parcelles du batch dans sa colonne dédiée :
  - status "concernee"   : au moins une intersection significative
  - status "non_concernee" : couche évaluée, aucune intersection

Usage (depuis api/cuas/, venv + .env Supabase) :

  # Dry-run : les 5 couches, toutes les parcelles
  python enrich_parcelles_resume.py --dry-run

  # Test sur 500 parcelles
  python enrich_parcelles_resume.py --limit 500 --dry-run

  # Une ou plusieurs couches seulement
  python enrich_parcelles_resume.py --only zonage_plu,hauteurs --dry-run

  # Écriture en base
  python enrich_parcelles_resume.py --apply
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from db import GEOM_COL, SCHEMA, get_engine, logger
from sig_resume_layers import layer_column

DEFAULT_CATALOGUE = Path(__file__).with_name("catalogue_parcelle_resume_argeles.json")
MIN_INTERSECTION_AREA_M2 = 0.01  # bruit numérique géométrique
MIN_INTERSECTION_PCT = 1.0  # ignore les micro-recouvrements (< 1 % parcelle)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_catalogue(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _progress(msg: str, *args) -> None:
    """Log + flush immédiat (visible pendant les requêtes SQL longues)."""
    logger.info(msg, *args)
    sys.stdout.flush()


@dataclass
class LayerResult:
    table: str
    batch_n: int
    hit_n: int
    total_objets: int
    elapsed_s: float
    updated_n: int = 0


def _parse_refs(raw: str) -> list[tuple[str, str]]:
    refs: list[tuple[str, str]] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        section, _, numero = tok.partition(":")
        if not numero:
            raise ValueError(f"Référence invalide {tok!r} (format SECTION:NUMERO)")
        refs.append((section.strip(), numero.strip()))
    return refs


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


def _table_has_column(engine, schema: str, table: str, column: str) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = :schema
                          AND table_name = :table
                          AND column_name = :column
                    )
                    """
                ),
                {"schema": schema, "table": table, "column": column},
            ).scalar()
        )


def _idu_select(engine, schema: str) -> str:
    if _table_has_column(engine, schema, "parcelles", "idu"):
        return "idu"
    return "NULL::text AS idu"


def count_parcelles(
    engine,
    schema: str,
    *,
    limit: int | None = None,
    refs: list[tuple[str, str]] | None = None,
) -> int:
    params: dict = {}
    refs_filter = ""
    if refs:
        clauses = []
        for i, (s, n) in enumerate(refs):
            clauses.append(f"(section = :s{i} AND numero = :n{i})")
            params[f"s{i}"], params[f"n{i}"] = s, n
        refs_filter = f" AND ({' OR '.join(clauses)})"
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    sql = f"""
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM {_safe_ident(schema)}.parcelles
            WHERE geom_2154 IS NOT NULL
              AND NOT ST_IsEmpty(geom_2154)
              {refs_filter}
            ORDER BY section, numero
            {limit_clause}
        ) sub
    """
    with engine.connect() as conn:
        return int(conn.execute(text(sql), params).scalar() or 0)


def ensure_layer_columns(engine, schema: str, catalogue: dict) -> None:
    """Ajoute une colonne JSONB par couche du catalogue (sig_<couche>)."""
    for table, cfg in catalogue.items():
        col = _safe_ident(layer_column(table, cfg))
        if _table_has_column(engine, schema, "parcelles", col):
            continue
        _progress("Ajout colonne %s sur %s.parcelles…", col, schema)
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    ALTER TABLE {_safe_ident(schema)}.parcelles
                    ADD COLUMN IF NOT EXISTS {col} jsonb
                    """
                )
            )
        _progress("Colonne %s ajoutée.", col)


def _parcelles_batch_cte(
    engine,
    schema: str,
    limit: int | None,
    refs: list[tuple[str, str]] | None,
    *,
    offset: int = 0,
    chunk_size: int | None = None,
) -> tuple[str, dict]:
    params: dict = {"offset": int(offset)}
    refs_filter = ""
    if refs:
        clauses = []
        for i, (s, n) in enumerate(refs):
            clauses.append(f"(section = :s{i} AND numero = :n{i})")
            params[f"s{i}"], params[f"n{i}"] = s, n
        refs_filter = f" AND ({' OR '.join(clauses)})"

    if chunk_size:
        limit_clause = f"LIMIT {int(chunk_size)} OFFSET :offset"
    elif limit:
        limit_clause = f"LIMIT {int(limit)}"
    else:
        limit_clause = ""
    idu_sel = _idu_select(engine, schema)
    sql = f"""
        parcelles_batch AS (
            SELECT section,
                   numero,
                   {idu_sel},
                   contenance,
                   ST_MakeValid(geom_2154) AS geom,
                   GREATEST(ST_Area(ST_MakeValid(geom_2154)), 0.01) AS surface_sig
            FROM {_safe_ident(schema)}.parcelles
            WHERE geom_2154 IS NOT NULL
              AND NOT ST_IsEmpty(geom_2154)
              {refs_filter}
            ORDER BY section, numero
            {limit_clause}
        )
    """
    return sql, params


def _intersection_ctes(schema: str, table: str, cfg: dict, batch_cte: str) -> str:
    table_id = _safe_ident(table)
    geom_col = _safe_ident(cfg.get("geom_col", GEOM_COL))
    keep = [_safe_ident(k) for k in cfg.get("keep", [])]
    if not keep:
        raise ValueError(f"Couche {table} : attributs 'keep' vides")

    obj_fields = ",\n".join(f"'{k}', i.{k}" for k in keep)
    obj_fields += ",\n'surface_inter_m2', ROUND(i.inter_area::numeric, 2)"
    obj_fields += ",\n'pct_sig', ROUND((i.inter_area / i.surface_sig * 100)::numeric, 4)"

    nom = cfg.get("nom", table).replace("'", "''")
    layer_type = (cfg.get("type") or "").replace("'", "''")

    return f"""
        WITH {batch_cte},
        inter AS (
            SELECT p.section, p.numero, p.idu, p.contenance, p.surface_sig,
                   {", ".join(f"z.{k}" for k in keep)},
                   ST_Intersection(p.geom, ST_MakeValid(z.{geom_col})) AS inter_geom
            FROM parcelles_batch p
            INNER JOIN {_safe_ident(schema)}.{table_id} z
                ON z.{geom_col} IS NOT NULL
               AND p.geom && z.{geom_col}
               AND ST_Intersects(p.geom, z.{geom_col})
        ),
        inter_ok AS (
            SELECT *, ST_Area(inter_geom) AS inter_area
            FROM inter
            WHERE ST_Area(inter_geom) > {MIN_INTERSECTION_AREA_M2}
              AND (ST_Area(inter_geom) / surface_sig * 100) >= {MIN_INTERSECTION_PCT}
        ),
        totals AS (
            SELECT section, numero,
                   COALESCE(ST_Area(ST_Union(inter_geom)), 0.0) AS total_area
            FROM inter_ok
            GROUP BY section, numero
        ),
        objets AS (
            SELECT i.section, i.numero,
                   jsonb_agg(
                       jsonb_build_object({obj_fields})
                       ORDER BY i.inter_area DESC
                   ) AS objets_json
            FROM inter_ok i
            GROUP BY i.section, i.numero
        ),
        layer AS (
            SELECT p.section, p.numero, p.idu, p.contenance, p.surface_sig,
                   CASE
                       WHEN o.objets_json IS NOT NULL THEN
                           jsonb_build_object(
                               'nom', '{nom}',
                               'type', '{layer_type}',
                               'geom_type', 'surfacique',
                               'status', 'concernee',
                               'pct_sig', ROUND((t.total_area / p.surface_sig * 100)::numeric, 4),
                               'objets', o.objets_json
                           )
                       ELSE
                           jsonb_build_object(
                               'nom', '{nom}',
                               'type', '{layer_type}',
                               'geom_type', 'surfacique',
                               'status', 'non_concernee',
                               'pct_sig', 0,
                               'objets', '[]'::jsonb
                           )
                   END AS layer_json
            FROM parcelles_batch p
            LEFT JOIN objets o USING (section, numero)
            LEFT JOIN totals t USING (section, numero)
        )
    """


def _build_stats_sql(
    engine,
    schema: str,
    table: str,
    cfg: dict,
    *,
    limit: int | None,
    refs: list[tuple[str, str]] | None,
    offset: int = 0,
    chunk_size: int | None = None,
) -> tuple[str, dict]:
    batch_cte, params = _parcelles_batch_cte(
        engine, schema, limit, refs, offset=offset, chunk_size=chunk_size
    )
    body = _intersection_ctes(schema, table, cfg, batch_cte)
    sql = f"""
        {body}
        SELECT
            (SELECT COUNT(*) FROM parcelles_batch) AS parcelles_batch,
            COUNT(*) FILTER (WHERE layer_json->>'status' = 'concernee')
                AS parcelles_avec_intersection,
            COALESCE(
                SUM(jsonb_array_length(layer_json->'objets'))
                    FILTER (WHERE layer_json->>'status' = 'concernee'),
                0
            ) AS total_objets
        FROM layer
    """
    return sql, params


def _build_apply_sql(
    engine,
    schema: str,
    table: str,
    cfg: dict,
    *,
    limit: int | None,
    refs: list[tuple[str, str]] | None,
    offset: int = 0,
    chunk_size: int | None = None,
) -> tuple[str, dict]:
    col = _safe_ident(layer_column(table, cfg))
    batch_cte, params = _parcelles_batch_cte(
        engine, schema, limit, refs, offset=offset, chunk_size=chunk_size
    )
    computed_at = datetime.now(timezone.utc).isoformat()

    body = _intersection_ctes(schema, table, cfg, batch_cte)
    sql = f"""
        {body},
        payload AS (
            SELECT l.section, l.numero,
                   jsonb_set(
                       l.layer_json,
                       '{{computed_at}}',
                       to_jsonb('{computed_at}'::text),
                       true
                   ) AS layer_payload
            FROM layer l
        ),
        upd AS (
            UPDATE {_safe_ident(schema)}.parcelles p
            SET {col} = src.layer_payload
            FROM payload src
            WHERE p.section = src.section AND p.numero = src.numero
            RETURNING 1
        )
        SELECT
            (SELECT COUNT(*) FROM parcelles_batch) AS parcelles_batch,
            (SELECT COUNT(*) FROM layer WHERE layer_json->>'status' = 'concernee')
                AS parcelles_avec_intersection,
            (SELECT COALESCE(SUM(jsonb_array_length(layer_json->'objets')), 0)
             FROM layer WHERE layer_json->>'status' = 'concernee') AS total_objets,
            (SELECT COUNT(*) FROM upd) AS updated_n
    """
    return sql, params


def _resolve_chunk_size(cfg: dict, default_chunk: int) -> int | None:
    """chunk_size dans le catalogue > --chunk-size global > pas de découpage."""
    if cfg.get("chunk_size"):
        return int(cfg["chunk_size"])
    if default_chunk > 0:
        return default_chunk
    return None


def _execute_layer_sql(
    engine,
    sql: str,
    params: dict,
    *,
    apply: bool,
    statement_timeout_s: int,
) -> tuple[int, int, int]:
    """Exécute une requête couche (ou un chunk). Retourne (batch, concernées, objets|updates)."""
    _progress("  → requête SQL en cours… (Ctrl+C pour annuler)")
    with engine.begin() as conn:
        if statement_timeout_s > 0:
            conn.execute(text(f"SET LOCAL statement_timeout = '{statement_timeout_s}s'"))
        row = conn.execute(text(sql), params).mappings().one()
        batch_n = int(row["parcelles_batch"] or 0)
        hit_n = int(row["parcelles_avec_intersection"] or 0)
        if apply:
            return batch_n, hit_n, int(row["updated_n"] or 0)
        return batch_n, hit_n, int(row["total_objets"] or 0)


def run_layer(
    engine,
    schema: str,
    table: str,
    cfg: dict,
    *,
    batch_n: int,
    limit: int | None,
    refs: list[tuple[str, str]] | None,
    apply: bool,
    default_chunk: int = 0,
    statement_timeout_s: int = 600,
) -> LayerResult:
    if cfg.get("geom_type", "surfacique") != "surfacique":
        raise ValueError(f"Couche {table} : seules les couches surfaciques sont supportées.")

    chunk_size = _resolve_chunk_size(cfg, default_chunk)
    t0 = time.perf_counter()
    total_hits = 0
    total_objets = 0
    total_updates = 0

    if chunk_size and batch_n > chunk_size:
        n_chunks = (batch_n + chunk_size - 1) // chunk_size
        _progress(
            "  découpage : %s chunk(s) × %s parcelles (total %s)",
            n_chunks,
            chunk_size,
            batch_n,
        )
        for ci in range(n_chunks):
            offset = ci * chunk_size
            _progress("  chunk %s/%s — offset %s — démarrage…", ci + 1, n_chunks, offset)
            ct0 = time.perf_counter()
            if apply:
                sql, params = _build_apply_sql(
                    engine, schema, table, cfg,
                    limit=limit, refs=refs, offset=offset, chunk_size=chunk_size,
                )
            else:
                sql, params = _build_stats_sql(
                    engine, schema, table, cfg,
                    limit=limit, refs=refs, offset=offset, chunk_size=chunk_size,
                )
            _, hits, extra = _execute_layer_sql(
                engine, sql, params, apply=apply, statement_timeout_s=statement_timeout_s,
            )
            total_hits += hits
            if apply:
                total_updates += extra
            else:
                total_objets += extra
            _progress(
                "  chunk %s/%s terminé — %.2f s — %s %s",
                ci + 1,
                n_chunks,
                time.perf_counter() - ct0,
                hits,
                "MAJ" if apply else "parcelles",
            )
    else:
        _progress("  pas de découpage (chunk_size=%s) — 1 requête pour %s parcelles", chunk_size, batch_n)
        if apply:
            sql, params = _build_apply_sql(engine, schema, table, cfg, limit=limit, refs=refs)
        else:
            sql, params = _build_stats_sql(engine, schema, table, cfg, limit=limit, refs=refs)
        _, total_hits, extra = _execute_layer_sql(
            engine, sql, params, apply=apply, statement_timeout_s=statement_timeout_s,
        )
        if apply:
            total_updates = extra
        else:
            total_objets = extra

    elapsed = time.perf_counter() - t0
    return LayerResult(
        table=table,
        batch_n=batch_n,
        hit_n=total_hits,
        total_objets=total_objets,
        elapsed_s=elapsed,
        updated_n=total_updates,
    )


def _log_layer_result(i: int, total: int, cfg: dict, result: LayerResult, apply: bool) -> None:
    nom = cfg.get("nom", result.table)
    pct = round(result.hit_n / result.batch_n * 100, 1) if result.batch_n else 0.0
    logger.info(
        "  [%s/%s] %s — %.2f s | %s parcelles concernées / %s (%.1f%%)",
        i,
        total,
        nom,
        result.elapsed_s,
        result.hit_n,
        result.batch_n,
        pct,
    )
    if not apply:
        logger.info("           %s objets intersectés", result.total_objets)
    else:
        logger.info("           %s lignes mises à jour", result.updated_n)


def _print_summary(results: list[LayerResult], total_sec: float, apply: bool) -> None:
    print("\n" + "=" * 72)
    print(f"RÉSUMÉ — {len(results)} couche(s) — {'APPLY' if apply else 'DRY-RUN'}")
    print("=" * 72)
    for r in results:
        pct = round(r.hit_n / r.batch_n * 100, 1) if r.batch_n else 0.0
        extra = f"{r.updated_n} MAJ" if apply else f"{r.total_objets} objets"
        print(f"  {r.table:<20} {r.elapsed_s:>6.2f} s  |  {r.hit_n}/{r.batch_n} ({pct}%)  |  {extra}")
    print("-" * 72)
    print(f"  {'TOTAL':<20} {total_sec:>6.2f} s")
    print("=" * 72)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Enrichissement parcelles (set-based) → colonnes sig_<couche> JSONB"
    )
    ap.add_argument("--schema", default=SCHEMA)
    ap.add_argument("--catalogue", type=Path, default=DEFAULT_CATALOGUE)
    ap.add_argument("--limit", type=int, help="Limiter le nombre de parcelles")
    ap.add_argument("--refs", help='Parcelles cibles, ex: "AB:0123,AC:0456"')
    ap.add_argument(
        "--only",
        help="Couches comma-separées (défaut : tout le catalogue résumé)",
    )
    ap.add_argument("--dry-run", action="store_true", help="Calcule sans UPDATE (défaut)")
    ap.add_argument("--apply", action="store_true", help="Écrit les colonnes sig_<couche> en base")
    ap.add_argument(
        "--chunk-size",
        type=int,
        default=0,
        help="Découper les parcelles par tranches (défaut : chunk_size du catalogue ou tout d'un coup)",
    )
    ap.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="statement_timeout PostgreSQL par requête en secondes (défaut : 600)",
    )
    args = ap.parse_args()

    if args.apply and args.dry_run:
        ap.error("Utiliser --apply OU --dry-run, pas les deux.")

    apply = args.apply
    if not args.catalogue.is_file():
        raise SystemExit(f"Catalogue introuvable : {args.catalogue}")

    catalogue = load_catalogue(str(args.catalogue))
    if args.only:
        wanted = {t.strip() for t in args.only.split(",") if t.strip()}
        catalogue = {k: v for k, v in catalogue.items() if k in wanted}
        if not catalogue:
            raise SystemExit("Aucune couche après filtre --only")

    refs = _parse_refs(args.refs) if args.refs else None
    engine = get_engine()
    batch_n = count_parcelles(engine, args.schema, limit=args.limit, refs=refs)

    if batch_n == 0:
        logger.warning("Aucune parcelle dans le batch.")
        return 1

    logger.info(
        "Enrichissement set-based — %s parcelle(s) — %s couche(s) — mode %s",
        batch_n,
        len(catalogue),
        "APPLY" if apply else "DRY-RUN",
    )

    if apply:
        ensure_layer_columns(engine, args.schema, catalogue)

    results: list[LayerResult] = []
    t_global = time.perf_counter()
    layers = list(catalogue.items())

    for i, (table, cfg) in enumerate(layers, 1):
        chunk = _resolve_chunk_size(cfg, args.chunk_size)
        _progress(
            "── Couche %s/%s : %s (%s) — chunk_size=%s ──",
            i,
            len(layers),
            cfg.get("nom", table),
            table,
            chunk or "tout",
        )
        try:
            result = run_layer(
                engine,
                args.schema,
                table,
                cfg,
                batch_n=batch_n,
                limit=args.limit,
                refs=refs,
                apply=apply,
                default_chunk=args.chunk_size,
                statement_timeout_s=args.timeout,
            )
        except Exception as exc:
            logger.error("Échec sur %s : %s", table, exc)
            raise
        results.append(result)
        _log_layer_result(i, len(layers), cfg, result, apply)

    total_sec = time.perf_counter() - t_global
    _print_summary(results, total_sec, apply)

    if apply:
        logger.info("Terminé — colonnes sig_<couche> mises à jour (%s couche(s), %.1f s).", len(results), total_sec)
    else:
        logger.info("Dry-run terminé — relancer avec --apply pour persister.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
