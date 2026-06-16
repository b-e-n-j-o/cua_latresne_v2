# -*- coding: utf-8 -*-
"""
Module métier dédié : logique croisée Natura 2000 / Prairies sensibles.

Le but est de restituer les réglementations à appliquer selon 4 cas :
  - Natura seule
  - Prairie seule (hors Natura)
  - Double intersection Natura + Prairie
  - Aucun des deux
"""

from __future__ import annotations

import re

from sqlalchemy import text

try:
    from api.cuas.db import GEOM_COL, SCHEMA, SRID, get_engine
except ImportError:
    from db import GEOM_COL, SCHEMA, SRID, get_engine


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


def _table_exists(engine, schema: str, table: str) -> bool:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = :schema AND table_name = :table
                    )
                    """
                ),
                {"schema": schema, "table": table},
            ).scalar()
        )


def compute_prairies_natura_reglementation(
    uf_wkt: str,
    *,
    engine=None,
    schema: str = SCHEMA,
    natura_table: str = "natura_2000",
    prairie_table: str = "prairies_sensibles",
    reglement_table: str = "natura_2000_et_prairies_reglements",
    geom_col: str = GEOM_COL,
) -> dict:
    """
    Retourne le diagnostic métier et les blocs réglementaires associés.
    """
    engine = engine or get_engine()

    schema = _safe_ident(schema)
    natura_table = _safe_ident(natura_table)
    prairie_table = _safe_ident(prairie_table)
    reglement_table = _safe_ident(reglement_table)
    geom_col = _safe_ident(geom_col)

    required_tables = (natura_table, prairie_table, reglement_table)
    missing_tables = [t for t in required_tables if not _table_exists(engine, schema, t)]
    if missing_tables:
        return {
            "status": "table_absente",
            "diagnostic_metier": "Module non exécutable : table(s) manquante(s)",
            "tables_manquantes": missing_tables,
            "natura": None,
            "prairie": None,
        }

    sql = text(
        f"""
        WITH zone_etude AS (
            SELECT ST_GeomFromText(:wkt, {SRID}) AS geom
        ),
        check_natura AS (
            SELECT
                n.id AS natura_id,
                n.c_site,
                n.n_site,
                reg.statut_juridique AS natura_statut,
                REPLACE(
                    REPLACE(reg.laius_technique, '{{c_site}}', COALESCE(n.c_site, 'Inconnu')),
                    '{{n_site}}',
                    COALESCE(n.n_site, 'Sans nom')
                ) AS natura_laius,
                reg.base_legale AS natura_base
            FROM {schema}.{natura_table} n
            CROSS JOIN zone_etude ze
            CROSS JOIN {schema}.{reglement_table} reg
            WHERE ST_Intersects(ST_MakeValid(n.{geom_col}), ze.geom)
              AND reg.code_regime = 'NATURA_2000'
            LIMIT 1
        ),
        check_prairie AS (
            SELECT p.id AS prairie_id
            FROM {schema}.{prairie_table} p
            CROSS JOIN zone_etude ze
            WHERE ST_Intersects(ST_MakeValid(p.{geom_col}), ze.geom)
            LIMIT 1
        )
        SELECT
            CASE
                WHEN n.natura_id IS NOT NULL AND p.prairie_id IS NOT NULL
                    THEN 'double_intersection_natura_et_prairie'
                WHEN n.natura_id IS NOT NULL
                    THEN 'natura_seule'
                WHEN p.prairie_id IS NOT NULL
                    THEN 'prairie_seule_hors_natura'
                ELSE 'hors_natura_et_prairie'
            END AS regime_code,

            CASE
                WHEN n.natura_id IS NOT NULL AND p.prairie_id IS NOT NULL
                    THEN 'CAS 2 : Double intersection (Natura 2000 + Prairie Sensible)'
                WHEN n.natura_id IS NOT NULL
                    THEN 'CAS 1 : Intersection Natura 2000 seule'
                WHEN p.prairie_id IS NOT NULL
                    THEN 'CAS 3 : Intersection Prairie seule (Hors Natura 2000)'
                ELSE 'RAS : UF hors contraintes Natura 2000 / Prairies'
            END AS diagnostic_metier,

            n.natura_id,
            n.c_site AS natura_code_site,
            n.n_site AS natura_nom_site,
            n.natura_statut,
            n.natura_laius,
            n.natura_base,

            p.prairie_id,
            CASE
                WHEN n.natura_id IS NOT NULL AND p.prairie_id IS NOT NULL
                    THEN (SELECT statut_juridique FROM {schema}.{reglement_table} WHERE code_regime = 'BCAE9_NATURA')
                WHEN p.prairie_id IS NOT NULL
                    THEN (SELECT statut_juridique FROM {schema}.{reglement_table} WHERE code_regime = 'BCAE1_HORS_NATURA')
                ELSE NULL
            END AS prairie_statut,
            CASE
                WHEN n.natura_id IS NOT NULL AND p.prairie_id IS NOT NULL
                    THEN (SELECT laius_technique FROM {schema}.{reglement_table} WHERE code_regime = 'BCAE9_NATURA')
                WHEN p.prairie_id IS NOT NULL
                    THEN (SELECT laius_technique FROM {schema}.{reglement_table} WHERE code_regime = 'BCAE1_HORS_NATURA')
                ELSE NULL
            END AS prairie_laius,
            CASE
                WHEN n.natura_id IS NOT NULL AND p.prairie_id IS NOT NULL
                    THEN (SELECT base_legale FROM {schema}.{reglement_table} WHERE code_regime = 'BCAE9_NATURA')
                WHEN p.prairie_id IS NOT NULL
                    THEN (SELECT base_legale FROM {schema}.{reglement_table} WHERE code_regime = 'BCAE1_HORS_NATURA')
                ELSE NULL
            END AS prairie_base
        FROM (SELECT 1) dummy
        LEFT JOIN check_natura n ON TRUE
        LEFT JOIN check_prairie p ON TRUE
        """
    )

    with engine.connect() as conn:
        row = conn.execute(sql, {"wkt": uf_wkt}).mappings().one()

    has_natura = row["natura_id"] is not None
    has_prairie = row["prairie_id"] is not None

    natura_block = None
    if has_natura:
        natura_block = {
            "id": row["natura_id"],
            "code_site": row["natura_code_site"],
            "nom_site": row["natura_nom_site"],
            "statut": row["natura_statut"],
            "laius": row["natura_laius"],
            "base_legale": row["natura_base"],
        }

    prairie_block = None
    if has_prairie:
        prairie_block = {
            "id": row["prairie_id"],
            "statut": row["prairie_statut"],
            "laius": row["prairie_laius"],
            "base_legale": row["prairie_base"],
        }

    return {
        "status": "concernee" if (has_natura or has_prairie) else "non_concernee",
        "regime_code": row["regime_code"],
        "diagnostic_metier": row["diagnostic_metier"],
        "natura": natura_block,
        "prairie": prairie_block,
        "has_natura": has_natura,
        "has_prairie": has_prairie,
    }
