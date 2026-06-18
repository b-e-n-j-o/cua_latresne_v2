# -*- coding: utf-8 -*-
"""
Module métier dédié : analyse experte raccordement ENEDIS (linéaires BT).

Entrée : géométrie UF (WKT, SRID projet).
Sortie : diagnostic de raccordement par type de réseau BT.
"""

from __future__ import annotations

import re

from sqlalchemy import text

try:
    from api.cuas.argeles.db import GEOM_COL, SCHEMA, SRID, get_engine
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
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


def compute_enedis_raccordement(
    uf_wkt: str,
    *,
    engine=None,
    schema: str = SCHEMA,
    reseau_table: str = "reseaux_enedis_lineaires",
    parcelles_table: str = "parcelles",
    geom_col: str = GEOM_COL,
    zone_rue_buffer_m: float = 20.0,
    seuil_regle_30m: float = 30.0,
    seuil_lineaire_rue_m: float = 15.0,
    seuil_voisin_inter_m: float = 0.2,
) -> dict:
    """
    Analyse de raccordement ENEDIS BT sur UF.
    """
    engine = engine or get_engine()

    schema = _safe_ident(schema)
    reseau_table = _safe_ident(reseau_table)
    parcelles_table = _safe_ident(parcelles_table)
    geom_col = _safe_ident(geom_col)

    required_tables = (reseau_table, parcelles_table)
    missing_tables = [t for t in required_tables if not _table_exists(engine, schema, t)]
    if missing_tables:
        return {
            "status": "table_absente",
            "diagnostic_metier": "Module non exécutable : table(s) manquante(s)",
            "tables_manquantes": missing_tables,
            "analyses": [],
        }

    sql = text(
        f"""
        WITH target_uf AS (
            SELECT ST_GeomFromText(:wkt, {SRID}) AS geom
        ),
        closest_line AS (
            SELECT DISTINCT ON (l.type)
                l.type,
                l.source_id,
                ST_MakeValid(l.{geom_col}) AS geom
            FROM {schema}.{reseau_table} l
            CROSS JOIN target_uf tu
            WHERE l.type IN ('reseau-souterrain-bt', 'reseau-bt')
              AND l.{geom_col} IS NOT NULL
            ORDER BY l.type, ST_MakeValid(l.{geom_col}) <-> tu.geom ASC
        ),
        shortest_paths AS (
            SELECT
                cl.type,
                cl.source_id,
                ST_Distance(tu.geom, cl.geom) AS dist_brute,
                ST_ShortestLine(tu.geom, cl.geom) AS geom_path,
                ST_Length(
                    ST_Intersection(cl.geom, ST_Buffer(tu.geom, :zone_rue_buffer_m))
                ) AS longueur_dans_zone_rue
            FROM closest_line cl
            CROSS JOIN target_uf tu
        ),
        global_intersections AS (
            SELECT
                l.type,
                COUNT(l.id) AS nb_geometries,
                COALESCE(
                    SUM(ST_Length(ST_Intersection(ST_MakeValid(l.{geom_col}), tu.geom))),
                    0
                ) AS total_length
            FROM {schema}.{reseau_table} l
            CROSS JOIN target_uf tu
            WHERE l.type IN ('reseau-souterrain-bt', 'reseau-bt')
              AND l.{geom_col} IS NOT NULL
              AND ST_Intersects(ST_MakeValid(l.{geom_col}), tu.geom)
            GROUP BY l.type
        ),
        detect_blocage_voisin AS (
            SELECT
                sp.type,
                COUNT(p.id) AS nb_voisins_traverses,
                STRING_AGG(
                    p.section || ' n°' || p.numero,
                    ', ' ORDER BY p.section, p.numero
                ) AS liste_voisins
            FROM {schema}.{parcelles_table} p
            CROSS JOIN shortest_paths sp
            CROSS JOIN target_uf tu
            WHERE p.{geom_col} IS NOT NULL
              AND ST_Intersects(ST_MakeValid(p.{geom_col}), sp.geom_path)
              AND ST_Area(ST_Intersection(ST_MakeValid(p.{geom_col}), tu.geom)) <= 0
              AND ST_Length(
                    ST_Intersection(ST_MakeValid(p.{geom_col}), sp.geom_path)
              ) > :seuil_voisin_inter_m
            GROUP BY sp.type
        )
        SELECT
            sp.type AS type_code,
            CASE
                WHEN sp.type = 'reseau-souterrain-bt'
                    THEN 'Souterrain Basse Tension (BT)'
                WHEN sp.type = 'reseau-bt'
                    THEN 'Aérien Basse Tension (BT)'
                ELSE sp.type
            END AS type_reseau,
            sp.source_id AS id_cable_plus_proche,
            ROUND(sp.dist_brute::numeric, 2) AS distance_directe_metres,
            COALESCE(gi.nb_geometries, 0) AS nb_lignes_dans_uf,
            ROUND(COALESCE(gi.total_length, 0)::numeric, 2) AS lineaire_interieur_metres,
            COALESCE(
                db.liste_voisins,
                'Aucun (Accès direct via domaine public)'
            ) AS parcelles_voisines_fait_obstacle,
            CASE
                WHEN sp.dist_brute > :seuil_regle_30m
                    THEN 'Réseau éloigné (' || ROUND(sp.dist_brute::numeric, 0) || 'm) - Extension publique obligatoire (art. L.332-15 Code de l''urbanisme).'
                WHEN COALESCE(db.nb_voisins_traverses, 0) > 0
                    THEN 'Raccordement indirect contraint - Servitude de passage ou convention de survol obligatoire sur propriété privée (' || db.liste_voisins || ').'
                WHEN sp.longueur_dans_zone_rue >= :seuil_lineaire_rue_m
                    THEN 'Au droit de la parcelle - Le réseau longe le terrain sur le domaine public (raccordement simple).'
                WHEN sp.longueur_dans_zone_rue < :seuil_lineaire_rue_m
                    THEN 'Au droit partiel - Le réseau frôle le terrain, vérifier l''emplacement de l''accès au domaine public.'
                ELSE 'À analyser manuellement.'
            END AS diagnostic_expert_raccordement
        FROM shortest_paths sp
        LEFT JOIN global_intersections gi ON gi.type = sp.type
        LEFT JOIN detect_blocage_voisin db ON db.type = sp.type
        ORDER BY sp.dist_brute ASC
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "wkt": uf_wkt,
                "zone_rue_buffer_m": float(zone_rue_buffer_m),
                "seuil_regle_30m": float(seuil_regle_30m),
                "seuil_lineaire_rue_m": float(seuil_lineaire_rue_m),
                "seuil_voisin_inter_m": float(seuil_voisin_inter_m),
            },
        ).mappings().all()

    analyses = [
        {
            "type_code": r["type_code"],
            "type_reseau": r["type_reseau"],
            "id_cable_plus_proche": r["id_cable_plus_proche"],
            "distance_directe_metres": float(r["distance_directe_metres"]),
            "nb_lignes_dans_uf": int(r["nb_lignes_dans_uf"]),
            "lineaire_interieur_metres": float(r["lineaire_interieur_metres"]),
            "parcelles_voisines_fait_obstacle": r["parcelles_voisines_fait_obstacle"],
            "diagnostic_expert_raccordement": r["diagnostic_expert_raccordement"],
        }
        for r in rows
    ]

    if not analyses:
        return {
            "status": "non_concernee",
            "diagnostic_metier": "RAS : aucun réseau BT ENEDIS exploitable trouvé à proximité",
            "analyses": [],
        }

    return {
        "status": "concernee",
        "diagnostic_metier": "Analyse raccordement ENEDIS calculée",
        "analyses": analyses,
    }
