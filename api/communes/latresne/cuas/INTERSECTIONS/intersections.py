#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
intersections.py - intersections_v11
----------------------------------------------------
Analyse les intersections entre une parcelle et les couches du catalogue.

Filtrage centralisé :
  - surfacique : aire > 0,01 m² ET part UF > 1 % (min_pct_sig, surchargeable dans le catalogue)
  - lineaire   : longueur d'intersection > 0,01 m
  - ponctuel   : ST_Within (entité dans l'UF)
"""

import os
import re
import json
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
# WARNING pour limiter la RAM (logs verbeux désactivés temporairement)
logging.basicConfig(level=logging.WARNING, format="%(levelname)s | %(message)s")
logger = logging.getLogger("intersections")

SUPABASE_HOST = os.getenv('SUPABASE_HOST')
SUPABASE_DB = os.getenv('SUPABASE_DB')
SUPABASE_USER = os.getenv('SUPABASE_USER')
SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')
SUPABASE_PORT = str(os.getenv('SUPABASE_PORT') or "5432").strip().strip('"').strip("'")
if SUPABASE_HOST and "pooler.supabase.com" in SUPABASE_HOST and SUPABASE_PORT == "5432":
    logger.warning("SUPABASE_PORT=5432 detecte sur pooler; bascule auto vers 6543 (transaction mode).")
    SUPABASE_PORT = "6543"

DATABASE_URL = f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
engine = create_engine(
    DATABASE_URL,
    pool_size=1,
    max_overflow=0,
    pool_pre_ping=True,
)

SCHEMA = "latresne"
SRID = 2154
GEOM_COL = "geom_2154"

# Seuils d'intersection : micro-artefacts numériques et recouvrements frontaliers.
MIN_INTERSECTION_AREA_M2 = 0.01
MIN_INTERSECTION_LENGTH_M = 0.01
DEFAULT_MIN_PCT_SIG = 1.0

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Détermination du chemin absolu du fichier catalogue
PROJECT_ROOT = Path(__file__).resolve().parents[1]   # remonte d’un niveau
CATALOGUE_PATH = PROJECT_ROOT / "catalogues" / "catalogue_intersections_tagged.json"

with open(CATALOGUE_PATH, 'r', encoding='utf-8') as f:
    CATALOGUE = json.load(f)

from api.communes.latresne.cuas.INTERSECTIONS.intersection_modules.enrichment import enrich_intersections_rapport


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


def _normalize_geom_type(raw: str | None) -> str:
    geom_type = (raw or "surfacique").strip().lower()
    if geom_type in ("ponctuel", "ponctuelle"):
        return "ponctuel"
    if geom_type == "lineaire":
        return "lineaire"
    return "surfacique"


def resolve_min_pct_sig(cfg: dict) -> float:
    """Seuil % UF pour les couches surfaciques (catalogue min_pct_sig, défaut 1 %)."""
    if _normalize_geom_type(cfg.get("geom_type")) != "surfacique":
        return 0.0
    if "min_pct_sig" in cfg:
        try:
            return float(cfg["min_pct_sig"])
        except (TypeError, ValueError):
            return DEFAULT_MIN_PCT_SIG
    return DEFAULT_MIN_PCT_SIG


def _convert_row_types(obj: dict) -> dict:
    for k, v in obj.items():
        cls = getattr(v, "__class__", None)
        name = getattr(cls, "__name__", "")
        if name == "Decimal":
            obj[k] = float(v)
        elif name == "datetime":
            obj[k] = v.isoformat()
    return obj


def _pct_from_area(area: float, surface_sig: float) -> float:
    if surface_sig <= 0:
        return 0.0
    return round(area / surface_sig * 100, 4)


def format_intersection_layer(
    config: dict,
    objets: list,
    total_metric: float,
    area_parcelle_sig: float,
) -> dict:
    """Assemble l'entrée rapport JSON pour une couche du catalogue."""
    geom_type = _normalize_geom_type(config.get("geom_type"))
    pct_sig = 0.0
    if geom_type == "surfacique" and objets and area_parcelle_sig > 0:
        pct_sig = _pct_from_area(total_metric, area_parcelle_sig)
        if pct_sig > 100:
            pct_sig = 100.0
    return {
        "nom": config["nom"],
        "type": config["type"],
        "geom_type": geom_type,
        "pct_sig": pct_sig,
        "objets": objets,
    }


def fetch_superficie_indicative(parcelles: list, code_insee: str) -> float:
    """Récupère la superficie indicative (contenance) depuis la base locale."""
    try:
        requested = []
        seen = set()
        for p in parcelles:
            section = str((p or {}).get("section", "")).upper().strip()
            numero = str((p or {}).get("numero", "")).strip().zfill(4)
            if not section or not numero:
                continue
            key = (section, numero)
            if key in seen:
                continue
            seen.add(key)
            requested.append(key)

        if not requested:
            return None

        values_sql = ", ".join([f"(:s{i}, :n{i})" for i in range(len(requested))])
        params = {"code_insee": code_insee}
        for i, (section, numero) in enumerate(requested):
            params[f"s{i}"] = section
            params[f"n{i}"] = numero

        q = text(
            f"""
            WITH requested(section, numero) AS (
                VALUES {values_sql}
            )
            SELECT SUM(p.contenance) AS superficie_indicative
            FROM requested r
            JOIN latresne.parcelles p
              ON UPPER(TRIM(p.section)) = r.section
             AND LPAD(TRIM(p.numero), 4, '0') = r.numero
             AND p.code_insee = :code_insee
            """
        )

        with engine.connect() as conn:
            superficie = conn.execute(q, params).scalar()

        if superficie is None:
            return None
        return round(float(superficie), 2)

    except Exception as e:
        logger.warning(f"⚠️ Erreur récupération contenance base : {e}")
        return None

def get_parcelle_geometry(section, numero):
    query = text("SELECT ST_AsText(geom_2154) FROM latresne.parcelles WHERE section = :s AND numero = :n")
    with engine.connect() as conn:
        result = conn.execute(query, {"s": section, "n": numero})
        row = result.fetchone()
        if row:
            return row[0]
        raise ValueError(f"Parcelle {section} {numero} introuvable")

def calculate_intersection(parcelle_wkt, table_name, area_parcelle_sig):
    """
    Intersection UF × couche catalogue selon geom_type :
      - surfacique : aire d'intersection, seuil 0,01 m² + 1 % UF (min_pct_sig)
      - lineaire   : longueur d'intersection, seuil 0,01 m
      - ponctuel   : ST_Within (présence dans l'UF)

    Retourne (objets, total_metric, metadata).
    total_metric = aire union (surfacique) | longueur totale (lineaire) | 0 (ponctuel).
    """
    config = CATALOGUE.get(table_name)
    if not config:
        logger.warning(f"⚠️ {table_name}: non catalogué")
        return [], 0.0, {"nb_raw": 0, "nb_grouped": 0, "items": []}

    keep_cols = [_safe_ident(c) for c in (config.get("keep") or [])]
    if not keep_cols:
        return [], 0.0, {"nb_raw": 0, "nb_grouped": 0, "items": []}

    geom_type = _normalize_geom_type(config.get("geom_type"))
    table_sql = _safe_ident(table_name)
    geom_col = _safe_ident(config.get("geom_col", GEOM_COL))
    min_pct_sig = resolve_min_pct_sig(config)

    group_by_cfg = config.get("group_by")
    if not group_by_cfg:
        group_by = []
    elif isinstance(group_by_cfg, str):
        group_by = [_safe_ident(group_by_cfg)]
    else:
        group_by = [_safe_ident(c) for c in group_by_cfg]

    logger.info(f"\n────────────────────────────────────────")
    logger.info(f"🧩 CALCUL INTERSECTION : {table_name} ({geom_type})")
    logger.info(f"→ group_by = {group_by or 'Aucun'}")

    sql_params = {
        "wkt": parcelle_wkt,
        "surface_sig": float(area_parcelle_sig or 0),
        "min_pct_sig": min_pct_sig,
    }

    with engine.connect() as conn:
        try:
            if geom_type == "lineaire":
                return _calculate_lineaire(conn, table_sql, geom_col, keep_cols, sql_params)
            if geom_type == "ponctuel":
                return _calculate_ponctuel(conn, table_sql, geom_col, keep_cols, sql_params)
            if group_by:
                return _calculate_surfacique_group_by(
                    conn, table_sql, geom_col, keep_cols, group_by, sql_params
                )
            return _calculate_surfacique_simple(
                conn, table_sql, geom_col, keep_cols, sql_params, area_parcelle_sig
            )
        except Exception as e:
            logger.error(f"💥 {table_name}: {e}")
            return [], 0.0, {"nb_raw": 0, "nb_grouped": 0, "items": []}


def _calculate_surfacique_simple(conn, table_sql, geom_col, keep_cols, sql_params, area_parcelle_sig):
    t_cols = "".join(f"t.{c}, " for c in keep_cols)
    raw_cols = "".join(f"{c}, " for c in keep_cols)
    dedup_cols = "".join(f", {c}" for c in keep_cols)

    q = f"""
        WITH p AS (
            SELECT ST_MakeValid(ST_GeomFromText(:wkt, {SRID})) AS g
        ),
        inter_raw AS (
            SELECT {t_cols}
                   ST_Intersection(ST_MakeValid(t.{geom_col}), p.g) AS inter_geom
            FROM {SCHEMA}.{table_sql} t, p
            WHERE t.{geom_col} IS NOT NULL
              AND ST_Intersects(ST_MakeValid(t.{geom_col}), p.g)
        ),
        inter_filtered AS (
            SELECT {raw_cols} inter_geom
            FROM inter_raw
            WHERE ST_Area(inter_geom) > {MIN_INTERSECTION_AREA_M2}
              AND (
                  :min_pct_sig <= 0
                  OR :surface_sig <= 0
                  OR (ST_Area(inter_geom) / :surface_sig * 100) > :min_pct_sig
              )
        ),
        inter AS (
            SELECT DISTINCT ON (ST_AsBinary(inter_geom){dedup_cols})
                   {raw_cols} inter_geom
            FROM inter_filtered
        ),
        union_area AS (
            SELECT COALESCE(ST_Area(ST_Union(inter_geom)), 0.0) AS uarea
            FROM inter
        )
        SELECT {raw_cols}
               ST_Area(inter_geom) AS metric,
               union_area.uarea    AS total_area
        FROM inter, union_area
    """

    rs = conn.execute(text(q), sql_params)
    cols = [c[0] for c in rs.cursor.description]
    rows = rs.fetchall()

    objects = []
    total_surface = 0.0
    for i, row in enumerate(rows):
        d = dict(zip(cols, row))
        surf = float(d.pop("metric", 0) or 0)
        if i == 0:
            total_surface = float(d.pop("total_area", 0) or 0)
        else:
            d.pop("total_area", None)

        pct = _pct_from_area(surf, area_parcelle_sig)
        d["pct_uf"] = pct
        d["pct_sig"] = pct
        objects.append(_convert_row_types(d))

    return objects, total_surface, {
        "nb_raw": len(rows),
        "nb_grouped": len(rows),
        "items": [{"label": "N/A", "count": len(rows), "surface": total_surface}],
    }


def _calculate_surfacique_group_by(conn, table_sql, geom_col, keep_cols, group_by, sql_params):
    gb_cols_sql = ", ".join(f"t.{c}" for c in group_by)
    non_group_kept = [c for c in keep_cols if c not in group_by]
    gb_cols_list = ", ".join(group_by)

    raw_inter_attrs = ""
    if non_group_kept:
        raw_inter_attrs = ", " + ", ".join(f"t.{c}" for c in non_group_kept)

    stats_agg_attrs = ""
    if non_group_kept:
        stats_agg_attrs = ", " + ", ".join(
            f"(array_agg({c}) FILTER (WHERE {c} IS NOT NULL))[1] AS {c}"
            for c in non_group_kept
        )

    q = f"""
        WITH p AS (
            SELECT ST_MakeValid(ST_GeomFromText(:wkt, {SRID})) AS g
        ),
        raw_inter AS (
            SELECT
                {gb_cols_sql},
                ST_Intersection(ST_MakeValid(t.{geom_col}), p.g) AS geom_inter
                {raw_inter_attrs}
            FROM {SCHEMA}.{table_sql} t, p
            WHERE t.{geom_col} IS NOT NULL
              AND ST_Intersects(ST_MakeValid(t.{geom_col}), p.g)
        ),
        filtered_inter AS (
            SELECT *
            FROM raw_inter
            WHERE ST_Area(geom_inter) > {MIN_INTERSECTION_AREA_M2}
              AND (
                  :min_pct_sig <= 0
                  OR :surface_sig <= 0
                  OR (ST_Area(geom_inter) / :surface_sig * 100) > :min_pct_sig
              )
        ),
        stats AS (
            SELECT
                {gb_cols_list},
                COUNT(*) AS nb_entites,
                ROUND(CAST(SUM(ST_Area(geom_inter)) AS numeric), 2) AS somme_brute,
                ROUND(CAST(ST_Area(ST_UnaryUnion(ST_Collect(geom_inter))) AS numeric), 2) AS union_area
                {stats_agg_attrs}
            FROM filtered_inter
            GROUP BY {gb_cols_list}
        )
        SELECT * FROM stats
        WHERE union_area > 0
    """

    rs = conn.execute(text(q), sql_params)
    cols = [c[0] for c in rs.cursor.description]
    rows = rs.fetchall()

    logger.info("   📊 Surfacique group_by : filtrage 1 % + union (dissolve)")

    objects = []
    surfaces = []
    metadata_items = []
    nb_raw = 0
    surface_sig = float(sql_params.get("surface_sig") or 0)

    for row in rows:
        d = dict(zip(cols, row))
        surf_union = float(d.pop("union_area", 0) or 0)
        somme_brute = float(d.pop("somme_brute", 0) or 0)
        nb_entites = int(d.pop("nb_entites", 0))
        nb_raw += nb_entites

        pct = _pct_from_area(surf_union, surface_sig)
        d["pct_uf"] = pct
        d["pct_sig"] = pct

        key = tuple(d[c] for c in group_by)
        label = " / ".join(str(v) for v in key)
        chev = max(somme_brute - surf_union, 0)
        pct_chev = (chev / somme_brute * 100) if somme_brute > 0 else 0

        logger.info(
            f"      • Groupe '{label}': {nb_entites} entités, "
            f"brute={somme_brute:.2f} m², union={surf_union:.2f} m², "
            f"chev={chev:.2f} m² ({pct_chev:.1f}%)"
        )

        metadata_items.append({
            "label": label,
            "count": nb_entites,
            "surface": surf_union,
            "surface_avant_union": somme_brute,
            "chevauchement_m2": round(chev, 2),
            "pct_chevauchement": round(pct_chev, 2),
        })
        objects.append(_convert_row_types(d))
        surfaces.append(surf_union)

    logger.info(f"   📦 Entités brutes : {nb_raw}, groupes après union : {len(objects)}")

    return objects, sum(surfaces), {
        "nb_raw": nb_raw,
        "nb_grouped": len(objects),
        "items": metadata_items,
    }


def _calculate_lineaire(conn, table_sql, geom_col, keep_cols, sql_params):
    t_cols = "".join(f"t.{c}, " for c in keep_cols)
    raw_cols = "".join(f"{c}, " for c in keep_cols)
    dedup_cols = "".join(f", {c}" for c in keep_cols)

    q = f"""
        WITH p AS (
            SELECT ST_MakeValid(ST_GeomFromText(:wkt, {SRID})) AS g
        ),
        inter_raw AS (
            SELECT {t_cols}
                   ST_Intersection(ST_MakeValid(t.{geom_col}), p.g) AS inter_geom
            FROM {SCHEMA}.{table_sql} t, p
            WHERE t.{geom_col} IS NOT NULL
              AND ST_Intersects(ST_MakeValid(t.{geom_col}), p.g)
              AND ST_Length(ST_Intersection(ST_MakeValid(t.{geom_col}), p.g))
                  > {MIN_INTERSECTION_LENGTH_M}
        )
        SELECT DISTINCT ON (ST_AsBinary(inter_geom){dedup_cols})
               {raw_cols} ST_Length(inter_geom) AS metric
        FROM inter_raw
    """

    rs = conn.execute(text(q), sql_params)
    cols = [c[0] for c in rs.cursor.description]
    rows = rs.fetchall()

    objects = []
    total_length = 0.0
    for row in rows:
        d = dict(zip(cols, row))
        length = float(d.pop("metric", 0) or 0)
        d["longueur_inter_m"] = round(length, 2)
        total_length += length
        objects.append(_convert_row_types(d))

    return objects, total_length, {
        "nb_raw": len(rows),
        "nb_grouped": len(rows),
        "items": [{"label": "N/A", "count": len(rows), "longueur": total_length}],
    }


def _calculate_ponctuel(conn, table_sql, geom_col, keep_cols, sql_params):
    t_cols = "".join(f"t.{c}, " for c in keep_cols)
    raw_cols = "".join(f"{c}, " for c in keep_cols)
    dedup_cols = "".join(f", {c}" for c in keep_cols)

    q = f"""
        WITH p AS (
            SELECT ST_MakeValid(ST_GeomFromText(:wkt, {SRID})) AS g
        )
        SELECT DISTINCT ON (ST_AsBinary(t.{geom_col}){dedup_cols})
               {raw_cols} NULL::float AS metric
        FROM {SCHEMA}.{table_sql} t, p
        WHERE t.{geom_col} IS NOT NULL
          AND ST_Within(t.{geom_col}, p.g)
    """

    rs = conn.execute(text(q), sql_params)
    cols = [c[0] for c in rs.cursor.description]
    rows = rs.fetchall()

    objects = []
    for row in rows:
        d = dict(zip(cols, row))
        d.pop("metric", None)
        objects.append(_convert_row_types(d))

    return objects, 0.0, {
        "nb_raw": len(rows),
        "nb_grouped": len(rows),
        "items": [{"label": "N/A", "count": len(rows)}],
    }


def _log_intersection_metadata(metadata: dict) -> None:
    """Logs détaillés regroupement / chevauchements (mode group_by surfacique)."""
    if metadata["nb_raw"] > metadata["nb_grouped"]:
        logger.info("   🔧 REGROUPEMENT DÉTECTÉ :")
        logger.info(f"      → {metadata['nb_raw']} entités initiales")
        logger.info(f"      → {metadata['nb_grouped']} groupe(s) après unification")
        logger.info("      ──────────────────────────────────────────")

        for item in metadata["items"]:
            if item.get("count", 0) > 1:
                label = item["label"]
                nb_entites = item.get("count", 0)
                surface_union = item.get("surface", 0)
                surface_avant = item.get("surface_avant_union", 0)
                chevauchement = item.get("chevauchement_m2", 0)
                pct_chev = item.get("pct_chevauchement", 0)

                logger.info(f"      📦 Groupe '{label}' :")
                logger.info(f"         • {nb_entites} entités regroupées")
                logger.info(f"         • Surface avant union (somme) : {surface_avant:.2f} m²")
                logger.info(f"         • Surface après union : {surface_union:.2f} m²")

                if chevauchement > MIN_INTERSECTION_AREA_M2:
                    logger.info(
                        f"         • ⚠️ Chevauchement détecté : {chevauchement:.2f} m² ({pct_chev:.1f}%)"
                    )
                else:
                    reduction = surface_avant - surface_union
                    if reduction > MIN_INTERSECTION_AREA_M2:
                        logger.info(f"         • ℹ️ Réduction de {reduction:.2f} m² (arrondis/artefacts)")
                    else:
                        logger.info("         • ✅ Pas de chevauchement (géométries adjacentes ou disjointes)")

                if nb_entites > 0:
                    surface_moyenne = surface_union / nb_entites
                    logger.info(
                        f"         • Surface moyenne d'intersection par entité : {surface_moyenne:.2f} m²"
                    )
                logger.info("         ──────────────────────────────────────────")

        logger.info("   🔧 Résumé regroupement :")
        for item in metadata["items"]:
            logger.info(
                f"      -> {item['label']}: "
                f"{item['count']} entités → {item.get('surface', 0)} m² "
                f"(avant union: {item.get('surface_avant_union', 0)} m², "
                f"chevauchement: {item.get('chevauchement_m2', 0)} m²)"
            )
    elif any(item.get("count", 0) > 1 for item in metadata["items"]):
        logger.info("   ➕ Plusieurs entités détectées (sans regroupement) :")
        for item in metadata["items"]:
            if item.get("count", 0) > 1:
                logger.info(f"      • Zone '{item['label']}' : {item['count']} entités distinctes")


def analyse_parcelle(section, numero):
    logger.info(f"🚀 Analyse parcelle {section} {numero}")
    
    parcelle_wkt = get_parcelle_geometry(section, numero)
    
    with engine.connect() as conn:
        area_parcelle_sig = float(conn.execute(
            text("SELECT ST_Area(ST_GeomFromText(:wkt, 2154))"),
            {"wkt": parcelle_wkt}
        ).scalar())
    
    rapport = {
        "parcelle": f"{section} {numero}",
        "surface_m2": round(area_parcelle_sig, 2),
        "intersections": {}
    }
    
    for table, config in CATALOGUE.items():
        logger.info(f"→ {table}")
        
        objets, total_metric, metadata = calculate_intersection(parcelle_wkt, table, area_parcelle_sig)
        layer = format_intersection_layer(config, objets, total_metric, area_parcelle_sig)

        if objets:
            geom_type = layer["geom_type"]
            if geom_type == "lineaire":
                logger.info(f"  ✅ {len(objets)} objet(s) | {total_metric:.2f} m")
            elif geom_type == "ponctuel":
                logger.info(f"  ✅ {len(objets)} objet(s) ponctuel(s)")
            else:
                logger.info(
                    f"  ✅ {len(objets)} objet(s) | {total_metric:.2f} m² ({layer['pct_sig']:.4f} %)"
                )
            _log_intersection_metadata(metadata)
        else:
            logger.info("  ⚠️ Aucune intersection")

        rapport["intersections"][table] = layer

    enrich_intersections_rapport(rapport, parcelle_wkt, engine)
    return rapport

def generate_html(rapport):
    parcelle = rapport['parcelle']
    area = rapport['surface_m2']
    results = rapport['intersections']
    
    # Grouper par type
    by_type = {}
    for table, data in results.items():
        t = data['type']
        if t not in by_type:
            by_type[t] = []
        by_type[t].append((table, data))
    
    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<title>Rapport {parcelle}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 20px; }}
h1 {{ color: #333; }}
.info {{ background: #f0f0f0; padding: 10px; margin-bottom: 20px; }}
.type-section {{ margin-bottom: 30px; }}
.type-header {{ background: #2c5aa0; color: white; padding: 10px; }}
.couche {{ margin: 10px 0; padding: 10px; border: 1px solid #ddd; }}
.couche h3 {{ margin: 0 0 10px 0; color: #2c5aa0; }}
table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
th {{ background: #f5f5f5; }}
.no-intersect {{ color: #999; }}
</style>
</head>
<body>
<h1>Rapport d'intersection</h1>
<div class="info">
<strong>Parcelle:</strong> {parcelle}<br>
<strong>Surface:</strong> {area:,.2f} m²
</div>
"""
    
    for type_name in sorted(by_type.keys()):
        items = by_type[type_name]
        intersected = [(t, d) for t, d in items if d['objets']]
        
        html += f"""
<div class="type-section">
<div class="type-header">
<h2>{type_name.upper()} ({len(intersected)}/{len(items)} intersections)</h2>
</div>
"""
        
        for table, data in items:
            if data['objets']:
                html += f"""
<div class="couche">
<h3>✓ {data['nom']}</h3>
<p><strong>Part concernée:</strong> {data['pct_sig']:.4f}% de la surface cadastrale indicative</p>
"""
                # Headers (exclure les colonnes de surfaces)
                obj_keys = [k for k in data['objets'][0].keys() 
                           if not k.lower().startswith("surface") 
                           and not k.lower().endswith("_m2")]
                
                # Afficher le tableau seulement s'il y a des colonnes après filtrage
                if obj_keys:
                    html += "<table>\n<tr>\n"
                    for key in obj_keys:
                        html += f"<th>{key}</th>"
                    html += "</tr>\n"
                    
                    # Rows (exclure les colonnes de surfaces)
                    for obj in data['objets']:
                        html += "<tr>"
                        for key in obj_keys:
                            html += f"<td>{obj.get(key, '')}</td>"
                        html += "</tr>\n"
                    
                    html += "</table>\n"
                
                html += "</div>\n"
            else:
                html += f"""<div class="couche no-intersect"><h3>✗ {data['nom']}</h3><p>Aucune intersection</p></div>\n"""
        
        html += "</div>\n"
    
    html += "</body></html>"
    return html

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Analyse les intersections entre une unité foncière (WKT) et les couches du catalogue.")
    parser.add_argument("--section", help="Section de la parcelle (optionnel si --geom-wkt est fourni)")
    parser.add_argument("--numero", help="Numéro de la parcelle (optionnel si --geom-wkt est fourni)")
    parser.add_argument("--geom-wkt", help="Chemin vers un fichier WKT représentant l'unité foncière (optionnel)")
    parser.add_argument("--out-dir", default="../out_pipeline", help="Dossier de sortie pour les rapports")
    args = parser.parse_args()

    if args.geom_wkt:
        with open(args.geom_wkt, "r", encoding="utf-8") as f:
            parcelle_wkt = f.read()
        logger.info(f"📐 Utilisation de la géométrie fournie : {args.geom_wkt}")
        section, numero = "UF", "0000"  # Valeurs génériques
    elif args.section and args.numero:
        section, numero = args.section, args.numero
        parcelle_wkt = get_parcelle_geometry(section, numero)
    else:
        raise SystemExit("Fournir soit (--section & --numero) soit --geom-wkt")

    # Calcul surface
    with engine.connect() as conn:
        area_parcelle_sig = float(conn.execute(
            text("SELECT ST_Area(ST_GeomFromText(:wkt, 2154))"),
            {"wkt": parcelle_wkt}
        ).scalar())

    rapport = {
        "parcelle": f"{section} {numero}",
        "surface_m2": round(area_parcelle_sig, 2),
        "intersections": {}
    }

    # Lancer l'analyse
    for table, config in CATALOGUE.items():
        logger.info(f"→ {table}")
        objets, total_metric, metadata = calculate_intersection(parcelle_wkt, table, area_parcelle_sig)
        layer = format_intersection_layer(config, objets, total_metric, area_parcelle_sig)

        if objets:
            geom_type = layer["geom_type"]
            if geom_type == "lineaire":
                logger.info(f"  ✅ {len(objets)} objet(s) | {total_metric:.2f} m")
            elif geom_type == "ponctuel":
                logger.info(f"  ✅ {len(objets)} objet(s) ponctuel(s)")
            else:
                logger.info(
                    f"  ✅ {len(objets)} objet(s) | {total_metric:.2f} m² ({layer['pct_sig']:.4f} %)"
                )
            _log_intersection_metadata(metadata)
        else:
            logger.info("  ⚠️ Aucune intersection")

        rapport["intersections"][table] = layer

    # Nettoyage final : retirer toutes les surfaces en m2
    for layer_key, layer in rapport["intersections"].items():
        # Supprimer les surfaces globales inutiles
        layer.pop("surface_sig_m2", None)
        layer.pop("surface_inter_m2", None)
        layer.pop("surface_inter_sig_m2", None)
        layer.pop("surface_parcelle_m2", None)

        # Nettoyage des objets
        for obj in layer.get("objets", []):
            # Plus besoin de nettoyer surface_inter_m2 car déjà supprimé dans calculate_intersection
            obj.pop("surface_zone_m2", None)
            obj.pop("surface_parcelle_m2", None)

    enrich_intersections_rapport(rapport, parcelle_wkt, engine)

    # Sauvegarde des rapports
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    OUT_DIR = Path(args.out_dir)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    out_json = OUT_DIR / f"rapport_intersections_{timestamp}.json"
    out_html = OUT_DIR / f"rapport_intersections_{timestamp}.html"
    
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(rapport, f, indent=2, ensure_ascii=False)

    html = generate_html(rapport)
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"\n✅ Rapports exportés ({out_json}, {out_html})")