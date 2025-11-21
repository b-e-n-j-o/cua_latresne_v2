#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
intersections.py - intersections_v10
----------------------------------------------------
Analyse les intersections entre une parcelle et les couches du catalogue.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("intersections")

SUPABASE_HOST = os.getenv('SUPABASE_HOST')
SUPABASE_DB = os.getenv('SUPABASE_DB')
SUPABASE_USER = os.getenv('SUPABASE_USER')
SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')
SUPABASE_PORT = os.getenv('SUPABASE_PORT')

DATABASE_URL = f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
engine = create_engine(DATABASE_URL)

SCHEMA = "latresne"

# Détermination du chemin absolu du fichier catalogue
PROJECT_ROOT = Path(__file__).resolve().parents[1]   # remonte d’un niveau
CATALOGUE_PATH = PROJECT_ROOT / "catalogues" / "catalogue_intersections_tagged.json"

with open(CATALOGUE_PATH, 'r', encoding='utf-8') as f:
    CATALOGUE = json.load(f)

def get_parcelle_geometry(section, numero):
    query = text("SELECT ST_AsText(geom_2154) FROM latresne.parcelles_latresne WHERE section = :s AND numero = :n")
    with engine.connect() as conn:
        result = conn.execute(query, {"s": section, "n": numero})
        row = result.fetchone()
        if row:
            return row[0]
        raise ValueError(f"Parcelle {section} {numero} introuvable")

def calculate_intersection(parcelle_wkt, table_name):
    """
    Version alignée EXACTEMENT sur test_intersections_standalone.py
    - Dissolve par group_by (ST_UnaryUnion + ST_Collect)
    - Surfaces brutes + surfaces après union
    - Détection chevauchements
    """

    config = CATALOGUE.get(table_name)
    if not config:
        logger.warning(f"⚠️ {table_name}: non catalogué")
        return [], 0.0, {"nb_raw": 0, "nb_grouped": 0, "items": []}

    keep_cols = config.get("keep") or []
    group_by_cfg = config.get("group_by")

    # --- Toujours transformer group_by en LISTE ----
    if not group_by_cfg:
        group_by = []
    elif isinstance(group_by_cfg, str):
        group_by = [group_by_cfg]
    else:
        group_by = list(group_by_cfg)

    if not keep_cols:
        return [], 0.0, {"nb_raw": 0, "nb_grouped": 0, "items": []}

    logger.info(f"\n────────────────────────────────────────")
    logger.info(f"🧩 CALCUL INTERSECTION : {table_name}")
    logger.info(f"→ group_by = {group_by or 'Aucun'}")

    with engine.connect() as conn:
        try:

            # --------------------------------------------------------------------
            # 1️⃣ MODE SANS GROUP BY (simple)
            # --------------------------------------------------------------------
            if not group_by:
                select_cols = ", ".join([f"t.{c}" for c in keep_cols])

                q = f"""
                    WITH p AS (
                        SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g
                    )
                    SELECT
                        {select_cols},
                        ROUND(CAST(ST_Area(
                            ST_Intersection(ST_MakeValid(t.geom_2154), p.g)
                        ) AS numeric), 2) AS surface_inter_m2
                    FROM {SCHEMA}.{table_name} t, p
                    WHERE t.geom_2154 IS NOT NULL
                      AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
                """

                rs = conn.execute(text(q), {"wkt": parcelle_wkt})
                cols = [c[0] for c in rs.cursor.description]
                rows = rs.fetchall()

                objects = []
                total_surface = 0.0

                for row in rows:
                    d = dict(zip(cols, row))
                    surf = float(d.pop("surface_inter_m2", 0) or 0)
                    total_surface += surf
                    objects.append(d)

                # Conversions types non JSON
                for obj in objects:
                    for k, v in obj.items():
                        cls = getattr(v, "__class__", None)
                        name = getattr(cls, "__name__", "")
                        if name == "Decimal":
                            obj[k] = float(v)
                        elif name == "datetime":
                            obj[k] = v.isoformat()

                return objects, total_surface, {
                    "nb_raw": len(rows),
                    "nb_grouped": len(rows),
                    "items": [{"label": "N/A", "count": 1, "surface": total_surface}]
                }

            # --------------------------------------------------------------------
            # 2️⃣ MODE GROUP BY → DISSOLVE
            # --------------------------------------------------------------------

            gb_cols_sql = ", ".join([f"t.{c}" for c in group_by])
            non_group_kept = [c for c in keep_cols if c not in group_by]

            # Attributs non-groupés → première valeur non nulle
            agg_attrs = []
            for col in non_group_kept:
                agg_attrs.append(
                    f"(array_agg(t.{col}) FILTER (WHERE t.{col} IS NOT NULL))[1] AS {col}"
                )
            agg_attrs_sql = ", " + ", ".join(agg_attrs) if agg_attrs else ""

            # -------- Détails avant union : surfaces brutes --------
            q_details = f"""
                WITH p AS (SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g)
                SELECT
                    {gb_cols_sql},
                    COUNT(*) AS nb_entites,
                    ROUND(CAST(
                        SUM(ST_Area(ST_Intersection(ST_MakeValid(t.geom_2154), p.g)))
                        AS numeric
                    ), 2) AS somme_surfaces_brutes
                FROM {SCHEMA}.{table_name} t, p
                WHERE t.geom_2154 IS NOT NULL
                  AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
                GROUP BY {gb_cols_sql}
            """

            rs_details = conn.execute(text(q_details), {"wkt": parcelle_wkt})
            cols_details = [c[0] for c in rs_details.cursor.description]
            rows_details = rs_details.fetchall()

            details = {}
            for row in rows_details:
                d = dict(zip(cols_details, row))
                key = tuple(d[c] for c in group_by)
                details[key] = {
                    "nb_entites": d["nb_entites"],
                    "somme_surfaces_brutes": float(d["somme_surfaces_brutes"] or 0)
                }

            logger.info("   📊 Étape 1 — Surfaces brutes avant union")
            for key, det in details.items():
                label = " / ".join(str(x) for x in key)
                logger.info(f"      • Groupe '{label}': {det['nb_entites']} entités, "
                            f"somme brute = {det['somme_surfaces_brutes']} m²")

            # -------- UNION géométrique + surface réelle --------
            select_cols_final = ", ".join(group_by)
            if non_group_kept:
                select_cols_final += ", " + ", ".join(non_group_kept)

            q_union = f"""
                WITH p AS (
                    SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g
                ),
                raw AS (
                    SELECT
                        {gb_cols_sql},
                        ST_UnaryUnion(
                            ST_Collect(
                                ST_Intersection(ST_MakeValid(t.geom_2154), p.g)
                            )
                        ) AS geom_union
                        {agg_attrs_sql}
                    FROM {SCHEMA}.{table_name} t, p
                    WHERE t.geom_2154 IS NOT NULL
                      AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
                    GROUP BY {gb_cols_sql}
                )
                SELECT
                    {select_cols_final},
                    ROUND(CAST(ST_Area(geom_union) AS numeric), 2) AS union_area
                FROM raw
                WHERE geom_union IS NOT NULL
                  AND NOT ST_IsEmpty(geom_union)
            """

            rs = conn.execute(text(q_union), {"wkt": parcelle_wkt})
            cols = [c[0] for c in rs.cursor.description]
            rows = rs.fetchall()

            logger.info("   🔧 Étape 2 — Surfaces après union (dissolve)")
            for row in rows:
                d = dict(zip(cols, row))
                surf_union = float(d.get("union_area", 0) or 0)
                key = tuple(d.get(c) for c in group_by)
                label = " / ".join(str(x) for x in key)

                det = details.get(key, {"nb_entites": 0, "somme_surfaces_brutes": 0})
                somme = det["somme_surfaces_brutes"]
                chev = max(somme - surf_union, 0)
                pct = (chev / somme * 100) if somme > 0 else 0

                logger.info(f"      • Groupe '{label}':")
                logger.info(f"         - Surface union = {surf_union:.2f} m²")
                logger.info(f"         - Surface brute = {somme:.2f} m²")
                logger.info(f"         - Chevauchement = {chev:.2f} m² ({pct:.1f}%)")
                logger.info(f"         - Nb entités = {det['nb_entites']}")

            objects = []
            surfaces = []
            metadata_items = []

            for row in rows:
                d = dict(zip(cols, row))
                surf_union = float(d.pop("union_area", 0) or 0)

                key = tuple(d[c] for c in group_by)
                label = " / ".join(str(v) for v in key)

                det = details.get(key, {"nb_entites": 0, "somme_surfaces_brutes": 0})
                somme = det["somme_surfaces_brutes"]
                chev = max(somme - surf_union, 0)
                pct_chev = (chev / somme * 100) if somme > 0 else 0

                metadata_items.append({
                    "label": label,
                    "count": det["nb_entites"],
                    "surface": surf_union,
                    "surface_avant_union": somme,
                    "chevauchement_m2": round(chev, 2),
                    "pct_chevauchement": round(pct_chev, 2)
                })

                objects.append(d)
                surfaces.append(surf_union)

            # Conversions types non JSON
            for obj in objects:
                for k, v in obj.items():
                    cls = getattr(v, "__class__", None)
                    name = getattr(cls, "__name__", "")
                    if name == "Decimal":
                        obj[k] = float(v)
                    elif name == "datetime":
                        obj[k] = v.isoformat()

            # Nombre brut d'entités initiales
            nb_raw = conn.execute(
                text(f"""
                    WITH p AS (SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g)
                    SELECT COUNT(*)
                    FROM {SCHEMA}.{table_name} t, p
                    WHERE t.geom_2154 IS NOT NULL
                      AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
                """),
                {"wkt": parcelle_wkt}
            ).scalar()

            logger.info(f"   📦 Étape 3 — Comptage")
            logger.info(f"      - Entités brutes : {nb_raw}")
            logger.info(f"      - Groupes après union : {len(objects)}")

            metadata = {
                "nb_raw": nb_raw,
                "nb_grouped": len(objects),
                "items": metadata_items
            }

            return objects, sum(surfaces), metadata

        except Exception as e:
            logger.error(f"❌ {table_name}: {e}")
            return [], 0.0, {"nb_raw": 0, "nb_grouped": 0, "items": []}

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
        
        objets, surface_totale_sig, metadata = calculate_intersection(parcelle_wkt, table)
        
        if objets:
            logger.info(f"  ✅ {len(objets)} objet(s) | {surface_totale_sig:.2f} m²")
            
            # === LOGS détaillés d'unification / agrégation =========================
            if metadata["nb_raw"] > metadata["nb_grouped"]:
                logger.info(f"   🔧 REGROUPEMENT DÉTECTÉ :")
                logger.info(f"      → {metadata['nb_raw']} entités initiales")
                logger.info(f"      → {metadata['nb_grouped']} groupe(s) après unification")
                logger.info(f"      ──────────────────────────────────────────")
                
                # Détails par groupe
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
                        
                        if chevauchement > 0.01:  # Seuil de 0.01 m² pour éviter les erreurs d'arrondi
                            logger.info(f"         • ⚠️ Chevauchement détecté : {chevauchement:.2f} m² ({pct_chev:.1f}%)")
                            logger.info(f"           → Les géométries se chevauchent partiellement")
                        else:
                            reduction = surface_avant - surface_union
                            if reduction > 0.01:
                                logger.info(f"         • ℹ️ Réduction de {reduction:.2f} m² (arrondis/artefacts)")
                            else:
                                logger.info(f"         • ✅ Pas de chevauchement (géométries adjacentes ou disjointes)")
                        
                        # Calculer la surface moyenne par entité (basée sur la surface réelle après union)
                        if nb_entites > 0:
                            surface_moyenne = surface_union / nb_entites
                            logger.info(f"         • Surface moyenne d'intersection par entité : {surface_moyenne:.2f} m²")
                        
                        logger.info(f"         ──────────────────────────────────────────")
            
            # Résumé regroupement
            if metadata["nb_raw"] > metadata["nb_grouped"]:
                logger.info("   🔧 Résumé regroupement :")
                for item in metadata["items"]:
                    logger.info(f"      -> {item['label']}: "
                                f"{item['count']} entités → {item['surface']} m² "
                                f"(avant union: {item['surface_avant_union']} m², "
                                f"chevauchement: {item['chevauchement_m2']} m²)")
            
            # Cas où on additionne plusieurs morceaux d'une même zone (sans regroupement)
            elif any(item.get("count", 0) > 1 for item in metadata["items"]):
                logger.info(f"   ➕ Plusieurs entités détectées (sans regroupement) :")
                for item in metadata["items"]:
                    if item.get("count", 0) > 1:
                        logger.info(f"      • Zone '{item['label']}' : {item['count']} entités distinctes")
            
            pct_sig = round(surface_totale_sig / area_parcelle_sig * 100, 4)
            pct_real = pct_sig  # Pourcentage réel avant clamp
            
            # Cas où le % dépasse 100 mais a été clampé
            if pct_sig > 100:
                logger.info(f"   ⚠️ Pourcentage réel {pct_real:.2f}% > 100% "
                            f"→ corrigé à 100%.")
                pct_sig = 100.0
            
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "pct_sig": pct_sig,
                "objets": objets
            }
        else:
            logger.info(f"  ❌ Aucune intersection")
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "pct_sig": 0.0,
                "objets": []
            }
    
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
        raise SystemExit("❌ Fournir soit (--section & --numero) soit --geom-wkt")

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
        objets, surface_totale_sig, metadata = calculate_intersection(parcelle_wkt, table)

        if objets:
            logger.info(f"  ✅ {len(objets)} objet(s) | {surface_totale_sig:.2f} m²")
            
            # === LOGS détaillés d'unification / agrégation =========================
            if metadata["nb_raw"] > metadata["nb_grouped"]:
                logger.info(f"   🔧 REGROUPEMENT DÉTECTÉ :")
                logger.info(f"      → {metadata['nb_raw']} entités initiales")
                logger.info(f"      → {metadata['nb_grouped']} groupe(s) après unification")
                logger.info(f"      ──────────────────────────────────────────")
                
                # Détails par groupe
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
                        
                        if chevauchement > 0.01:  # Seuil de 0.01 m² pour éviter les erreurs d'arrondi
                            logger.info(f"         • ⚠️ Chevauchement détecté : {chevauchement:.2f} m² ({pct_chev:.1f}%)")
                            logger.info(f"           → Les géométries se chevauchent partiellement")
                        else:
                            reduction = surface_avant - surface_union
                            if reduction > 0.01:
                                logger.info(f"         • ℹ️ Réduction de {reduction:.2f} m² (arrondis/artefacts)")
                            else:
                                logger.info(f"         • ✅ Pas de chevauchement (géométries adjacentes ou disjointes)")
                        
                        # Calculer la surface moyenne par entité (basée sur la surface réelle après union)
                        if nb_entites > 0:
                            surface_moyenne = surface_union / nb_entites
                            logger.info(f"         • Surface moyenne d'intersection par entité : {surface_moyenne:.2f} m²")
                        
                        logger.info(f"         ──────────────────────────────────────────")
            
            # Résumé regroupement
            if metadata["nb_raw"] > metadata["nb_grouped"]:
                logger.info("   🔧 Résumé regroupement :")
                for item in metadata["items"]:
                    logger.info(f"      -> {item['label']}: "
                                f"{item['count']} entités → {item['surface']} m² "
                                f"(avant union: {item['surface_avant_union']} m², "
                                f"chevauchement: {item['chevauchement_m2']} m²)")
            
            # Cas où on additionne plusieurs morceaux d'une même zone (sans regroupement)
            elif any(item.get("count", 0) > 1 for item in metadata["items"]):
                logger.info(f"   ➕ Plusieurs entités détectées (sans regroupement) :")
                for item in metadata["items"]:
                    if item.get("count", 0) > 1:
                        logger.info(f"      • Zone '{item['label']}' : {item['count']} entités distinctes")
            
            pct_sig = round(surface_totale_sig / area_parcelle_sig * 100, 4)
            pct_real = pct_sig  # Pourcentage réel avant clamp
            
            # Cas où le % dépasse 100 mais a été clampé
            if pct_sig > 100:
                logger.info(f"   ⚠️ Pourcentage réel {pct_real:.2f}% > 100% "
                            f"→ corrigé à 100%.")
                pct_sig = 100.0
            
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "pct_sig": pct_sig,
                "objets": objets
            }
        else:
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "pct_sig": 0.0,
                "objets": []
            }

    # Nettoyage final : retirer toutes les clés de surfaces en m²
    for layer_key, layer in rapport["intersections"].items():
        # On garde pct_sig, on nettoie les surfaces brutes
        layer.pop("surface_sig_m2", None)
        layer.pop("surface_inter_m2", None)
        layer.pop("surface_inter_sig_m2", None)
        layer.pop("surface_parcelle_m2", None)

        for obj in layer.get("objets", []):
            obj.pop("surface_inter_m2", None)
            obj.pop("surface_zone_m2", None)
            obj.pop("surface_parcelle_m2", None)

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