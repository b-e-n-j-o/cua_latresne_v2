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
    config = CATALOGUE.get(table_name)
    if not config:
        logger.warning(f"  ⚠️  {table_name}: Non catalogué")
        return []

    keep_cols = list(config.get('keep', []) or [])
    group_by = list(config.get('group_by', []) or [])

    if not keep_cols:
        return []

    with engine.connect() as conn:
        try:
            # Colonnes à sélectionner pour les attributs non-groupés
            # (on les agrégera par "première valeur non nulle")
            non_group_kept = [c for c in keep_cols if c not in group_by]

            # Construction SQL selon présence de group_by
            if group_by:
                # ====== MODE "DISSOLVE" PAR GROUP_BY ======
                # On regroupe les géométries par clés (ex: zonage_reglement),
                # puis on intersecte UNE seule géométrie par groupe avec la parcelle.
                # Pour les attributs :
                # - group_by : renvoyés tels quels (clés)
                # - reglementation : on prend la plus longue (souvent la plus complète)
                # - autres colonnes de keep : première valeur non nulle

                gb_cols_sql = ", ".join([f"t.{c}" for c in group_by])

                # Agrégations attributaires
                agg_attr_sql_parts = []
                if "reglementation" in non_group_kept:
                    agg_attr_sql_parts.append(
                        "(array_agg(t.reglementation ORDER BY length(t.reglementation) DESC) FILTER (WHERE t.reglementation IS NOT NULL))[1] AS reglementation"
                    )
                    non_group_kept_copy = [c for c in non_group_kept if c != "reglementation"]
                else:
                    non_group_kept_copy = list(non_group_kept)

                for col in non_group_kept_copy:
                    agg_attr_sql_parts.append(
                        f"(array_agg(t.{col}) FILTER (WHERE t.{col} IS NOT NULL))[1] AS {col}"
                    )

                agg_attr_sql = ",\n                    ".join(agg_attr_sql_parts) if agg_attr_sql_parts else ""

                # Requête avec union par groupe et intersection unique
                query_sql = f"""
                    WITH p AS (
                        SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g
                    ),
                    raw AS (
                        SELECT
                            {gb_cols_sql},
                            ST_UnaryUnion(ST_Collect(ST_MakeValid(t.geom_2154))) AS geom_union
                            {("," if agg_attr_sql else "")}
                            {agg_attr_sql}
                        FROM {SCHEMA}.{table_name} t, p
                        WHERE t.geom_2154 IS NOT NULL
                          AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
                        GROUP BY {gb_cols_sql}
                    )
                    SELECT
                        {", ".join(group_by)},
                        {(", ".join([c for c in non_group_kept_copy]) + "," if non_group_kept_copy else "")}
                        {( "reglementation," if "reglementation" in keep_cols else "" )}
                        ROUND(CAST(ST_Area(ST_Intersection(geom_union, (SELECT g FROM p))) AS numeric), 2) AS surface_inter_m2
                    FROM raw
                    WHERE ST_Intersects(geom_union, (SELECT g FROM p))
                """

            else:
                # ====== MODE STANDARD (pas de dissolve) ======
                select_cols = ", ".join([f"t.{col}" for col in keep_cols])
                query_sql = f"""
                    WITH p AS (SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g)
                    SELECT 
                        {select_cols},
                        ROUND(CAST(ST_Area(ST_Intersection(ST_MakeValid(t.geom_2154), p.g)) AS numeric), 2) AS surface_inter_m2
                    FROM {SCHEMA}.{table_name} t, p
                    WHERE t.geom_2154 IS NOT NULL
                      AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
                """

            result = conn.execute(text(query_sql), {"wkt": parcelle_wkt})
            cols = [col[0] for col in result.cursor.description]
            rows = result.fetchall()
            objects = [dict(zip(cols, row)) for row in rows]

            # Conversions types non JSON
            unique = []
            seen = set()
            for obj in objects:
                # convertir Decimal→float, datetime→iso
                for k, v in obj.items():
                    cls = getattr(v, "__class__", None)
                    name = getattr(cls, "__name__", "")
                    if name == "Decimal":
                        obj[k] = float(v)
                    elif name == "datetime":
                        obj[k] = v.isoformat()

                # Dédoublonnage exact si nécessaire
                key = tuple(sorted(obj.items()))
                if key not in seen:
                    unique.append(obj)
                    seen.add(key)

            return unique

        except Exception as e:
            logger.error(f"  ❌ {table_name}: {e}")
            return []

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
        
        objets = calculate_intersection(parcelle_wkt, table)
        surface_totale_sig = sum(obj['surface_inter_m2'] for obj in objets)
        
        if objets:
            logger.info(f"  ✅ {len(objets)} objet(s) | {surface_totale_sig:.2f} m²")
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "surface_inter_sig_m2": round(surface_totale_sig, 2),
                "pct_sig": round(surface_totale_sig / area_parcelle_sig * 100, 4),
                "objets": objets
            }
        else:
            logger.info(f"  ❌ Aucune intersection")
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "surface_inter_sig_m2": 0.0,
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
<p><strong>Surface:</strong> {data['surface_inter_sig_m2']:,.2f} m² ({data['pct_sig']:.4f}%)</p>
<table>
<tr>
"""
                # Headers
                for key in data['objets'][0].keys():
                    html += f"<th>{key}</th>"
                html += "</tr>\n"
                
                # Rows
                for obj in data['objets']:
                    html += "<tr>"
                    for val in obj.values():
                        html += f"<td>{val}</td>"
                    html += "</tr>\n"
                
                html += "</table></div>\n"
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
        objets = calculate_intersection(parcelle_wkt, table)
        surface_totale_sig = sum(obj['surface_inter_m2'] for obj in objets)

        if objets:
            logger.info(f"  ✅ {len(objets)} objet(s) | {surface_totale_sig:.2f} m²")
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "surface_inter_sig_m2": round(surface_totale_sig, 2),
                "pct_sig": round(surface_totale_sig / area_parcelle_sig * 100, 4),
                "objets": objets
            }
        else:
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "surface_inter_sig_m2": 0.0,
                "pct_sig": 0.0,
                "objets": []
            }

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