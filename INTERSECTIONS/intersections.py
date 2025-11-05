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
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CATALOGUE_PATH = os.path.join(BASE_DIR, "catalogue_intersections.json")

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
    with engine.connect() as conn:
        try:
            config = CATALOGUE.get(table_name)
            if not config:
                logger.warning(f"  ⚠️  {table_name}: Non catalogué")
                return []
            
            keep_cols = config.get('keep', [])
            if not keep_cols:
                return []
            
            select_cols = ", ".join([f"t.{col}" for col in keep_cols])
            
            query = text(f"""
                WITH p AS (SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g)
                SELECT 
                    {select_cols},
                    ROUND(CAST(ST_Area(ST_Intersection(ST_MakeValid(t.geom_2154), p.g)) AS numeric), 2) AS surface_inter_m2
                FROM {SCHEMA}.{table_name} t, p
                WHERE t.geom_2154 IS NOT NULL
                  AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
                  AND ST_Area(ST_Intersection(ST_MakeValid(t.geom_2154), p.g)) > 0.01
            """)
            
            result = conn.execute(query, {"wkt": parcelle_wkt})
            cols = [col[0] for col in result.cursor.description]
            objects = [dict(zip(cols, row)) for row in result.fetchall()]
            
            # Dédoublonnage
            unique = []
            seen = set()
            for obj in objects:
                # Convertir Decimal en float
                for k, v in obj.items():
                    if hasattr(v, '__class__') and v.__class__.__name__ == 'Decimal':
                        obj[k] = float(v)
                
                key = tuple(obj.values())
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
        area_parcelle = float(conn.execute(
            text("SELECT ST_Area(ST_GeomFromText(:wkt, 2154))"),
            {"wkt": parcelle_wkt}
        ).scalar())
    
    rapport = {
        "parcelle": f"{section} {numero}",
        "surface_m2": round(area_parcelle, 2),
        "intersections": {}
    }
    
    for table, config in CATALOGUE.items():
        logger.info(f"→ {table}")
        
        objets = calculate_intersection(parcelle_wkt, table)
        surface_totale = sum(obj['surface_inter_m2'] for obj in objets)
        
        if objets:
            logger.info(f"  ✅ {len(objets)} objet(s) | {surface_totale:.2f} m²")
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "surface_m2": round(surface_totale, 2),
                "pourcentage": round(surface_totale / area_parcelle * 100, 2),
                "objets": objets
            }
        else:
            logger.info(f"  ❌ Aucune intersection")
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "surface_m2": 0.0,
                "pourcentage": 0.0,
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
<p><strong>Surface:</strong> {data['surface_m2']:,.2f} m² ({data['pourcentage']:.1f}%)</p>
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
        area_parcelle = float(conn.execute(
            text("SELECT ST_Area(ST_GeomFromText(:wkt, 2154))"),
            {"wkt": parcelle_wkt}
        ).scalar())

    rapport = {
        "parcelle": f"{section} {numero}",
        "surface_m2": round(area_parcelle, 2),
        "intersections": {}
    }

    # Lancer l'analyse
    for table, config in CATALOGUE.items():
        logger.info(f"→ {table}")
        objets = calculate_intersection(parcelle_wkt, table)
        surface_totale = sum(obj['surface_inter_m2'] for obj in objets)

        if objets:
            logger.info(f"  ✅ {len(objets)} objet(s) | {surface_totale:.2f} m²")
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "surface_m2": round(surface_totale, 2),
                "pourcentage": round(surface_totale / area_parcelle * 100, 2),
                "objets": objets
            }
        else:
            rapport["intersections"][table] = {
                "nom": config['nom'],
                "type": config['type'],
                "surface_m2": 0.0,
                "pourcentage": 0.0,
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