# -*- coding: utf-8 -*-
"""fetch_context.py — Extrait le contexte Enedis textuel d'une parcelle."""

import json
import os
from sqlalchemy import create_engine, text, Engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('SUPABASE_USER')}:{os.getenv('SUPABASE_PASSWORD')}@{os.getenv('SUPABASE_HOST')}:{os.getenv('SUPABASE_PORT')}/{os.getenv('SUPABASE_DB')}"
engine = create_engine(DATABASE_URL)

def get_enedis_context(section: str, numero: str) -> dict:
    sql_query = text("""
        WITH target_parcelle AS (
            SELECT id, section, numero, geom_2154 
            FROM argeles.parcelles 
            WHERE section = :section AND numero = :numero 
            LIMIT 1
        ),
        closest_line AS (
            SELECT DISTINCT ON (l.type) l.type, l.source_id, l.geom_2154
            FROM argeles.reseaux_enedis_lineaires l, target_parcelle tp
            WHERE l.type IN ('reseau-souterrain-bt', 'reseau-bt')
            ORDER BY l.type, l.geom_2154 <-> tp.geom_2154 ASC
        ),
        shortest_paths AS (
            SELECT 
                cl.type, cl.source_id,
                ST_Distance(tp.geom_2154, cl.geom_2154) AS dist_brute,
                ST_ShortestLine(tp.geom_2154, cl.geom_2154) AS geom_path,
                ST_Length(ST_Intersection(cl.geom_2154, ST_Buffer(tp.geom_2154, 20))) AS longueur_dans_zone_rue
            FROM closest_line cl, target_parcelle tp
        ),
        global_intersections AS (
            SELECT l.type, COUNT(l.id) AS nb_geometries,
                   COALESCE(SUM(ST_Length(ST_Intersection(l.geom_2154, tp.geom_2154))), 0) AS total_length
            FROM argeles.reseaux_enedis_lineaires l, target_parcelle tp
            WHERE l.type IN ('reseau-souterrain-bt', 'reseau-bt') AND ST_Intersects(l.geom_2154, tp.geom_2154)
            GROUP BY l.type
        ),
        detect_blocage_voisin AS (
            SELECT sp.type, COUNT(p.id) AS nb_voisins_traverses,
                   STRING_AGG(p.section || ' n°' || p.numero, ', ' ORDER BY p.section, p.numero) AS liste_voisins
            FROM argeles.parcelles p, shortest_paths sp, target_parcelle tp
            WHERE ST_Intersects(p.geom_2154, sp.geom_path) AND p.id != tp.id
              AND ST_Length(ST_Intersection(p.geom_2154, sp.geom_path)) > 0.2
            GROUP BY sp.type
        )
        SELECT 
            sp.type, sp.source_id,
            ROUND(sp.dist_brute::numeric, 2) AS distance_directe_m,
            COALESCE(gi.nb_geometries, 0) AS nb_lignes_dans_parcelle,
            ROUND(COALESCE(gi.total_length, 0)::numeric, 2) AS lineaire_interieur_m,
            COALESCE(db.liste_voisins, 'Aucun') AS voisins_obstacles,
            CASE 
                WHEN sp.dist_brute > 30 THEN 'Réseau Éloigné (' || ROUND(sp.dist_brute::numeric, 0) || 'm) - Extension publique obligatoire'
                WHEN COALESCE(db.nb_voisins_traverses, 0) > 0 THEN 'Raccordement indirect contraint - Traversée de propriété privée (' || db.liste_voisins || ')'
                WHEN sp.longueur_dans_zone_rue >= 15 THEN 'Au droit de la parcelle - Réseau longeant le domaine public'
                ELSE 'Au droit partiel - Réseau en limite, accès public à vérifier'
            END AS diagnostic_expert
        FROM shortest_paths sp
        LEFT JOIN global_intersections gi ON gi.type = sp.type
        LEFT JOIN detect_blocage_voisin db ON db.type = sp.type;
    """)
    
    context = {
        "parcelle_analyse": f"{section} {numero}",
        "reseaux": {}
    }
    
    with engine.connect() as conn:
        results = conn.execute(sql_query, {"section": section, "numero": numero}).mappings().all()
        for row in results:
            tech = "aerien_bt" if "aerien" in row["type"] or "reseau-bt" in row["type"] else "souterrain_bt"
            context["reseaux"][tech] = {
                "id_cable_proche": row["source_id"],
                "distance_m": float(row["distance_directe_m"]),
                "deja_dans_parcelle": {
                    "present": row["nb_lignes_dans_parcelle"] > 0,
                    "lineaire_m": float(row["lineaire_interieur_m"])
                },
                "obstacles_voisins": row["voisins_obstacles"],
                "diagnostic_legal": row["diagnostic_expert"]
            }
            
    return context

if __name__ == "__main__":
    ctx = get_enedis_context("BR", "303")
    print(json.dumps(ctx, indent=2, ensure_ascii=False))