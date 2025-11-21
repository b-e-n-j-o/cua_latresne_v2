#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
test_intersections_standalone.py
--------------------------------
Script minimal pour tester les intersections entre une unité foncière (WKT)
et les couches réglementaires présentes en base PostGIS.

Ce script :
 - lit un WKT de parcelle / unité foncière
 - lit le catalogue `catalogue_intersections_tagged.json`
 - calcule ST_Intersection pour chaque table
 - renvoie pct_sig + objets
"""

import os
import json
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ============================================================
# CONFIG
# ============================================================
load_dotenv()

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")

SCHEMA = "latresne"

DATABASE_URL = (
    f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@"
    f"{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
)

engine = create_engine(DATABASE_URL)

# ============================================================
# LOADER CATALOGUE
# ============================================================
def load_catalogue():
    CATALOGUE_PATH = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/catalogues/catalogue_intersections_tagged.json"
    with open(CATALOGUE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

CATALOGUE = load_catalogue()


# ============================================================
# CALCUL INTERSECTION
# ============================================================
def calculate_intersection(parcelle_wkt, table_name, config):
    keep_cols = config.get("keep") or []
    group_by_config = config.get("group_by")

    # Toujours convertir group_by en LISTE
    if not group_by_config:
        group_by = []
    elif isinstance(group_by_config, str):
        group_by = [group_by_config]   # "nom" -> ["nom"]
    else:
        group_by = list(group_by_config)

    if not keep_cols:
        return [], 0.0, {"nb_raw": 0, "nb_grouped": 0, "items": []}

    # ------------------------------------------------------
    # 1️⃣ Pas de regroupement → mode simple
    # ------------------------------------------------------
    if not group_by:
        select_cols = ", ".join([f"t.{c}" for c in keep_cols])

        query = f"""
            WITH p AS (
                SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g
            )
            SELECT
                {select_cols},
                ROUND(CAST(ST_Area(ST_Intersection(ST_MakeValid(t.geom_2154), p.g)) AS numeric), 2)
                    AS surface_inter_m2
            FROM {SCHEMA}.{table_name} t, p
            WHERE t.geom_2154 IS NOT NULL
              AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g);
        """

        with engine.connect() as conn:
            rs = conn.execute(text(query), {"wkt": parcelle_wkt})
            cols = [c[0] for c in rs.cursor.description]
            rows = rs.fetchall()

        raw_obj = []
        for row in rows:
            row_dict = dict(zip(cols, row))
            surface = float(row_dict.pop("surface_inter_m2", 0) or 0)
            raw_obj.append({"obj": row_dict, "surface": surface})

        objects = [x["obj"] for x in raw_obj]
        surfaces = [x["surface"] for x in raw_obj]

        metadata = {
            "nb_raw": len(raw_obj),
            "nb_grouped": len(raw_obj),
            "items": [{"label": "N/A", "count": 1, "surface": s} for s in surfaces],
        }

        return objects, sum(surfaces), metadata

    # ------------------------------------------------------
    # 2️⃣ Regroupement avec UNION géométrique
    # ------------------------------------------------------
    if group_by:
        # Colonnes utilisées pour le groupement
        gb_cols_sql = ", ".join([f"t.{c}" for c in group_by])

        # Colonnes à ramener après union (toutes sauf group_by)
        non_group_kept = [c for c in keep_cols if c not in group_by]

        # Préparer extraction attributaire
        agg_attrs = []
        for col in non_group_kept:
            agg_attrs.append(
                f"(array_agg(t.{col}) FILTER (WHERE t.{col} IS NOT NULL))[1] AS {col}"
            )
        agg_attrs_sql = ", " + ", ".join(agg_attrs) if agg_attrs else ""

        # Construction du SELECT final (depuis raw)
        select_cols_final = ", ".join(group_by)

        if non_group_kept:
            select_cols_final += ", " + ", ".join(non_group_kept)

        # Requête pour récupérer les détails AVANT union (surfaces individuelles)
        query_details = f"""
            WITH p AS (
                SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g
            )
            SELECT
                {gb_cols_sql},
                COUNT(*) AS nb_entites,
                ROUND(CAST(SUM(ST_Area(ST_Intersection(ST_MakeValid(t.geom_2154), p.g))) AS numeric), 2) AS somme_surfaces_brutes
            FROM {SCHEMA}.{table_name} t, p
            WHERE t.geom_2154 IS NOT NULL
              AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
            GROUP BY {gb_cols_sql}
        """

        # Construction de la requête UNION correcte
        query_union = f"""
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
              AND NOT ST_IsEmpty(geom_union);
        """

        # Récupérer les détails avant union
        with engine.connect() as conn:
            rs_details = conn.execute(text(query_details), {"wkt": parcelle_wkt})
            cols_details = [c[0] for c in rs_details.cursor.description]
            rows_details = rs_details.fetchall()
        
        details_dict = {}
        for row in rows_details:
            row_dict = dict(zip(cols_details, row))
            key = tuple(row_dict.get(c) for c in group_by)
            details_dict[key] = {
                "nb_entites": row_dict.get("nb_entites", 0),
                "somme_surfaces_brutes": float(row_dict.get("somme_surfaces_brutes", 0) or 0)
            }

        # Exécuter la requête UNION
        with engine.connect() as conn:
            rs = conn.execute(text(query_union), {"wkt": parcelle_wkt})
            cols = [c[0] for c in rs.cursor.description]
            rows = rs.fetchall()

        objects = []
        surfaces = []
        metadata_items = []

        for row in rows:
            row_dict = dict(zip(cols, row))

            surf = float(row_dict.pop("union_area", 0) or 0)
            surfaces.append(surf)

            # Construire l'objet
            props = row_dict.copy()

            # Label (group_by)
            key = tuple(row_dict.get(c) for c in group_by)
            key_label = " / ".join(str(v) for v in key)

            objects.append(props)

            # Récupérer les détails pour ce groupe
            details = details_dict.get(key, {"nb_entites": 0, "somme_surfaces_brutes": 0})
            nb_entites = details["nb_entites"]
            somme_brutes = details["somme_surfaces_brutes"]
            
            # Calculer le chevauchement
            chevauchement_m2 = somme_brutes - surf if somme_brutes > 0 else 0
            pct_chevauchement = (chevauchement_m2 / somme_brutes * 100) if somme_brutes > 0 else 0

            metadata_items.append({
                "label": key_label,
                "count": nb_entites,
                "surface": surf,
                "surface_avant_union": somme_brutes,
                "chevauchement_m2": round(chevauchement_m2, 2),
                "pct_chevauchement": round(pct_chevauchement, 2)
            })

        # Compte brut initial
        with engine.connect() as conn:
            count_raw = conn.execute(
                text(f"""
                    WITH p AS (
                        SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g
                    )
                    SELECT COUNT(*)
                    FROM {SCHEMA}.{table_name} t, p
                    WHERE t.geom_2154 IS NOT NULL
                      AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
                """),
                {"wkt": parcelle_wkt}
            ).scalar()

        metadata = {
            "nb_raw": count_raw,
            "nb_grouped": len(objects),
            "items": metadata_items
        }

        return objects, sum(surfaces), metadata


# ============================================================
# MAIN TEST
# ============================================================
def test_intersections(wkt_path):
    # lecture du WKT
    parcelle_wkt = Path(wkt_path).read_text(encoding="utf-8").strip()

    # surface totale SIG
    with engine.connect() as conn:
        area_sig = float(conn.execute(
            text("SELECT ST_Area(ST_GeomFromText(:wkt, 2154))"),
            {"wkt": parcelle_wkt}
        ).scalar())

    print("\n========================================")
    print("📐 TEST INTERSECTIONS")
    print("UF WKT :", wkt_path)
    print(f"Surface SIG : {area_sig:.2f} m²")
    print("========================================\n")

    results = {}

    for table, config in CATALOGUE.items():
        print(f"→ {table} ({config.get('nom')})")

        objets, surface, metadata = calculate_intersection(parcelle_wkt, table, config)

        if surface > 0:
            pct = (surface / area_sig) * 100
            print(f"   ✅ {len(objets)} objets | {surface:.2f} m² | {pct:.3f}%")
            
            # === LOGS détaillés d'unification / agrégation =========================
            if metadata["nb_raw"] > metadata["nb_grouped"]:
                print(f"   🔧 REGROUPEMENT DÉTECTÉ :")
                print(f"      → {metadata['nb_raw']} entités initiales")
                print(f"      → {metadata['nb_grouped']} groupe(s) après unification")
                print(f"      ──────────────────────────────────────────")
                
                # Détails par groupe
                for item in metadata["items"]:
                    if item.get("count", 0) > 1:
                        label = item["label"]
                        nb_entites = item.get("count", 0)
                        surface_union = item.get("surface", 0)
                        surface_avant = item.get("surface_avant_union", 0)
                        chevauchement = item.get("chevauchement_m2", 0)
                        pct_chev = item.get("pct_chevauchement", 0)
                        
                        print(f"      📦 Groupe '{label}' :")
                        print(f"         • {nb_entites} entités regroupées")
                        print(f"         • Surface avant union (somme) : {surface_avant:.2f} m²")
                        print(f"         • Surface après union : {surface_union:.2f} m²")
                        
                        if chevauchement > 0.01:  # Seuil de 0.01 m² pour éviter les erreurs d'arrondi
                            print(f"         • ⚠️ Chevauchement détecté : {chevauchement:.2f} m² ({pct_chev:.1f}%)")
                            print(f"           → Les géométries se chevauchent partiellement")
                        else:
                            reduction = surface_avant - surface_union
                            if reduction > 0.01:
                                print(f"         • ℹ️ Réduction de {reduction:.2f} m² (arrondis/artefacts)")
                            else:
                                print(f"         • ✅ Pas de chevauchement (géométries adjacentes ou disjointes)")
                        
                        # Calculer la surface moyenne par entité (basée sur la surface réelle après union)
                        if nb_entites > 0:
                            surface_moyenne = surface_union / nb_entites
                            print(f"         • Surface moyenne d'intersection par entité : {surface_moyenne:.2f} m²")
                        
                        print(f"         ──────────────────────────────────────────")
            
            # Cas où on additionne plusieurs morceaux d'une même zone (sans regroupement)
            elif any(item.get("count", 0) > 1 for item in metadata["items"]):
                print(f"   ➕ Plusieurs entités détectées (sans regroupement) :")
                for item in metadata["items"]:
                    if item.get("count", 0) > 1:
                        print(f"      • Zone '{item['label']}' : {item['count']} entités distinctes")
        else:
            print("   ✗ aucune intersection")

        pct_sig = round((surface / area_sig * 100), 4) if area_sig else 0.0
        pct_real = pct_sig  # Pourcentage réel avant clamp
        
        # Cas où le % dépasse 100 mais a été clampé
        if pct_sig > 100:
            print(f"   ⚠️ Pourcentage réel {pct_real:.2f}% > 100% "
                  f"→ corrigé à 100%.")
            pct_sig = 100.0
        
        results[table] = {
            "nom": config.get("nom"),
            "pct_sig": pct_sig,
            "objets": objets
        }

    return results


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Test intersections sur une UF WKT")
    ap.add_argument("--wkt", required=True, help="Chemin vers geom_unite_fonciere.wkt")
    args = ap.parse_args()

    res = test_intersections(args.wkt)

    # ---------- Nettoyage JSON global ----------
    def clean(obj):
        if isinstance(obj, dict):
            return {k: clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [clean(v) for v in obj]
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return obj

    res = clean(res)
    # -------------------------------------------

    out = Path("test_intersections_output.json")
    out.write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\n\n🎉 Résultats sauvegardés dans :", out)
