# -*- coding: utf-8 -*-
"""generate_map.py — Génère une carte PNG annotée pour le contexte VLM."""

import os
import geopandas as gpd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from shapely import wkt
from sqlalchemy import create_engine

load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('SUPABASE_USER')}:{os.getenv('SUPABASE_PASSWORD')}@{os.getenv('SUPABASE_HOST')}:{os.getenv('SUPABASE_PORT')}/{os.getenv('SUPABASE_DB')}"
engine = create_engine(DATABASE_URL)

def generate_annotated_map(section: str, numero: str, output_path: str = "zone_etude.png", buffer_m: float = 100.0):
    # 1. Récupérer la parcelle cible et calculer son Centroïde / Buffer de 100m
    query_target = f"""
        SELECT id, section, numero, geom_2154 
        FROM argeles.parcelles 
        WHERE section = '{section}' AND numero = '{numero}' LIMIT 1
    """
    gdf_target = gpd.read_postgis(query_target, engine, geom_col="geom_2154", crs="EPSG:2154")
    if gdf_target.empty:
        print("Parcelle introuvable.")
        return

    # Création de la géométrie du buffer d'étude (100m)
    target_geom = gdf_target.iloc[0]["geom_2154"]
    study_buffer = target_geom.buffer(buffer_m)
    buffer_wkt = study_buffer.wkt

    # 2. Charger toutes les parcelles situées dans ce rayon de 100m
    query_neighbors = f"""
        SELECT section, numero, geom_2154 
        FROM argeles.parcelles 
        WHERE ST_DWithin(geom_2154, ST_GeomFromText('{buffer_wkt}', 2154), 0)
    """
    gdf_neighbors = gpd.read_postgis(query_neighbors, engine, geom_col="geom_2154", crs="EPSG:2154")

    # 3. Charger les lignes Enedis situées dans ce rayon de 100m
    query_enedis = f"""
        SELECT type, geom_2154 
        FROM argeles.reseaux_enedis_lineaires 
        WHERE ST_DWithin(geom_2154, ST_GeomFromText('{buffer_wkt}', 2154), 0)
    """
    gdf_enedis = gpd.read_postgis(query_enedis, engine, geom_col="geom_2154", crs="EPSG:2154")

    # 4. Initialisation du plot Matplotlib (Résolution standard 800x600 équivalente)
    fig, ax = plt.subplots(figsize=(10, 8), dpi=100)

    # Dessiner les parcelles voisines (Fond blanc, contours noirs fins)
    gdf_neighbors.plot(ax=ax, facecolor="#FDFDFD", edgecolor="#2C2C2C", linewidth=0.6)

    # Annoter les parcelles avec leur numéro (uniquement celles visibles)
    for idx, row in gdf_neighbors.iterrows():
        centroid = row["geom_2154"].centroid
        ax.text(centroid.x, centroid.y, f"{row['section']}{row['numero']}", 
                fontsize=7, ha='center', color="#555555", weight='bold')

    # Dessiner les réseaux Enedis (Code couleur distinct)
    if not gdf_enedis.empty:
        # Séparation Aérien (Violet continu) / Souterrain (Bleu tirets)
        aerien = gdf_enedis[gdf_enedis["type"].str.contains("aerien|reseau-bt|reseau-hta", na=False)]
        souterrain = gdf_enedis[gdf_enedis["type"].str.contains("souterrain", na=False)]
        
        if not aerien.empty:
            aerien.plot(ax=ax, color="#6A0D91", linewidth=2, label="Réseau Aérien (Enedis)")
        if not souterrain.empty:
            souterrain.plot(ax=ax, color="#0055FF", linewidth=1.5, linestyle="--", label="Réseau Souterrain (Enedis)")

    # Dessiner la parcelle Cible (Contour Rouge épais très visible)
    gdf_target.plot(ax=ax, facecolor="none", edgecolor="#FF0000", linewidth=2.5, label="Parcelle Cible")

    # Cadrer la carte STRICTEMENT sur l'emprise du buffer de 100m
    minx, miny, maxx, maxy = study_buffer.bounds
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)

    # Nettoyage cosmétique pour l'IA (On enlève les axes de coordonnées inutiles)
    ax.set_axis_off()
    plt.title(f"Plan de situation Viabilisation Enedis — Parcelle {section} n°{numero}", fontsize=12, weight='bold', pad=10)
    plt.legend(loc="lower right", fontsize=8)

    # Sauvegarde
    plt.savefig(output_path, bbox_inches='tight', dpi=150)
    plt.close()
    print(f" Carte PNG générée avec succès : {output_path}")

if __name__ == "__main__":
    generate_annotated_map("BR", "303", "context_vlm_BR303.png", buffer_m=100.0)