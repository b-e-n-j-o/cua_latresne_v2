#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
map_3d.py
----------------------------------------------------
Génère une carte 3D Pydeck avec zonages surfaciques projetés sur le terrain.
Version complète avec légende interactive et support catalogue.
"""

import os
import json
import logging
import geopandas as gpd
import pydeck as pdk
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ============================================================
# IMPORT DES UTILITAIRES
# ============================================================
from map_utils import (
    random_color,
    clean_properties,
    get_layers_on_parcel_with_buffer,
)

# ============================================================
# CONFIG LOGGING
# ============================================================
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("carte_3d")

# ============================================================
# CONNEXION BASE DE DONNÉES
# ============================================================
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")

DATABASE_URL = (
    f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@"
    f"{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
)

logger.info("🔌 Connexion à la base de données...")
try:
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={
            "connect_timeout": 10,
            "sslmode": "require"
        }
    )
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        version = result.fetchone()[0]
        logger.info(f"✅ Connexion établie : {version[:50]}...")
        
except Exception as e:
    logger.error(f"❌ Erreur de connexion : {e}")
    raise

SCHEMA = "latresne"
CATALOGUE_PATH = os.path.join(os.path.dirname(__file__), "catalogue_couches_map.json")
BUFFER_DIST = 200

logger.info(f"📂 Chargement du catalogue depuis {CATALOGUE_PATH}...")
with open(CATALOGUE_PATH, "r", encoding="utf-8") as f:
    CATALOGUE = json.load(f)
logger.info(f"✅ {len(CATALOGUE)} couches dans le catalogue")

# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================

def hex_to_rgb(hex_color):
    """Convertit une couleur hex (#RRGGBB) en liste RGB [R, G, B]"""
    hex_color = hex_color.lstrip('#')
    return [int(hex_color[i:i+2], 16) for i in (0, 2, 4)]


def polygon_to_coordinates(geometry):
    """Convertit une géométrie Shapely en format Pydeck coordinates"""
    if geometry.geom_type == 'Polygon':
        return [list(geometry.exterior.coords)]
    elif geometry.geom_type == 'MultiPolygon':
        return [list(poly.exterior.coords) for poly in geometry.geoms]
    else:
        return []


# ============================================================
# GÉNÉRATION DE LA CARTE 3D COMPLÈTE
# ============================================================

def generate_map_3d_from_wkt(wkt_path, inclure_ppri=False, code_insee="33234"):
    """
    Génère une carte 3D Pydeck avec toutes les couches urbanistiques.
    
    Args:
        wkt_path (str): Chemin vers le fichier WKT
        inclure_ppri (bool): Inclure le PPRI (TODO)
        code_insee (str): Code INSEE de la commune
    
    Returns:
        tuple: (html_string, metadata)
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"🌍 GÉNÉRATION CARTE 3D")
    logger.info(f"{'='*60}\n")

    # ============================================================
    # ÉTAPE 1 : Lecture du WKT
    # ============================================================
    wkt_path = Path(wkt_path).resolve()
    if not wkt_path.exists():
        logger.error(f"❌ Fichier WKT introuvable : {wkt_path}")
        raise FileNotFoundError(f"Fichier WKT introuvable : {wkt_path}")

    logger.info(f"📄 Étape 1/4 : Lecture du fichier WKT : {wkt_path}")
    wkt_geom = wkt_path.read_text(encoding="utf-8").strip()
    logger.info(f"📏 Longueur du WKT : {len(wkt_geom)} caractères")

    gdf_parcelle = gpd.GeoDataFrame(
        geometry=[gpd.GeoSeries.from_wkt([wkt_geom])[0]], 
        crs="EPSG:2154"
    )
    gdf_parcelle_4326 = gdf_parcelle.to_crs(4326)
    centroid = gdf_parcelle_4326.geometry.iloc[0].centroid
    logger.info(f"   ✅ Centroïde : lat={centroid.y:.6f}, lon={centroid.x:.6f}")

    # ============================================================
    # ÉTAPE 2 : Recherche des couches intersectant
    # ============================================================
    logger.info("\n🔍 Étape 2/4 : Recherche couches...")
    layers_on_parcel = get_layers_on_parcel_with_buffer(
        engine, SCHEMA, CATALOGUE, wkt_geom, BUFFER_DIST
    )
    logger.info(f"   ✅ {len(layers_on_parcel)} couches trouvées")

    # Tri par importance (Zonage → Servitudes → Prescriptions → Informations)
    ordre_types = {
        "Zonage PLU": 1,
        "Servitudes": 2,
        "Prescriptions": 3,
        "Informations": 4
    }
    
    layers_on_parcel = dict(
        sorted(
            layers_on_parcel.items(),
            key=lambda item: ordre_types.get(
                CATALOGUE.get(item[0], {}).get("type", ""), 999
            )
        )
    )
    
    logger.info(f"   🔄 Couches triées par importance")

    # ============================================================
    # ÉTAPE 3 : Génération des couches Pydeck
    # ============================================================
    logger.info("\n🎨 Étape 3/4 : Génération des couches Pydeck...")
    
    pydeck_layers = []
    registry = {
        "mapVar": "deckgl",
        "layers": []
    }
    
    for table, config in layers_on_parcel.items():
        nom = config.get("nom", table)
        keep = config.get("keep", [])
        attribut_map = config.get("attribut_map", None)
        attribut_split = config.get("attribut_split", None)
        color = random_color()
        
        logger.info(f"   📊 {nom}...")
        
        # Déterminer le mode d'affichage
        mode_couche_entiere = (
            attribut_map is None or 
            attribut_map == "None" or 
            attribut_map == "" or
            str(attribut_map).lower() == "none"
        )
        
        if mode_couche_entiere:
            logger.info(f"      🎯 Mode : Couche entière")
        else:
            logger.info(f"      🔑 Mode : Groupement par '{attribut_map}'")

        # Construction de la requête SQL
        if not mode_couche_entiere and attribut_map:
            select_cols_list = list(keep[:3]) if keep else []
            if attribut_map not in select_cols_list:
                select_cols_list.insert(0, attribut_map)
            if attribut_split and attribut_split not in select_cols_list:
                select_cols_list.append(attribut_split)
            select_cols = ", ".join(select_cols_list)
        else:
            select_cols = ", ".join(keep[:3]) if keep else "gml_id"

        q = f"""
            WITH
              p AS (SELECT ST_GeomFromText('{wkt_geom}',2154) AS g),
              centroid AS (SELECT ST_Centroid(g) AS c FROM p),
              buffer AS (SELECT ST_Buffer(c,{BUFFER_DIST}) AS b FROM centroid)
            SELECT
              ST_AsGeoJSON(ST_Transform(ST_Intersection(ST_MakeValid(t.geom_2154), buffer.b),4326)) AS geom,
              ROW_NUMBER() OVER() AS fid,
              {select_cols}
            FROM {SCHEMA}.{table} t, p, buffer
            WHERE t.geom_2154 IS NOT NULL
              AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
            LIMIT 300;
        """

        try:
            with engine.connect() as conn:
                rs = conn.execute(text(q))
                rows = rs.fetchall()
                if not rows:
                    logger.warning(f"      ⚠️  Aucune entité")
                    continue
                keys = list(rs.keys())
        except Exception as e:
            logger.error(f"      ❌ ERREUR : {e}")
            continue

        # ============================================================
        # Traitement selon le mode
        # ============================================================
        if mode_couche_entiere:
            # MODE 1 : Couche entière
            all_features_data = []
            for idx, row in enumerate(rows, start=1):
                try:
                    geom = json.loads(row[0])
                    props_raw = {keys[j + 1]: str(row[j + 1]) for j in range(len(keys) - 1)}
                    
                    props_clean = clean_properties(props_raw, nom)
                    
                    ignore_patterns = ["id", "uuid", "gid", "fid", "globalid"]
                    props_full = {
                        k: v for k, v in props_raw.items()
                        if not any(pat in k.lower() for pat in ignore_patterns)
                    }
                    props_full = {"__layer_name__": nom, **props_full}
                    
                    all_features_data.append({
                        "geometry": geom,
                        "props_clean": props_clean,
                        "props_full": props_full,
                        "fid": idx
                    })
                except Exception as e:
                    logger.error(f"      ❌ Entité {idx} : {e}")
                    continue

            if not all_features_data:
                logger.warning(f"      ⚠️  Aucune feature valide")
                continue

            # Création du GeoDataFrame
            features = []
            for feat_data in all_features_data:
                features.append({
                    "type": "Feature",
                    "geometry": feat_data["geometry"],
                    "properties": {
                        **feat_data["props_clean"],
                        "props_full": json.dumps(feat_data["props_full"]),
                        "entity_id": f"entity_entire_{len(registry['layers'])}_{feat_data['fid']}"
                    }
                })
            
            gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
            gdf['coordinates'] = gdf.geometry.apply(polygon_to_coordinates)
            rgb_color = hex_to_rgb(color)
            gdf['fill_color'] = [[*rgb_color, 150]] * len(gdf)
            gdf['line_color'] = [[255, 255, 255, 200]] * len(gdf)
            
            # Couche Pydeck
            layer = pdk.Layer(
                "PolygonLayer",
                data=gdf,
                id=f"layer_entire_{len(registry['layers'])}",
                get_polygon="coordinates",
                get_fill_color="fill_color",
                get_line_color="line_color",
                get_line_width=2,
                pickable=True,
                extruded=False,
                auto_highlight=True,
                highlight_color=[255, 255, 0, 200],
                visible=False  # Caché par défaut
            )
            
            pydeck_layers.append(layer)
            
            logger.info(f"      ✅ 1 groupe (couche entière avec {len(all_features_data)} entité(s))")
            
            registry["layers"].append({
                "name": nom,
                "color": color,
                "mode": "entire",
                "attribut_map": None,
                "nom_attribut_map": "",
                "layer_id": f"layer_entire_{len(registry['layers'])}",
                "entities": [{
                    "name": nom,
                    "entity_ids": [f["properties"]["entity_id"] for f in features],
                    "count": len(all_features_data)
                }]
            })
            
        else:
            # MODE 2 : Groupement par attribut
            grouped_entities = {}
            entity_split_values = {}
            
            for idx, row in enumerate(rows, start=1):
                try:
                    geom = json.loads(row[0])
                    props_raw = {keys[j + 1]: str(row[j + 1]) for j in range(len(keys) - 1)}
                    
                    props_clean = clean_properties(props_raw, nom)
                    
                    ignore_patterns = ["id", "uuid", "gid", "fid", "globalid"]
                    props_full = {
                        k: v for k, v in props_raw.items()
                        if not any(pat in k.lower() for pat in ignore_patterns)
                    }
                    props_full = {"__layer_name__": nom, **props_full}

                    # Déterminer la valeur de groupement
                    if attribut_map in props_clean:
                        group_value = props_clean[attribut_map]
                        if not group_value or group_value.lower() in ['none', 'null', '']:
                            group_value = f"Entité #{idx}"
                    else:
                        group_value = next(
                            (v for k, v in props_clean.items()
                             if v and v.lower() not in ['none', 'null'] and k.lower() not in {"id","gid","uuid","fid"} and k != "__layer_name__"),
                            f"Entité #{idx}"
                        )

                    # Valeur de split pour sous-groupes
                    split_value = None
                    if attribut_split and attribut_split in props_clean:
                        split_value = props_clean[attribut_split]
                        if not split_value or split_value.lower() in ['none', 'null', '']:
                            split_value = "Autres"

                    entity_id = f"entity_{len(registry['layers'])}_{idx}"
                    
                    if group_value not in grouped_entities:
                        grouped_entities[group_value] = []
                    
                    grouped_entities[group_value].append({
                        "geometry": geom,
                        "props_clean": props_clean,
                        "props_full": props_full,
                        "entity_id": entity_id,
                        "split_value": split_value
                    })
                        
                except Exception as e:
                    logger.error(f"      ❌ Entité {idx} : {e}")
                    continue

            # Création des GeoDataFrames et couches par groupe
            entities_registry = []
            
            for group_value, entity_list in grouped_entities.items():
                features = []
                for entity_data in entity_list:
                    features.append({
                        "type": "Feature",
                        "geometry": entity_data["geometry"],
                        "properties": {
                            **entity_data["props_clean"],
                            "props_full": json.dumps(entity_data["props_full"]),
                            "entity_id": entity_data["entity_id"]
                        }
                    })
                
                gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
                gdf['coordinates'] = gdf.geometry.apply(polygon_to_coordinates)
                rgb_color = hex_to_rgb(color)
                gdf['fill_color'] = [[*rgb_color, 150]] * len(gdf)
                gdf['line_color'] = [[255, 255, 255, 200]] * len(gdf)
                
                layer_id = f"layer_{len(registry['layers'])}_{group_value.replace(' ', '_')}"
                
                layer = pdk.Layer(
                    "PolygonLayer",
                    data=gdf,
                    id=layer_id,
                    get_polygon="coordinates",
                    get_fill_color="fill_color",
                    get_line_color="line_color",
                    get_line_width=2,
                    pickable=True,
                    extruded=False,
                    auto_highlight=True,
                    highlight_color=[255, 255, 0, 200],
                    visible=False  # Caché par défaut
                )
                
                pydeck_layers.append(layer)
                
                # Récupérer la valeur de split
                split_value_for_entity = entity_list[0].get("split_value") if entity_list else None
                
                entities_registry.append({
                    "name": group_value,
                    "layer_id": layer_id,
                    "entity_ids": [e["entity_id"] for e in entity_list],
                    "count": len(entity_list),
                    "split_value": split_value_for_entity
                })

            logger.info(f"      ✅ {len(entities_registry)} groupe(s) ({sum(e['count'] for e in entities_registry)} entités)")
            
            registry["layers"].append({
                "name": nom,
                "color": color,
                "mode": "grouped",
                "attribut_map": attribut_map,
                "nom_attribut_map": config.get("nom_attribut_map", ""),
                "attribut_split": attribut_split,
                "entities": entities_registry
            })

    # ============================================================
    # Ajout de la parcelle
    # ============================================================
    logger.info("\n📦 Ajout de la parcelle...")
    gdf_parcelle_4326['coordinates'] = gdf_parcelle_4326.geometry.apply(polygon_to_coordinates)
    gdf_parcelle_4326['fill_color'] = [[255, 0, 0, 0]] * len(gdf_parcelle_4326)
    gdf_parcelle_4326['line_color'] = [[255, 0, 0, 255]] * len(gdf_parcelle_4326)
    
    parcelle_layer = pdk.Layer(
        "PolygonLayer",
        data=gdf_parcelle_4326,
        id="parcelle",
        get_polygon="coordinates",
        get_fill_color="fill_color",
        get_line_color="line_color",
        get_line_width=4,
        line_width_min_pixels=2,
        pickable=False,
        visible=True  # Toujours visible
    )
    
    pydeck_layers.append(parcelle_layer)
    logger.info("   ✅ Parcelle ajoutée")

    total_groups = sum(len(l['entities']) for l in registry['layers'])
    total_entities = sum(sum(e['count'] for e in l['entities']) for l in registry['layers'])
    logger.info(f"\n   ✅ TOTAL : {len(registry['layers'])} couches, {total_groups} groupes, {total_entities} entités")

    # ============================================================
    # ÉTAPE 4 : Création de la carte avec légende
    # ============================================================
    logger.info("\n🗺️  Étape 4/4 : Création de la carte avec légende...")
    
    view_state = pdk.ViewState(
        latitude=centroid.y,
        longitude=centroid.x,
        zoom=16,
        pitch=45,
        bearing=0,
        height=800
    )
    
    deck = pdk.Deck(
        layers=pydeck_layers,
        initial_view_state=view_state,
        map_style="https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
        tooltip={
            "html": "<b>{__layer_name__}</b><br/>{props_display}",
            "style": {
                "backgroundColor": "white",
                "color": "#333",
                "fontSize": "12px",
                "padding": "8px",
                "borderRadius": "4px",
                "border": "1px solid #ccc"
            }
        },
        parameters={
            "controller": True,
            "dragRotate": True,
            "touchRotate": True,
            "keyboard": True
        }
    )
    
    html_base = deck.to_html(as_string=True)
    logger.info("   ✅ Carte Pydeck générée")
    
    # ============================================================
    # LÉGENDE INTERACTIVE (identique à map_2d.py)
    # ============================================================
    logger.info("\n🏷️  Génération de la légende...")
    
    # Groupement par type
    grouped_layers = {}
    for layer in registry["layers"]:
        table_name = next((t for t, cfg in layers_on_parcel.items() if cfg.get("nom") == layer["name"]), None)
        if table_name:
            type_couche = CATALOGUE.get(table_name, {}).get("type", "Autres")
        else:
            type_couche = "Autres"
        grouped_layers.setdefault(type_couche, []).append(layer)

    ordre_types_legend = ["Zonage PLU", "Servitudes", "Prescriptions", "Informations", "Autres"]
    sorted_grouped_layers = [
        (t, grouped_layers[t]) for t in ordre_types_legend if t in grouped_layers
    ]
    
    layers_json = json.dumps(sorted_grouped_layers, ensure_ascii=False)
    
    legend_html = f"""
<div id="legend-panel" style="
  position:absolute;right:10px;top:10px;z-index:500;
  background:white;border:1px solid #ccc;border-radius:8px;
  box-shadow:0 2px 8px rgba(0,0,0,0.15);max-height:70vh;
  overflow:auto;font:12px/1.4 'Inter',sans-serif;padding:12px;
  width:360px;">
  <b style="font-size:14px;display:block;margin-bottom:8px;">📍 Surfaces urbanistiques 3D</b>
  <div id="legend-content"></div>
</div>
"""
    
    legend_css = """
<style>
  .legend-layer {
    margin: 8px 0;
    padding: 8px;
    background: #f9f9f9;
    border-radius: 6px;
    border: 1px solid #e5e5e5;
  }
  .legend-group-title {
    font-weight: 700;
    font-size: 13px;
    margin-top: 12px;
    color: #2a2a2a;
    border-bottom: 1px solid #ddd;
    padding-bottom: 4px;
  }
  .layer-header {
    display: flex;
    align-items: center;
    gap: 8px;
    cursor: pointer;
    user-select: none;
    padding: 4px 0;
  }
  .toggle-arrow {
    width: 16px;
    text-align: center;
    font-weight: bold;
    color: #666;
    transition: transform 0.2s ease;
    font-size: 11px;
  }
  .toggle-arrow.open {
    transform: rotate(90deg);
  }
  .color-swatch {
    width: 16px;
    height: 16px;
    border-radius: 3px;
    border: 1px solid #999;
    flex-shrink: 0;
  }
  .layer-title {
    flex: 1;
    font-weight: 600;
    color: #333;
    display: block;
  }
  .entities-list {
    margin-left: 24px;
    margin-top: 6px;
    display: none;
  }
  .entities-list.open {
    display: block;
  }
  .entity-item {
    padding: 4px 0;
  }
  .entity-item label {
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .entity-badge {
    font-size: 9px;
    color: #666;
    background: #e8e8e8;
    padding: 1px 5px;
    border-radius: 8px;
    margin-left: auto;
  }
  .layer-checkbox {
    width: 16px;
    height: 16px;
    cursor: pointer;
    margin: 0;
    vertical-align: middle;
  }
  .split-group {
    margin-top: 6px;
    padding: 4px 0;
  }
  .split-header {
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    color: #333;
    padding-left: 8px;
  }
  .split-header .toggle-arrow {
    width: 12px;
    text-align: center;
    transition: transform 0.2s ease;
    font-size: 10px;
    color: #666;
  }
  .split-header .toggle-arrow.open {
    transform: rotate(90deg);
  }
  .split-entities-list .entity-item {
    padding-left: 10px;
  }
</style>
"""
    
    # Le JavaScript de la légende (adapté pour Pydeck)
    legend_js_content = """
window.addEventListener('load', function() {
  const grouped = """ + layers_json + """;
  const container = document.getElementById('legend-content');
  let html = '';

  if (!grouped || grouped.length === 0) {
    html = '<div style="padding:10px;color:#999;">Aucune couche</div>';
  } else {
    grouped.forEach(([typeName, layers], gIdx) => {
      html += `<div class="legend-group">
        <div class="legend-group-title">${typeName}</div>`;

      layers.forEach((L, idx) => {
        const layerId = 'layer_' + gIdx + '_' + idx;
        const entityListId = 'entities_' + layerId;
        const arrowId = 'arrow_' + layerId;
        const isEntire = L.mode === 'entire';

        let splitGroups = {};
        if (L.attribut_split && L.entities && L.entities.length > 0) {
          L.entities.forEach((E) => {
            const splitVal = E.split_value || 'Autres';
            if (!splitGroups[splitVal]) splitGroups[splitVal] = [];
            splitGroups[splitVal].push(E);
          });
        }

        html += `
          <div class="legend-layer">
            <div class="layer-header" ${!isEntire ? 'data-target="' + entityListId + '" data-arrow="' + arrowId + '"' : ''}>
              <input type="checkbox" class="layer-checkbox" id="toggle_${layerId}" data-layer-id="${layerId}" data-pydeck-layer="${L.layer_id || L.entities[0].layer_id}">
              ${!isEntire ? '<span id="' + arrowId + '" class="toggle-arrow">▸</span>' : ''}
              <span class="color-swatch" style="background-color:${L.color};"></span>
              <div>
                <div class="layer-title">${L.name}</div>
                ${L.nom_attribut_map ? '<div style="font-size:10px;color:#666;font-style:italic;margin-top:2px;">Classé selon: ' + L.nom_attribut_map + '</div>' : ''}
              </div>
            </div>
            ${!isEntire ? 
            `<div id="${entityListId}" class="entities-list">
              ${L.attribut_split && Object.keys(splitGroups).length > 0
                ? Object.keys(splitGroups).map(splitVal => {
                    const splitId = splitVal.replace(/\\s+/g, '_');
                    return `
                    <div class="split-group">
                      <div class="split-header" data-target="split_${layerId}_${splitId}">
                        <span class="toggle-arrow">▸</span>
                        <span style="font-weight:600;">${splitVal}</span>
                      </div>
                      <div id="split_${layerId}_${splitId}" class="split-entities-list" style="margin-left:18px;display:none;">
                        ${splitGroups[splitVal].map((E) => `
                          <div class="entity-item">
                            <label>
                              <input type="checkbox" class="entity-cb" data-pydeck-layer="${E.layer_id}">
                              <span>${E.name}</span>
                              ${E.count > 1 ? '<span class="entity-badge">×' + E.count + '</span>' : ''}
                            </label>
                          </div>
                        `).join('')}
                      </div>
                    </div>
                    `;
                  }).join('')
                : L.entities.map((E) => `
                    <div class="entity-item">
                      <label>
                        <input type="checkbox" class="entity-cb" data-pydeck-layer="${E.layer_id}">
                        <span>${E.name}</span>
                        ${E.count > 1 ? '<span class="entity-badge">×' + E.count + '</span>' : ''}
                      </label>
                    </div>
                  `).join('')
              }
            </div>`
             : 
            `<input type="checkbox" class="entity-cb" data-pydeck-layer="${L.entities[0].layer_id}" style="display:none;">`
            }
          </div>`;
      });
      html += `</div>`;
    });
  }

  container.innerHTML = html;

  // Gestion des toggles
  document.querySelectorAll('.split-header').forEach(header => {
    header.addEventListener('click', function() {
      const targetId = this.dataset.target;
      const target = document.getElementById(targetId);
      const arrow = this.querySelector('.toggle-arrow');
      if (target && arrow) {
        target.style.display = target.style.display === 'none' ? 'block' : 'none';
        arrow.classList.toggle('open');
      }
    });
  });

  document.querySelectorAll('.layer-header[data-target]').forEach(header => {
    header.addEventListener('click', function(e) {
      if (e.target.classList.contains('layer-checkbox')) return;
      const target = document.getElementById(this.dataset.target);
      const arrow = document.getElementById(this.dataset.arrow);
      if (target && arrow) {
        target.classList.toggle('open');
        arrow.classList.toggle('open');
      }
    });
  });

  // Gestion de la visibilité des couches Pydeck
  const deckglDiv = document.querySelector('.deck-container');
  if (deckglDiv && deckglDiv.deck) {
    const deck = deckglDiv.deck;
    
    document.querySelectorAll('.layer-checkbox').forEach(cb => {
      cb.addEventListener('change', function() {
        const pydeckLayerId = this.dataset.pydeckLayer;
        const isChecked = this.checked;
        
        // Récupérer toutes les entity checkboxes associées
        const entityList = this.closest('.legend-layer').querySelectorAll('.entity-cb');
        entityList.forEach(entityCb => {
          entityCb.checked = isChecked;
          const layerId = entityCb.dataset.pydeckLayer;
          togglePydeckLayer(deck, layerId, isChecked);
        });
      });
    });
    
    document.querySelectorAll('.entity-cb').forEach(cb => {
      cb.addEventListener('change', function() {
        const layerId = this.dataset.pydeckLayer;
        const isChecked = this.checked;
        togglePydeckLayer(deck, layerId, isChecked);
      });
    });
  }
  
  function togglePydeckLayer(deck, layerId, visible) {
    const layers = deck.props.layers;
    const updatedLayers = layers.map(layer => {
      if (layer.id === layerId) {
        return layer.clone({ visible: visible });
      }
      return layer;
    });
    deck.setProps({ layers: updatedLayers });
  }
});
"""
    
    legend_js = f"<script>{legend_js_content}</script>"
    
    # Popup modal
    popup_html = """
<style>
#modal-overlay {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: rgba(0, 0, 0, 0.5);
    z-index: 9999;
    display: none;
}
#modal-content {
    position: fixed;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    background: white;
    padding: 20px;
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    z-index: 10000;
    width: 450px;
    max-height: 400px;
    overflow-y: auto;
    display: none;
    border-left: 4px solid #27ae60;
}
#modal-close {
    background: #e74c3c;
    color: white;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
    cursor: pointer;
    margin-top: 10px;
}
#modal-close:hover {
    background: #c0392b;
}
</style>

<div id="modal-overlay" onclick="closeModal()"></div>
<div id="modal-content">
    <div id="modal-body"></div>
    <button id="modal-close" onclick="closeModal()">Fermer</button>
</div>

<script>
window.addEventListener('load', function() {
    const deckglDiv = document.querySelector('.deck-container');
    if (deckglDiv && deckglDiv.deck) {
        const deck = deckglDiv.deck;
        deck.setProps({
            onClick: (info) => {
                if (!info.object) return;
                
                const props = info.object.properties || info.object;
                let props_full = {};
                try {
                    props_full = JSON.parse(props.props_full || '{}');
                } catch(e) {
                    props_full = props;
                }
                
                const layerName = props_full.__layer_name__ || props.__layer_name__ || 'Zone';
                
                // Chercher la réglementation
                let reglement = null;
                for (let key in props_full) {
                    if (key.toLowerCase().includes('reglementation')) {
                        reglement = props_full[key];
                        break;
                    }
                }
                
                const modalBody = document.getElementById('modal-body');
                
                if (reglement) {
                    modalBody.innerHTML = `
                        <h4 style="margin-top:0;color:#003366;">${layerName}</h4>
                        <p style="font-size:13px;color:#333;white-space:pre-wrap;line-height:1.4;">
                            ${reglement}
                        </p>
                    `;
                } else {
                    let content = '';
                    for (let key in props_full) {
                        if (key !== '__layer_name__') {
                            content += `<b>${key.replace(/_/g, ' ')}:</b> ${props_full[key]}<br>`;
                        }
                    }
                    modalBody.innerHTML = `
                        <h4 style="margin-top:0;color:#003366;">${layerName}</h4>
                        <p style="font-size:13px;color:#333;line-height:1.4;">
                            ${content}
                        </p>
                    `;
                }
                
                document.getElementById('modal-overlay').style.display = 'block';
                document.getElementById('modal-content').style.display = 'block';
            }
        });
    }
});

function closeModal() {
    document.getElementById('modal-overlay').style.display = 'none';
    document.getElementById('modal-content').style.display = 'none';
}

document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') closeModal();
});
</script>
"""
    
    # Trackpad controls
    trackpad_script = """
<script>
window.addEventListener('load', function() {
    const deckglDiv = document.querySelector('.deck-container');
    if (deckglDiv && deckglDiv.deck) {
        const deck = deckglDiv.deck;
        deck.setProps({
            controller: {
                dragRotate: true,
                dragPan: true,
                scrollZoom: true,
                touchRotate: true,
                touchZoom: true,
                doubleClickZoom: true,
                keyboard: true
            }
        });
        console.log('✅ Contrôles trackpad activés');
    }
});
</script>
"""
    
    # Assemblage final
    html_final = html_base.replace("</head>", legend_css + "</head>")
    html_final = html_final.replace("</body>", legend_html + legend_js + popup_html + trackpad_script + "</body>")
    
    logger.info("   ✅ Légende interactive ajoutée")
    logger.info("   ✅ Popups modals ajoutés")
    logger.info("   ✅ Contrôles trackpad activés")
    
    # ============================================================
    # RÉSUMÉ FINAL
    # ============================================================
    logger.info(f"\n{'='*60}")
    logger.info(f"✅ CARTE 3D TERMINÉE !")
    logger.info(f"{'='*60}")
    logger.info(f"📊 {len(registry['layers'])} couches, {total_groups} groupes, {total_entities} entités")
    logger.info(f"🎯 Vue 3D avec pitch 45°\n")
    
    metadata = {
        "type": "carte_3d",
        "nb_couches": len(registry['layers']),
        "nb_groupes": total_groups,
        "nb_entites": total_entities
    }
    
    return html_final, metadata


# ============================================================
# 🧪 TEST CLI
# ============================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Génère une carte 3D Pydeck complète")
    parser.add_argument("--wkt", required=True, help="Chemin du fichier WKT")
    parser.add_argument("--output", default="./out_3d", help="Dossier de sortie")
    args = parser.parse_args()
    
    os.makedirs(args.output, exist_ok=True)
    
    try:
        html_string, metadata = generate_map_3d_from_wkt(
            wkt_path=args.wkt,
            inclure_ppri=False
        )
        
        output_file = os.path.join(args.output, "carte_3d_complete.html")
        
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_string)
        
        print(f"\n✅ Carte générée : {output_file}")
        print(json.dumps(metadata, indent=2, ensure_ascii=False))
        
    except Exception as e:
        print(f"\n❌ Erreur : {e}")
        import traceback
        traceback.print_exc()
        raise