#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
map_2d.py
----------------------------------------------------
Génère une carte 2D Folium pour une parcelle ou plusieurs parcelles.
"""

import os
import json
import logging
import folium
import geopandas as gpd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from folium import Element

# ============================================================
# IMPORT DES UTILITAIRES
# ============================================================
from map_utils import (
    random_color,
    clean_properties,
    get_parcelle_geometry,
    get_layers_on_parcel_with_buffer,
)

# 🆕 Import du module PPRI (cartographie uniquement)
try:
    from ppri_map_module import ajouter_ppri_a_carte
    PPRI_DISPONIBLE = True
except ImportError:
    PPRI_DISPONIBLE = False
    print("⚠️ Module PPRI cartographique non disponible (ppri_map_module.py)")

# ============================================================
# CONFIG LOGGING
# ============================================================
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("carte")

# ============================================================
# 🔧 CONNEXION BASE DE DONNÉES - VERSION CORRIGÉE
# ============================================================

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")  # 🔧 Défaut 5432

# 🆕 VÉRIFICATION : Êtes-vous sur le Session Pooler ou connexion directe ?
if "pooler.supabase.com" in SUPABASE_HOST:
    logger.info("🔄 Utilisation du Session Pooler Supabase")
    # Le Session Pooler nécessite le format: postgres.project_ref
    if not SUPABASE_USER.startswith("postgres."):
        logger.warning("⚠️  Format utilisateur incorrect pour Pooler !")
        logger.warning(f"   Actuel: {SUPABASE_USER}")
        logger.warning(f"   Attendu: postgres.XXXXXX")
else:
    logger.info("📡 Utilisation de la connexion directe")

# Construction de l'URL de connexion SQLAlchemy
DATABASE_URL = (
    f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@"
    f"{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
)

logger.info("🔌 Connexion à la base de données...")
try:
    # 🆕 Paramètres de connexion améliorés
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,          # Vérifie la connexion avant utilisation
        pool_recycle=3600,            # Recycle les connexions après 1h
        connect_args={
            "connect_timeout": 10,    # Timeout de 10 secondes
            "sslmode": "require"      # Force SSL (important pour Supabase)
        }
    )
    
    # 🆕 Test de la connexion
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        version = result.fetchone()[0]
        logger.info(f"✅ Connexion établie : {version[:50]}...")
        
except Exception as e:
    logger.error(f"❌ Erreur de connexion : {e}")
    logger.error("\n💡 Vérifications à faire :")
    logger.error("   1. Votre .env contient-il les bonnes valeurs ?")
    logger.error("   2. Utilisez-vous le Session Pooler ?")
    logger.error("      → Host: aws-0-eu-west-3.pooler.supabase.com")
    logger.error("      → User: postgres.odlkagfeqkbrruajlcxm")
    logger.error("   3. Ou la connexion directe ?")
    logger.error("      → Host: db.odlkagfeqkbrruajlcxm.supabase.co")
    logger.error("      → User: postgres")
    raise

SCHEMA = "latresne"
CATALOGUE_PATH = os.path.join(os.path.dirname(__file__), "catalogue_couches_map.json")
BUFFER_DIST = 200

BASEMAP_CONF = {
    "tiles": "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
    "attr": "© OpenStreetMap contributors © CARTO"
}

logger.info(f"📂 Chargement du catalogue depuis {CATALOGUE_PATH}...")
with open(CATALOGUE_PATH, "r", encoding="utf-8") as f:
    CATALOGUE = json.load(f)
logger.info(f"✅ {len(CATALOGUE)} couches dans le catalogue")

# ============================================================
# GÉNÉRATION DE LA CARTE - MODULE RÉUTILISABLE
# ============================================================
def generate_map_from_wkt(wkt_path, inclure_ppri=True, code_insee="33234", ppri_table="pm1_detaillee_gironde"):
    """
    Génère une carte Folium interactive à partir d'un fichier WKT.
    
    Args:
        wkt_path (str): Chemin vers le fichier WKT contenant la géométrie de l'unité foncière
        inclure_ppri (bool): Inclure ou non le PPRI
        code_insee (str): Code INSEE de la commune
        ppri_table (str): Nom de la table PPRI
    
    Returns:
        tuple: (html_string, metadata_dict) où html_string est le HTML de la carte
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"🌍 DÉBUT - Génération carte depuis WKT")
    logger.info(f"{'='*60}\n")

    # 🆕 Initialiser par défaut
    metadata_ppri = None

    # ============================================================
    # 📄 Lecture du fichier WKT - VERSION ROBUSTE
    # ============================================================
    # 🩹 Correction : gestion robuste du chemin WKT
    wkt_path = Path(wkt_path).resolve()
    if not wkt_path.exists():
        logger.error(f"❌ Fichier WKT introuvable : {wkt_path}")
        raise FileNotFoundError(f"Fichier WKT introuvable : {wkt_path}")

    logger.info(f"📄 Étape 1/5 : Lecture du fichier WKT : {wkt_path}")
    wkt_geom = wkt_path.read_text(encoding="utf-8").strip()
    logger.info(f"📏 Longueur du WKT : {len(wkt_geom)} caractères")
    logger.debug(wkt_geom[:200] + "..." if len(wkt_geom) > 200 else wkt_geom)

    # Construction du GeoDataFrame
    gdf_parcelle = gpd.GeoDataFrame(
        geometry=[gpd.GeoSeries.from_wkt([wkt_geom])[0]], 
        crs="EPSG:2154"
    )
    gdf_parcelle_4326 = gdf_parcelle.to_crs(4326)
    centroid = gdf_parcelle_4326.geometry.iloc[0].centroid
    logger.info(f"   ✅ Géométrie chargée : {gdf_parcelle.geometry.iloc[0].geom_type}")
    logger.info(f"   📍 Centroïde : lat={centroid.y:.6f}, lon={centroid.x:.6f}")

    # Label générique (unité foncière)
    label_parcelle = "Unité foncière"
    parcelle_wkt = wkt_geom

    # --- Carte Folium
    logger.info("\n🗺️  Étape 2/5 : Création carte Folium...")
    m = folium.Map(
        location=[centroid.y, centroid.x], 
        zoom_start=17,
        tiles=BASEMAP_CONF["tiles"], 
        attr=BASEMAP_CONF["attr"]
    )
    map_var = m.get_name()
    logger.info(f"   ✅ Carte créée : {map_var}")
    logger.info(f"   ✅ Ajout échelle métrique (bas à droite)")
    logger.info(f"   ✅ Ajout rose des vents (haut à gauche)")
    logger.info(f"   ✅ Ajout outils de mesure (trait, polygone, gomme)")

    # ============================================================
    # 🎯 OUTIL DE MESURE SIMPLIFIÉ (mètres et m²)
    # ============================================================
    measure_plugin = """
<!-- Leaflet.Draw plugin -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet-draw@1.0.4/dist/leaflet.draw.css" />
<script src="https://cdn.jsdelivr.net/npm/leaflet-draw@1.0.4/dist/leaflet.draw.js"></script>

<style>
/* Positionnement à mi-hauteur sur le côté gauche */
.leaflet-draw {
  position: absolute !important;
  left: 10px !important;
  top: 200px !important;
  z-index: 1000 !important;
}
.leaflet-draw-toolbar a {
  background-color: white !important;
  border: 1px solid #ccc !important;
  border-radius: 4px !important;
  box-shadow: 0 2px 6px rgba(0,0,0,0.15);
}
.leaflet-draw-actions a {
  background: #f8f9fa !important;
  color: #333 !important;
}
</style>

<script>
document.addEventListener("DOMContentLoaded", function() {
  const interval = setInterval(function() {
    const map = window["%s"];
    if (!map) return;

    clearInterval(interval);

    // Couche pour stocker les mesures
    const drawnItems = new L.FeatureGroup();
    map.addLayer(drawnItems);

    // Contrôle de dessin : uniquement polyligne et polygone
    const drawControl = new L.Control.Draw({
      draw: {
        marker: false,
        circle: false,
        circlemarker: false,
        rectangle: false,
        polyline: {
          shapeOptions: { color: '#e74c3c', weight: 3 },
          metric: true,
          feet: false,
          showLength: true
        },
        polygon: {
          shapeOptions: { color: '#2ecc71', weight: 2 },
          allowIntersection: false,
          showArea: true,
          showLength: false,
          metric: true
        }
      },
      edit: {
        featureGroup: drawnItems,
        edit: false,
        remove: true
      }
    });
    map.addControl(drawControl);

    // Fermeture auto du polygone et affichage surface
    map.on(L.Draw.Event.CREATED, function (e) {
      drawnItems.clearLayers(); // supprime les mesures précédentes
      const layer = e.layer;
      drawnItems.addLayer(layer);

      if (e.layerType === 'polygon') {
        const latlngs = layer.getLatLngs()[0];
        if (latlngs.length > 2) {
          const area = L.GeometryUtil.geodesicArea(latlngs);
          const surface_m2 = area.toFixed(2);
          layer.bindTooltip(surface_m2 + ' m²', { permanent: true, direction: 'center' }).openTooltip();
        }
      }
      if (e.layerType === 'polyline') {
        const latlngs = layer.getLatLngs();
        let distance = 0;
        for (let i = 0; i < latlngs.length - 1; i++) {
          distance += latlngs[i].distanceTo(latlngs[i + 1]);
        }
        const distance_m = distance.toFixed(2);
        layer.bindTooltip(distance_m + ' m', { permanent: true, direction: 'center' }).openTooltip();
      }
    });

    console.log("✅ Outil de mesure chargé (mètres et m²)");
  }, 500);
});
</script>
""" % map_var

    m.get_root().html.add_child(Element(measure_plugin))

    # --- Échelle métrique (en bas à droite) via JavaScript
    scale_control = '''
    <script>
    window.addEventListener('load', function() {
        const mapVar = ''' + f"'{map_var}'" + ''';
        const map = window[mapVar];
        if (map) {
            L.control.scale({
                position: 'bottomright',
                metric: true,
                imperial: false,
                maxWidth: 200
            }).addTo(map);
        }
    });
    </script>
    '''
    m.get_root().html.add_child(Element(scale_control))
    
    # --- Rose des vents (en haut à gauche)
    compass_svg = '''
    <div style="position:absolute;top:80px;left:10px;z-index:1000;">
        <svg width="80" height="80" viewBox="0 0 100 100">
            <circle cx="50" cy="50" r="75" fill="white" stroke="#333" stroke-width="2" opacity="0"/>
            <g transform="translate(50,50)">
                <!-- Nord (rouge) -->
                <polygon points="0,-35 -6,-10 0,-15 6,-10" fill="#e74c3c" stroke="#c0392b" stroke-width="1"/>
                <!-- Sud -->
                <polygon points="0,35 -6,10 0,15 6,10" fill="#34495e" stroke="#2c3e50" stroke-width="1"/>
                <!-- Est -->
                <polygon points="35,0 10,-6 15,0 10,6" fill="#34495e" stroke="#2c3e50" stroke-width="1"/>
                <!-- Ouest -->
                <polygon points="-35,0 -10,-6 -15,0 -10,6" fill="#34495e" stroke="#2c3e50" stroke-width="1"/>
                <!-- Lettres cardinales -->
                <text x="0" y="-42" text-anchor="middle" font-size="14" font-weight="bold" fill="#e74c3c">N</text>
                <text x="0" y="50" text-anchor="middle" font-size="12" font-weight="bold" fill="#34495e">S</text>
                <text x="42" y="5" text-anchor="middle" font-size="12" font-weight="bold" fill="#34495e">E</text>
                <text x="-42" y="5" text-anchor="middle" font-size="12" font-weight="bold" fill="#34495e">O</text>
            </g>
        </svg>
    </div>
    '''
    m.get_root().html.add_child(Element(compass_svg))
    
    # --- Parcelle (toujours visible)
    logger.info("\n📦 Étape 3/5 : Ajout de la parcelle...")
    folium.GeoJson(
        gdf_parcelle_4326,
        name="Parcelle",
        style_function=lambda x: {
            "color": "red", 
            "weight": 4, 
            "fillOpacity": 0,
            "dashArray": "5, 5"
        },
        show=True
    ).add_to(m)
    logger.info(f"   ✅ Parcelle ajoutée (rouge, pointillés)")

    # --- Couches intersectant
    logger.info("\n🔍 Étape 4/5 : Recherche couches...")
    layers_on_parcel = get_layers_on_parcel_with_buffer(
        engine, SCHEMA, CATALOGUE, parcelle_wkt, BUFFER_DIST
    )
    logger.info(f"   ✅ {len(layers_on_parcel)} couches trouvées")

    # --- Registry
    registry = {
        "mapVar": map_var,
        "layers": []
    }

    # ============================================================
    # AJOUT DES ENTITÉS AVEC LOGIQUE FLEXIBLE
    # ============================================================
    logger.info("\n🎨 Étape 5/5 : Ajout des entités...")
    
    def add_layer(table, config):
        nom = config.get("nom", table)
        keep = config.get("keep", [])
        attribut_map = config.get("attribut_map", None)
        attribut_split = config.get("attribut_split", None)  # 🆕 Nouveau champ pour sous-groupes
        color = random_color()
        
        logger.info(f"   📊 {nom}...")
        
        # 🆕 Déterminer le mode d'affichage
        # Mode "couche entière" si attribut_map est None, "None", absent, ou vide
        mode_couche_entiere = (
            attribut_map is None or 
            attribut_map == "None" or 
            attribut_map == "" or
            str(attribut_map).lower() == "none"
        )
        
        if mode_couche_entiere:
            logger.info(f"      🎯 Mode : Couche entière (pas de distinction d'entités)")
        else:
            logger.info(f"      🔑 Mode : Groupement par attribut '{attribut_map}'")

        # Construire la requête SQL
        if not mode_couche_entiere and attribut_map:
            select_cols_list = list(keep[:3]) if keep else []
            if attribut_map not in select_cols_list:
                select_cols_list.insert(0, attribut_map)
            # 🆕 Ajouter l'attribut de split si défini
            if attribut_split and attribut_split not in select_cols_list:
                select_cols_list.append(attribut_split)
            select_cols = ", ".join(select_cols_list)
        else:
            select_cols = ", ".join(keep[:3]) if keep else "gml_id"

        q = f"""
            WITH
              p AS (SELECT ST_GeomFromText('{parcelle_wkt}',2154) AS g),
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
                    return
                keys = list(rs.keys())
        except Exception as e:
            logger.error(f"      ❌ ERREUR : {e}")
            return

        # 🆕 DEUX MODES DIFFÉRENTS
        if mode_couche_entiere:
            # ============================================================
            # MODE 1 : COUCHE ENTIÈRE (pas de distinction)
            # ============================================================
            # Stocker features avec données nettoyées ET originales
            all_features_data = []
            for idx, row in enumerate(rows, start=1):
                try:
                    geom = json.loads(row[0])
                    props_raw = {keys[j + 1]: str(row[j + 1]) for j in range(len(keys) - 1)}
                    
                    # Props nettoyées pour tooltip (avec troncature)
                    props_clean = clean_properties(props_raw, nom)
                    
                    # Props complètes pour popup (SANS troncature, juste filtrage des IDs)
                    ignore_patterns = ["id", "uuid", "gid", "fid", "globalid"]
                    props_full = {
                        k: v for k, v in props_raw.items()
                        if not any(pat in k.lower() for pat in ignore_patterns)
                    }
                    props_full = {"__layer_name__": nom, **props_full}
                    
                    all_features_data.append({
                        "feature": {"type": "Feature", "geometry": geom, "properties": props_clean},
                        "props_full": props_full
                    })
                except Exception as e:
                    logger.error(f"      ❌ Entité {idx} : {e}")
                    continue

            if not all_features_data:
                logger.warning(f"      ⚠️  Aucune feature valide")
                return

            layer_id = f"layer_entire_{len(registry['layers'])}"
            all_feat_vars = []
            
            # Créer un GeoJson avec popup expansible pour chaque feature
            for feat_idx, feat_data in enumerate(all_features_data):
                feat = feat_data["feature"]
                props_full = feat_data["props_full"]
                props_clean = feat["properties"]
                
                feat_id = f"{layer_id}_feat_{feat_idx}"
                
                # Tooltip avec message "Cliquer pour afficher plus"
                tooltip_fields = list(props_clean.keys())
                tooltip_html = f'<div style="background:white;color:#111;font-size:12px;border-radius:4px;padding:8px;border:1px solid #ccc;">'
                for field in tooltip_fields:
                    alias = "" if field == "__layer_name__" else f"{field}:"
                    value = str(props_clean.get(field, ""))
                    if len(value) > 50:
                        value = value[:50] + "..."
                    tooltip_html += f'<div><strong>{alias}</strong> {value}</div>'
                tooltip_html += '<div class="tooltip-footer">👆 Cliquer pour afficher plus</div></div>'
                
                gj = folium.GeoJson(
                    {"type": "FeatureCollection", "features": [feat]},
                    name=feat_id,
                    style_function=lambda x, c=color: {"color": c, "weight": 2, "fillOpacity": 0.35},
                    highlight_function=lambda x, c=color: {"weight": 4, "fillOpacity": 0.65},
                    tooltip=folium.Tooltip(tooltip_html),
                    show=False
                )
                gj.add_to(m)
                all_feat_vars.append(gj.get_name())
                
                # Déterminer le type de couche pour la couleur de bordure
                type_couche = CATALOGUE.get(table, {}).get("type", "")
                couleur_bordure = {
                    "Zonage PLU": "#27ae60",
                    "Servitudes": "#2980b9",
                    "Prescriptions": "#8e44ad",
                    "Informations": "#e67e22"
                }.get(type_couche, "#7f8c8d")
                
                # Déterminer le texte de réglementation si présent
                reglement = None
                for k, v in props_full.items():
                    if "reglementation" in k.lower():
                        reglement = v
                        break

                # Construction du popup harmonisé (même modèle que PPRI)
                if reglement:
                    popup_content = f"""
                    <div style="width:450px;max-height:400px;overflow-y:auto;padding:10px;border-left:4px solid {couleur_bordure};">
                        <h4 style="margin-top:0;color:#003366;">{props_full.get('__layer_name__','')}</h4>
                        <p style="font-size:13px;color:#333;white-space:pre-wrap;line-height:1.4;">
                            {reglement.strip()}
                        </p>
                    </div>
                    """
                else:
                    popup_content = f"""
                    <div style="width:450px;max-height:400px;overflow-y:auto;padding:10px;border-left:4px solid {couleur_bordure};">
                        <h4 style="margin-top:0;color:#003366;">{props_full.get('__layer_name__','')}</h4>
                        <p style="font-size:13px;color:#333;white-space:pre-wrap;line-height:1.4;">
                            {''.join([f"<b>{k.replace('_',' ').title()}:</b> {v}<br>" for k,v in props_full.items() if k!='__layer_name__'])}
                        </p>
                    </div>
                    """
                
                folium.Popup(popup_content, max_width=480).add_to(gj)

            logger.info(f"      ✅ 1 groupe (couche entière avec {len(all_features_data)} entité(s))")

            # Ajouter à la registry (format spécial pour couche entière)
            registry["layers"].append({
                "name": nom,
                "color": color,
                "mode": "entire",  # 🆕 Indicateur de mode
                "attribut_map": None,
                "nom_attribut_map": "",
                "entities": [{
                    "name": nom,  # Nom de la couche = nom de l'entrée
                    "vars": all_feat_vars,
                    "count": len(all_features_data)
                }]
            })

        else:
            # ============================================================
            # MODE 2 : GROUPEMENT PAR ATTRIBUT (mode actuel)
            # ============================================================
            grouped_entities = {}
            entity_split_values = {}  # 🆕 Stocker les valeurs de split par entité
            
            for idx, row in enumerate(rows, start=1):
                try:
                    geom = json.loads(row[0])
                    props_raw = {keys[j + 1]: str(row[j + 1]) for j in range(len(keys) - 1)}
                    
                    # Props nettoyées pour tooltip (avec troncature)
                    props_clean = clean_properties(props_raw, nom)
                    
                    # Props complètes pour popup (SANS troncature, juste filtrage des IDs)
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

                    # 🆕 Déterminer la valeur de split pour les sous-groupes
                    split_value = None
                    if attribut_split and attribut_split in props_clean:
                        split_value = props_clean[attribut_split]
                        if not split_value or split_value.lower() in ['none', 'null', '']:
                            split_value = "Autres"

                    entity_id = f"entity_{len(registry['layers'])}_{idx}"
                    
                    # Tooltip avec message "Cliquer pour afficher plus"
                    tooltip_fields = list(props_clean.keys())
                    tooltip_html = f'<div style="background:white;color:#111;font-size:12px;border-radius:4px;padding:8px;border:1px solid #ccc;">'
                    for field in tooltip_fields:
                        alias = "" if field == "__layer_name__" else f"{field}:"
                        value = str(props_clean.get(field, ""))
                        if len(value) > 50:
                            value = value[:50] + "..."
                        tooltip_html += f'<div><strong>{alias}</strong> {value}</div>'
                    tooltip_html += '<div class="tooltip-footer">👆 Cliquer pour afficher plus</div></div>'

                    feature_obj = {"type": "Feature", "geometry": geom, "properties": props_clean}
                    
                    gj = folium.GeoJson(
                        {"type": "FeatureCollection", "features": [feature_obj]},
                        name=entity_id,
                        style_function=lambda x, c=color: {"color": c, "weight": 2, "fillOpacity": 0.35},
                        highlight_function=lambda x, c=color: {"weight": 4, "fillOpacity": 0.65},
                        tooltip=folium.Tooltip(tooltip_html),
                        show=False
                    )
                    gj.add_to(m)
                    
                    # Déterminer le type de couche pour la couleur de bordure
                    type_couche = CATALOGUE.get(table, {}).get("type", "")
                    couleur_bordure = {
                        "Zonage PLU": "#27ae60",
                        "Servitudes": "#2980b9",
                        "Prescriptions": "#8e44ad",
                        "Informations": "#e67e22"
                    }.get(type_couche, "#7f8c8d")
                    
                    # Déterminer le texte de réglementation si présent
                    reglement = None
                    for k, v in props_full.items():
                        if "reglementation" in k.lower():
                            reglement = v
                            break

                    # Construction du popup harmonisé (même modèle que PPRI)
                    if reglement:
                        popup_content = f"""
                        <div style="width:450px;max-height:400px;overflow-y:auto;padding:10px;border-left:4px solid {couleur_bordure};">
                            <h4 style="margin-top:0;color:#003366;">{props_full.get('__layer_name__','')}</h4>
                            <p style="font-size:13px;color:#333;white-space:pre-wrap;line-height:1.4;">
                                {reglement.strip()}
                            </p>
                        </div>
                        """
                    else:
                        popup_content = f"""
                        <div style="width:450px;max-height:400px;overflow-y:auto;padding:10px;border-left:4px solid {couleur_bordure};">
                            <h4 style="margin-top:0;color:#003366;">{props_full.get('__layer_name__','')}</h4>
                            <p style="font-size:13px;color:#333;white-space:pre-wrap;line-height:1.4;">
                                {''.join([f"<b>{k.replace('_',' ').title()}:</b> {v}<br>" for k,v in props_full.items() if k!='__layer_name__'])}
                            </p>
                        </div>
                        """
                    
                    folium.Popup(popup_content, max_width=480).add_to(gj)
                    
                    if group_value not in grouped_entities:
                        grouped_entities[group_value] = []
                    grouped_entities[group_value].append(gj.get_name())
                    
                    # 🆕 Stocker la valeur de split pour cette entité
                    entity_split_values[gj.get_name()] = split_value
                        
                except Exception as e:
                    logger.error(f"      ❌ Entité {idx} : {e}")
                    continue

            entities = []
            for group_value, var_list in grouped_entities.items():
                # 🆕 Récupérer la valeur de split pour cette entité
                split_value_for_entity = None
                if attribut_split and var_list:
                    # Prendre la première valeur de split trouvée dans ce groupe
                    first_var = var_list[0]
                    split_value_for_entity = entity_split_values.get(first_var, "Autres")
                
                entities.append({
                    "name": group_value,
                    "vars": var_list,
                    "count": len(var_list),
                    "split_value": split_value_for_entity  # 🆕 Ajouter la valeur de split
                })

            logger.info(f"      ✅ {len(entities)} groupe(s) d'entités ({sum(e['count'] for e in entities)} entités totales)")
            for ent in entities[:3]:
                logger.info(f"         • {ent['name']} ({ent['count']} entité(s))")
            
            registry["layers"].append({
                "name": nom,
                "color": color,
                "mode": "grouped",  # 🆕 Indicateur de mode
                "attribut_map": attribut_map,
                "nom_attribut_map": config.get("nom_attribut_map", ""),
                "attribut_split": attribut_split,  # 🆕 Ajouter l'attribut de split
                "entities": entities
            })

    # ============================================================
    # 🆕 TRI DES COUCHES PAR IMPORTANCE (Zonage → SUP → Prescriptions → Informations)
    # ============================================================
    ordre_types = {
        "Zonage PLU": 1,          # PLU
        "Servitudes": 2,             # Servitudes d'utilité publique
        "Prescriptions": 3,   # Prescriptions particulières
        "Informations": 4     # Informations diverses
    }

    # Trier les couches du dictionnaire layers_on_parcel
    layers_on_parcel = dict(
        sorted(
            layers_on_parcel.items(),
            key=lambda item: ordre_types.get(
                CATALOGUE.get(item[0], {}).get("type", ""), 999
            )
        )
    )
    
    logger.info(f"   🔄 Couches triées par importance (Zonage → SUP → Prescriptions → Informations)")

    for table, config in layers_on_parcel.items():
        add_layer(table, config)

    total_groups = sum(len(l['entities']) for l in registry['layers'])
    total_entities = sum(sum(e['count'] for e in l['entities']) for l in registry['layers'])
    logger.info(f"\n   ✅ TOTAL : {len(registry['layers'])} couches, {total_groups} groupes, {total_entities} entités")

    # =========================================================
    # 🆕 AJOUT DU PPRI (optionnel) - AVANT LA LÉGENDE
    # =========================================================
    if inclure_ppri and PPRI_DISPONIBLE:
        logger.info("\n🌊 Étape 6/6 : Intégration du module PPRI...")
        try:
            metadata_ppri = ajouter_ppri_a_carte(
                map_folium=m,
                geom_wkt=parcelle_wkt,  # ✅ support du WKT
                code_insee=code_insee,
                ppri_table=ppri_table,
                engine=engine,
                show=False,  # Caché par défaut
                registry=registry  # 🆕 Passer le registry pour la légende
            )
            
            # 🆕 Contrôle du résultat
            if metadata_ppri and metadata_ppri.get('success'):
                logger.info(f"   ✅ PPRI intégré : {metadata_ppri['zones_conservees']} zones conservées")
                logger.info(f"   📊 Taux d'absorption : {metadata_ppri.get('taux_absorption', 0):.1f}%")
                
                # 🆕 Mettre à jour les totaux pour inclure le PPRI
                nb_groupes_ppri = len(metadata_ppri.get('zones_par_type', {}))
                nb_entites_ppri = metadata_ppri['zones_conservees'] + metadata_ppri['zones_remplacement']
                total_groups += nb_groupes_ppri
                total_entities += nb_entites_ppri
                logger.info(f"   📊 {nb_groupes_ppri} groupes PPRI, {nb_entites_ppri} entités PPRI")
            else:
                logger.warning("   ⚠️ PPRI : aucune zone trouvée")
                
        except Exception as e:
            logger.error(f"   ❌ Erreur PPRI : {e}")
            metadata_ppri = {"success": False, "error": str(e)}  # 🆕 Valeur par défaut
    
    elif inclure_ppri and not PPRI_DISPONIBLE:
        logger.warning("   ⚠️ PPRI demandé mais module non disponible (ppri_map_module.py introuvable)")

    # ============================================================
    # LÉGENDE INTERACTIVE (APRÈS L'AJOUT DU PPRI)
    # ============================================================
    logger.info("\n🏷️  Génération de la légende...")
    
    # ============================================================
    # 🆕 GROUPEMENT DES COUCHES PAR TYPE (pour affichage légende)
    # ============================================================
    grouped_layers = {}
    for layer in registry["layers"]:
        # Récupère le type depuis le catalogue
        table_name = next((t for t, cfg in layers_on_parcel.items() if cfg.get("nom") == layer["name"]), None)
        if table_name:
            type_couche = CATALOGUE.get(table_name, {}).get("type", "Autres")
        else:
            # Pour le PPRI qui n'est pas dans layers_on_parcel
            type_couche = "PPRI" if "PPRI" in layer["name"] else "Autres"
        grouped_layers.setdefault(type_couche, []).append(layer)

    # On remplace registry["layers"] par la version regroupée (ordonnée par importance)
    ordre_types = ["Zonage PLU", "Servitudes", "Prescriptions", "Informations", "PPRI", "Autres"]
    sorted_grouped_layers = [
        (t, grouped_layers[t]) for t in ordre_types if t in grouped_layers
    ]
    
    # JSON sérialisé en amont pour éviter les erreurs d'accolades dans la f-string
    layers_json = json.dumps(sorted_grouped_layers, ensure_ascii=False)
    
    legend_html = f"""
<div id="legend-panel" style="
  position:absolute;right:10px;top:10px;z-index:500;
  background:white;border:1px solid #ccc;border-radius:8px;
  box-shadow:0 2px 8px rgba(0,0,0,0.15);max-height:70vh;
  overflow:auto;font:12px/1.4 'Inter',sans-serif;padding:12px;
  width:360px;">
  <b style="font-size:14px;display:block;margin-bottom:8px;">📍 Surfaces urbanistiques de l'unité foncière</b>
  <div id="legend-content"></div>
</div>
<style>
  #legend-panel {{
    position: absolute;
    right: 10px;
    top: 10px;
    z-index: 500;
    background: white;
    border: 1px solid #ccc;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    max-height: 70vh;
    overflow: auto;
    font: 12px/1.4 'Inter', sans-serif;
    padding: 12px;
    width: 360px;
  }}
  .legend-layer {{
    margin: 8px 0;
    padding: 8px;
    background: #f9f9f9;
    border-radius: 6px;
    border: 1px solid #e5e5e5;
  }}
  .legend-group-title {{
    font-weight: 700;
    font-size: 13px;
    margin-top: 12px;
    color: #2a2a2a;
    border-bottom: 1px solid #ddd;
    padding-bottom: 4px;
  }}
  .layer-header {{
    display: flex;
    align-items: center;
    gap: 8px;
    cursor: pointer;
    user-select: none;
    padding: 4px 0;
  }}
  .toggle-arrow {{
    width: 16px;
    text-align: center;
    font-weight: bold;
    color: #666;
    transition: transform 0.2s ease;
    font-size: 11px;
  }}
  .toggle-arrow.open {{
    transform: rotate(90deg);
  }}
  .color-swatch {{
    width: 16px;
    height: 16px;
    border-radius: 3px;
    border: 1px solid #999;
    flex-shrink: 0;
  }}
  .layer-title {{
    flex: 1;
    font-weight: 600;
    color: #333;
    display: block;
  }}
  .entities-list {{
    margin-left: 24px;
    margin-top: 6px;
    display: none;
  }}
  .entities-list.open {{
    display: block;
  }}
  .entity-item {{
    padding: 4px 0;
  }}
  .entity-item label {{
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 6px;
  }}
  .entity-badge {{
    font-size: 9px;
    color: #666;
    background: #e8e8e8;
    padding: 1px 5px;
    border-radius: 8px;
    margin-left: auto;
  }}
  .layer-checkbox {{
    width: 16px;
    height: 16px;
    cursor: pointer;
    margin: 0;
    vertical-align: middle;
  }}
  .tooltip-footer {{
    margin-top: 8px;
    padding-top: 8px;
    border-top: 1px solid #ddd;
    text-align: center;
    font-size: 10px;
    color: #999;
    font-style: italic;
  }}
  .leaflet-popup {{ z-index: 1000 !important; }}
  .leaflet-tooltip {{ z-index: 900 !important; }}
  .leaflet-control-layers {{ display: none !important; }}
  
  /* 🆕 Styles pour les sous-groupes (jour/nuit, etc.) */
  .split-group {{
    margin-top: 6px;
    padding: 4px 0;
  }}
  
  .split-header {{
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 12px;
    color: #333;
    padding-left: 8px;
  }}
  
  .split-header .toggle-arrow {{
    width: 12px;
    text-align: center;
    transition: transform 0.2s ease;
    font-size: 10px;
    color: #666;
  }}
  
  .split-header .toggle-arrow.open {{
    transform: rotate(90deg);
  }}
  
  .split-entities-list .entity-item {{
    padding-left: 10px;
  }}
</style>

<script>
window.addEventListener('load', function() {{
  const mapVar = '{map_var}';
  const map = window[mapVar];
  const grouped = {layers_json};

  const container = document.getElementById('legend-content');
  let html = '';

  if (!grouped || grouped.length === 0) {{
    html = '<div style="padding:10px;color:#999;">Aucune couche</div>';
  }} else {{
    grouped.forEach(([typeName, layers], gIdx) => {{
      html += `<div class="legend-group">
        <div class="legend-group-title">${{typeName}}</div>`;

      layers.forEach((L, idx) => {{
        const layerId = 'layer_' + gIdx + '_' + idx;
        const entityListId = 'entities_' + layerId;
        const arrowId = 'arrow_' + layerId;
        const isEntire = L.mode === 'entire';
        const layerClass = 'legend-layer';

        // Si attribut_split présent → on regroupe les entités par valeur du split
        let splitGroups = {{}};
        if (L.attribut_split && L.entities && L.entities.length > 0) {{
          L.entities.forEach((E) => {{
            const splitVal = E.split_value || 'Autres';
            if (!splitGroups[splitVal]) splitGroups[splitVal] = [];
            splitGroups[splitVal].push(E);
          }});
        }}

        html += `
          <div class="${{layerClass}}">
            <div class="layer-header" ${{!isEntire ? 'data-target="' + entityListId + '" data-arrow="' + arrowId + '"' : ''}}>
              <input type="checkbox" class="layer-checkbox" id="toggle_${{layerId}}" data-layer-id="${{layerId}}">
              ${{!isEntire ? '<span id="' + arrowId + '" class="toggle-arrow">▸</span>' : ''}}
              <span class="color-swatch" style="background-color:${{L.color}};"></span>
              <div>
                <div class="layer-title">${{L.name}}</div>
                ${{L.nom_attribut_map ? '<div style="font-size:10px;color:#666;font-style:italic;margin-top:2px;">Classé selon: ' + L.nom_attribut_map + '</div>' : ''}}
              </div>
            </div>
            ${{!isEntire ? `
            <div id="${{entityListId}}" class="entities-list">
              ${{L.attribut_split && Object.keys(splitGroups).length > 0
                ? Object.keys(splitGroups).map(splitVal => {{
                    const splitId = splitVal.replace(/\\s+/g, '_');
                    return `
                    <div class="split-group">
                      <div class="split-header" data-target="split_${{layerId}}_${{splitId}}">
                        <span class="toggle-arrow">▸</span>
                        <span style="font-weight:600;">${{splitVal}}</span>
                      </div>
                      <div id="split_${{layerId}}_${{splitId}}" class="split-entities-list" style="margin-left:18px;display:none;">
                        ${{splitGroups[splitVal].map((E) => `
                          <div class="entity-item">
                            <label>
                              <input type="checkbox" class="entity-cb" data-vars="${{E.vars.join(',')}}">
                              <span>${{E.name}}</span>
                              ${{E.count > 1 ? '<span class="entity-badge">×' + E.count + '</span>' : ''}}
                            </label>
                          </div>
                        `).join('')}}
                      </div>
                    </div>
                    `;
                  }}).join('')
                : L.entities.map((E) => `
                    <div class="entity-item">
                      <label>
                        <input type="checkbox" class="entity-cb" data-vars="${{E.vars.join(',')}}">
                        <span>${{E.name}}</span>
                        ${{E.count > 1 ? '<span class="entity-badge">×' + E.count + '</span>' : ''}}
                      </label>
                    </div>
                  `).join('')
              }}
            </div>
            ` : `
            <input type="checkbox" class="entity-cb" data-vars="${{L.entities[0].vars.join(',')}}" style="display:none;">
            `}}
          </div>`;
      }});
      html += `</div>`;
    }});
  }}

  container.innerHTML = html;

  // 🆕 Gestion des toggles de sous-groupes (jour/nuit, etc.)
  document.querySelectorAll('.split-header').forEach(header => {{
    header.addEventListener('click', function() {{
      const targetId = this.dataset.target;
      const target = document.getElementById(targetId);
      const arrow = this.querySelector('.toggle-arrow');
      if (target && arrow) {{
        target.style.display = target.style.display === 'none' ? 'block' : 'none';
        arrow.classList.toggle('open');
      }}
    }});
  }});

  // Dépliage
  document.querySelectorAll('.layer-header[data-target]').forEach(header => {{
    header.addEventListener('click', function(e) {{
      if (e.target.classList.contains('layer-checkbox')) return;
      const target = document.getElementById(this.dataset.target);
      const arrow = document.getElementById(this.dataset.arrow);
      if (target && arrow) {{
        target.classList.toggle('open');
        arrow.classList.toggle('open');
      }}
    }});
  }});

  // Gestion des couches et entités
  const allEntityCheckboxes = document.querySelectorAll('.entity-cb');
  const allLayerCheckboxes = document.querySelectorAll('.layer-checkbox');

  allLayerCheckboxes.forEach(layerCb => {{
    layerCb.addEventListener('change', function(e) {{
      const layerId = this.dataset.layerId;
      const isChecked = this.checked;
      const entityList = document.getElementById('entities_' + layerId);
      let entityCheckboxes = entityList ? entityList.querySelectorAll('.entity-cb') : this.closest('.legend-layer').querySelectorAll('.entity-cb');
      entityCheckboxes.forEach(cb => {{
        const varNames = cb.getAttribute('data-vars').split(',');
        const layers = varNames.map(v => window[v]).filter(l => l);
        cb.checked = isChecked;
        layers.forEach(layer => {{
          if (isChecked) map.addLayer(layer);
          else if (map.hasLayer(layer)) map.removeLayer(layer);
        }});
      }});
    }});
  }});

  allEntityCheckboxes.forEach(cb => {{
    cb.addEventListener('change', function() {{
      const varNames = this.getAttribute('data-vars').split(',');
      const layers = varNames.map(v => window[v]).filter(l => l);
      if (this.checked) {{
        layers.forEach(layer => map.addLayer(layer));
      }} else {{
        layers.forEach(layer => map.removeLayer(layer));
      }}
    }});
  }});

  // Masquer la légende quand un popup s'ouvre
  map.on('popupopen', function() {{
    const legend = document.getElementById('legend-panel');
    if (legend) legend.style.display = 'none';
  }});

  // Réafficher la légende quand le popup se ferme
  map.on('popupclose', function() {{
    const legend = document.getElementById('legend-panel');
    if (legend) legend.style.display = 'block';
  }});
}});
</script>
  """

    # ============================================================
    # 🧭 BOUTON DE TOGGLE POUR LA LÉGENDE
    # ============================================================
    legend_toggle_html = """
<!-- 🧭 Bouton pour afficher/masquer la légende -->
<style>
  #legend-toggle-btn {
    position: absolute;
    right: 10px;
    top: 10px;
    z-index: 600;
    background: #2c3e50;
    color: white;
    border: none;
    border-radius: 50%;
    width: 32px;
    height: 32px;
    font-size: 16px;
    cursor: pointer;
    box-shadow: 0 2px 6px rgba(0,0,0,0.3);
    transition: all 0.3s ease;
  }
  #legend-toggle-btn:hover {
    background: #34495e;
  }
  #legend-panel.hidden {
    transform: translateX(380px);
    opacity: 0;
    pointer-events: none;
    transition: all 0.4s ease;
  }
</style>

<button id="legend-toggle-btn" title="Afficher/Masquer la légende">☰</button>

<script>
  document.addEventListener("DOMContentLoaded", function() {
    const btn = document.getElementById("legend-toggle-btn");
    const legend = document.getElementById("legend-panel");

    if (btn && legend) {
      btn.addEventListener("click", () => {
        legend.classList.toggle("hidden");
        // Changement de symbole
        btn.textContent = legend.classList.contains("hidden") ? "📋" : "☰";
      });
    }
  });
</script>
"""

    m.get_root().html.add_child(Element(legend_html))
    m.get_root().html.add_child(Element(legend_toggle_html))
    
    # ============================================================
    # 📱 CSS RESPONSIVE POUR MOBILE/TABLETTE
    # ============================================================
    responsive_css = """
<!-- 📱 Adaptation responsive pour mobile et tablette -->
<style>
/* ✅ Adapter la légende sur petits écrans */
@media (max-width: 768px) {
  #legend-panel {
    width: 85vw !important;
    right: 0 !important;
    left: 0 !important;
    margin: auto;
    top: auto;
    bottom: 10px;
    max-height: 40vh !important;
    font-size: 13px !important;
  }

  #legend-toggle-btn {
    right: 10px !important;
    top: 10px !important;
    width: 38px !important;
    height: 38px !important;
    font-size: 18px !important;
  }

  .leaflet-draw {
    top: 140px !important;
    left: 10px !important;
    transform: scale(0.9);
  }

  .leaflet-control-scale {
    transform: scale(0.9);
  }

  .legend-group-title {
    font-size: 13px !important;
  }

  .layer-title {
    font-size: 12px !important;
  }

  .leaflet-popup-content {
    max-width: 90vw !important;
  }

  /* ✅ Lorsqu'on masque la légende sur mobile, elle descend discrètement */
  #legend-panel.hidden {
    transform: translateY(80vh) !important;
    opacity: 0 !important;
  }
}

/* ✅ Tablette (768px - 1024px) */
@media (min-width: 769px) and (max-width: 1024px) {
  #legend-panel {
    width: 320px !important;
  }
}
</style>
"""
    m.get_root().html.add_child(Element(responsive_css))
    
    # ============================================================
    # 📱 META VIEWPORT POUR RENDU OPTIMAL SUR MOBILE
    # ============================================================
    viewport_meta = """
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
"""
    m.get_root().header.add_child(Element(viewport_meta))
    
    logger.info("   ✅ Légende ajoutée avec bouton de toggle")
    logger.info("   📱 CSS responsive et meta viewport ajoutés")

    # Recalculer les totaux finaux (après ajout du PPRI)
    total_groups_final = sum(len(l['entities']) for l in registry['layers'])
    total_entities_final = sum(sum(e['count'] for e in l['entities']) for l in registry['layers'])
    
    logger.info(f"\n{'='*60}")
    logger.info(f"✅ TERMINÉ !")
    logger.info(f"{'='*60}")
    logger.info(f"📊 {len(registry['layers'])} couches, {total_groups_final} groupes, {total_entities_final} entités")
    logger.info(f"📦 {label_parcelle}\n")
    
    # Générer le HTML string
    html_string = m._repr_html_()
    
    # Métadonnées de retour
    metadata = {
        "label": label_parcelle,
        "nb_couches": len(registry['layers']),
        "nb_groupes": total_groups_final,
        "nb_entites": total_entities_final,
        "ppri": metadata_ppri if inclure_ppri else None
    }
    
    return html_string, metadata


# ============================================================
# 🧩 ADAPTATION POUR ORCHESTRATEUR
# ============================================================
def generate_map_for_orchestrator(wkt_path, output_dir="./out_2d", **kwargs):
    """
    Variante adaptée à l'orchestrateur :
    - Génère la carte 2D depuis un fichier WKT
    - Écrit le fichier HTML dans le dossier de sortie
    - Retourne un dict {path, filename, metadata}
    
    Args:
        wkt_path (str): Chemin vers le fichier WKT
        output_dir (str): Répertoire de sortie
        **kwargs: Arguments passés à generate_map_from_wkt (inclure_ppri, ppri_table, code_insee)
    
    Returns:
        dict: {
            "path": chemin_complet_fichier,
            "filename": nom_fichier,
            "metadata": métadonnées_carte
        }
    """
    os.makedirs(output_dir, exist_ok=True)

    html_string, metadata = generate_map_from_wkt(wkt_path=wkt_path, **kwargs)
    output_path = os.path.join(output_dir, "carte_2d_unite_fonciere.html")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_string)

    return {
        "path": output_path,
        "filename": os.path.basename(output_path),
        "metadata": metadata
    }


# ============================================================
# 🧪 TEST CLI - Génération carte 2D autonome
# ============================================================
if __name__ == "__main__":
    import argparse
    import json
    import os

    parser = argparse.ArgumentParser(description="Génère une carte Folium 2D à partir d'un fichier WKT.")
    parser.add_argument("--wkt", required=True, help="Chemin du fichier WKT contenant la géométrie")
    parser.add_argument("--code_insee", default="33234", help="Code INSEE de la commune (ex: 33234)")
    parser.add_argument("--output", default="./out_2d", help="Dossier de sortie du fichier HTML")
    parser.add_argument("--ppri", action="store_true", default=True, help="Inclure le PPRI dans la carte")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    try:
        html_string, metadata = generate_map_from_wkt(
            wkt_path=args.wkt,
            code_insee=args.code_insee,
            inclure_ppri=args.ppri,
            ppri_table="pm1_detaillee_gironde"
        )

        output_file = os.path.join(
            args.output,
            "carte_2d_unite_fonciere.html"
        )

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_string)

        print(f"\n✅ Carte générée : {output_file}")
        print(json.dumps(metadata, indent=2, ensure_ascii=False))

    except Exception as e:
        print(f"\n❌ Erreur : {e}")
        raise
