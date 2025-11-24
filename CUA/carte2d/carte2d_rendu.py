# CUA/carte2d/carte2d_rendu.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
carte2d_rendu.py
----------------------------------------------------
Responsable de :
- créer la carte Folium
- ajouter parcelle, couches métiers, PPRI
- générer la légende interactive
- ajouter CSS/JS (outils, responsive)
- exporter en HTML
"""

import json
import logging
from pathlib import Path

import folium
import geopandas as gpd
from folium import Element

from CUA.carte2d.carte2d_extraction import (
    ENGINE,
    charger_catalogue,
    selectionner_couches_pour_parcelle,
    extraire_entites_pour_couche,
)
from CUA.carte2d.carte2d_metier import construire_couche_metier
from CUA.map_utils import random_color
from CUA.ppri_map_module import ajouter_ppri_a_carte

logger = logging.getLogger("carte2d.rendu")

SCHEMA = "latresne"
BUFFER_DIST = 200

BASEMAP_CONF = {
    "tiles": "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
    "attr": "© OpenStreetMap contributors © CARTO",
}


# ============================================================
# FONCTION PRINCIPALE
# ============================================================
def generer_carte_2d_depuis_wkt(
    wkt_path: str,
    code_insee: str = "33234",
    inclure_ppri: bool = True,
    ppri_table: str = "pm1_detaillee_gironde",
    catalogue_path: str | None = None,
):
    """
    Génère une carte 2D Folium à partir d'un WKT, en utilisant :
      - carte2d_extraction pour la DB
      - carte2d_metier pour les couches métiers + registry
      - ici pour le rendu global, PPRI, légende, CSS/JS, HTML.

    Retourne :
      html_string, metadata
    """
    logger.info("=" * 60)
    logger.info("🌍 DÉBUT — Génération carte 2D")
    logger.info("=" * 60)

    # --------------------------------------------------------
    # 1) Lecture WKT
    # --------------------------------------------------------
    wkt_file = Path(wkt_path).resolve()
    if not wkt_file.exists():
        raise FileNotFoundError(wkt_file)

    wkt_geom = wkt_file.read_text(encoding="utf-8").strip()
    logger.info(f"📄 Fichier WKT : {wkt_file}")
    logger.info(f"📏 Longueur du WKT : {len(wkt_geom)} caractères")

    gdf_parcelle = gpd.GeoDataFrame(
        geometry=[gpd.GeoSeries.from_wkt([wkt_geom])[0]],
        crs="EPSG:2154",
    )
    gdf_parcelle_4326 = gdf_parcelle.to_crs(4326)
    centroid = gdf_parcelle_4326.geometry.iloc[0].centroid

    logger.info(
        f"   ✅ Géométrie chargée : {gdf_parcelle.geometry.iloc[0].geom_type}, "
        f"centroïde = ({centroid.y:.6f}, {centroid.x:.6f})"
    )

    # --------------------------------------------------------
    # 2) Création de la carte Folium
    # --------------------------------------------------------
    m = folium.Map(
        location=[centroid.y, centroid.x],
        zoom_start=17,
        max_zoom=25,
        tiles=BASEMAP_CONF["tiles"],
        attr=BASEMAP_CONF["attr"],
    )
    map_var = m.get_name()
    logger.info(f"🗺️  Carte Folium créée : {map_var}")

    # --------------------------------------------------------
    # 3) Outils UI : mesure, échelle, rose des vents
    # --------------------------------------------------------

    # === Outil de mesure (mètres & m²) ===
    measure_plugin = f"""
<!-- Leaflet.Draw plugin -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet-draw@1.0.4/dist/leaflet.draw.css" />
<script src="https://cdn.jsdelivr.net/npm/leaflet-draw@1.0.4/dist/leaflet.draw.js"></script>

<style>
.leaflet-draw {{
  position: absolute !important;
  left: 10px !important;
  top: 200px !important;
  z-index: 1000 !important;
}}
.leaflet-draw-toolbar a {{
  background-color: white !important;
  border: 1px solid #ccc !important;
  border-radius: 4px !important;
  box-shadow: 0 2px 6px rgba(0,0,0,0.15);
}}
.leaflet-draw-actions a {{
  background: #f8f9fa !important;
  color: #333 !important;
}}
</style>

<script>
document.addEventListener("DOMContentLoaded", function() {{
  const interval = setInterval(function() {{
    const map = window["{map_var}"];
    if (!map) return;

    clearInterval(interval);

    const drawnItems = new L.FeatureGroup();
    map.addLayer(drawnItems);

    const drawControl = new L.Control.Draw({{
      draw: {{
        marker: false,
        circle: false,
        circlemarker: false,
        rectangle: false,
        polyline: {{
          shapeOptions: {{ color: '#e74c3c', weight: 3 }},
          metric: true,
          feet: false,
          showLength: true
        }},
        polygon: {{
          shapeOptions: {{ color: '#2ecc71', weight: 2 }},
          allowIntersection: false,
          showArea: true,
          showLength: false,
          metric: true
        }}
      }},
      edit: {{
        featureGroup: drawnItems,
        edit: false,
        remove: true
      }}
    }});
    map.addControl(drawControl);

    map.on(L.Draw.Event.CREATED, function (e) {{
      drawnItems.clearLayers();
      const layer = e.layer;
      drawnItems.addLayer(layer);

      if (e.layerType === 'polygon') {{
        const latlngs = layer.getLatLngs()[0];
        if (latlngs.length > 2) {{
          const area = L.GeometryUtil.geodesicArea(latlngs);
          const surface_m2 = area.toFixed(2);
          layer.bindTooltip(surface_m2 + ' m²', {{ permanent: true, direction: 'center' }}).openTooltip();
        }}
      }}
      if (e.layerType === 'polyline') {{
        const latlngs = layer.getLatLngs();
        let distance = 0;
        for (let i = 0; i < latlngs.length - 1; i++) {{
          distance += latlngs[i].distanceTo(latlngs[i + 1]);
        }}
        const distance_m = distance.toFixed(2);
        layer.bindTooltip(distance_m + ' m', {{ permanent: true, direction: 'center' }}).openTooltip();
      }}
    }});

    console.log("✅ Outil de mesure chargé (mètres et m²)");
  }}, 500);
}});
</script>
"""
    m.get_root().html.add_child(Element(measure_plugin))

    # === Échelle métrique (en bas à droite) ===
    scale_control = f"""
<script>
window.addEventListener('load', function() {{
    const mapVar = '{map_var}';
    const map = window[mapVar];
    if (map) {{
        L.control.scale({{
            position: 'bottomright',
            metric: true,
            imperial: false,
            maxWidth: 200
        }}).addTo(map);
    }}
}});
</script>
"""
    m.get_root().html.add_child(Element(scale_control))

    # === Rose des vents (haut à gauche) ===
    compass_svg = """
<div style="position:absolute;top:80px;left:10px;z-index:1000;">
    <svg width="80" height="80" viewBox="0 0 100 100">
        <circle cx="50" cy="50" r="75" fill="white" stroke="#333" stroke-width="2" opacity="0"/>
        <g transform="translate(50,50)">
            <polygon points="0,-35 -6,-10 0,-15 6,-10" fill="#e74c3c" stroke="#c0392b" stroke-width="1"/>
            <polygon points="0,35 -6,10 0,15 6,10" fill="#34495e" stroke="#2c3e50" stroke-width="1"/>
            <polygon points="35,0 10,-6 15,0 10,6" fill="#34495e" stroke="#2c3e50" stroke-width="1"/>
            <polygon points="-35,0 -10,-6 -15,0 -10,6" fill="#34495e" stroke="#2c3e50" stroke-width="1"/>
            <text x="0" y="-42" text-anchor="middle" font-size="14" font-weight="bold" fill="#e74c3c">N</text>
            <text x="0" y="50" text-anchor="middle" font-size="12" font-weight="bold" fill="#34495e">S</text>
            <text x="42" y="5" text-anchor="middle" font-size="12" font-weight="bold" fill="#34495e">E</text>
            <text x="-42" y="5" text-anchor="middle" font-size="12" font-weight="bold" fill="#34495e">O</text>
        </g>
    </svg>
</div>
"""
    m.get_root().html.add_child(Element(compass_svg))

    # --------------------------------------------------------
    # 4) Ajout de la parcelle (UF) en rouge pointillé
    # --------------------------------------------------------
    logger.info("📦 Ajout de la parcelle (unité foncière) sur la carte...")
    folium.GeoJson(
        gdf_parcelle_4326,
        name="Parcelle",
        style_function=lambda x: {
            "color": "red",
            "weight": 4,
            "fillOpacity": 0,
            "dashArray": "5, 5",
        },
        show=True,
    ).add_to(m)
    logger.info("   ✅ Parcelle ajoutée")

    parcelle_wkt = wkt_geom
    label_parcelle = "Unité foncière"

    # --------------------------------------------------------
    # 5) Chargement du catalogue + sélection des couches
    # --------------------------------------------------------
    logger.info("\n📚 Chargement du catalogue de couches...")
    catalogue = charger_catalogue(catalogue_path)
    logger.info(f"   ✅ {len(catalogue)} couches dans le catalogue")

    logger.info("\n🔍 Sélection des couches intersectant l'unité foncière...")
    layers_on_parcel = selectionner_couches_pour_parcelle(
        parcelle_wkt=parcelle_wkt,
        schema=SCHEMA,
        buffer_dist=BUFFER_DIST,
        catalogue=catalogue,
        engine=ENGINE,
    )
    logger.info(f"   ✅ {len(layers_on_parcel)} couches candidates")

    # --------------------------------------------------------
    # 6) Initialisation du registry pour la légende
    # --------------------------------------------------------
    registry = {
        "mapVar": map_var,
        "layers": [],
    }

    # --------------------------------------------------------
    # 7) Tri des couches par type (importance) avant rendu
    # --------------------------------------------------------
    ordre_types = {
        "Zonage PLU": 1,
        "Servitudes": 2,
        "Prescriptions": 3,
        "Informations": 4,
    }

    layers_on_parcel = dict(
        sorted(
            layers_on_parcel.items(),
            key=lambda item: ordre_types.get(
                catalogue.get(item[0], {}).get("type", ""), 999
            ),
        )
    )
    logger.info(
        "   🔄 Couches triées par importance "
        "(Zonage PLU → Servitudes → Prescriptions → Informations)"
    )

    # --------------------------------------------------------
    # 8) Extraction + couche métier (avec ajout Folium) pour chaque table
    # --------------------------------------------------------
    for table, config in layers_on_parcel.items():
        logger.info(f"\n🔎 Couche : {table}")

        rows, keys = extraire_entites_pour_couche(
            table=table,
            config=config,
            parcelle_wkt=parcelle_wkt,
            buffer_dist=BUFFER_DIST,
            schema=SCHEMA,
            engine=ENGINE,
        )
        if not rows:
            continue

        # 👉 Ici, construire_couche_metier :
        #    - crée les GeoJson sur la carte “m”
        #    - remplit registry["layers"] (mode, entities, vars, counts, etc.)
        construire_couche_metier(
            table=table,
            config=config,
            rows=rows,
            keys=keys,
            registry=registry,
            catalogue=catalogue,
            random_color_fn=random_color,
            map_obj=m,  # 🔴 IMPORTANT : prévoir ce param dans carte2d_metier
        )

    # --------------------------------------------------------
    # 9) Ajout du PPRI (optionnel)
    # --------------------------------------------------------
    metadata_ppri = None
    if inclure_ppri:
        logger.info("\n🌊 Intégration du module PPRI...")
        try:
            metadata_ppri = ajouter_ppri_a_carte(
                map_folium=m,
                geom_wkt=parcelle_wkt,
                code_insee=code_insee,
                ppri_table=f"{SCHEMA}.{ppri_table}",
                engine=ENGINE,
                show=False,
                registry=registry,
            )
            if metadata_ppri and metadata_ppri.get("success"):
                logger.info(
                    f"   ✅ PPRI intégré : "
                    f"{metadata_ppri['zones_conservees']} zones conservées, "
                    f"taux absorption = {metadata_ppri.get('taux_absorption', 0):.1f}%"
                )
            else:
                logger.warning("   ⚠️ PPRI : aucune zone trouvée ou succès=False")
        except Exception as e:
            logger.error(f"   ❌ Erreur PPRI : {e}")
            metadata_ppri = {"success": False, "error": str(e)}

    # --------------------------------------------------------
    # 10) Légende interactive (JS) + regroupement par type
    # --------------------------------------------------------
    logger.info("\n🏷️  Génération de la légende interactive...")

    # Regrouper les couches du registry par type (catalogue)
    grouped_layers = {}
    for layer in registry["layers"]:
        table_name = None
        # Retrouver le nom de table via le nom métier si possible
        for t, cfg in layers_on_parcel.items():
            if cfg.get("nom", t) == layer["name"]:
                table_name = t
                break

        if table_name:
            type_couche = catalogue.get(table_name, {}).get("type", "Autres")
        else:
            type_couche = "PPRI" if "PPRI" in layer["name"] else "Autres"

        grouped_layers.setdefault(type_couche, []).append(layer)

    ordre_types_legende = [
        "Zonage PLU",
        "Servitudes",
        "Prescriptions",
        "Informations",
        "PPRI",
        "Autres",
    ]
    sorted_grouped_layers = [
        (t, grouped_layers[t]) for t in ordre_types_legende if t in grouped_layers
    ]

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

  map.on('popupopen', function() {{
    const legend = document.getElementById('legend-panel');
    if (legend) legend.style.display = 'none';
  }});

  map.on('popupclose', function() {{
    const legend = document.getElementById('legend-panel');
    if (legend) legend.style.display = 'block';
  }});
}});
</script>
"""
    m.get_root().html.add_child(Element(legend_html))

    # --------------------------------------------------------
    # 11) Bouton toggle légende
    # --------------------------------------------------------
    legend_toggle_html = """
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
        btn.textContent = legend.classList.contains("hidden") ? "📋" : "☰";
      });
    }
  });
</script>
"""
    m.get_root().html.add_child(Element(legend_toggle_html))

    # --------------------------------------------------------
    # 12) CSS responsive + meta viewport
    # --------------------------------------------------------
    responsive_css = """
<style>
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

  #legend-panel.hidden {
    transform: translateY(80vh) !important;
    opacity: 0 !important;
  }
}

@media (min-width: 769px) and (max-width: 1024px) {
  #legend-panel {
    width: 320px !important;
  }
}
</style>
"""
    m.get_root().html.add_child(Element(responsive_css))

    viewport_meta = """
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
"""
    m.get_root().header.add_child(Element(viewport_meta))

    # --------------------------------------------------------
    # 13) Résumé & métadonnées
    # --------------------------------------------------------
    nb_couches = len(registry["layers"])
    nb_groupes = sum(len(l["entities"]) for l in registry["layers"])
    nb_entites = sum(sum(e["count"] for e in l["entities"]) for l in registry["layers"])

    logger.info("\n" + "=" * 60)
    logger.info("✅ CARTE 2D TERMINÉE")
    logger.info("=" * 60)
    logger.info(
        f"📊 {nb_couches} couches, {nb_groupes} groupes, {nb_entites} entités"
    )
    logger.info(f"📦 {label_parcelle}\n")

    html_string = m._repr_html_()
    metadata = {
        "label": label_parcelle,
        "nb_couches": nb_couches,
        "nb_groupes": nb_groupes,
        "nb_entites": nb_entites,
        "ppri": metadata_ppri,
    }

    return html_string, metadata


# ============================================================
# Variante orchestrateur : écrire le HTML dans un dossier
# ============================================================
def generer_carte_2d_orchestrateur(
    wkt_path: str,
    output_dir: str = "./out_2d",
    **kwargs,
):
    """
    Variante pour orchestrateur_global :
      - génère la carte 2D depuis un WKT
      - écrit un fichier HTML dans output_dir
      - retourne {path, filename, metadata}
    """
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    html_string, metadata = generer_carte_2d_depuis_wkt(
        wkt_path=wkt_path,
        **kwargs,
    )

    output_file = output_dir_path / "carte_2d_unite_fonciere.html"
    output_file.write_text(html_string, encoding="utf-8")

    return {
        "path": str(output_file),
        "filename": output_file.name,
        "metadata": metadata,
    }


if __name__ == "__main__":
    import argparse
    import os

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Génère une carte Folium 2D à partir d'un fichier WKT."
    )
    parser.add_argument("--wkt", required=True, help="Chemin du fichier WKT")
    parser.add_argument("--code_insee", default="33234", help="Code INSEE (ex: 33234)")
    parser.add_argument(
        "--output", default="./out_2d", help="Dossier de sortie du fichier HTML"
    )
    parser.add_argument(
        "--ppri",
        action="store_true",
        default=True,
        help="Inclure le PPRI dans la carte",
    )
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    try:
        html_string, metadata = generer_carte_2d_depuis_wkt(
            wkt_path=args.wkt,
            code_insee=args.code_insee,
            inclure_ppri=args.ppri,
            ppri_table="pm1_detaillee_gironde",
        )
        output_file = Path(args.output) / "carte_2d_unite_fonciere.html"
        output_file.write_text(html_string, encoding="utf-8")

        print(f"\n✅ Carte générée : {output_file}")
        print(json.dumps(metadata, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"\n❌ Erreur : {e}")
        raise
