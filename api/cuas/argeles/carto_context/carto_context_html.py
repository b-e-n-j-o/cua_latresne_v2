# -*- coding: utf-8 -*-
"""
carto_context_html.py — Carte HTML autonome MapLibre (GeoJSON gelé + légende interactive).

Aligné sur l'interface studyZone Argelès : fond IGN, toggles couches / groupes
discriminants, couleurs par attribut.
"""

from __future__ import annotations

import json
from datetime import datetime
from html import escape

from api.cuas.argeles.carto_context.carto_context_enrich import prepare_layers_payload

IGN_STYLE = "https://data.geopf.fr/annexes/ressources/vectorTiles/styles/PLAN.IGN/standard.json"


def _parcel_label(context: dict) -> str:
    refs = context.get("parcelles") or []
    if len(refs) == 1:
        r = refs[0]
        return f"{r.get('section', '')} {r.get('numero', '')}".strip()
    if len(refs) > 1:
        return f"UF · {len(refs)} parcelles"
    return "Unité foncière"


def render_carto_context_html(
    context: dict,
    *,
    commune_nom: str = "Argelès-sur-Mer",
    numero_cu: str | None = None,
    carto_catalogue: dict | None = None,
) -> str:
    """Construit le HTML autonome MapLibre à partir du payload run_carto_context."""
    label = _parcel_label(context)
    computed = context.get("computed_at") or datetime.utcnow().isoformat()
    buffer_m = context.get("context_buffer_m", 200)
    surface = context.get("surface_m2")
    layers = prepare_layers_payload(context, carto_catalogue)
    parcelle = context.get("parcelle")

    title_parts = [f"Carte d'identité d'urbanisme — {commune_nom}", label]
    if numero_cu:
        title_parts.append(f"({numero_cu})")
    page_title = " · ".join(title_parts)

    payload = {
        "parcelle": parcelle,
        "buffer_m": buffer_m,
        "layers": layers,
        "label": label,
        "commune": commune_nom,
        "computed_at": computed,
        "surface_m2": surface,
        "ignStyle": IGN_STYLE,
    }
    data_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{escape(page_title)}</title>
  <link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet"/>
  <style>
    * {{ box-sizing: border-box; }}
    html, body {{ margin: 0; height: 100%; font-family: system-ui, -apple-system, sans-serif; }}
    #map {{ position: absolute; inset: 0; }}
    .header {{
      position: absolute; top: 10px; left: 48px; z-index: 2;
      background: rgba(255,255,255,0.95); border-radius: 8px; padding: 8px 12px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.12); max-width: 420px; pointer-events: none;
    }}
    .header h1 {{ margin: 0 0 2px; font-size: 14px; color: #1e3a5f; }}
    .header p {{ margin: 0; font-size: 11px; color: #555; }}
    .legend {{
      position: absolute; top: 10px; right: 10px; bottom: 10px; z-index: 2;
      width: min(300px, 38vw); background: rgba(255,255,255,0.97);
      border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.15);
      display: flex; flex-direction: column; overflow: hidden;
    }}
    .legend__head {{ padding: 10px 12px 6px; border-bottom: 1px solid #e5e7eb; }}
    .legend__head h2 {{ margin: 0; font-size: 13px; color: #111; }}
    .legend__scroll {{ overflow-y: auto; padding: 6px 8px 10px; flex: 1; }}
    .fam {{ margin-top: 6px; }}
    .fam__row {{ display: flex; align-items: center; gap: 6px; padding: 4px 2px; }}
    .fam__title {{ font-size: 10px; font-weight: 700; text-transform: uppercase;
      letter-spacing: 0.04em; color: #475569; flex: 1; }}
    .layer {{ border-top: 1px solid #f1f5f9; padding: 4px 0; }}
    .layer__row {{ display: flex; align-items: center; gap: 6px; padding: 2px 4px; }}
    .layer__title {{ font-size: 12px; color: #0f172a; flex: 1; cursor: pointer; }}
    .layer__title.off {{ color: #94a3b8; }}
    .layer__meta {{ font-size: 10px; color: #64748b; }}
    .groups {{ margin: 2px 0 4px 22px; }}
    .grp {{ display: flex; align-items: center; gap: 6px; padding: 2px 0; }}
    .swatch {{ width: 12px; height: 12px; border-radius: 2px; border: 1px solid rgba(0,0,0,.15); flex-shrink: 0; }}
    .grp__label {{ font-size: 10px; color: #334155; flex: 1; truncate; overflow: hidden;
      text-overflow: ellipsis; white-space: nowrap; }}
    .grp__label.disabled {{ color: #cbd5e1; }}
    .grp input:disabled {{ cursor: not-allowed; opacity: 0.45; }}
    .btn {{ background: none; border: none; cursor: pointer; color: #64748b; font-size: 11px; padding: 0 4px; }}
    input[type=checkbox] {{ accent-color: #2563eb; cursor: pointer; }}
    .popup {{ font-size: 12px; max-width: 300px; }}
    .popup h3 {{ margin: 0 0 6px; font-size: 13px; color: #1e3a5f; }}
    .popup .near {{ color: #c05621; font-weight: 600; font-size: 11px; margin-bottom: 4px; }}
    .popup table {{ border-collapse: collapse; width: 100%; }}
    .popup th {{ text-align: left; color: #64748b; padding: 1px 6px 1px 0; vertical-align: top; font-weight: 500; }}
    .popup td {{ color: #111; padding: 1px 0; }}
    .maplibregl-popup-content {{ padding: 10px 12px; border-radius: 6px; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="header">
    <h1>{escape(page_title)}</h1>
    <p>Données figées · {escape(str(computed)[:19].replace("T", " "))} UTC · {buffer_m:.0f} m
      {f" · {surface:.0f} m²" if surface else ""}</p>
  </div>
  <aside class="legend" id="legend">
    <div class="legend__head"><h2>Couches &amp; entités</h2></div>
    <div class="legend__scroll" id="legend-scroll"></div>
  </aside>
  <script src="https://unpkg.com/@turf/turf@7.2.0/dist/turf.min.js"></script>
  <script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
  <script>
    const DATA = {data_json};

    const state = {{
      visibleLayers: new Set(DATA.layers.map(l => l.layer_id)),
      visibleGroups: {{}},
      expandedLayers: new Set(),
      expandedFamilies: new Set(),
    }};
    for (const layer of DATA.layers) {{
      const keys = layer.legend_items.map(i => i.key);
      state.visibleGroups[layer.layer_id] = new Set(keys);
      state.expandedLayers.add(layer.layer_id);
    }}
    for (const layer of DATA.layers) state.expandedFamilies.add(layer.family_title);

    const mapLayerIds = {{}};
    const mapSourceIds = {{}};

    function sid(layerId) {{ return "ctx-src-" + layerId.replace(/[^a-zA-Z0-9_-]/g, "_"); }}
    function lid(layerId, suffix) {{ return "ctx-" + layerId.replace(/[^a-zA-Z0-9_-]/g, "_") + "-" + suffix; }}

    function groupFilter(layer) {{
      const keys = Array.from(state.visibleGroups[layer.layer_id] || []);
      if (!layer.filterable || keys.length === 0) return ["literal", true];
      return ["in", ["get", "_studyKey"], ["literal", keys]];
    }}

    function setLayerVisibility(layerId, on) {{
      const ids = mapLayerIds[layerId] || [];
      for (const id of ids) {{
        if (map.getLayer(id)) map.setLayoutProperty(id, "visibility", on ? "visible" : "none");
      }}
    }}

    function applyGroupFilter(layer) {{
      const ids = mapLayerIds[layer.layer_id] || [];
      const filt = groupFilter(layer);
      for (const id of ids) {{
        if (map.getLayer(id)) map.setFilter(id, filt);
      }}
    }}

    const UF_OUTLINE = "#FBBF24";
    const BUFFER_LINE = "#FACC15";

    function setLayerVisible(layerId, on) {{
      const layer = DATA.layers.find(l => l.layer_id === layerId);
      if (!layer) return;
      if (on) {{
        state.visibleLayers.add(layerId);
        state.visibleGroups[layerId] = new Set(layer.legend_items.map(i => i.key));
      }} else {{
        state.visibleLayers.delete(layerId);
        state.visibleGroups[layerId] = new Set();
      }}
      refreshMap();
      renderLegend();
    }}

    function bringUfAndBufferToFront() {{
      for (const id of ["ctx-buffer-line", "ctx-parcelle-fill", "ctx-parcelle-outline"]) {{
        if (map.getLayer(id)) map.moveLayer(id);
      }}
    }}

    function addStudyZoneOverlays() {{
      if (!DATA.parcelle) return;

      if (DATA.buffer_m > 0 && typeof turf !== "undefined") {{
        try {{
          const bufPoly = turf.buffer(DATA.parcelle, DATA.buffer_m / 1000, {{
            units: "kilometers", steps: 32,
          }});
          const bufLine = turf.polygonToLine(bufPoly);
          map.addSource("ctx-buffer", {{ type: "geojson", data: bufLine }});
          map.addLayer({{
            id: "ctx-buffer-line", type: "line", source: "ctx-buffer",
            paint: {{
              "line-color": BUFFER_LINE,
              "line-width": 2,
              "line-dasharray": [4, 3],
              "line-opacity": 0.95,
            }},
          }});
        }} catch (e) {{ console.warn("buffer", e); }}
      }}

      map.addSource("ctx-parcelle", {{ type: "geojson", data: DATA.parcelle }});
      map.addLayer({{
        id: "ctx-parcelle-fill", type: "fill", source: "ctx-parcelle",
        paint: {{ "fill-color": UF_OUTLINE, "fill-opacity": 0.12 }},
      }});
      map.addLayer({{
        id: "ctx-parcelle-outline", type: "line", source: "ctx-parcelle",
        paint: {{ "line-color": UF_OUTLINE, "line-width": 3.5, "line-opacity": 1 }},
      }});
      bringUfAndBufferToFront();
    }}

    function refreshMap() {{
      for (const layer of DATA.layers) {{
        const on = state.visibleLayers.has(layer.layer_id);
        setLayerVisibility(layer.layer_id, on);
        if (on) applyGroupFilter(layer);
      }}
      bringUfAndBufferToFront();
    }}

    function popupHtml(layer, props) {{
      let html = '<div class="popup"><h3>' + esc(layer.title) + '</h3>';
      if (props.intersects_parcel === false) html += '<div class="near">À proximité (hors UF)</div>';
      if (props._studyLabel) html += '<p><strong>' + esc(props._studyLabel) + '</strong></p>';
      html += '<table>';
      const skip = new Set(["_fid","_studyKey","_studyColor","_studyLabel","_layerId","intersects_parcel"]);
      for (const [k,v] of Object.entries(props)) {{
        if (skip.has(k) || v == null || v === "") continue;
        html += "<tr><th>" + esc(k) + "</th><td>" + esc(String(v).slice(0,140)) + "</td></tr>";
      }}
      return html + "</table></div>";
    }}

    function esc(s) {{
      return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
    }}

    const map = new maplibregl.Map({{
      container: "map",
      style: DATA.ignStyle,
      center: [3.017, 42.548],
      zoom: 14,
      attributionControl: true,
    }});

    map.addControl(new maplibregl.NavigationControl(), "top-left");

    const popup = new maplibregl.Popup({{ closeButton: true, maxWidth: "320px" }});

    map.on("load", () => {{
      const clickable = [];

      for (const layer of DATA.layers) {{
        const src = sid(layer.layer_id);
        const fc = {{ type: "FeatureCollection", features: layer.features }};
        map.addSource(src, {{ type: "geojson", data: fc }});
        mapSourceIds[layer.layer_id] = src;
        const lids = [];
        const gt = layer.geom_type;
        const colorExpr = ["coalesce", ["get", "_studyColor"], "#888888"];
        const filt = groupFilter(layer);

        if (gt === "lineaire") {{
          const id = lid(layer.layer_id, "line");
          map.addLayer({{
            id, type: "line", source: src, filter: filt,
            paint: {{
              "line-color": colorExpr,
              "line-width": ["case", ["==", ["get", "intersects_parcel"], true], 4, 2],
              "line-opacity": ["case", ["==", ["get", "intersects_parcel"], true], 0.9, 0.45],
            }},
          }});
          lids.push(id); clickable.push(id);
        }} else if (gt === "ponctuel") {{
          const id = lid(layer.layer_id, "circle");
          map.addLayer({{
            id, type: "circle", source: src, filter: filt,
            paint: {{
              "circle-color": colorExpr,
              "circle-radius": ["case", ["==", ["get", "intersects_parcel"], true], 7, 5],
              "circle-stroke-color": "#fff", "circle-stroke-width": 1,
              "circle-opacity": ["case", ["==", ["get", "intersects_parcel"], true], 0.9, 0.5],
            }},
          }});
          lids.push(id); clickable.push(id);
        }} else {{
          const fillId = lid(layer.layer_id, "fill");
          map.addLayer({{
            id: fillId, type: "fill", source: src, filter: filt,
            paint: {{
              "fill-color": colorExpr,
              "fill-opacity": ["case", ["==", ["get", "intersects_parcel"], true], 0.42, 0.18],
            }},
          }});
          const lineId = lid(layer.layer_id, "outline");
          map.addLayer({{
            id: lineId, type: "line", source: src, filter: filt,
            paint: {{ "line-color": colorExpr, "line-width": 1.2, "line-opacity": 0.85 }},
          }});
          lids.push(fillId, lineId);
          clickable.push(fillId, lineId);
        }}
        mapLayerIds[layer.layer_id] = lids;
      }}

      addStudyZoneOverlays();

      map.on("click", (e) => {{
        const features = map.queryRenderedFeatures(e.point, {{ layers: clickable }});
        if (!features.length) return;
        const f = features[0];
        const layerId = f.properties && f.properties._layerId;
        const meta = DATA.layers.find(l => l.layer_id === layerId);
        if (meta) popup.setLngLat(e.lngLat).setHTML(popupHtml(meta, f.properties || {{}})).addTo(map);
      }});

      map.on("mouseenter", clickable, () => {{ map.getCanvas().style.cursor = "pointer"; }});
      map.on("mouseleave", clickable, () => {{ map.getCanvas().style.cursor = ""; }});

      const bounds = new maplibregl.LngLatBounds();
      const extend = (gj) => {{
        if (!gj || !gj.geometry) return;
        const g = gj.geometry;
        const ring = (c) => {{ for (const p of c) bounds.extend(p); }};
        if (g.type === "Point") bounds.extend(g.coordinates);
        else if (g.type === "MultiPoint" || g.type === "LineString") ring(g.coordinates);
        else if (g.type === "MultiLineString" || g.type === "Polygon") g.coordinates.forEach(ring);
        else if (g.type === "MultiPolygon") g.coordinates.forEach(poly => poly.forEach(ring));
      }};
      if (DATA.parcelle) extend(DATA.parcelle);
      for (const layer of DATA.layers) layer.features.forEach(extend);
      if (!bounds.isEmpty()) map.fitBounds(bounds, {{ padding: 60, maxZoom: 17 }});

      buildLegend();
      refreshMap();
    }});

    function toggleGroup(layerId, key) {{
      if (!state.visibleLayers.has(layerId)) return;
      const set = state.visibleGroups[layerId];
      if (!set) return;
      if (set.has(key)) set.delete(key); else set.add(key);
      applyGroupFilter(DATA.layers.find(l => l.layer_id === layerId));
      renderLegend();
    }}

    function buildLegend() {{ renderLegend(); }}

    function renderLegend() {{
      const root = document.getElementById("legend-scroll");
      root.innerHTML = "";
      const byFam = {{}};
      for (const layer of DATA.layers) {{
        if (!byFam[layer.family_title]) byFam[layer.family_title] = [];
        byFam[layer.family_title].push(layer);
      }}

      for (const [famTitle, layers] of Object.entries(byFam)) {{
        const famOn = layers.some(l => state.visibleLayers.has(l.layer_id));
        const famExp = state.expandedFamilies.has(famTitle);
        const famDiv = document.createElement("div");
        famDiv.className = "fam";
        famDiv.innerHTML = `<div class="fam__row">
          <button class="btn" data-fam="${{esc(famTitle)}}">${{famExp ? "▾" : "▸"}}</button>
          <span class="fam__title">${{esc(famTitle)}}</span>
          <input type="checkbox" ${{famOn ? "checked" : ""}} data-fam-toggle="${{esc(famTitle)}}"/>
        </div>`;
        root.appendChild(famDiv);

        famDiv.querySelector("[data-fam]").onclick = () => {{
          if (famExp) state.expandedFamilies.delete(famTitle); else state.expandedFamilies.add(famTitle);
          renderLegend();
        }};
        famDiv.querySelector("[data-fam-toggle]").onchange = (e) => {{
          const checked = e.target.checked;
          for (const l of layers) {{
            if (checked) {{
              state.visibleLayers.add(l.layer_id);
              state.visibleGroups[l.layer_id] = new Set(l.legend_items.map(i => i.key));
            }} else {{
              state.visibleLayers.delete(l.layer_id);
              state.visibleGroups[l.layer_id] = new Set();
            }}
          }}
          refreshMap();
          renderLegend();
        }};

        if (!famExp) continue;

        for (const layer of layers) {{
          const on = state.visibleLayers.has(layer.layer_id);
          const exp = state.expandedLayers.has(layer.layer_id);
          const layerDiv = document.createElement("div");
          layerDiv.className = "layer";
          const hits = layer.features.filter(f => f.properties && f.properties.intersects_parcel).length;
          layerDiv.innerHTML = `<div class="layer__row">
            <button class="btn" data-layer-exp="${{layer.layer_id}}">${{exp ? "▾" : "▸"}}</button>
            <input type="checkbox" ${{on ? "checked" : ""}} data-layer="${{layer.layer_id}}"/>
            <span class="layer__title ${{on ? "" : "off"}}">${{esc(layer.title)}}
              <span class="layer__meta">(${{layer.count}}${{hits ? ", " + hits + " sur UF" : ""}})</span></span>
          </div>`;
          famDiv.appendChild(layerDiv);

          layerDiv.querySelector("[data-layer-exp]").onclick = () => {{
            if (exp) state.expandedLayers.delete(layer.layer_id);
            else state.expandedLayers.add(layer.layer_id);
            renderLegend();
          }};
          layerDiv.querySelector("[data-layer]").onchange = (e) => setLayerVisible(layer.layer_id, e.target.checked);

          if (!exp || !layer.filterable || layer.legend_items.length <= 1) continue;

          const groups = document.createElement("div");
          groups.className = "groups";

          for (const item of layer.legend_items) {{
            if (item.count === 0) continue;
            const gOn = on && state.visibleGroups[layer.layer_id].has(item.key);
            const row = document.createElement("div");
            row.className = "grp";
            row.innerHTML = `<input type="checkbox" ${{gOn ? "checked" : ""}} ${{on ? "" : "disabled"}}/>
              <span class="swatch" style="background:${{item.color}};${{on ? "" : "opacity:0.35"}}"></span>
              <span class="grp__label ${{on ? "" : "disabled"}}" title="${{esc(item.label)}}">${{esc(item.label)}}</span>
              <span class="grp__count">${{item.count}}</span>`;
            if (on) row.querySelector("input").onchange = () => toggleGroup(layer.layer_id, item.key);
            groups.appendChild(row);
          }}
          layerDiv.appendChild(groups);
        }}
      }}
    }}
  </script>
</body>
</html>"""
