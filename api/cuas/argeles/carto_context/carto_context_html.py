# -*- coding: utf-8 -*-
"""
carto_context_html.py — Carte HTML autonome (GeoJSON gelé) depuis run_carto_context.

Génère une page Leaflet self-contained : données embarquées en JSON, consultable
sans appel API ultérieur (snapshot réglementaire au moment du certificat).
"""

from __future__ import annotations

import json
from datetime import datetime
from html import escape

FAMILY_COLORS: dict[str, str] = {
    "zonages_plu": "#2563eb",
    "prescriptions": "#f97316",
    "informations": "#7c3aed",
    "servitudes": "#f43f5e",
    "risques": "#dc2626",
    "environnement": "#059669",
    "reseaux": "#475569",
    "cadastre": "#6b7280",
    "_other": "#9ca3af",
}


def _parcel_label(context: dict) -> str:
    refs = context.get("parcelles") or []
    if len(refs) == 1:
        r = refs[0]
        return f"{r.get('section', '')} {r.get('numero', '')}".strip()
    if len(refs) > 1:
        return f"UF · {len(refs)} parcelles"
    return "Unité foncière"


def _prepare_layers(context: dict) -> list[dict]:
    layers_out = []
    for layer_id, layer in (context.get("layers") or {}).items():
        features = (layer.get("features") or {}).get("features") or []
        if not features:
            continue
        family = layer.get("family") or "_other"
        color = FAMILY_COLORS.get(family, FAMILY_COLORS["_other"])
        layers_out.append({
            "id": layer_id,
            "title": layer.get("title") or layer_id,
            "family": family,
            "family_title": layer.get("family_title") or family,
            "geom_type": layer.get("geom_type") or "surfacique",
            "color": color,
            "count": len(features),
            "features": features,
        })
    layers_out.sort(key=lambda x: (x["family"], x["title"]))
    return layers_out


def render_carto_context_html(
    context: dict,
    *,
    commune_nom: str = "Argelès-sur-Mer",
    numero_cu: str | None = None,
) -> str:
    """Construit le HTML autonome à partir du payload run_carto_context."""
    label = _parcel_label(context)
    computed = context.get("computed_at") or datetime.utcnow().isoformat()
    buffer_m = context.get("context_buffer_m", 200)
    surface = context.get("surface_m2")
    layers = _prepare_layers(context)
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
    }
    data_json = json.dumps(payload, ensure_ascii=False)
    data_json = data_json.replace("</", "<\\/")

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{escape(page_title)}</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    * {{ box-sizing: border-box; }}
    html, body {{ margin: 0; height: 100%; font-family: system-ui, -apple-system, sans-serif; }}
    #map {{ position: absolute; inset: 0; }}
    .header {{
      position: absolute; top: 12px; left: 56px; right: 12px; z-index: 1000;
      background: rgba(255,255,255,0.95); border-radius: 8px; padding: 10px 14px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.15); max-width: 520px;
    }}
    .header h1 {{ margin: 0 0 4px; font-size: 15px; color: #1e3a5f; }}
    .header p {{ margin: 0; font-size: 12px; color: #555; line-height: 1.4; }}
    .legend {{
      position: absolute; bottom: 24px; right: 12px; z-index: 1000;
      background: rgba(255,255,255,0.96); border-radius: 8px; padding: 10px 12px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.15); max-height: 45vh; overflow-y: auto;
      min-width: 200px; max-width: 280px; font-size: 12px;
    }}
    .legend h2 {{ margin: 0 0 8px; font-size: 13px; color: #333; }}
    .legend-item {{ display: flex; align-items: center; gap: 8px; margin: 4px 0; }}
    .swatch {{ width: 14px; height: 14px; border-radius: 3px; flex-shrink: 0; border: 1px solid rgba(0,0,0,0.2); }}
    .legend-family {{ font-weight: 600; color: #444; margin-top: 8px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.03em; }}
    .badge {{ display: inline-block; background: #e8f4fc; color: #1e5a8a; padding: 2px 6px; border-radius: 4px; font-size: 11px; margin-left: 4px; }}
    .popup-table {{ border-collapse: collapse; font-size: 12px; }}
    .popup-table th {{ text-align: left; padding: 2px 8px 2px 0; color: #666; vertical-align: top; white-space: nowrap; }}
    .popup-table td {{ padding: 2px 0; color: #222; }}
    .leaflet-popup-content {{ margin: 10px 12px; max-width: 320px; }}
    .near-label {{ color: #c05621; font-weight: 600; font-size: 11px; }}
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="header">
    <h1>{escape(page_title)}</h1>
    <p>
      Données figées au {escape(str(computed)[:19].replace("T", " "))} UTC
      · zone d'étude {buffer_m:.0f} m
      {f"· {surface:.0f} m²" if surface else ""}
    </p>
  </div>
  <div class="legend" id="legend"></div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const DATA = {data_json};

    const map = L.map("map", {{ zoomControl: true }});
    L.tileLayer("https://{{s}}.basemaps.cartocdn.com/rastertiles/voyager/{{z}}/{{x}}/{{y}}{{r}}.png", {{
      attribution: "© OpenStreetMap © CARTO",
      maxZoom: 20,
    }}).addTo(map);

    const bounds = L.latLngBounds([]);
    function extendBounds(geojson) {{
      if (!geojson) return;
      try {{
        const layer = L.geoJSON(geojson);
        const b = layer.getBounds();
        if (b.isValid()) bounds.extend(b);
      }} catch (e) {{}}
    }}

    if (DATA.parcelle) {{
      const uf = L.geoJSON(DATA.parcelle, {{
        style: {{ color: "#b91c1c", weight: 3, fillColor: "#ef4444", fillOpacity: 0.15 }},
      }}).addTo(map);
      uf.bindPopup("<strong>Unité foncière</strong><br>" + (DATA.label || ""));
      extendBounds(DATA.parcelle);
    }}

    const legendEl = document.getElementById("legend");
    legendEl.innerHTML = "<h2>Couches</h2>";
    let lastFamily = "";

    for (const layer of DATA.layers || []) {{
      if (layer.family_title !== lastFamily) {{
        lastFamily = layer.family_title;
        const fam = document.createElement("div");
        fam.className = "legend-family";
        fam.textContent = layer.family_title;
        legendEl.appendChild(fam);
      }}
      const item = document.createElement("div");
      item.className = "legend-item";
      item.innerHTML = `<span class="swatch" style="background:${{layer.color}}"></span>
        <span>${{layer.title}}<span class="badge">${{layer.count}}</span></span>`;
      legendEl.appendChild(item);

      const fc = {{ type: "FeatureCollection", features: layer.features }};
      const isLine = layer.geom_type === "lineaire";
      const isPoint = layer.geom_type === "ponctuel";

      L.geoJSON(fc, {{
        style: function(f) {{
          const inside = f.properties && f.properties.intersects_parcel;
          const alpha = inside ? 0.45 : 0.2;
          if (isLine) return {{ color: layer.color, weight: inside ? 4 : 2, opacity: inside ? 0.9 : 0.5 }};
          if (isPoint) return {{}};
          return {{ color: layer.color, weight: inside ? 2 : 1, fillColor: layer.color, fillOpacity: alpha }};
        }},
        pointToLayer: function(f, latlng) {{
          const inside = f.properties && f.properties.intersects_parcel;
          return L.circleMarker(latlng, {{
            radius: inside ? 7 : 5,
            color: layer.color,
            fillColor: layer.color,
            fillOpacity: inside ? 0.85 : 0.45,
            weight: 2,
          }});
        }},
        onEachFeature: function(f, l) {{
          const p = f.properties || {{}};
          let html = "<strong>" + layer.title + "</strong>";
          if (!p.intersects_parcel) html += '<div class="near-label">À proximité (hors UF)</div>';
          const rows = [];
          for (const [k, v] of Object.entries(p)) {{
            if (["_fid","intersects_parcel"].includes(k) || v == null || v === "") continue;
            rows.push("<tr><th>" + k + "</th><td>" + String(v).slice(0,120) + "</td></tr>");
          }}
          if (rows.length) html += "<table class='popup-table'>" + rows.join("") + "</table>";
          l.bindPopup(html);
        }},
      }}).addTo(map);
      extendBounds(fc);
    }}

    if (bounds.isValid()) {{
      map.fitBounds(bounds, {{ padding: [40, 40], maxZoom: 17 }});
    }} else {{
      map.setView([42.55, 3.02], 14);
    }}
  </script>
</body>
</html>"""
