#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnostic visuel : 10 parcelles aléatoires (latresne.parcelles) sur le Plan IGN V2.

Usage (depuis cua_latresne_v4) :
  python -m api.cuas.latresne.CUA.map2d.test_alignement_cadastre
  python -m api.cuas.latresne.CUA.map2d.test_alignement_cadastre --n 10 --output ./out_alignement

Dans le HTML : sliders dx / dy (mètres) pour tester un décalage uniforme.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import folium
from folium import Element
from sqlalchemy import text

from api.cuas.latresne.CUA.map2d.carte2d.carte2d_extraction import ENGINE

BASEMAP_TILES = (
    "https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0"
    "&LAYER=GEOGRAPHICALGRIDSYSTEMS.PLANIGNV2&STYLE=normal"
    "&TILEMATRIXSET=PM&FORMAT=image/png"
    "&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}"
)
BASEMAP_ATTR = "© IGN — Géoplateforme (Plan IGN)"

SQL_RANDOM = text("""
    SELECT
        idu,
        section,
        numero,
        code_insee,
        millesime_pci,
        ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS gj_2154,
        ST_AsGeoJSON(ST_Transform(geom_3857, 4326)) AS gj_3857,
        ST_Y(ST_Centroid(ST_Transform(geom_2154, 4326))) AS lat,
        ST_X(ST_Centroid(ST_Transform(geom_2154, 4326))) AS lon
    FROM latresne.parcelles
    WHERE geom_2154 IS NOT NULL
      AND NOT ST_IsEmpty(geom_2154)
    ORDER BY random()
    LIMIT :n
""")


def fetch_parcelles(n: int) -> list[dict]:
    with ENGINE.connect() as conn:
        rows = conn.execute(SQL_RANDOM, {"n": n}).mappings().all()
    out = []
    for r in rows:
        out.append(
            {
                "idu": r["idu"],
                "section": r["section"],
                "numero": r["numero"],
                "code_insee": r["code_insee"],
                "millesime_pci": str(r["millesime_pci"]) if r["millesime_pci"] else None,
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "geom_2154": json.loads(r["gj_2154"]) if r["gj_2154"] else None,
                "geom_3857": json.loads(r["gj_3857"]) if r["gj_3857"] else None,
            }
        )
    if not out:
        raise RuntimeError("Aucune parcelle trouvée dans latresne.parcelles")
    return out


def build_map(parcelles: list[dict]) -> folium.Map:
    lat0 = sum(p["lat"] for p in parcelles) / len(parcelles)
    lon0 = sum(p["lon"] for p in parcelles) / len(parcelles)

    m = folium.Map(
        location=[lat0, lon0],
        zoom_start=16,
        max_zoom=22,
        tiles=BASEMAP_TILES,
        attr=BASEMAP_ATTR,
    )
    map_var = m.get_name()

    features_2154 = []
    features_3857 = []
    for p in parcelles:
        props = {
            "idu": p["idu"] or "",
            "label": f"{p['section'] or ''} {p['numero'] or ''}".strip(),
            "millesime": p["millesime_pci"] or "",
        }
        if p["geom_2154"]:
            features_2154.append(
                {"type": "Feature", "properties": props, "geometry": p["geom_2154"]}
            )
        if p["geom_3857"]:
            features_3857.append(
                {"type": "Feature", "properties": props, "geometry": p["geom_3857"]}
            )

    fc_2154 = {"type": "FeatureCollection", "features": features_2154}
    fc_3857 = {"type": "FeatureCollection", "features": features_3857}

    folium.GeoJson(
        fc_2154,
        name="geom_2154 → 4326",
        style_function=lambda _x: {
            "color": "#e11d48",
            "weight": 3,
            "fillOpacity": 0.08,
            "fillColor": "#fb7185",
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["label", "idu", "millesime"],
            aliases=["Parcelle", "IDU", "Millésime PCI"],
        ),
    ).add_to(m)

    if features_3857:
        folium.GeoJson(
            fc_3857,
            name="geom_3857 → 4326",
            style_function=lambda _x: {
                "color": "#2563eb",
                "weight": 2,
                "dashArray": "6, 4",
                "fillOpacity": 0,
            },
            show=False,
        ).add_to(m)

    for p in parcelles:
        folium.Marker(
            location=[p["lat"], p["lon"]],
            icon=folium.DivIcon(
                html=(
                    f'<div style="background:rgba(255,255,255,.92);border:1px solid #e11d48;'
                    f'border-radius:4px;padding:1px 5px;font:11px/1.2 system-ui,sans-serif;'
                    f'color:#9f1239;white-space:nowrap;">'
                    f"{p['section'] or ''} {p['numero'] or ''}</div>"
                )
            ),
        ).add_to(m)

    bounds = [[p["lat"], p["lon"]] for p in parcelles]
    if len(bounds) >= 2:
        m.fit_bounds(bounds, padding=(40, 40))

    folium.LayerControl(collapsed=False).add_to(m)

    geojson_js = json.dumps(fc_2154, ensure_ascii=False)
    offset_ui = f"""
<style>
  #align-panel {{
    position: absolute; left: 10px; bottom: 30px; z-index: 1000;
    background: white; border: 1px solid #ccc; border-radius: 8px;
    padding: 10px 12px; font: 12px/1.4 system-ui, sans-serif;
    box-shadow: 0 2px 8px rgba(0,0,0,.15); width: 280px;
  }}
  #align-panel label {{ display: block; margin-top: 8px; }}
  #align-panel input[type=range] {{ width: 100%; }}
</style>
<div id="align-panel">
  <b>Test décalage uniforme (m)</b>
  <div style="color:#666;margin:4px 0 8px;">Rouge = geom_2154. Bleu pointillé = geom_3857 (calque).</div>
  <label>dx Est (+) / Ouest (−) : <span id="dxv">0</span> m</label>
  <input id="dx" type="range" min="-15" max="15" step="0.2" value="0">
  <label>dy Nord (+) / Sud (−) : <span id="dyv">0</span> m</label>
  <input id="dy" type="range" min="-15" max="15" step="0.2" value="0">
  <button id="reset-off" style="margin-top:8px;">Reset</button>
</div>
<script>
window.addEventListener('load', function() {{
  const map = window["{map_var}"];
  if (!map) return;
  const raw = {geojson_js};
  const shifted = L.geoJSON(null, {{
    style: {{ color: '#16a34a', weight: 3, fillOpacity: 0.05, dashArray: '2, 6' }}
  }}).addTo(map);

  function shiftCoords(coords, dx, dy, latRef) {{
    const mLat = 111320;
    const mLon = 111320 * Math.cos(latRef * Math.PI / 180);
    const dLon = dx / mLon;
    const dLat = dy / mLat;
    function walk(c, depth) {{
      if (typeof c[0] === 'number') return [c[0] + dLon, c[1] + dLat];
      return c.map(x => walk(x, depth + 1));
    }}
    return walk(coords, 0);
  }}

  function redraw() {{
    const dx = parseFloat(document.getElementById('dx').value);
    const dy = parseFloat(document.getElementById('dy').value);
    document.getElementById('dxv').textContent = dx.toFixed(1);
    document.getElementById('dyv').textContent = dy.toFixed(1);
    shifted.clearLayers();
    if (dx === 0 && dy === 0) return;
    const latRef = map.getCenter().lat;
    const fc = JSON.parse(JSON.stringify(raw));
    fc.features.forEach(f => {{
      f.geometry.coordinates = shiftCoords(f.geometry.coordinates, dx, dy, latRef);
    }});
    shifted.addData(fc);
  }}
  document.getElementById('dx').addEventListener('input', redraw);
  document.getElementById('dy').addEventListener('input', redraw);
  document.getElementById('reset-off').addEventListener('click', () => {{
    document.getElementById('dx').value = 0;
    document.getElementById('dy').value = 0;
    redraw();
  }});
}});
</script>
"""
    m.get_root().html.add_child(Element(offset_ui))
    return m


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trace N parcelles aléatoires sur le Plan IGN pour juger du décalage cadastral."
    )
    parser.add_argument("--n", type=int, default=10, help="Nombre de parcelles (défaut 10)")
    parser.add_argument("--output", default="./out_alignement", help="Dossier de sortie HTML")
    args = parser.parse_args()

    parcelles = fetch_parcelles(args.n)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / "test_alignement_cadastre.html"

    m = build_map(parcelles)
    m.save(str(html_path))

    print(f"Carte : {html_path.resolve()}")
    print("Parcelles :")
    for p in parcelles:
        print(
            f"  {p['section']} {p['numero']}  idu={p['idu']}  "
            f"pci={p['millesime_pci']}  ({p['lat']:.6f}, {p['lon']:.6f})"
        )
    print(
        "\nOuvrir le HTML, zoomer au max sur une limite, comparer le trait rouge "
        "au cadastre du fond IGN. Sliders dx/dy : décalage uniforme en mètres."
    )


if __name__ == "__main__":
    main()
