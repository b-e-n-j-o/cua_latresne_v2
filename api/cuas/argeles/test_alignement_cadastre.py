#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnostic visuel : N parcelles aléatoires (argeles.parcelles) sur le Plan IGN raster
(même fond WMTS que Latresne), pour comparer l'alignement cadastral.

La CUA Argelès utilise le même fond raster Plan IGN V2 (carto_context_html).

Usage (depuis cua_latresne_v4) :
  python -m api.cuas.argeles.test_alignement_cadastre
  python -m api.cuas.argeles.test_alignement_cadastre --n 10 --output ./out_alignement_argeles
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import folium
from folium import Element
from sqlalchemy import text

from api.cuas.argeles.db import SCHEMA, get_engine

BASEMAP_TILES = (
    "https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0"
    "&LAYER=GEOGRAPHICALGRIDSYSTEMS.PLANIGNV2&STYLE=normal"
    "&TILEMATRIXSET=PM&FORMAT=image/png"
    "&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}"
)
BASEMAP_ATTR = "© IGN — Géoplateforme (Plan IGN)"


def _has_column(engine, schema: str, table: str, col: str) -> bool:
    q = text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t AND column_name = :c
        """
    )
    with engine.connect() as conn:
        return conn.execute(q, {"s": schema, "t": table, "c": col}).scalar() is not None


def fetch_parcelles(n: int) -> list[dict]:
    engine = get_engine()
    has_idu = _has_column(engine, SCHEMA, "parcelles", "idu")
    has_pci = _has_column(engine, SCHEMA, "parcelles", "millesime_pci")
    has_3857 = _has_column(engine, SCHEMA, "parcelles", "geom_3857")

    idu_sel = "idu" if has_idu else "NULL::text AS idu"
    pci_sel = "millesime_pci" if has_pci else "NULL::date AS millesime_pci"
    g3857_sel = (
        "ST_AsGeoJSON(ST_Transform(geom_3857, 4326)) AS gj_3857"
        if has_3857
        else "NULL::text AS gj_3857"
    )

    sql = text(f"""
        SELECT
            {idu_sel},
            section,
            numero,
            {pci_sel},
            ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS gj_2154,
            {g3857_sel},
            ST_Y(ST_Centroid(ST_Transform(geom_2154, 4326))) AS lat,
            ST_X(ST_Centroid(ST_Transform(geom_2154, 4326))) AS lon
        FROM {SCHEMA}.parcelles
        WHERE geom_2154 IS NOT NULL
          AND NOT ST_IsEmpty(geom_2154)
        ORDER BY random()
        LIMIT :n
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"n": n}).mappings().all()

    out = []
    for r in rows:
        out.append(
            {
                "idu": r["idu"],
                "section": r["section"],
                "numero": r["numero"],
                "millesime_pci": str(r["millesime_pci"]) if r["millesime_pci"] else None,
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "geom_2154": json.loads(r["gj_2154"]) if r["gj_2154"] else None,
                "geom_3857": json.loads(r["gj_3857"]) if r["gj_3857"] else None,
            }
        )
    if not out:
        raise RuntimeError(f"Aucune parcelle dans {SCHEMA}.parcelles")
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
    box-shadow: 0 2px 8px rgba(0,0,0,.15); width: 300px;
  }}
  #align-panel label {{ display: block; margin-top: 8px; }}
  #align-panel input[type=range] {{ width: 100%; }}
</style>
<div id="align-panel">
  <b>Argelès — test décalage (m)</b>
  <div style="color:#666;margin:4px 0 8px;">
    Fond = Plan IGN raster (même WMTS que la CUA Argelès / Latresne).
  </div>
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
    function walk(c) {{
      if (typeof c[0] === 'number') return [c[0] + dLon, c[1] + dLat];
      return c.map(walk);
    }}
    return walk(coords);
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
        description="Trace N parcelles Argelès aléatoires sur le Plan IGN raster."
    )
    parser.add_argument("--n", type=int, default=10)
    parser.add_argument("--output", default="./out_alignement_argeles")
    args = parser.parse_args()

    parcelles = fetch_parcelles(args.n)
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / "test_alignement_cadastre_argeles.html"

    build_map(parcelles).save(str(html_path))

    print(f"Carte : {html_path.resolve()}")
    print(f"Schéma : {SCHEMA}.parcelles")
    for p in parcelles:
        print(
            f"  {p['section']} {p['numero']}  idu={p['idu']}  "
            f"pci={p['millesime_pci']}  ({p['lat']:.6f}, {p['lon']:.6f})"
        )


if __name__ == "__main__":
    main()
