# -*- coding: utf-8 -*-
"""
Overlay SIG 2D dans le visualiseur Three.js (module optionnel).

Réutilise prepare_layers_payload (mêmes familles, couleurs, groupes que la carte MapLibre).
Les géométries restent des plans horizontaux : pas de drapage sur le MNT.
Pour revenir à la vue terrain seule : ne plus appeler attach_sig_overlay().
"""

from __future__ import annotations

import shapely
from pyproj import Transformer
from shapely import affinity
from shapely.geometry import shape
from shapely.ops import transform as shapely_transform

_TO_2154 = Transformer.from_crs(4326, 2154, always_xy=True)

_MAX_FEATURES = 250
_MAX_RING = 80
_SIMPLIFY_M = 2.0


def _ring(coords) -> list[list[float]]:
    pts = [[round(float(x), 1), round(float(y), 1)] for x, y, *_ in coords]
    if len(pts) >= 2 and pts[0] == pts[-1]:
        pts = pts[:-1]
    if len(pts) > _MAX_RING:
        step = max(1, len(pts) // _MAX_RING)
        pts = pts[::step]
    return pts if len(pts) >= 2 else []


def _parts(geom):
    if geom.is_empty:
        return
    if not geom.is_valid:
        geom = shapely.make_valid(geom)
    for part in shapely.get_parts(geom):
        yield part


def build_sig_overlay(carto_context: dict | None, carto_catalogue: dict | None,
                      cx: float, cy: float) -> list[dict]:
    """Couches légendées, coordonnées relatives au centre du MNT (m)."""
    if not carto_context:
        return []
    from api.cuas.argeles.carto_context.carto_context_enrich import prepare_layers_payload

    layers_in = prepare_layers_payload(carto_context, carto_catalogue)
    out = []
    for layer in layers_in:
        items = []
        for feat in (layer.get("features") or [])[:_MAX_FEATURES]:
            gj = feat.get("geometry")
            if not gj:
                continue
            props = feat.get("properties") or {}
            try:
                geom = shapely_transform(_TO_2154.transform, shape(gj))
            except Exception:
                continue
            geom = affinity.translate(geom, xoff=-cx, yoff=-cy)
            geom = geom.simplify(_SIMPLIFY_M, preserve_topology=True)
            key = props.get("_studyKey") or "_all"
            color = props.get("_studyColor") or "#888888"
            for part in _parts(geom):
                gt = part.geom_type
                if gt == "Polygon":
                    exterior = _ring(part.exterior.coords)
                    if len(exterior) < 3:
                        continue
                    holes = [h for h in (_ring(r.coords) for r in part.interiors) if len(h) >= 3]
                    items.append({"t": "p", "k": key, "c": color, "e": exterior, "h": holes})
                elif gt == "LineString":
                    line = _ring(part.coords)
                    if len(line) >= 2:
                        items.append({"t": "l", "k": key, "c": color, "e": line})
                elif gt == "Point":
                    items.append({"t": "d", "k": key, "c": color,
                                  "xy": [round(float(part.x), 1), round(float(part.y), 1)]})
        if not items:
            continue
        out.append({
            "id": layer["layer_id"],
            "title": layer.get("title") or layer["layer_id"],
            "family": layer.get("family_title") or "Autres",
            "legend": layer.get("legend"),
            "items": [
                {"key": i["key"], "label": i["label"], "color": i["color"], "count": i.get("count") or 0}
                for i in (layer.get("legend_items") or [])
            ],
            "geoms": items,
        })
    return out


def viewer_assets() -> tuple[str, str]:
    """CSS + JS injectés dans la page terrain. No-op si T.sig_layers est absent."""
    css = """
  #sig-legend {
    position:absolute; top:12px; right:12px; bottom:12px; z-index:3;
    width:min(300px, 40vw); background:rgba(255,255,255,.96); color:#0f172a;
    border-radius:8px; box-shadow:0 2px 10px rgba(0,0,0,.25);
    display:flex; flex-direction:column; overflow:hidden; font-family:system-ui,sans-serif;
  }
  #sig-legend h2 { margin:0; font-size:13px; padding:10px 12px 6px; border-bottom:1px solid #e5e7eb; }
  #sig-legend .hint { margin:0; padding:0 12px 8px; font-size:10px; color:#64748b; border-bottom:1px solid #e5e7eb; }
  #sig-scroll { overflow:auto; padding:6px 8px 12px; flex:1; }
  #sig-legend .fam { margin-top:8px; font-size:10px; font-weight:700; letter-spacing:.04em;
    text-transform:uppercase; color:#475569; }
  #sig-legend .ly { border-top:1px solid #f1f5f9; padding:4px 0; }
  #sig-legend label { display:flex; align-items:center; gap:6px; font-size:12px; cursor:pointer; }
  #sig-legend .grp { margin-left:18px; font-size:11px; color:#334155; }
  #sig-legend .sw { width:12px; height:12px; border-radius:2px; border:1px solid rgba(0,0,0,.15); flex:none; }
"""
    js = r"""
(function () {
  const layers = T.sig_layers || [];
  if (!layers.length) return;
  const host = document.createElement('aside');
  host.id = 'sig-legend';
  host.innerHTML = '<h2>Légende (même catalogue que la carte 2D)</h2>'
    + '<p class="hint">Plans 2D flottants au-dessus du MNT, sans drapage. Seul le zonage est affiché au départ.</p>'
    + '<div id="sig-scroll"></div>';
  document.body.appendChild(host);
  const scroll = host.querySelector('#sig-scroll');

  const zLift = (T.elev_max - T.elev_min) * T.exaggeration + 4;
  const groups = {};
  function bucket(id, key) {
    const k = id + '|' + key;
    if (!groups[k]) {
      const g = new THREE.Group();
      g.visible = false;
      scene.add(g);
      groups[k] = g;
    }
    return groups[k];
  }
  function hex(c) { return new THREE.Color(c || '#888888'); }
  function ringShape(ring) {
    const s = new THREE.Shape();
    ring.forEach((p, i) => { i === 0 ? s.moveTo(p[0], p[1]) : s.lineTo(p[0], p[1]); });
    return s;
  }
  for (const layer of layers) {
    for (const g of layer.geoms) {
      const parent = bucket(layer.id, g.k);
      const color = hex(g.c);
      if (g.t === 'p' && g.e && g.e.length >= 3) {
        const shape = ringShape(g.e);
        for (const hole of (g.h || [])) {
          if (hole.length < 3) continue;
          const path = new THREE.Path();
          hole.forEach((p, i) => { i === 0 ? path.moveTo(p[0], p[1]) : path.lineTo(p[0], p[1]); });
          shape.holes.push(path);
        }
        const mesh = new THREE.Mesh(
          new THREE.ShapeGeometry(shape),
          new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.45, side: THREE.DoubleSide, depthWrite: false })
        );
        mesh.position.z = zLift;
        parent.add(mesh);
        const line = g.e.concat([g.e[0]]).map(p => new THREE.Vector3(p[0], p[1], zLift));
        parent.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(line), new THREE.LineBasicMaterial({ color })));
      } else if (g.t === 'l' && g.e && g.e.length >= 2) {
        const pts = g.e.map(p => new THREE.Vector3(p[0], p[1], zLift));
        parent.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts), new THREE.LineBasicMaterial({ color })));
      } else if (g.t === 'd' && g.xy) {
        const dot = new THREE.Mesh(
          new THREE.CircleGeometry(2.2, 10),
          new THREE.MeshBasicMaterial({ color, side: THREE.DoubleSide, depthWrite: false })
        );
        dot.position.set(g.xy[0], g.xy[1], zLift);
        parent.add(dot);
      }
    }
  }

  const byFamily = {};
  for (const layer of layers) (byFamily[layer.family] = byFamily[layer.family] || []).push(layer);
  const defaultOn = (layer) => layer.id === 'zonage_plu' || layer.legend === 'zonage';

  function sync(layer) {
    const on = layer._on;
    for (const item of layer.items) {
      const g = groups[layer.id + '|' + item.key];
      if (g) g.visible = on && item._on;
    }
  }

  for (const [family, famLayers] of Object.entries(byFamily)) {
    const famEl = document.createElement('div');
    famEl.className = 'fam';
    famEl.textContent = family;
    scroll.appendChild(famEl);
    for (const layer of famLayers) {
      layer._on = defaultOn(layer);
      const box = document.createElement('div');
      box.className = 'ly';
      const row = document.createElement('label');
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.checked = layer._on;
      cb.addEventListener('change', () => { layer._on = cb.checked; sync(layer); });
      row.appendChild(cb);
      row.appendChild(document.createTextNode(layer.title + ' (' + layer.geoms.length + ')'));
      box.appendChild(row);
      for (const item of layer.items) {
        item._on = true;
        const grow = document.createElement('label');
        grow.className = 'grp';
        const gcb = document.createElement('input');
        gcb.type = 'checkbox';
        gcb.checked = true;
        gcb.addEventListener('change', () => { item._on = gcb.checked; sync(layer); });
        const sw = document.createElement('span');
        sw.className = 'sw';
        sw.style.background = item.color;
        grow.appendChild(gcb);
        grow.appendChild(sw);
        grow.appendChild(document.createTextNode((item.label || item.key) + (item.count ? ' · ' + item.count : '')));
        box.appendChild(grow);
      }
      scroll.appendChild(box);
      sync(layer);
    }
  }
})();
"""
    return css, js
