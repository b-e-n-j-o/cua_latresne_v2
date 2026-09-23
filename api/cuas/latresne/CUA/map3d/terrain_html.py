# -*- coding: utf-8 -*-
"""HTML Three.js autonome : MNT figé + contour UF (aucune requête live)."""

from __future__ import annotations

import json


def render_terrain_html(payload: dict) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Carte 3D — unité foncière (figée)</title>
<style>
  html, body {{ margin:0; height:100%; background:#0d0d10; overflow:hidden; font-family:system-ui,sans-serif; }}
  #c {{ display:block; width:100%; height:100%; }}
  #hud {{
    position:absolute; left:12px; top:12px; z-index:2;
    color:#e8e8ea; font-size:12px; line-height:1.45;
    background:rgba(13,13,16,.72); padding:10px 12px; border-radius:8px;
    pointer-events:none;
  }}
  #hud strong {{ color:#ffea00; font-weight:600; }}
</style>
<script type="importmap">
{{
  "imports": {{
    "three": "https://unpkg.com/three@0.160.0/build/three.module.js",
    "three/addons/": "https://unpkg.com/three@0.160.0/examples/jsm/"
  }}
}}
</script>
</head>
<body>
<canvas id="c"></canvas>
<div id="hud"></div>
<script type="application/json" id="terrain-data">{data}</script>
<script type="module">
import * as THREE from 'three';
import {{ OrbitControls }} from 'three/addons/controls/OrbitControls.js';

const T = JSON.parse(document.getElementById('terrain-data').textContent);
const CONTOUR_Z_OFFSET = 1.25;

function altitudeColor(t) {{
  const c = new THREE.Color();
  if (t < 0.25) c.setRGB(0.35 + t * 0.8, 0.22 + t * 0.6, 0.10 + t * 0.2);
  else if (t < 0.55) {{
    const s = (t - 0.25) / 0.3;
    c.setRGB(0.55 - s * 0.25, 0.37 + s * 0.25, 0.12 - s * 0.05);
  }} else if (t < 0.80) {{
    const s = (t - 0.55) / 0.25;
    c.setRGB(0.30 - s * 0.10, 0.62 - s * 0.15, 0.07 + s * 0.05);
  }} else {{
    const s = (t - 0.80) / 0.20;
    c.setRGB(0.20 + s * 0.80, 0.47 + s * 0.53, 0.12 + s * 0.88);
  }}
  return c;
}}

function decodeElevations(b64) {{
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return new Float32Array(bytes.buffer);
}}

function buildTerrainGeometry(data, elevations) {{
  const {{ width, height, resolution_m, elev_min, elev_max, exaggeration }} = data;
  const W = width * resolution_m;
  const H = height * resolution_m;
  const geo = new THREE.PlaneGeometry(W, H, width - 1, height - 1);
  const positions = geo.attributes.position.array;
  const colors = new Float32Array(width * height * 3);
  const elevRange = elev_max - elev_min || 1;
  for (let row = 0; row < height; row++) {{
    for (let col = 0; col < width; col++) {{
      const idx = row * width + col;
      const vertIdx = idx * 3;
      const elev = elevations[idx];
      positions[vertIdx + 2] = (elev - elev_min) * exaggeration;
      const t = Math.max(0, Math.min(1, (elev - elev_min) / elevRange));
      const c = altitudeColor(t);
      colors[vertIdx] = c.r;
      colors[vertIdx + 1] = c.g;
      colors[vertIdx + 2] = c.b;
    }}
  }}
  geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
  geo.attributes.position.needsUpdate = true;
  geo.computeVertexNormals();
  return geo;
}}

function sampleElevAt(rx, ry, elevations, width, height, resolution_m) {{
  const W = width * resolution_m;
  const H = height * resolution_m;
  const col = Math.round((rx + W / 2) / resolution_m);
  const row = Math.round((H / 2 - ry) / resolution_m);
  const c = Math.max(0, Math.min(width - 1, col));
  const r = Math.max(0, Math.min(height - 1, row));
  return elevations[r * width + c];
}}

function contourPoints(geojson, elevations, data) {{
  const {{ width, height, resolution_m, elev_min, exaggeration }} = data;
  let rings = [];
  if (geojson.type === 'Polygon') rings = geojson.coordinates;
  else if (geojson.type === 'MultiPolygon') {{
    for (const poly of geojson.coordinates) rings.push(...poly);
  }}
  const points = [];
  for (const ring of rings) {{
    for (const [rx, ry] of ring) {{
      const elev = sampleElevAt(rx, ry, elevations, width, height, resolution_m);
      const z = (elev - elev_min) * exaggeration + CONTOUR_Z_OFFSET;
      points.push(new THREE.Vector3(rx, ry, z));
    }}
    if (ring.length) {{
      const [rx, ry] = ring[0];
      const elev = sampleElevAt(rx, ry, elevations, width, height, resolution_m);
      points.push(new THREE.Vector3(rx, ry, (elev - elev_min) * exaggeration + CONTOUR_Z_OFFSET));
    }}
  }}
  return points;
}}

const elevations = decodeElevations(T.elevations_b64);
const canvas = document.getElementById('c');
const renderer = new THREE.WebGLRenderer({{ canvas, antialias: true }});
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
renderer.setSize(window.innerWidth, window.innerHeight, false);
renderer.setClearColor(0x0d0d10, 1);

const scene = new THREE.Scene();
const camera = new THREE.PerspectiveCamera(45, window.innerWidth / window.innerHeight, 0.1, 50000);
const W = T.width * T.resolution_m;
const H = T.height * T.resolution_m;
const span = Math.max(W, H);
camera.position.set(span * 0.35, -span * 0.85, span * 0.55);
camera.up.set(0, 0, 1);

const controls = new OrbitControls(camera, renderer.domElement);
controls.enableDamping = true;
controls.target.set(0, 0, (T.elev_max - T.elev_min) * T.exaggeration * 0.3);

scene.add(new THREE.AmbientLight(0xffffff, 0.55));
const dir = new THREE.DirectionalLight(0xffffff, 0.85);
dir.position.set(span * 0.4, span * 0.2, span);
scene.add(dir);

const mesh = new THREE.Mesh(
  buildTerrainGeometry(T, elevations),
  new THREE.MeshLambertMaterial({{ vertexColors: true, side: THREE.DoubleSide }})
);
scene.add(mesh);

for (const g of (T.contours || [])) {{
  const pts = contourPoints(g, elevations, T);
  if (pts.length < 2) continue;
  const line = new THREE.Line(
    new THREE.BufferGeometry().setFromPoints(pts),
    new THREE.LineBasicMaterial({{ color: 0xffea00, linewidth: 2 }})
  );
  scene.add(line);
}}

document.getElementById('hud').innerHTML =
  '<strong>Carte 3D figée</strong><br/>' +
  (T.frozen_at ? ('Générée le ' + T.frozen_at.replace('T', ' ').slice(0, 19) + ' UTC<br/>') : '') +
  'MNT ' + T.width + '×' + T.height + ' · ' + T.resolution_m + ' m<br/>' +
  'Z ' + T.elev_min.toFixed(1) + ' – ' + T.elev_max.toFixed(1) + ' m NGF<br/>' +
  'Buffer ' + (T.buffer_m || 0) + ' m · UF ' + (T.surface_m2 || 0).toLocaleString('fr-FR') + ' m²';

function onResize() {{
  camera.aspect = window.innerWidth / window.innerHeight;
  camera.updateProjectionMatrix();
  renderer.setSize(window.innerWidth, window.innerHeight, false);
}}
window.addEventListener('resize', onResize);

function tick() {{
  controls.update();
  renderer.render(scene, camera);
  requestAnimationFrame(tick);
}}
tick();
</script>
</body>
</html>
"""
