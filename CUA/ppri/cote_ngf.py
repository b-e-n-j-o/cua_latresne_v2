# -*- coding: utf-8 -*-
"""
Analyse altimétrique d'une unité foncière (version WKT uniquement)
- Utilise une géométrie WKT (EPSG:2154)
- Échantillonne jusqu'à 50 points réguliers dans le polygone
- Interroge l'API Altimétrie IGN (RGE ALTI)
- Calcule min / max / moyenne NGF
- Produit un paragraphe textuel synthétique
"""

import requests, numpy as np
from shapely.geometry import Point
from shapely import wkt
from pyproj import Transformer
from urllib.parse import urlencode

# ================== CONFIG ==================
ALTIM_URL = "https://data.geopf.fr/altimetrie/1.0/calcul/alti/rest/elevation.json"
RESOURCE = "ign_rge_alti_wld"
MAX_POINTS = 50


# ================== FONCTIONS ==================
def sample_points_equitable(poly, max_points=MAX_POINTS):
    """Crée un échantillonnage régulier limité à max_points"""
    minx, miny, maxx, maxy = poly.bounds
    n = int(np.sqrt(max_points))
    xs = np.linspace(minx, maxx, n)
    ys = np.linspace(miny, maxy, n)
    pts = [Point(x, y) for x in xs for y in ys if Point(x, y).within(poly)]
    if len(pts) > max_points:
        indices = np.round(np.linspace(0, len(pts) - 1, max_points)).astype(int)
        pts = [pts[i] for i in indices]
    print(f"🧩 Échantillonnage : {len(pts)} points répartis équitablement")
    return pts


def fetch_altitudes(points):
    """Appelle l'API Altimétrie IGN via GET et renvoie les altitudes NGF"""
    to_wgs84 = Transformer.from_crs(2154, 4326, always_xy=True).transform
    pts_wgs = [to_wgs84(p.x, p.y) for p in points]
    lons = [f"{lon:.6f}" for lon, lat in pts_wgs]
    lats = [f"{lat:.6f}" for lon, lat in pts_wgs]

    params = {
        "lon": "|".join(lons),
        "lat": "|".join(lats),
        "resource": RESOURCE,
        "delimiter": "|",
        "zonly": "true"
    }
    url = f"{ALTIM_URL}?{urlencode(params)}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        print(f"⚠️ Erreur API ({r.status_code}): {r.text[:200]}")
        return []
    data = r.json()
    zs = [z for z in data.get("elevations", []) if z != -99999]
    print(f"📡 Altitudes reçues : {len(zs)} points valides")
    return zs


def cote_ngf_parcelle(geom_wkt):
    """
    Renvoie un paragraphe synthétique des altitudes NGF pour une géométrie WKT
    """
    if not geom_wkt:
        raise ValueError("Une géométrie WKT est requise pour le calcul altimétrique.")

    poly = wkt.loads(geom_wkt)
    pts = sample_points_equitable(poly, max_points=MAX_POINTS)

    zs = fetch_altitudes(pts)
    if not zs:
        raise RuntimeError("Aucune altitude renvoyée par l'API Altimétrie IGN.")
    arr = np.array(zs, dtype=float)
    zmin, zmax, zmean = round(float(arr.min()), 2), round(float(arr.max()), 2), round(float(arr.mean()), 2)

    paragraphe = (
        f"L'unité foncière présente une altitude moyenne de {zmean} mètres NGF, "
        f"avec un point le plus bas relevé à {zmin} m NGF et un point le plus haut à {zmax} m NGF. "
        f"Ces valeurs sont calculées à partir d'un échantillon de {len(zs)} points répartis sur la surface "
        f"de l'unité foncière selon le modèle altimétrique IGN (RGE ALTI)."
    )
    return paragraphe


# ================== TEST LOCAL ==================
if __name__ == "__main__":
    geom = "POLYGON((684000 6438000,684050 6438000,684050 6438050,684000 6438050,684000 6438000))"
    print(cote_ngf_parcelle(geom))
