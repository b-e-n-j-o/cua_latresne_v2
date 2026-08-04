"""
Sentinel-2 L2A sur une emprise GeoJSON via Sentinel Hub (Planet Insights Platform).

La Data API / Orders API de Planet ne servent plus Sentinel-2 depuis le 31/07/2024.
Sentinel-2 passe désormais exclusivement par services.sentinel-hub.com.

Auth : Bearer TOKEN dans api/satelite/.env (généré via get_token.py, valide ~1h).

Prérequis :
    pip install requests python-dotenv pyproj shapely

Usage :
    python planet-insights.py --list          # dates disponibles sur l'AOI
    python planet-insights.py --date 2026-07-23
    python planet-insights.py --date 2026-07-23 --preset truecolor
"""

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv
from pyproj import Transformer
from shapely.geometry import mapping, shape
from shapely.ops import transform as shapely_transform

load_dotenv(Path(__file__).with_name(".env"))

CATALOG_URL = "https://services.sentinel-hub.com/api/v1/catalog/1.0.0/search"
PROCESS_URL = "https://services.sentinel-hub.com/api/v1/process"

COLLECTION = "sentinel-2-l2a"
GEOJSON_FILE = Path(__file__).with_name("explorer-aoi.geojson")
OUTPUT_DIR = Path(__file__).with_name("downloads")

# CRS de sortie projeté : resx/resy sont exprimés dans ses unités.
# 3035 = LAEA Europe (mètres). 32630 serait le CRS natif des tuiles S2 sur Bordeaux.
OUTPUT_EPSG = 3035
RESOLUTION_M = 10
MAX_CLOUD_COVER = 30          # en %, contrairement à la Data API Planet qui était en 0-1
MAX_DIMENSION_PX = 2500       # limite Process API par requête

# Réflectances brutes, prêtes pour NDVI / NDWI / EVI. dataMask = 0 hors emprise.
EVALSCRIPT_BANDS = """//VERSION=3
function setup() {
  return {
    input: [{
      bands: ["B02", "B03", "B04", "B08", "SCL", "dataMask"],
      units: ["REFLECTANCE", "REFLECTANCE", "REFLECTANCE", "REFLECTANCE", "DN", "DN"]
    }],
    output: { bands: 6, sampleType: "FLOAT32" }
  };
}
function evaluatePixel(s) {
  return [s.B02, s.B03, s.B04, s.B08, s.SCL, s.dataMask];
}
"""

EVALSCRIPT_TRUECOLOR = """//VERSION=3
function setup() {
  return {
    input: [{ bands: ["B04", "B03", "B02"] }],
    output: { bands: 3, sampleType: "UINT8" }
  };
}
function evaluatePixel(s) {
  return [255 * 2.5 * s.B04, 255 * 2.5 * s.B03, 255 * 2.5 * s.B02];
}
"""

PRESETS = {
    "bands": (EVALSCRIPT_BANDS, "tif"),
    "truecolor": (EVALSCRIPT_TRUECOLOR, "tif"),
}


def load_geometry(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        gj = json.load(f)
    if gj.get("type") == "FeatureCollection":
        features = gj.get("features") or []
        if not features:
            raise ValueError("FeatureCollection vide")
        return features[0]["geometry"]
    if gj.get("type") == "Feature":
        return gj["geometry"]
    return gj


def get_session() -> requests.Session:
    token = os.getenv("TOKEN")
    if not token:
        raise SystemExit("TOKEN absent du .env (lancer get_token.py puis coller le jeton).")

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return session


def list_scenes(session: requests.Session, geometry: dict, start: date, end: date) -> list[dict]:
    body = {
        "collections": [COLLECTION],
        "intersects": geometry,
        "datetime": f"{start.isoformat()}T00:00:00Z/{end.isoformat()}T23:59:59Z",
        "limit": 100,
        "filter": {
            "op": "<=",
            "args": [{"property": "eo:cloud_cover"}, MAX_CLOUD_COVER],
        },
        "filter-lang": "cql2-json",
        "fields": {"include": ["id", "properties.datetime", "properties.eo:cloud_cover"]},
    }
    resp = session.post(CATALOG_URL, json=body)
    resp.raise_for_status()
    return resp.json().get("features", [])


def reproject_geometry(geometry: dict, epsg: int) -> tuple[dict, tuple[float, ...]]:
    transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{epsg}", always_xy=True)
    geom = shapely_transform(transformer.transform, shape(geometry))
    return mapping(geom), geom.bounds


def fetch_image(
    session: requests.Session, geometry: dict, day: date, preset: str, out_path: Path
) -> None:
    evalscript, _ = PRESETS[preset]
    geom_proj, (minx, miny, maxx, maxy) = reproject_geometry(geometry, OUTPUT_EPSG)

    width = round((maxx - minx) / RESOLUTION_M)
    height = round((maxy - miny) / RESOLUTION_M)
    if max(width, height) > MAX_DIMENSION_PX:
        raise SystemExit(
            f"AOI trop grande : {width}x{height} px à {RESOLUTION_M} m "
            f"(max {MAX_DIMENSION_PX}). Baisser la résolution ou découper l'emprise."
        )
    print(f"  sortie : {width}x{height} px, EPSG:{OUTPUT_EPSG}, {RESOLUTION_M} m")

    body = {
        "input": {
            "bounds": {
                "geometry": geom_proj,
                "properties": {
                    "crs": f"http://www.opengis.net/def/crs/EPSG/0/{OUTPUT_EPSG}"
                },
            },
            "data": [
                {
                    "type": COLLECTION,
                    "dataFilter": {
                        "timeRange": {
                            "from": f"{day.isoformat()}T00:00:00Z",
                            "to": f"{day.isoformat()}T23:59:59Z",
                        },
                        "maxCloudCoverage": MAX_CLOUD_COVER,
                        "mosaickingOrder": "leastCC",
                    },
                }
            ],
        },
        "output": {
            "resx": RESOLUTION_M,
            "resy": RESOLUTION_M,
            "responses": [
                {"identifier": "default", "format": {"type": "image/tiff"}}
            ],
        },
        "evalscript": evalscript,
    }

    resp = session.post(PROCESS_URL, json=body, headers={"Accept": "image/tiff"})
    if resp.status_code != 200:
        print(f"Process API {resp.status_code} : {resp.text[:800]}", file=sys.stderr)
        resp.raise_for_status()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(resp.content)
    print(f"  écrit : {out_path} ({len(resp.content) / 1e6:.1f} Mo)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true", help="lister les scènes disponibles")
    parser.add_argument("--date", help="date d'acquisition, format YYYY-MM-DD")
    parser.add_argument("--from", dest="start", default="2026-07-01")
    parser.add_argument("--to", dest="end", default="2026-07-31")
    parser.add_argument("--preset", choices=sorted(PRESETS), default="bands")
    args = parser.parse_args()

    if not GEOJSON_FILE.exists():
        print(f"Fichier introuvable : {GEOJSON_FILE}", file=sys.stderr)
        return 1

    geometry = load_geometry(GEOJSON_FILE)
    session = get_session()

    if args.list or not args.date:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        scenes = list_scenes(session, geometry, start, end)
        if not scenes:
            print(f"Aucune scène (nuages <= {MAX_CLOUD_COVER}%) entre {start} et {end}.")
            return 0
        print(f"{len(scenes)} scène(s) :")
        for feat in sorted(scenes, key=lambda f: f["properties"]["datetime"]):
            props = feat["properties"]
            print(f"  {props['datetime'][:19]}  nuages={props.get('eo:cloud_cover', '?')}%  "
                  f"{feat['id']}")
        if not args.date:
            print("\nRelancer avec --date YYYY-MM-DD pour télécharger.")
            return 0

    day = date.fromisoformat(args.date)
    print(f"Téléchargement {COLLECTION} du {day} (preset={args.preset})...")
    out_path = OUTPUT_DIR / f"s2_l2a_{day.isoformat()}_{args.preset}.tif"
    fetch_image(session, geometry, day, args.preset, out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())