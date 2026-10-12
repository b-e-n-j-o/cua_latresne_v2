# -*- coding: utf-8 -*-
"""Géométries de parcelles distinctes (avant union UF), partagées 2D / 3D / DXF."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyproj import Transformer
from shapely import wkt as shapely_wkt
from shapely.affinity import translate
from shapely.geometry import mapping
from shapely.ops import transform as shapely_transform

PARCELLES_GEOM_FILENAME = "parcelles_geom.json"

_TO_4326 = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)


def parcel_label(section: str, numero: str) -> str:
    sec = (section or "").strip().upper()
    num = (numero or "").strip()
    return f"{sec} {num}".strip()


def normalize_parcel_record(item: dict[str, Any]) -> dict[str, Any] | None:
    wkt = (item.get("wkt") or item.get("geometry") or "").strip()
    if not wkt:
        return None
    section = str(item.get("section") or "").strip().upper()
    numero = str(item.get("numero") or "").strip()
    label = (item.get("label") or parcel_label(section, numero)).strip()
    rec = {"section": section, "numero": numero, "label": label, "wkt": wkt}
    if item.get("contenance") is not None:
        rec["contenance"] = item["contenance"]
    return rec


def records_from_section_wkt(rows) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        rec = normalize_parcel_record(
            {
                "section": row.get("section") if isinstance(row, dict) else row[0],
                "numero": row.get("numero") if isinstance(row, dict) else row[1],
                "wkt": row.get("wkt") if isinstance(row, dict) else row[2],
            }
        )
        if rec:
            out.append(rec)
    return out


def write_parcelles_geom(out_dir, records: list[dict[str, Any]]) -> Path:
    path = Path(out_dir) / PARCELLES_GEOM_FILENAME
    clean = [r for r in (normalize_parcel_record(x) for x in records) if r]
    path.write_text(json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_parcelles_geom(source) -> list[dict[str, Any]]:
    if source is None:
        return []
    if isinstance(source, (str, Path)):
        path = Path(source)
        if path.is_dir():
            path = path / PARCELLES_GEOM_FILENAME
        if not path.exists():
            return []
        source = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(source, list):
        return []
    return [r for r in (normalize_parcel_record(x) for x in source) if r]


def sibling_parcelles_path(wkt_path) -> Path | None:
    path = Path(wkt_path).parent / PARCELLES_GEOM_FILENAME
    return path if path.exists() else None


def relative_parcel_features(records: list[dict[str, Any]], cx: float, cy: float) -> list[dict[str, Any]]:
    features = []
    for rec in records:
        geom = shapely_wkt.loads(rec["wkt"])
        if geom.is_empty:
            continue
        rel = translate(geom, xoff=-cx, yoff=-cy)
        c = rel.representative_point()
        features.append(
            {
                "label": rec["label"],
                "section": rec["section"],
                "numero": rec["numero"],
                "geometry": mapping(rel),
                "centroid": [round(float(c.x), 3), round(float(c.y), 3)],
            }
        )
    return features


def parcels_geojson_4326(records: list[dict[str, Any]]) -> dict[str, Any]:
    features = []
    for rec in records:
        geom = shapely_wkt.loads(rec["wkt"])
        if geom.is_empty:
            continue
        geom4326 = shapely_transform(_TO_4326.transform, geom)
        c = geom4326.representative_point()
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(geom4326),
                "properties": {
                    "label": rec["label"],
                    "section": rec["section"],
                    "numero": rec["numero"],
                    "lon": float(c.x),
                    "lat": float(c.y),
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}
