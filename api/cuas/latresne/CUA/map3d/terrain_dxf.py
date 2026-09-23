# -*- coding: utf-8 -*-
"""Export DXF CAO (3DFACE + POLYLINE 3D) depuis le payload MNT figé."""

from __future__ import annotations

import base64
import math
import struct
from pathlib import Path

import ezdxf


def _decode_elevations(elevations_b64: str) -> tuple[float, ...]:
    binary_data = base64.b64decode(elevations_b64)
    num_floats = len(binary_data) // 4
    return struct.unpack(f"{num_floats}f", binary_data)


def _is_finite(z: float) -> bool:
    return z is not None and math.isfinite(z)


def export_terrain_to_dxf(payload: dict, output_dxf_path: str) -> str:
    """
    Maillage MNT + contour UF en DXF R2000.

    Coordonnées Lambert-93 (EPSG:2154) via center_x / center_y du payload,
    altitudes NGF réelles (sans exagération verticale web).
    """
    elevations = _decode_elevations(payload["elevations_b64"])
    width = int(payload["width"])
    height = int(payload["height"])
    res = float(payload["resolution_m"])
    cx = float(payload.get("center_x") or 0.0)
    cy = float(payload.get("center_y") or 0.0)

    W = width * res
    H = height * res

    doc = ezdxf.new("R2000")
    doc.header["$INSUNITS"] = 6  # mètres
    msp = doc.modelspace()
    try:
        doc.layers.add("MNT_MAILLAGE_3D", color=3)
        doc.layers.add("CONTOUR_UNITE_FONCIERE", color=2)
    except (AttributeError, TypeError):
        doc.layers.new(name="MNT_MAILLAGE_3D", dxfattribs={"color": 3})
        doc.layers.new(name="CONTOUR_UNITE_FONCIERE", dxfattribs={"color": 2})

    vertices: list[tuple[float, float, float] | None] = []
    for row in range(height):
        for col in range(width):
            x = cx + (col * res) - (W / 2.0)
            y = cy + (H / 2.0) - (row * res)
            z = elevations[row * width + col]
            vertices.append((x, y, float(z)) if _is_finite(z) else None)

    for row in range(height - 1):
        for col in range(width - 1):
            i0 = row * width + col
            i1 = row * width + (col + 1)
            i2 = (row + 1) * width + col
            i3 = (row + 1) * width + (col + 1)
            v0, v1, v2, v3 = vertices[i0], vertices[i1], vertices[i2], vertices[i3]
            if None in (v0, v1, v2, v3):
                continue
            msp.add_3dface(
                [v0, v1, v3, v3],
                dxfattribs={"layer": "MNT_MAILLAGE_3D"},
            )
            msp.add_3dface(
                [v0, v3, v2, v2],
                dxfattribs={"layer": "MNT_MAILLAGE_3D"},
            )

    def sample_elev_at(rx: float, ry: float) -> float | None:
        col = max(0, min(width - 1, round((rx + W / 2.0) / res)))
        row = max(0, min(height - 1, round((H / 2.0 - ry) / res)))
        z = elevations[row * width + col]
        return float(z) if _is_finite(z) else None

    for geojson in payload.get("contours") or []:
        geom_type = geojson.get("type")
        rings = []
        if geom_type == "Polygon":
            rings = geojson.get("coordinates") or []
        elif geom_type == "MultiPolygon":
            for poly in geojson.get("coordinates") or []:
                rings.extend(poly)
        for ring in rings:
            dxf_pts = []
            for pair in ring:
                if len(pair) < 2:
                    continue
                rx, ry = float(pair[0]), float(pair[1])
                elev = sample_elev_at(rx, ry)
                if elev is None:
                    continue
                dxf_pts.append((cx + rx, cy + ry, elev))
            if len(dxf_pts) >= 2:
                msp.add_polyline3d(
                    dxf_pts,
                    close=True,
                    dxfattribs={"layer": "CONTOUR_UNITE_FONCIERE"},
                )

    out = Path(output_dxf_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    doc.saveas(str(out))
    return str(out)
