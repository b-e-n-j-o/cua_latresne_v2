# -*- coding: utf-8 -*-
"""MNT figé pour CUA : grille + contours UF (indépendant des mises à jour cadastre / dalles)."""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from shapely import wkt as shapely_wkt
from shapely.affinity import translate
from shapely.geometry import mapping

from api.mnt.parcelle_to_mnt import fetch_mnt_from_geometry
from api.mnt.router_mnt import (
    MAX_VERTICES,
    UF_CONTEXT_BUFFER_M,
    _encode_elevations,
    _geom_to_2154,
)

from api.cuas.latresne.CUA.map3d.terrain_dxf import export_terrain_to_dxf
from api.cuas.latresne.CUA.map3d.terrain_html import render_terrain_html

logger = logging.getLogger("cua.latresne.terrain3d")


def _contour_relative(geom, cx: float, cy: float) -> dict:
    return mapping(translate(geom, xoff=-cx, yoff=-cy))


def build_frozen_terrain_payload_from_wkt(
    wkt: str,
    *,
    exaggeration: float = 1.5,
    buffer_m: float = UF_CONTEXT_BUFFER_M,
) -> dict:
    geom_uf = _geom_to_2154(shapely_wkt.loads((wkt or "").strip()))
    if geom_uf.is_empty:
        raise ValueError("Géométrie UF vide")

    emprise = geom_uf.buffer(buffer_m) if buffer_m and buffer_m > 0 else geom_uf
    mnt, transform, resolution = fetch_mnt_from_geometry(emprise)
    rows, cols = mnt.shape
    total = rows * cols
    if total > MAX_VERTICES:
        step = math.ceil(math.sqrt(total / MAX_VERTICES))
        mnt = mnt[::step, ::step]
        resolution = resolution * step
        rows, cols = mnt.shape
        logger.info("MNT CUA décimé ×%s → %sx%s", step, cols, rows)

    elev_min = float(np.nanmin(mnt))
    elev_max = float(np.nanmax(mnt))
    west = float(transform.c)
    north = float(transform.f)
    east = west + cols * resolution
    south = north - rows * resolution
    cx = (west + east) / 2.0
    cy = (south + north) / 2.0

    return {
        "frozen_at": datetime.now(timezone.utc).isoformat(),
        "buffer_m": buffer_m,
        "width": cols,
        "height": rows,
        "resolution_m": round(float(resolution), 4),
        "elev_min": round(elev_min, 3),
        "elev_max": round(elev_max, 3),
        "elevations_b64": _encode_elevations(mnt),
        "contours": [_contour_relative(geom_uf, cx, cy)],
        "center_x": round(cx, 2),
        "center_y": round(cy, 2),
        "surface_m2": round(float(geom_uf.area), 1),
        "exaggeration": exaggeration,
        "n_voisins": 0,
    }


def build_frozen_terrain_payload(
    wkt_path: str,
    *,
    exaggeration: float = 1.5,
    buffer_m: float = UF_CONTEXT_BUFFER_M,
) -> dict:
    wkt_file = Path(wkt_path)
    if not wkt_file.exists():
        raise FileNotFoundError(f"WKT introuvable : {wkt_path}")
    return build_frozen_terrain_payload_from_wkt(
        wkt_file.read_text(encoding="utf-8"),
        exaggeration=exaggeration,
        buffer_m=buffer_m,
    )


def write_frozen_terrain_files(
    payload: dict,
    output_dir,
    *,
    html_name: str = "carte_3d.html",
) -> dict:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "terrain.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    html_path = out / html_name
    html_path.write_text(render_terrain_html(payload), encoding="utf-8")
    dxf_name = "topo_mnt.dxf"
    dxf_path = out / dxf_name
    dxf_written = None
    try:
        export_terrain_to_dxf(payload, str(dxf_path))
        dxf_written = str(dxf_path)
    except Exception as exc:
        logger.warning("Export DXF topo ignoré : %s", exc)
    return {
        "path": str(html_path),
        "filename": html_name,
        "dxf_path": dxf_written,
        "dxf_filename": dxf_name if dxf_written else None,
        "metadata": {
            "frozen_at": payload["frozen_at"],
            "buffer_m": payload["buffer_m"],
            "resolution": payload["resolution_m"],
            "rows": payload["height"],
            "cols": payload["width"],
            "exaggeration": payload.get("exaggeration"),
            "surface_m2": payload["surface_m2"],
            "terrain_json": str(json_path),
            "dxf_path": dxf_written,
            "dxf_filename": dxf_name if dxf_written else None,
        },
    }


def exporter_visualisation_3d_from_wkt(
    wkt_path,
    output_dir="./out_3d",
    exaggeration=1.5,
):
    """
    Artefact 3D gelé (HTML Three.js + terrain.json).
    Même contrat que l'ancien Plotly : {path, filename, metadata} ou {error, path: None}.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    try:
        payload = build_frozen_terrain_payload(wkt_path, exaggeration=exaggeration)
        result = write_frozen_terrain_files(
            payload, out, html_name="carte_3d_unite_fonciere.html"
        )
        logger.info("3D CUA figée : %s", result["path"])
        return result
    except Exception as exc:
        logger.exception("Erreur génération 3D CUA figée")
        return {"error": str(exc), "path": None, "filename": None}
