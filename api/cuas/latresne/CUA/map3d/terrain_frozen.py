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
from shapely.geometry import mapping, shape
from shapely.geometry.base import BaseGeometry
import shapely

from api.mnt.parcelle_to_mnt import fetch_mnt_from_geometry
from api.mnt.router_mnt import (
    MAX_VERTICES,
    UF_CONTEXT_BUFFER_M,
    _encode_elevations,
    _geom_to_2154,
)

from api.cuas.latresne.CUA.map3d.parcels_overlay import (
    load_parcelles_geom,
    relative_parcel_features,
    sibling_parcelles_path,
)
from api.cuas.latresne.CUA.map3d.dxf import PackOptions, build_cad_package
from api.cuas.latresne.CUA.map3d.dxf.cote_de_seuil import CoteSeuilOptions
from api.cuas.latresne.CUA.map3d.terrain_html import render_terrain_html

logger = logging.getLogger("cua.latresne.terrain3d")


def _ring_xy(coords) -> list[list[float]]:
    pts = [[round(float(x), 2), round(float(y), 2)] for x, y, *_ in coords]
    if len(pts) >= 2 and pts[0] == pts[-1]:
        pts = pts[:-1]
    return pts


def cote_seuil_overlay_relative(features: list[dict] | None, cx: float, cy: float,
                                value_field: str = "codezone") -> list[dict]:
    """Zones de cote (EPSG:2154) → polygones relatifs au centre MNT, altitude NGF inchangée."""
    out = []
    for feat in features or []:
        props = feat.get("properties") or {}
        geom_in = feat.get("geometry")
        if not geom_in:
            continue
        try:
            cote = float(props[value_field])
            geom: BaseGeometry = shape(geom_in)
        except (KeyError, TypeError, ValueError):
            continue
        if not np.isfinite(cote) or geom.is_empty:
            continue
        if not geom.is_valid:
            geom = shapely.make_valid(geom)
        rel = translate(geom, xoff=-cx, yoff=-cy).simplify(1.0, preserve_topology=True)
        polygons = []
        for part in shapely.get_parts(rel):
            if part.geom_type != "Polygon" or part.is_empty:
                continue
            exterior = _ring_xy(part.exterior.coords)
            if len(exterior) < 3:
                continue
            holes = [h for h in (_ring_xy(r.coords) for r in part.interiors) if len(h) >= 3]
            polygons.append({"exterior": exterior, "holes": holes})
        if not polygons:
            continue
        c = rel.representative_point()
        out.append({
            "cote": round(cote, 2),
            "label": f"Cote de seuil {f'{cote:.2f}'.replace('.', ',')} m NGF",
            "centroid": [round(float(c.x), 2), round(float(c.y), 2)],
            "polygons": polygons,
        })
    return out


def _contour_relative(geom, cx: float, cy: float) -> dict:
    return mapping(translate(geom, xoff=-cx, yoff=-cy))


def build_frozen_terrain_payload_from_wkt(
    wkt: str,
    *,
    exaggeration: float = 1.5,
    buffer_m: float = UF_CONTEXT_BUFFER_M,
    parcels=None,
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
    parcel_records = load_parcelles_geom(parcels)

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
        "parcels": relative_parcel_features(parcel_records, cx, cy),
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
    parcels=None,
) -> dict:
    wkt_file = Path(wkt_path)
    if not wkt_file.exists():
        raise FileNotFoundError(f"WKT introuvable : {wkt_path}")
    if parcels is None:
        parcels = sibling_parcelles_path(wkt_file)
    return build_frozen_terrain_payload_from_wkt(
        wkt_file.read_text(encoding="utf-8"),
        exaggeration=exaggeration,
        buffer_m=buffer_m,
        parcels=parcels,
    )


def write_frozen_terrain_files(
    payload: dict,
    output_dir,
    *,
    html_name: str = "carte_3d.html",
    carto_context: dict | None = None,
    carto_catalogue: dict | None = None,
    pack_name: str = "topo_mnt",
    meta: dict | None = None,
    cote_seuil_features: list[dict] | None = None,
    cote_seuil_options: CoteSeuilOptions | None = None,
    include_dxf: bool = True,
) -> dict:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    html_payload = dict(payload)
    overlay = cote_seuil_overlay_relative(
        cote_seuil_features,
        float(payload.get("center_x") or 0.0),
        float(payload.get("center_y") or 0.0),
    )
    if overlay:
        html_payload["cote_seuil"] = overlay
    extra_css, extra_js = "", ""
    try:
        from api.cuas.latresne.CUA.map3d.sig_overlay_3d import build_sig_overlay, viewer_assets

        sig_layers = build_sig_overlay(
            carto_context,
            carto_catalogue,
            float(payload.get("center_x") or 0.0),
            float(payload.get("center_y") or 0.0),
        )
        if sig_layers:
            html_payload["sig_layers"] = sig_layers
            extra_css, extra_js = viewer_assets()
            logger.info("Overlay SIG 3D : %s couche(s)", len(sig_layers))
    except Exception as exc:
        logger.warning("Overlay SIG 3D ignoré : %s", exc)
    json_path = out / "terrain.json"
    json_path.write_text(json.dumps(html_payload, ensure_ascii=False), encoding="utf-8")
    html_path = out / html_name
    html_path.write_text(render_terrain_html(html_payload, extra_css, extra_js), encoding="utf-8")
    dxf_written = None
    if include_dxf:
        try:
            pack = build_cad_package(
                out,
                terrain_payload=payload,
                carto_context=carto_context,
                carto_catalogue=carto_catalogue,
                options=PackOptions(name=pack_name),
                meta=meta,
                cote_seuil_features=cote_seuil_features,
                cote_seuil_options=cote_seuil_options,
            )
            dxf_written = pack.get("zip")
        except Exception as exc:
            logger.warning("Export DXF CAO ignoré : %s", exc)
    else:
        logger.info("Pack DXF ignoré (include_dxf=False)")
    dxf_filename = Path(dxf_written).name if dxf_written else None
    return {
        "path": str(html_path),
        "filename": html_name,
        "dxf_path": dxf_written,
        "dxf_filename": dxf_filename,
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
            "dxf_filename": dxf_filename,
        },
    }


def exporter_visualisation_3d_from_wkt(
    wkt_path,
    output_dir="./out_3d",
    exaggeration=1.5,
    parcels_geom=None,
    include_dxf: bool = False,
    cote_seuil_features: list[dict] | None = None,
):
    """
    Artefact 3D gelé (HTML Three.js + terrain.json).
    Même contrat que l'ancien Plotly : {path, filename, metadata} ou {error, path: None}.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    try:
        payload = build_frozen_terrain_payload(
            wkt_path, exaggeration=exaggeration, parcels=parcels_geom
        )
        result = write_frozen_terrain_files(
            payload,
            out,
            html_name="carte_3d_unite_fonciere.html",
            include_dxf=include_dxf,
            cote_seuil_features=cote_seuil_features,
        )
        logger.info("3D CUA figée : %s", result["path"])
        return result
    except Exception as exc:
        logger.exception("Erreur génération 3D CUA figée")
        return {"error": str(exc), "path": None, "filename": None}
