"""
mnt_terrain_router.py
---------------------
Router FastAPI pour la visualisation 3D MNT via React Three Fiber.

Endpoints :
  POST /mnt/terrain/data   → JSON + Float32 base64 (pipeline complet)
  GET  /mnt/health         → healthcheck

Remplace l'ancien endpoint /mnt/visualisation/html (Plotly iframe).

Ajout dans main.py :
  from api.mnt.mnt_terrain_router import router as mnt_terrain_router
  app.include_router(mnt_terrain_router, prefix="/mnt")
"""

from __future__ import annotations

import base64
import logging
import math
import os

import numpy as np
import pyproj
import psutil
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from shapely.geometry import mapping, shape as shapely_shape
from shapely.ops import transform as shapely_transform

from api.mnt.parcelle_to_mnt import (
    build_emprise_mnt,
    fetch_mnt_from_geometry,
    fetch_parcelle_geometry,
    fetch_parcelles_contigues,
)

logger = logging.getLogger(__name__)
router = APIRouter()

_to_2154 = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True).transform
UF_CONTEXT_BUFFER_M = 100.0
MAX_VERTICES = 200_000  # budget fluide pour navigateur / GPU intégré


def log_ram(step: str):
    p = psutil.Process(os.getpid())
    rss = p.memory_info().rss / 1e6
    vm = psutil.virtual_memory()
    logger.info(
        "RAM [%s] process=%.0fMo | système=%.0f/%.0fMo (%.0f%%)",
        step,
        rss,
        vm.used / 1e6,
        vm.total / 1e6,
        vm.percent,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _looks_like_wgs84(geom) -> bool:
    minx, miny, maxx, maxy = geom.bounds
    return (
        -180.0 <= minx <= 180.0
        and -180.0 <= maxx <= 180.0
        and -90.0 <= miny <= 90.0
        and -90.0 <= maxy <= 90.0
    )


def _geom_to_2154(geom):
    if _looks_like_wgs84(geom):
        return shapely_transform(_to_2154, geom)
    return geom


def _contour_to_relative(geom, cx: float, cy: float) -> dict:
    """GeoJSON avec coordonnées relatives au centre du MNT (cx, cy)."""
    from shapely.affinity import translate
    return mapping(translate(geom, xoff=-cx, yoff=-cy))


def _encode_elevations(mnt: np.ndarray) -> str:
    """
    Aplatit le MNT (row-major, nord→sud), NaN→0, Float32, base64.
    623×555 px ≈ 1.4 Mo brut → 1.9 Mo base64.
    """
    flat = np.nan_to_num(mnt.astype(np.float32), nan=0.0).flatten()
    return base64.b64encode(flat.tobytes()).decode("ascii")


# ── Modèle ────────────────────────────────────────────────────────────────────

class MntTerrainRequest(BaseModel):
    code_insee: str
    section: str
    numero: str
    exaggeration: float = Field(default=1.5, ge=0.1, le=10.0)
    include_voisins: bool = Field(default=True)
    union_geometry: dict | None = Field(default=None)
    parcelles: list[dict[str, str]] | None = Field(default=None)


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post("/terrain/data")
async def get_terrain_data(body: MntTerrainRequest):
    """
    Pipeline MNT → données brutes pour React Three Fiber.

    Retourne :
      width, height       : dimensions grille (cols × rows)
      resolution_m        : résolution spatiale en mètres
      elev_min, elev_max  : bornes altitude NGF
      elevations_b64      : Float32[] row-major N→S, encodé base64
      contours            : liste GeoJSON Geometry en coords relatives (cx, cy)
      center_x, center_y  : centre Lambert-93 (info)
      surface_m2          : surface parcelle cible
      n_voisins           : voisins inclus dans l'emprise
      exaggeration        : valeur passée par le front
    """
    try:
        log_ram("start")

        # 1. Géométrie cible
        if body.union_geometry:
            geom_cible = _geom_to_2154(shapely_shape(body.union_geometry))
            if geom_cible.is_empty:
                raise ValueError("Géométrie UF vide.")
            logger.info("UF reçue du front : surface=%.1f m²", geom_cible.area)
        else:
            geom_cible = fetch_parcelle_geometry(body.code_insee, body.section, body.numero)

        # 2. Contours à dessiner (jaune)
        contour_geoms: list = []
        if body.parcelles:
            for ref in body.parcelles:
                try:
                    contour_geoms.append(
                        fetch_parcelle_geometry(
                            ref.get("code_insee", body.code_insee),
                            ref.get("section", ""),
                            ref.get("numero", ""),
                        )
                    )
                except Exception as e:
                    logger.warning("Contour ignoré (%s): %s", ref, e)
        if not contour_geoms:
            contour_geoms = [geom_cible]

        # 3. Emprise MNT
        if body.include_voisins:
            if body.union_geometry:
                emprise = geom_cible.buffer(UF_CONTEXT_BUFFER_M)
                n_voisins = 0
            else:
                voisins, n_voisins = fetch_parcelles_contigues(geom_cible, body.code_insee)
                emprise = build_emprise_mnt(geom_cible, voisins)
        else:
            emprise = geom_cible
            n_voisins = 0

        # 4. MNT
        mnt, transform, resolution = fetch_mnt_from_geometry(emprise)
        log_ram("after_fetch_mnt")
        rows, cols = mnt.shape
        total = rows * cols
        if total > MAX_VERTICES:
            step = math.ceil(math.sqrt(total / MAX_VERTICES))
            mnt = mnt[::step, ::step]
            resolution = resolution * step
            rows, cols = mnt.shape
            logger.info(
                "MNT décimé ×%d → %dx%d px (%.0fk vertices)",
                step,
                cols,
                rows,
                (rows * cols) / 1000,
            )
            log_ram("after_decimation")
        elev_min = float(np.nanmin(mnt))
        elev_max = float(np.nanmax(mnt))
        logger.info("MNT %dx%d px, résolution=%.2fm, alt=[%.2f, %.2f]",
                    cols, rows, resolution, elev_min, elev_max)

        # 5. Centre MNT (normalisation coords)
        west  = float(transform.c)
        north = float(transform.f)
        east  = west  + cols * resolution
        south = north - rows * resolution
        cx = (west + east) / 2.0
        cy = (south + north) / 2.0

        # 6. Float32 base64
        elevations_b64 = _encode_elevations(mnt)
        log_ram("after_encode_base64")
        logger.info("Float32 b64 : %.0f Ko", len(elevations_b64) / 1024)

        # 7. Contours relatifs
        contours_geojson = []
        for g in contour_geoms:
            try:
                contours_geojson.append(_contour_to_relative(g, cx, cy))
            except Exception as e:
                logger.warning("Contour non convertible : %s", e)

        response = JSONResponse(content={
            "width":          cols,
            "height":         rows,
            "resolution_m":   round(resolution, 4),
            "elev_min":       round(elev_min, 3),
            "elev_max":       round(elev_max, 3),
            "elevations_b64": elevations_b64,
            "contours":       contours_geojson,
            "center_x":       round(cx, 2),
            "center_y":       round(cy, 2),
            "surface_m2":     round(float(geom_cible.area), 1),
            "n_voisins":      n_voisins,
            "exaggeration":   body.exaggeration,
        })
        log_ram("before_return")
        return response

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erreur /mnt/terrain/data")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health():
    return {"status": "ok"}