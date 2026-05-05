"""
lidar_router.py
Placer dans le même dossier que affichage_nuage_parcelle.py
Importer dans main.py :
    from lidar_router import router as lidar_router
    app.include_router(lidar_router, prefix="/lidar")
"""

from __future__ import annotations

import hashlib
import io
import logging
import tempfile
from pathlib import Path
from typing import List, Optional

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

# --- Import des fonctions du script existant ---
# On suppose que affichage_nuage_parcelle.py est dans le même dossier
from lidar_metier_nuage_de_points import (
    fetch_parcelle_geometry,
    fetch_lidar_tiles_for_parcelle,
    download_lidar_tiles,
    laz_to_point_cloud,
)

logger = logging.getLogger(__name__)
router = APIRouter()

TEMP_DIR = Path(tempfile.gettempdir()) / "kerelia_lidar"
TEMP_DIR.mkdir(exist_ok=True)


# ────────────────────────────────────────────────
# Modèles de requête
# ────────────────────────────────────────────────

class ParcelleRef(BaseModel):
    code_insee: str
    section: str
    numero: str


class PointsRequest(BaseModel):
    parcelles: List[ParcelleRef]
    max_points: int = 0  # 0 = tous les points


# ────────────────────────────────────────────────
# Endpoint 1 : lister les dalles concernées (rapide)
# ────────────────────────────────────────────────

@router.post("/tiles")
async def get_tiles(body: ParcelleRef):
    """
    Retourne la liste des dalles LiDAR HD IGN qui intersectent la parcelle.
    Rapide (~1–3s), pas de téléchargement.
    """
    try:
        geom = fetch_parcelle_geometry(body.code_insee, body.section, body.numero)
        tiles = fetch_lidar_tiles_for_parcelle(geom)
        return {
            "count": len(tiles),
            "tiles": [
                {"name": t.get("name"), "url": t.get("url")}
                for t in tiles
            ],
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("Erreur /lidar/tiles")
        raise HTTPException(status_code=500, detail=str(e))


# ────────────────────────────────────────────────
# Endpoint 2 : télécharger, clipper, sérialiser → Arrow IPC
# ────────────────────────────────────────────────

@router.post("/points")
async def get_points(body: PointsRequest):
    """
    Pipeline complet :
      1. Récupération géométrie(s) cadastrale(s)
      2. Union des géométries si plusieurs parcelles
      3. Récupération des dalles IGN
      4. Téléchargement dans /tmp
      5. Clip + filtrage point-in-polygon
      6. Sérialisation Apache Arrow IPC (Float32, coordonnées relatives)
    Retourne un flux binaire application/octet-stream.
    """
    try:
        from shapely.ops import unary_union

        # 1. Géométries
        logger.info("Récupération de %d géométrie(s) parcellaire(s)", len(body.parcelles))
        geoms = []
        for ref in body.parcelles:
            g = fetch_parcelle_geometry(ref.code_insee, ref.section, ref.numero)
            geoms.append(g)

        study_geom = unary_union(geoms) if len(geoms) > 1 else geoms[0]
        logger.info("Géométrie d'étude : surface=%.2f m²", study_geom.area)

        # 2. Dalles IGN
        tiles = fetch_lidar_tiles_for_parcelle(study_geom)
        if not tiles:
            raise HTTPException(status_code=404, detail="Aucune dalle LiDAR trouvée pour cette zone.")

        # 3. Téléchargement dans un sous-dossier temporaire unique
        job_id = hashlib.md5(
            "".join(f"{r.code_insee}{r.section}{r.numero}" for r in body.parcelles).encode()
        ).hexdigest()[:10]
        job_dir = TEMP_DIR / job_id
        job_dir.mkdir(exist_ok=True)

        laz_paths = download_lidar_tiles(tiles, job_dir)
        logger.info("%d dalle(s) téléchargée(s)", len(laz_paths))

        # 4. Clip + merge de toutes les dalles
        import pyvista as pv

        all_points: list[np.ndarray] = []
        all_classes: list[np.ndarray] = []

        for laz_path in laz_paths:
            try:
                cloud = laz_to_point_cloud(laz_path, study_geom, body.max_points)
                pts = np.asarray(cloud.points)
                all_points.append(pts)
                if "classification" in cloud.array_names:
                    all_classes.append(np.asarray(cloud["classification"]))
                else:
                    all_classes.append(np.zeros(pts.shape[0], dtype=np.uint8))
            except ValueError as e:
                logger.warning("Dalle ignorée (%s) : %s", laz_path.name, e)

        if not all_points:
            raise HTTPException(status_code=404, detail="Aucun point dans la zone d'étude après clipping.")

        points = np.vstack(all_points)
        classes = np.concatenate(all_classes).astype(np.uint8)

        # Sous-échantillonnage global si demandé
        if body.max_points > 0 and points.shape[0] > body.max_points:
            idx = np.random.choice(points.shape[0], size=body.max_points, replace=False)
            points = points[idx]
            classes = classes[idx]

        logger.info("Total points après merge/clip : %d", points.shape[0])

        # 5. Normalisation relative (évite le jittering Float32 sur Lambert-93)
        cx, cy = float(points[:, 0].mean()), float(points[:, 1].mean())
        rel = points.copy()
        rel[:, 0] -= cx
        rel[:, 1] -= cy
        # Z : on garde absolu mais on peut aussi centrer, selon le besoin côté deck.gl
        # rel[:, 2] -= float(points[:, 2].mean())

        # 6. Sérialisation Arrow IPC
        table = pa.table({
            "x": pa.array(rel[:, 0].astype("float32")),
            "y": pa.array(rel[:, 1].astype("float32")),
            "z": pa.array(rel[:, 2].astype("float32")),
            "classification": pa.array(classes),
        }, metadata={
            "center_x": str(cx),
            "center_y": str(cy),
            "n_points": str(points.shape[0]),
            "epsg": "2154",
        })

        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)

        arrow_bytes = sink.getvalue().to_pybytes()
        logger.info("Arrow IPC sérialisé : %.2f Mo", len(arrow_bytes) / 1e6)

        return Response(
            content=arrow_bytes,
            media_type="application/octet-stream",
            headers={
                "X-Center-X": str(cx),
                "X-Center-Y": str(cy),
                "X-N-Points": str(points.shape[0]),
                "Access-Control-Expose-Headers": "X-Center-X, X-Center-Y, X-N-Points",
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erreur /lidar/points")
        raise HTTPException(status_code=500, detail=str(e))


# ────────────────────────────────────────────────
# Endpoint 3 : healthcheck rapide
# ────────────────────────────────────────────────

@router.get("/health")
async def health():
    return {"status": "ok"}