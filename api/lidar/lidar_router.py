"""
lidar_router.py
Placer dans le même dossier que affichage_nuage_parcelle.py
Importer dans main.py :
    from lidar_router import router as lidar_router
    app.include_router(lidar_router, prefix="/lidar")
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import List

import numpy as np
import psutil
import pyarrow as pa
import pyarrow.ipc as ipc
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from shapely.geometry import Polygon

# --- Import des fonctions du script existant ---
# On suppose que affichage_nuage_parcelle.py est dans le même dossier
from api.lidar.lidar_metier_nuage_de_points import (
    fetch_parcelle_geometry,
    fetch_lidar_tiles_for_parcelle,
    download_lidar_tiles,
    geometry_with_buffer,
    laz_to_point_cloud,
)

logger = logging.getLogger(__name__)
router = APIRouter()

TEMP_DIR = Path(tempfile.gettempdir()) / "kerelia_lidar"
TEMP_DIR.mkdir(exist_ok=True)


def _densify_exterior_xy(exterior, step_m: float) -> np.ndarray:
    """Échantillonne le pourtour d'un polygone (Shapely LinearRing) en XY (m)."""
    from shapely.geometry import LineString

    line = LineString(exterior.coords)
    length = line.length
    if length <= 0:
        return np.zeros((0, 2), dtype=np.float64)
    pts: list[tuple[float, float]] = []
    d = 0.0
    while d < length:
        p = line.interpolate(d)
        pts.append((float(p.x), float(p.y)))
        d += step_m
    c0 = exterior.coords[0]
    pts.append((float(c0[0]), float(c0[1])))
    return np.asarray(pts, dtype=np.float64)


def build_parcelle_outline_paths_relative(
    study_geom,
    points_lidar_xyz: np.ndarray,
    cx: float,
    cy: float,
    step_m: float = 2.0,
    z_lift_m: float = 0.15,
    ground_percentile: float = 4.0,
) -> list[list[list[float]]]:
    """
    Polylignes en coords relatives (comme le nuage Arrow).
    Z constant « au ras du sol » : percentile bas des altitudes LiDAR dans la zone
    (approximation terrain) + léger rehaussement pour rester lisible au-dessus du tapis de points.
    """
    if points_lidar_xyz.size == 0:
        return []

    zs = points_lidar_xyz[:, 2].astype(np.float64)
    zs = zs[np.isfinite(zs)]
    if zs.size == 0:
        return []

    p = float(np.clip(ground_percentile, 0.5, 20.0))
    z_sol = float(np.percentile(zs, p))
    z_line = z_sol + float(z_lift_m)
    logger.info(
        "Contour parcelle : Z plat ≈ sol (p%.1f=%.2f m) + %.2f m → %.2f m",
        p,
        z_sol,
        z_lift_m,
        z_line,
    )

    polys: list = []
    if study_geom.geom_type == "Polygon":
        polys = [study_geom]
    elif study_geom.geom_type == "MultiPolygon":
        polys = list(study_geom.geoms)
    else:
        logger.warning("Contour parcelle : type géométrique inattendu %s", study_geom.geom_type)
        return []

    paths: list[list[list[float]]] = []

    for poly in polys:
        if not isinstance(poly, Polygon):
            continue
        xy = _densify_exterior_xy(poly.exterior, step_m)
        if xy.shape[0] < 2:
            continue
        rel_x = xy[:, 0] - cx
        rel_y = xy[:, 1] - cy
        n = xy.shape[0]
        z_col = np.full(n, z_line, dtype=np.float64)
        path = np.column_stack([rel_x, rel_y, z_col]).tolist()
        paths.append(path)

    logger.info("Contour parcelle cible : %d chemin(s), ~%d points au total", len(paths), sum(len(p) for p in paths))
    return paths


def log_ram(step: str):
    process = psutil.Process(os.getpid())
    rss = process.memory_info().rss / 1e6
    vm = psutil.virtual_memory()
    logger.info(
        "RAM [%s] process=%.0f Mo | systeme=%.0f/%.0f Mo (%.0f%%)",
        step, rss, vm.used / 1e6, vm.total / 1e6, vm.percent
    )


# ────────────────────────────────────────────────
# Modèles de requête
# ────────────────────────────────────────────────

class ParcelleRef(BaseModel):
    code_insee: str
    section: str
    numero: str


class TilesRequest(BaseModel):
    """Référence parcelle + tampon optionnel pour la liste des dalles IGN."""

    code_insee: str
    section: str
    numero: str
    context_buffer_m: float = Field(
        default=5.0,
        ge=0.0,
        le=50.0,
        description="Tampon (m) autour de la parcelle pour intersecter les dalles LiDAR.",
    )


class PointsRequest(BaseModel):
    parcelles: List[ParcelleRef]
    max_points: int = 0  # 0 = tous les points
    context_buffer_m: float = Field(
        default=5.0,
        ge=0.0,
        le=50.0,
        description="Tampon (m) autour de l'union des parcelles pour dalles + clipping points.",
    )
    include_parcelle_outline: bool = Field(
        default=False,
        description="Si True : réponse JSON avec Arrow base64 + polylignes 3D jaunes (limite parcelle).",
    )


# ────────────────────────────────────────────────
# Endpoint 1 : lister les dalles concernées (rapide)
# ────────────────────────────────────────────────

@router.post("/tiles")
async def get_tiles(body: TilesRequest):
    """
    Retourne la liste des dalles LiDAR HD IGN qui intersectent la parcelle
    (éventuellement élargie d'un tampon en mètres).
    Rapide (~1–3s), pas de téléchargement.
    """
    try:
        geom = fetch_parcelle_geometry(body.code_insee, body.section, body.numero)
        zone = geometry_with_buffer(geom, body.context_buffer_m)
        tiles = fetch_lidar_tiles_for_parcelle(zone)
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
    Retourne soit un flux binaire Arrow (`include_parcelle_outline=false`),
    soit un JSON `{ arrow_ipc_base64, outline_paths }` (`include_parcelle_outline=true`).
    """
    job_dir: Path | None = None
    try:
        from shapely.ops import unary_union
        log_ram("debut requete")

        # 1. Géométries
        logger.info("Récupération de %d géométrie(s) parcellaire(s)", len(body.parcelles))
        geoms = []
        for ref in body.parcelles:
            g = fetch_parcelle_geometry(ref.code_insee, ref.section, ref.numero)
            geoms.append(g)

        study_geom = unary_union(geoms) if len(geoms) > 1 else geoms[0]
        clip_geom = geometry_with_buffer(study_geom, body.context_buffer_m)
        logger.info("Géométrie d'étude (parcelle(s)) : surface=%.2f m²", study_geom.area)
        logger.info(
            "Zone LiDAR (clip + tampon %.1f m) : surface≈%.0f m², bbox=%s",
            body.context_buffer_m,
            clip_geom.area,
            clip_geom.bounds,
        )
        log_ram("apres geometrie")

        # 2. Dalles IGN (emprise = clip_geom pour limiter la taille vs parcelles voisines entières)
        tiles = fetch_lidar_tiles_for_parcelle(clip_geom)
        if not tiles:
            raise HTTPException(status_code=404, detail="Aucune dalle LiDAR trouvée pour cette zone.")
        logger.info("Dalles intersectées : %d", len(tiles))
        for t in tiles:
            logger.info("  -> %s", t.get("url"))
        log_ram("apres fetch tiles IGN")

        # 3. Téléchargement dans un sous-dossier temporaire unique
        job_id = hashlib.md5(
            "".join(f"{r.code_insee}{r.section}{r.numero}" for r in body.parcelles).encode()
        ).hexdigest()[:10]
        job_dir = TEMP_DIR / job_id
        job_dir.mkdir(exist_ok=True)

        laz_paths = download_lidar_tiles(tiles, job_dir)
        logger.info("%d dalle(s) téléchargée(s)", len(laz_paths))
        total_mb = sum(p.stat().st_size for p in laz_paths) / 1e6
        logger.info("Dalles téléchargées : %d fichier(s), %.1f Mo total", len(laz_paths), total_mb)
        log_ram("apres telechargement")

        # 4. Clip + merge de toutes les dalles
        import pyvista as pv

        all_points: list[np.ndarray] = []
        all_classes: list[np.ndarray] = []
        points_bruts_total = 0

        for laz_path in laz_paths:
            try:
                cloud = laz_to_point_cloud(laz_path, clip_geom, body.max_points)
                pts = np.asarray(cloud.points)
                points_in_tile = int(pts.shape[0])
                points_bruts_total += points_in_tile
                logger.info("Dalle %s : %d points bruts dans bbox/clip", laz_path.name, points_in_tile)
                all_points.append(pts)
                if "classification" in cloud.array_names:
                    all_classes.append(np.asarray(cloud["classification"]))
                else:
                    all_classes.append(np.zeros(pts.shape[0], dtype=np.uint8))
                log_ram("apres clip dalle")
            except ValueError as e:
                logger.warning("Dalle ignorée (%s) : %s", laz_path.name, e)

        if not all_points:
            raise HTTPException(status_code=404, detail="Aucun point dans la zone d'étude après clipping.")

        points_merged = np.vstack(all_points)
        classes_merged = np.concatenate(all_classes).astype(np.uint8)
        points_for_outline = points_merged

        # Centre XY stable : nuage complet (aligne contour parcelle et points affichés)
        cx, cy = float(points_merged[:, 0].mean()), float(points_merged[:, 1].mean())

        points = points_merged
        classes = classes_merged

        # Sous-échantillonnage global si demandé (le contour utilise toujours le nuage complet)
        if body.max_points > 0 and points.shape[0] > body.max_points:
            idx = np.random.choice(points.shape[0], size=body.max_points, replace=False)
            points = points[idx]
            classes = classes[idx]

        logger.info("Points après merge/clip : %d (nuage complet)", int(points_merged.shape[0]))
        logger.info("Points sérialisés Arrow : %d", points.shape[0])
        log_ram("avant serialisation Arrow")

        # 5. Normalisation relative (évite le jittering Float32 sur Lambert-93)
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
        logger.info("Arrow IPC : %.1f Mo", len(arrow_bytes) / 1e6)
        log_ram("fin requete")

        outline_paths: list[list[list[float]]] = []
        if body.include_parcelle_outline:
            outline_paths = build_parcelle_outline_paths_relative(
                study_geom,
                points_for_outline,
                cx,
                cy,
            )

        common_headers = {
            "X-Center-X": str(cx),
            "X-Center-Y": str(cy),
            "X-N-Points": str(points.shape[0]),
            "X-Superficie-M2": str(round(study_geom.area, 1)),
            "X-Clip-Area-M2": str(round(float(clip_geom.area), 1)),
            "X-Context-Buffer-M": str(float(body.context_buffer_m)),
            "X-N-Tiles": str(len(tiles)),
            "X-Tiles-Mb": str(round(total_mb, 1)),
            "X-Points-Bruts": str(int(points_bruts_total)),
            "Access-Control-Expose-Headers": (
                "X-Center-X,X-Center-Y,X-N-Points,X-Superficie-M2,X-Clip-Area-M2,"
                "X-Context-Buffer-M,X-N-Tiles,X-Tiles-Mb,X-Points-Bruts,X-Parcelle-Outline"
            ),
            "X-Parcelle-Outline": "1" if body.include_parcelle_outline and outline_paths else "0",
        }

        if body.include_parcelle_outline:
            return JSONResponse(
                content={
                    "arrow_ipc_base64": base64.b64encode(arrow_bytes).decode("ascii"),
                    "outline_paths": outline_paths,
                },
                headers=common_headers,
            )

        return Response(
            content=arrow_bytes,
            media_type="application/octet-stream",
            headers=common_headers,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erreur /lidar/points")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Nettoyage systématique des dalles .laz téléchargées
        if job_dir is not None and job_dir.exists():
            try:
                shutil.rmtree(job_dir, ignore_errors=True)
                logger.info("Nettoyage temporaire termine: %s", job_dir)
            except Exception as cleanup_error:
                logger.warning("Impossible de supprimer le dossier temporaire %s: %s", job_dir, cleanup_error)


# ────────────────────────────────────────────────
# Endpoint 3 : healthcheck rapide
# ────────────────────────────────────────────────

@router.get("/health")
async def health():
    return {"status": "ok"}