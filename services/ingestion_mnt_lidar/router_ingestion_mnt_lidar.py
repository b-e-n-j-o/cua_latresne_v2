"""
Route FastAPI : ingester les dalles IGN MNT / LiDAR HD pour une géométrie
fournie en GeoJSON, avec téléversement vers les buckets Supabase
``dalles-lidar`` et ``mnt-dalles``.

Montage dans ``main.py`` (préfixe ``/admin``) :

    POST /admin/mnt-lidar/ingest
    GET  /admin/mnt-lidar/jobs/{job_id}

Variables d'environnement (téléversement) : ``SUPABASE_URL`` et
``SUPABASE_SERVICE_ROLE_KEY`` (ou ``SERVICE_KEY`` / ``SUPABASE_KEY``).

Par défaut ``background=true`` : la requête renvoie immédiatement un ``job_id``
(HTTP 202) pour éviter les timeouts HTTP sur Render pendant les gros volumes.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from services.ingestion_mnt_lidar.telecharger_mnt_ou_lidar import (
    geojson_to_geometry_2154,
    run_pipeline_geometry_to_supabase,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ingestion-mnt-lidar"])

JOBS: dict[str, dict[str, Any]] = {}


class MntLidarIngestRequest(BaseModel):
    geometry: dict[str, Any] = Field(
        ...,
        description="GeoJSON Geometry ou Feature (champ geometry).",
    )
    input_crs: str = Field(
        "EPSG:4326",
        description="CRS de la géométrie entrante (ex. EPSG:4326 ou EPSG:2154).",
    )
    storage_prefix: str = Field(
        ...,
        description="Dossier dans chaque bucket (ex. code INSEE), sans slash.",
    )
    include_lidar: bool = True
    include_mnt: bool = True
    dry_run: bool = Field(
        False,
        description="Si true : liste les dalles et URLs sans télécharger ni téléverser.",
    )
    download_timeout: int = Field(600, ge=30, le=7200)
    background: bool = Field(
        True,
        description="Si true : exécution en tâche de fond (recommandé sur le cloud).",
    )
    size_lidar_mb: float | None = None
    size_mnt_mb: float | None = None


def _execute_ingest(payload: MntLidarIngestRequest) -> dict[str, Any]:
    geom = geojson_to_geometry_2154(payload.geometry, payload.input_crs)
    return run_pipeline_geometry_to_supabase(
        geom,
        payload.storage_prefix,
        upload_lidar=payload.include_lidar,
        upload_mnt=payload.include_mnt,
        dry_run=payload.dry_run,
        download_timeout=payload.download_timeout,
        size_lidar_mb=payload.size_lidar_mb,
        size_mnt_mb=payload.size_mnt_mb,
    )


def _run_job(job_id: str, payload_dict: dict[str, Any]) -> None:
    payload = MntLidarIngestRequest(**payload_dict)
    try:
        JOBS[job_id]["status"] = "running"
        JOBS[job_id]["started_at"] = datetime.now(timezone.utc).isoformat()
        result = _execute_ingest(payload)
        JOBS[job_id]["status"] = "completed"
        JOBS[job_id]["result"] = result
        JOBS[job_id]["finished_at"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        logger.exception("Job MNT/LiDAR %s échoué", job_id)
        JOBS[job_id]["status"] = "failed"
        JOBS[job_id]["error"] = str(e)
        JOBS[job_id]["finished_at"] = datetime.now(timezone.utc).isoformat()


@router.post("/mnt-lidar/ingest")
def post_mnt_lidar_ingest(
    body: MntLidarIngestRequest,
    background_tasks: BackgroundTasks,
):
    if not body.include_lidar and not body.include_mnt:
        raise HTTPException(
            status_code=400,
            detail="include_lidar et include_mnt ne peuvent pas être tous les deux faux.",
        )
    if body.background:
        job_id = str(uuid.uuid4())
        JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        payload_dict = (
            body.model_dump() if hasattr(body, "model_dump") else body.dict()
        )
        background_tasks.add_task(_run_job, job_id, payload_dict)
        return JSONResponse(
            status_code=202,
            content={
                "job_id": job_id,
                "status": "queued",
                "poll_path": f"/admin/mnt-lidar/jobs/{job_id}",
            },
        )
    return _execute_ingest(body)


@router.get("/mnt-lidar/jobs/{job_id}")
def get_mnt_lidar_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job_id inconnu")
    return job
