"""
router_ingest_parcelles.py

Ingestion massive Etalab → parcelles.parcelles (sans check de date).
Réutilise ingest_commune / COPY de router_sync_parcelles.

Endpoints :
    POST /admin/parcelles/ingest
    GET  /admin/parcelles/status/{job_id}
    GET  /admin/parcelles/jobs
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from services.ingestion_cadastre.router_sync_parcelles import (
    NOUVELLE_AQUITAINE,
    get_communes_du_dep,
    ingest_commune,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["parcelles-ingest"])

INGEST_JOBS: dict[str, dict] = {}
PAUSE_S = 0.2


class IngestRequest(BaseModel):
    communes: Optional[list[str]] = None
    departements: Optional[list[str]] = None
    nouvelle_aquitaine: bool = False
    dry_run: bool = False


class IngestJobStatus(BaseModel):
    job_id: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    communes_done: int = 0
    communes_total: int = 0
    parcelles_upserted: int = 0
    communes_errors: int = 0
    log: list[str] = []


def resolve_ingest_communes(req: IngestRequest, job: dict) -> list[str]:
    if req.nouvelle_aquitaine:
        deps = NOUVELLE_AQUITAINE
    elif req.departements:
        deps = req.departements
    else:
        deps = []

    communes = list(req.communes or [])
    for dep in deps:
        job["log"].append(f"Résolution dep {dep}...")
        insee_list = get_communes_du_dep(dep)
        job["log"].append(f"  → {len(insee_list)} communes pour dep {dep}")
        communes.extend(insee_list)
    return communes


def run_ingest_job(job_id: str, req: IngestRequest) -> None:
    job = INGEST_JOBS[job_id]
    job["status"] = "running"
    job["started_at"] = datetime.utcnow().isoformat()

    communes = resolve_ingest_communes(req, job)
    if not communes:
        job["status"] = "error"
        job["log"].append("❌ Aucune commune résolue")
        return

    job["communes_total"] = len(communes)
    job["log"].append(f"→ {len(communes)} communes — dry_run={req.dry_run}")
    logger.info(
        "[parcelles_ingest job=%s] démarrage %d communes dry_run=%s",
        job_id, len(communes), req.dry_run,
    )

    for i, insee in enumerate(communes):
        label = f"[{i+1}/{len(communes)}] {insee}"
        job["log"].append(f"{label} — ingestion...")
        t0 = time.perf_counter()
        upserted, statut = ingest_commune(insee, job_id, req.dry_run)
        elapsed = round(time.perf_counter() - t0, 1)
        job["communes_done"] += 1

        if statut == "ok":
            job["parcelles_upserted"] += upserted
            suffix = "(dry run)" if req.dry_run else f"{upserted:,} upsertées"
            job["log"].append(f"{label} ✅ {suffix} en {elapsed}s")
        else:
            job["communes_errors"] += 1
            job["log"].append(f"{label} ❌ {statut} en {elapsed}s")

        time.sleep(PAUSE_S)

    job["status"] = "done"
    job["finished_at"] = datetime.utcnow().isoformat()
    job["log"].append(
        f"✅ Terminé — {job['parcelles_upserted']:,} parcelles, "
        f"{job['communes_errors']} erreurs"
    )


@router.post("/parcelles/ingest")
async def ingest_parcelles(req: IngestRequest, background_tasks: BackgroundTasks):
    if not any([req.communes, req.departements, req.nouvelle_aquitaine]):
        raise HTTPException(400, "Spécifier communes, departements ou nouvelle_aquitaine=true")

    job_id = str(uuid.uuid4())[:8]
    INGEST_JOBS[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "started_at": None,
        "finished_at": None,
        "communes_done": 0,
        "communes_total": 0,
        "parcelles_upserted": 0,
        "communes_errors": 0,
        "log": [],
    }
    background_tasks.add_task(run_ingest_job, job_id, req)
    return {
        "job_id": job_id,
        "message": "Ingestion lancée en background",
        "status_url": f"/admin/parcelles/status/{job_id}",
    }


@router.get("/parcelles/status/{job_id}", response_model=IngestJobStatus)
async def get_ingest_status(job_id: str):
    if job_id not in INGEST_JOBS:
        raise HTTPException(404, f"Job ingest {job_id} introuvable")
    return INGEST_JOBS[job_id]


@router.get("/parcelles/jobs")
async def list_ingest_jobs():
    return [
        {k: v for k, v in job.items() if k != "log"}
        for job in INGEST_JOBS.values()
    ]
