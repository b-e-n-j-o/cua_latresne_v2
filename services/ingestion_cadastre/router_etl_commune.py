"""
router_etl_commune.py
---------------------
ETL quotidien commune sur Render (parcelles → BAN → enrichissement).

Montage : app.include_router(etl_commune_router, prefix="/admin")

Endpoints :
    POST /admin/etl/commune              → lance le job en background
    GET  /admin/etl/commune/status/{id}
    GET  /admin/etl/commune/jobs

Déclenchement cron Render (recommandé) :
    Service type « Cron Job » qui exécute scripts/render_cron_etl_latresne.sh
    ou curl POST vers ce endpoint avec x-internal-token.

Exemple :
    curl -X POST https://api.kerelia.fr/admin/etl/commune \\
      -H "Content-Type: application/json" \\
      -H "x-internal-token: $INTERNAL_TOKEN" \\
      -d '{"schema":"latresne","insee":"33234","parcelles_mode":"etalab"}'
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from services.ingestion_cadastre.etl_pipeline import (
    DEFAULT_BACKEND_URL,
    EtlConfig,
    execute_etl,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["admin-etl-commune"])

ETL_JOBS: dict[str, dict] = {}


class EtlCommuneRequest(BaseModel):
    schema_name: str = Field(..., alias="schema", description="Schéma PG, ex: latresne")
    insee: str = Field(..., min_length=5, max_length=5)
    parcelles_mode: Literal["api", "etalab", "schema-only", "skip"] = "etalab"
    backend_url: Optional[str] = None
    force_parcelles: bool = False
    skip_ban: bool = False
    skip_enrich: bool = False
    dry_run: bool = False
    no_slack: bool = False

    model_config = {"populate_by_name": True}


class EtlJobStatus(BaseModel):
    job_id: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    schema: Optional[str] = None
    insee: Optional[str] = None
    parcelles_mode: Optional[str] = None
    elapsed_s: Optional[float] = None
    error: Optional[str] = None
    log: list[str]


def _run_etl_job(job_id: str, req: EtlCommuneRequest) -> None:
    job = ETL_JOBS[job_id]
    job["status"] = "running"
    job["started_at"] = datetime.now(timezone.utc).isoformat()
    job["log"].append(f"Démarrage ETL {req.schema_name} / {req.insee}")

    try:
        cfg = EtlConfig(
            schema=req.schema_name,
            insee=req.insee,
            parcelles_mode=req.parcelles_mode,
            backend_url=req.backend_url or DEFAULT_BACKEND_URL,
            force_parcelles=req.force_parcelles,
            skip_ban=req.skip_ban,
            skip_enrich=req.skip_enrich,
            dry_run=req.dry_run,
            no_slack=req.no_slack,
        )
        result = execute_etl(cfg)
        job["status"] = "done"
        job["elapsed_s"] = result.get("elapsed_s")
        job["log"].append(f"✅ Terminé en {result.get('elapsed_s')} s")
        logger.info("[etl_commune job=%s] terminé %s", job_id, result)
    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)
        job["log"].append(f"❌ {e}")
        logger.exception("[etl_commune job=%s] erreur", job_id)
    finally:
        job["finished_at"] = datetime.now(timezone.utc).isoformat()


@router.post("/etl/commune")
async def start_etl_commune(req: EtlCommuneRequest, background_tasks: BackgroundTasks):
    """
    Lance l'ETL commune en arrière-plan (même logique que run_etl_commune.py).
    Idéal pour un Cron Render qui POST cet endpoint.
    """
    job_id = str(uuid.uuid4())[:8]
    ETL_JOBS[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "started_at": None,
        "finished_at": None,
        "schema": req.schema_name,
        "insee": req.insee,
        "parcelles_mode": req.parcelles_mode,
        "elapsed_s": None,
        "error": None,
        "log": [],
    }
    background_tasks.add_task(_run_etl_job, job_id, req)
    logger.info(
        "[etl_commune job=%s] enqueued schema=%s insee=%s mode=%s",
        job_id, req.schema_name, req.insee, req.parcelles_mode,
    )
    return {
        "job_id": job_id,
        "message": "ETL commune lancé en background",
        "status_url": f"/admin/etl/commune/status/{job_id}",
    }


@router.get("/etl/commune/status/{job_id}", response_model=EtlJobStatus)
async def get_etl_status(job_id: str):
    if job_id not in ETL_JOBS:
        raise HTTPException(404, f"Job ETL {job_id} introuvable")
    return ETL_JOBS[job_id]


@router.get("/etl/commune/jobs")
async def list_etl_jobs():
    return [
        {k: v for k, v in job.items() if k != "log"}
        for job in ETL_JOBS.values()
    ]
