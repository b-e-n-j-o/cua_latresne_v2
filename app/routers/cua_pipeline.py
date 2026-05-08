"""
Pipeline CUA depuis parcelles uniquement : lancement async + polling (/status, /results).

L'analyse d'un PDF CERFA (pré-traitement) est exposée séparément via POST /cerfa/analyse.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.communes.latresne.cuas.CERFA_ANALYSE.auth_utils import get_user_insee_list
from app.pipeline_jobs import run_pipeline_from_parcelles_async
from app.state import JOBS

router = APIRouter(tags=["cua-pipeline"])


class ParcelleRequest(BaseModel):
    parcelles: List[Dict[str, str]]
    code_insee: str
    commune_nom: Optional[str] = None
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    demandeur: Optional[Dict[str, Any]] = None


class ParcelleWithCerfaDataRequest(BaseModel):
    parcelles: List[Dict[str, str]]
    code_insee: str
    commune_nom: Optional[str] = None
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    cerfa_data: Dict[str, Any]


@router.post("/analyze-parcelles")
async def analyze_parcelles(req: ParcelleRequest):
    """Lance le pipeline complet depuis une liste de parcelles (sans CERFA)."""
    print("Appel à analyze-parcelles")

    if not req.parcelles or len(req.parcelles) == 0:
        raise HTTPException(status_code=400, detail="Liste de parcelles vide")

    if len(req.parcelles) > 20:
        raise HTTPException(status_code=400, detail="Trop de parcelles (max 20)")

    for p in req.parcelles:
        if not isinstance(p, dict) or "section" not in p or "numero" not in p:
            raise HTTPException(
                status_code=400,
                detail="Format de parcelle invalide. Attendu: {'section': 'AC', 'numero': '0242'}",
            )

    job_id = str(uuid.uuid4())

    JOBS[job_id] = {
        "status": "queued",
        "created_at": datetime.now().isoformat(),
        "filename": f"{len(req.parcelles)} parcelle(s)",
        "user_id": req.user_id,
        "user_email": req.user_email,
    }

    user_insee_list = get_user_insee_list(req.user_id) if req.user_id else []
    print(f"👤 Droits utilisateur {req.user_email}: {user_insee_list or 'toutes communes'}")

    if user_insee_list and req.code_insee and req.code_insee not in user_insee_list:
        raise HTTPException(
            status_code=403,
            detail=(
                "Accès refusé : vous n'êtes autorisé qu'à analyser "
                f"les communes suivantes : {', '.join(user_insee_list)}"
            ),
        )

    env = os.environ.copy()
    if req.user_id:
        env["USER_ID"] = req.user_id
    if req.user_email:
        env["USER_EMAIL"] = req.user_email

    out_dir = f"/tmp/out_pipeline_{job_id}"
    JOBS[job_id]["out_dir"] = out_dir

    asyncio.create_task(
        run_pipeline_from_parcelles_async(
            job_id,
            req.parcelles,
            req.code_insee,
            req.commune_nom,
            env,
            out_dir=out_dir,
            demandeur=req.demandeur,
        ),
    )

    return {"success": True, "job_id": job_id}


@router.post("/analyze-parcelles-with-json-data")
async def analyze_parcelles_with_json_data(req: ParcelleWithCerfaDataRequest):
    """
    Parcelles + données CERFA complètes (front) pour le header CUA
    sans reconstruire un CERFA minimal côté backend.
    """
    print("Appel à analyze-parcelles-with-json-data")

    if not req.parcelles or len(req.parcelles) == 0:
        raise HTTPException(status_code=400, detail="Liste de parcelles vide")
    if len(req.parcelles) > 20:
        raise HTTPException(status_code=400, detail="Trop de parcelles (max 20)")
    for p in req.parcelles:
        if not isinstance(p, dict) or "section" not in p or "numero" not in p:
            raise HTTPException(
                status_code=400,
                detail="Format de parcelle invalide. Attendu: {'section': 'AC', 'numero': '0242'}",
            )

    job_id = str(uuid.uuid4())

    JOBS[job_id] = {
        "status": "queued",
        "created_at": datetime.now().isoformat(),
        "filename": f"{len(req.parcelles)} parcelle(s)",
        "user_id": req.user_id,
        "user_email": req.user_email,
    }

    user_insee_list = get_user_insee_list(req.user_id) if req.user_id else []
    print(f"👤 Droits utilisateur {req.user_email}: {user_insee_list or 'toutes communes'}")

    if user_insee_list and req.code_insee and req.code_insee not in user_insee_list:
        raise HTTPException(
            status_code=403,
            detail=(
                "Accès refusé : vous n'êtes autorisé qu'à analyser "
                f"les communes suivantes : {', '.join(user_insee_list)}"
            ),
        )

    env = os.environ.copy()
    if req.user_id:
        env["USER_ID"] = req.user_id
    if req.user_email:
        env["USER_EMAIL"] = req.user_email
    env["SKIP_3D"] = "1"

    out_dir = f"/tmp/out_pipeline_{job_id}"
    JOBS[job_id]["out_dir"] = out_dir

    cerfa_dir = Path(out_dir)
    cerfa_dir.mkdir(parents=True, exist_ok=True)

    cerfa_payload = req.cerfa_data or {}
    print(
        "[CUA] Backend /analyze-parcelles-with-json-data cerfa_data keys:",
        list(cerfa_payload.keys()),
    )

    data: Dict[str, Any] = cerfa_payload

    cerfa_json: Dict[str, Any] = {
        "meta": {
            "source": "frontend_cerfa",
            "generated_at": datetime.now().isoformat(),
            "commune_insee": data.get("commune_insee"),
            "commune_nom": data.get("commune_nom"),
        },
        "data": data,
        "errors": [],
    }

    cerfa_out = cerfa_dir / "cerfa_result.json"
    cerfa_out.write_text(json.dumps(cerfa_json, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"✅ cerfa_result.json (depuis cerfa_data front) écrit dans {cerfa_out}")

    asyncio.create_task(
        run_pipeline_from_parcelles_async(
            job_id,
            req.parcelles,
            req.code_insee,
            req.commune_nom,
            env,
            out_dir=out_dir,
            demandeur=data.get("demandeur") or {},
        ),
    )

    return {"success": True, "job_id": job_id}


@router.get("/status/{job_id}")
async def get_status(job_id: str):
    """Retourne l'état d'un job et ses résultats éventuels."""
    job = JOBS.get(job_id)
    print(
        f"🟣 DEBUG /status/{job_id} → job =",
        json.dumps(job, indent=2, default=str) if job else None,
    )
    if not job:
        return {"success": False, "error": "Job introuvable"}

    out_dir = job.get("out_dir")
    if out_dir and "result_enhanced" not in job:
        result_file = Path(out_dir) / "pipeline_result.json"
        sub_result_file = Path(out_dir) / "sub_orchestrator_result.json"

        if result_file.exists():
            try:
                job["result"] = json.loads(result_file.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"⚠️ Erreur lecture pipeline_result.json : {e}")

        if sub_result_file.exists():
            try:
                sub_result = json.loads(sub_result_file.read_text(encoding="utf-8"))
                job["result_enhanced"] = sub_result
                slug = sub_result.get("slug")
                if slug:
                    job["slug"] = slug
            except Exception as e:
                print(f"⚠️ Erreur lecture sub_orchestrator_result.json : {e}")

    if "slug" not in job:
        try:
            slug = job.get("result_enhanced", {}).get("slug")
            if slug:
                job["slug"] = slug
        except Exception:
            pass

    return job


@router.get("/results")
async def list_results(limit: int = 10):
    """Les N derniers jobs terminés (success, error ou timeout)."""
    finished_jobs = [
        {"id": job_id, **data}
        for job_id, data in JOBS.items()
        if data.get("status") in {"success", "error", "timeout"}
    ]

    finished_jobs.sort(key=lambda j: j.get("end_time", ""), reverse=True)

    return {
        "success": True,
        "count": len(finished_jobs),
        "results": finished_jobs[:limit],
    }
