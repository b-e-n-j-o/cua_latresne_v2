"""
API batch : fichiers .md (règlement complet) → laïus résumés (pipeline laius LLM).
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import shutil
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse

from .laius import process_md_content
from .router import (
    BatchJobCancelResponse,
    BatchJobStartResponse,
    BatchJobStatusResponse,
    FileCompareResponse,
    FileResultSummary,
    TokenUsageBreakdown,
    _aggregate_job_tokens,
    _breakdown_from_result,
)
from .shared import ALLOWED_GEMINI_MODELS, DEFAULT_GEMINI_MODEL, validate_gemini_model

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/plu-laius", tags=["plu-laius"])

MAX_FILES = 20
MAX_FILE_BYTES = 12 * 1024 * 1024

JOBS: dict[str, dict] = {}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gemini_configured() -> bool:
    return bool((os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip())


def _normalize_output_basename(filename: str) -> str:
    """Nom de fichier de sortie = basename d'entrée (extension .md)."""
    base = Path(filename).name.strip().replace("\x00", "")
    if not base:
        return "document.md"
    lower = base.lower()
    if lower.endswith(".markdown"):
        base = f"{base[:-9]}.md"
    elif not lower.endswith(".md"):
        base = f"{base}.md"
    return base[:200]


def _label_zone_from_basename(basename: str) -> str:
    """Libellé zone pour le prompt LLM (nom sans extension)."""
    name = basename
    lower = name.lower()
    if lower.endswith(".markdown"):
        name = name[:-9]
    elif lower.endswith(".md"):
        name = name[:-3]
    stem = name.strip() or "document"
    return stem[:120]


def _find_output_md(work_dir: Path, output_basename: str) -> Path | None:
    path = work_dir / "outputs" / output_basename
    return path if path.is_file() else None


def _has_any_md(work_dir: Path) -> bool:
    return len(_collect_md_paths(work_dir)) > 0


def _collect_md_paths(work_dir: Path) -> list[tuple[str, Path]]:
    """ZIP plat : un laïus par fichier, même nom que l'entrée."""
    outputs_root = work_dir / "outputs"
    if not outputs_root.is_dir():
        return []
    return [(p.name, p) for p in sorted(outputs_root.glob("*.md")) if p.is_file()]


def _build_zip_bytes(work_dir: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for arcname, path in _collect_md_paths(work_dir):
            zf.write(path, arcname=arcname)
    buf.seek(0)
    return buf.getvalue()


def _run_batch_job(job_id: str) -> None:
    job = JOBS[job_id]
    work_dir = Path(job["work_dir"])
    model_id = job.get("model", DEFAULT_GEMINI_MODEL)

    job["status"] = "running"
    job["started_at"] = job.get("started_at") or _utc_now_iso()

    cancelled = False
    try:
        for output_basename, label_zone, input_path in job["input_files"]:
            if JOBS.get(job_id, {}).get("cancel_requested"):
                cancelled = True
                break

            job["current_file"] = output_basename
            raw = input_path.read_text(encoding="utf-8")
            result = process_md_content(
                output_basename,
                label_zone,
                raw,
                work_dir / "outputs",
                model=model_id,
            )
            job["results"].append(result)
            job["processed"] += 1
            job["tokens_total"] = _aggregate_job_tokens(job["results"]).model_dump()
            logger.info(
                "Job laius %s %d/%d — %s → %s",
                job_id,
                job["processed"],
                job["total"],
                output_basename,
                result.get("status"),
            )

            if JOBS.get(job_id, {}).get("cancel_requested"):
                cancelled = True
                break

        if cancelled:
            job["status"] = "cancelled"
            job["error"] = "Annulé par l'utilisateur"
            job["download_ready"] = _has_any_md(work_dir)
        else:
            job["status"] = "done"
            job["download_ready"] = _has_any_md(work_dir)
    except Exception as e:
        logger.exception("Job batch laius %s en erreur", job_id)
        job["status"] = "failed"
        job["error"] = str(e)
    finally:
        job["current_file"] = None
        job["finished_at"] = _utc_now_iso()


@router.get("/batch/models")
async def list_gemini_models():
    return {"models": list(ALLOWED_GEMINI_MODELS), "default": DEFAULT_GEMINI_MODEL}


@router.post("/batch/jobs", response_model=BatchJobStartResponse)
async def start_batch_job(
    background_tasks: BackgroundTasks,
    files: list[UploadFile] = File(..., description="Fichiers .md (règlement complet)"),
    model: str = Query(default=DEFAULT_GEMINI_MODEL, description="Modèle Gemini"),
):
    if not _gemini_configured():
        raise HTTPException(
            status_code=503,
            detail="GEMINI_API_KEY (ou GOOGLE_API_KEY) non configurée sur le serveur",
        )

    try:
        model_id = validate_gemini_model(model)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    if not files:
        raise HTTPException(status_code=400, detail="Aucun fichier fourni")
    if len(files) > MAX_FILES:
        raise HTTPException(status_code=400, detail=f"Trop de fichiers (max {MAX_FILES})")

    job_id = uuid.uuid4().hex
    work_dir = Path(f"/tmp/plu_laius_{job_id}")
    inputs_dir = work_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)

    input_files: list[tuple[str, str, Path]] = []
    seen_names: set[str] = set()

    for upload in files:
        name = upload.filename or "document.md"
        lower = name.lower()
        if not (lower.endswith(".md") or lower.endswith(".markdown")):
            raise HTTPException(status_code=400, detail=f"Fichier non Markdown : {name}")

        data = await upload.read()
        if len(data) > MAX_FILE_BYTES:
            raise HTTPException(status_code=400, detail=f"Fichier trop volumineux : {name}")
        if not data.strip():
            raise HTTPException(status_code=400, detail=f"Fichier vide : {name}")

        output_basename = _normalize_output_basename(name)
        if output_basename in seen_names:
            raise HTTPException(
                status_code=400,
                detail=f"Nom de fichier en double : {output_basename}",
            )
        seen_names.add(output_basename)
        label_zone = _label_zone_from_basename(output_basename)

        dest = inputs_dir / output_basename
        dest.write_bytes(data)
        input_files.append((output_basename, label_zone, dest))

    JOBS[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "total": len(input_files),
        "processed": 0,
        "current_file": None,
        "started_at": None,
        "finished_at": None,
        "error": None,
        "results": [],
        "work_dir": str(work_dir),
        "input_files": input_files,
        "model": model_id,
        "download_ready": False,
        "tokens_total": None,
        "cancel_requested": False,
    }

    background_tasks.add_task(_run_batch_job, job_id)
    return BatchJobStartResponse(job_id=job_id, status="queued", total=len(input_files))


@router.get("/batch/jobs/{job_id}", response_model=BatchJobStatusResponse)
async def get_batch_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"job_id inconnu : {job_id}")

    results = [
        FileResultSummary(
            zone=r.get("zone", ""),
            status=r.get("status", ""),
            verdict=r.get("verdict"),
            routed_to=r.get("routed_to"),
            error=r.get("error"),
            total_cost_usd=r.get("total_cost_usd"),
            duration_s=r.get("duration_s"),
            tokens=_breakdown_from_result(r.get("tokens")),
        )
        for r in job.get("results", [])
    ]

    tokens_total = _breakdown_from_result(job.get("tokens_total"))
    if tokens_total is None and results:
        tokens_total = _aggregate_job_tokens(job.get("results", []))

    return BatchJobStatusResponse(
        job_id=job["job_id"],
        status=job["status"],
        total=job["total"],
        processed=job["processed"],
        current_file=job.get("current_file"),
        model=job.get("model"),
        started_at=job.get("started_at"),
        finished_at=job.get("finished_at"),
        error=job.get("error"),
        results=results,
        download_ready=bool(job.get("download_ready")),
        tokens_total=tokens_total,
    )


@router.get("/batch/jobs/{job_id}/files/{zone}/compare", response_model=FileCompareResponse)
async def compare_batch_file(job_id: str, zone: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"job_id inconnu : {job_id}")

    work_dir = Path(job["work_dir"])
    output_basename = Path(zone).name
    if output_basename != zone or ".." in zone:
        raise HTTPException(status_code=400, detail="Nom de fichier invalide")

    input_path = work_dir / "inputs" / output_basename
    if not input_path.is_file():
        raise HTTPException(status_code=404, detail=f"Fichier source introuvable : {output_basename}")

    source_txt = input_path.read_text(encoding="utf-8")
    out_path = _find_output_md(work_dir, output_basename)
    markdown = out_path.read_text(encoding="utf-8") if out_path else None

    result = next((r for r in job.get("results", []) if r.get("zone") == output_basename), None)
    return FileCompareResponse(
        zone=output_basename,
        source_txt=source_txt,
        markdown=markdown,
        status=result.get("status") if result else None,
        routed_to="outputs" if out_path else None,
    )


@router.get("/batch/jobs/{job_id}/download")
async def download_batch_zip(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"job_id inconnu : {job_id}")
    if job["status"] not in ("done", "failed", "cancelled"):
        raise HTTPException(status_code=409, detail="Job encore en cours")
    if not job.get("download_ready"):
        raise HTTPException(status_code=404, detail="Aucun laïus produit pour ce job")

    work_dir = Path(job["work_dir"])
    zip_bytes = await asyncio.to_thread(_build_zip_bytes, work_dir)

    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="plu-laius-{job_id[:8]}.zip"'},
    )


@router.post("/batch/jobs/{job_id}/cancel", response_model=BatchJobCancelResponse)
async def cancel_batch_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"job_id inconnu : {job_id}")

    status = job["status"]
    if status in ("done", "failed", "cancelled"):
        return BatchJobCancelResponse(
            job_id=job_id,
            status=status,
            message="Job déjà terminé",
        )

    job["cancel_requested"] = True
    return BatchJobCancelResponse(
        job_id=job_id,
        status=status,
        message=(
            "Annulation demandée — le traitement s'arrêtera après le fichier en cours "
            "(l'appel Gemini en cours ne peut pas être interrompu)."
        ),
    )


@router.delete("/batch/jobs/{job_id}")
async def delete_batch_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"job_id inconnu : {job_id}")

    if job["status"] in ("queued", "running"):
        job["cancel_requested"] = True

    JOBS.pop(job_id, None)
    work_dir = job.get("work_dir")
    if work_dir:
        shutil.rmtree(work_dir, ignore_errors=True)
    return {"ok": True, "job_id": job_id, "cancel_requested": True}
