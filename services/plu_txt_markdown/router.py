"""
API batch : fichiers .txt → markdown (pipeline PLU LLM).
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import re
import shutil
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .processor import process_txt_content
from .shared import merge_token_usages

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/plu-txt-markdown", tags=["plu-txt-markdown"])

MAX_FILES = 20
MAX_FILE_BYTES = 12 * 1024 * 1024  # 12 Mo par .txt
SAFE_STEM_RE = re.compile(r"^[a-zA-Z0-9._-]+$")

JOBS: dict[str, dict] = {}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gemini_configured() -> bool:
    return bool((os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip())


def _safe_stem(filename: str) -> str:
    base = Path(filename).name
    if base.lower().endswith(".txt"):
        base = base[:-4]
    stem = re.sub(r"[^\w.\-]", "_", base).strip("._") or "document"
    if not SAFE_STEM_RE.match(stem):
        stem = re.sub(r"[^a-zA-Z0-9._-]", "_", stem) or "document"
    return stem[:120]


class TokenUsage(BaseModel):
    """Compteurs issus de ``usage_metadata`` Gemini."""

    prompt_token_count: int = 0
    candidates_token_count: int = 0
    thoughts_token_count: int = 0
    cached_content_token_count: int = 0
    total_token_count: int = 0


class TokenUsageBreakdown(BaseModel):
    extract: TokenUsage
    judge: TokenUsage | None = None
    total: TokenUsage


class BatchJobStartResponse(BaseModel):
    job_id: str
    status: Literal["queued"]
    total: int


class FileResultSummary(BaseModel):
    zone: str
    status: str
    verdict: str | None = None
    routed_to: str | None = None
    error: str | None = None
    total_cost_usd: float | None = None
    duration_s: float | None = None
    tokens: TokenUsageBreakdown | None = None


class BatchJobStatusResponse(BaseModel):
    job_id: str
    status: Literal["queued", "running", "done", "failed"]
    total: int
    processed: int
    current_file: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None
    results: list[FileResultSummary] = Field(default_factory=list)
    download_ready: bool = False
    tokens_total: TokenUsageBreakdown | None = None


def _token_usage_from_dict(data: dict | None) -> TokenUsage | None:
    if not data:
        return None
    return TokenUsage(**{k: int(data.get(k, 0) or 0) for k in TokenUsage.model_fields})


def _breakdown_from_result(tokens: dict | None) -> TokenUsageBreakdown | None:
    if not tokens:
        return None
    extract = _token_usage_from_dict(tokens.get("extract")) or TokenUsage()
    judge_raw = tokens.get("judge")
    judge = _token_usage_from_dict(judge_raw) if judge_raw else None
    total = _token_usage_from_dict(tokens.get("total")) or TokenUsage()
    return TokenUsageBreakdown(extract=extract, judge=judge, total=total)


def _aggregate_job_tokens(results: list[dict]) -> TokenUsageBreakdown:
    extracts: list[dict] = []
    judges: list[dict] = []
    totals: list[dict] = []
    for r in results:
        t = r.get("tokens")
        if not t:
            continue
        if t.get("extract"):
            extracts.append(t["extract"])
        if t.get("judge"):
            judges.append(t["judge"])
        if t.get("total"):
            totals.append(t["total"])
    return TokenUsageBreakdown(
        extract=_token_usage_from_dict(merge_token_usages(*extracts)) or TokenUsage(),
        judge=_token_usage_from_dict(merge_token_usages(*judges)) if judges else None,
        total=_token_usage_from_dict(merge_token_usages(*totals)) or TokenUsage(),
    )


def _run_batch_job(job_id: str, *, skip_judge: bool) -> None:
    job = JOBS[job_id]
    work_dir = Path(job["work_dir"])
    files: list[tuple[str, Path]] = job["input_files"]

    job["status"] = "running"
    job["started_at"] = job.get("started_at") or _utc_now_iso()

    try:
        for stem, txt_path in files:
            job["current_file"] = stem
            raw = txt_path.read_text(encoding="utf-8")
            out_sub = work_dir / "outputs" / stem
            result = process_txt_content(stem, raw, out_sub, skip_judge=skip_judge)
            job["results"].append(result)
            job["processed"] += 1
            job["tokens_total"] = _aggregate_job_tokens(job["results"]).model_dump()
            logger.info(
                "Job %s %d/%d — %s → %s",
                job_id,
                job["processed"],
                job["total"],
                stem,
                result.get("status"),
            )
        job["status"] = "done"
        job["download_ready"] = _has_any_md(work_dir)
    except Exception as e:
        logger.exception("Job batch markdown %s en erreur", job_id)
        job["status"] = "failed"
        job["error"] = str(e)
    finally:
        job["current_file"] = None
        job["finished_at"] = _utc_now_iso()


def _has_any_md(work_dir: Path) -> bool:
    return len(_collect_md_paths(work_dir)) > 0


def _collect_md_paths(work_dir: Path) -> list[tuple[str, Path]]:
    """Chemins relatifs dans le zip → fichiers .md (validated/ et review_needed/)."""
    items: list[tuple[str, Path]] = []
    outputs_root = work_dir / "outputs"
    if not outputs_root.is_dir():
        return items
    for zone_dir in sorted(outputs_root.iterdir()):
        if not zone_dir.is_dir():
            continue
        for bucket in ("validated", "review_needed"):
            bucket_dir = zone_dir / bucket
            if not bucket_dir.is_dir():
                continue
            for md in sorted(bucket_dir.glob("*.md")):
                arcname = f"{zone_dir.name}/{bucket}/{md.name}"
                items.append((arcname, md))
    return items


def _collect_audit_paths(work_dir: Path) -> list[tuple[str, Path]]:
    items: list[tuple[str, Path]] = []
    for arcname, md_path in _collect_md_paths(work_dir):
        audit = md_path.with_suffix(".audit.json")
        if audit.is_file():
            items.append((arcname.replace(".md", ".audit.json"), audit))
    return items


def _build_zip_bytes(work_dir: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for arcname, path in _collect_md_paths(work_dir):
            zf.write(path, arcname=arcname)
        for arcname, path in _collect_audit_paths(work_dir):
            zf.write(path, arcname=arcname)
    buf.seek(0)
    return buf.getvalue()


@router.post("/batch/jobs", response_model=BatchJobStartResponse)
async def start_batch_job(
    background_tasks: BackgroundTasks,
    files: list[UploadFile] = File(..., description="Fichiers .txt à convertir"),
    skip_judge: bool = False,
):
    if not _gemini_configured():
        raise HTTPException(
            status_code=503,
            detail="GEMINI_API_KEY (ou GOOGLE_API_KEY) non configurée sur le serveur",
        )

    if not files:
        raise HTTPException(status_code=400, detail="Aucun fichier fourni")
    if len(files) > MAX_FILES:
        raise HTTPException(status_code=400, detail=f"Trop de fichiers (max {MAX_FILES})")

    job_id = uuid.uuid4().hex
    work_dir = Path(f"/tmp/plu_txt_md_{job_id}")
    inputs_dir = work_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)

    input_files: list[tuple[str, Path]] = []
    seen_stems: set[str] = set()

    for upload in files:
        name = upload.filename or "document.txt"
        if not name.lower().endswith(".txt"):
            raise HTTPException(status_code=400, detail=f"Fichier non .txt : {name}")

        data = await upload.read()
        if len(data) > MAX_FILE_BYTES:
            raise HTTPException(status_code=400, detail=f"Fichier trop volumineux : {name}")
        if not data.strip():
            raise HTTPException(status_code=400, detail=f"Fichier vide : {name}")

        stem = _safe_stem(name)
        if stem in seen_stems:
            stem = f"{stem}_{len(seen_stems)}"
        seen_stems.add(stem)

        dest = inputs_dir / f"{stem}.txt"
        dest.write_bytes(data)
        input_files.append((stem, dest))

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
        "skip_judge": skip_judge,
        "download_ready": False,
        "tokens_total": None,
    }

    background_tasks.add_task(_run_batch_job, job_id, skip_judge=skip_judge)
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
        started_at=job.get("started_at"),
        finished_at=job.get("finished_at"),
        error=job.get("error"),
        results=results,
        download_ready=bool(job.get("download_ready")),
        tokens_total=tokens_total,
    )


@router.get("/batch/jobs/{job_id}/download")
async def download_batch_zip(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"job_id inconnu : {job_id}")
    if job["status"] not in ("done", "failed"):
        raise HTTPException(status_code=409, detail="Job encore en cours")
    if not job.get("download_ready"):
        raise HTTPException(status_code=404, detail="Aucun markdown produit pour ce job")

    work_dir = Path(job["work_dir"])
    zip_bytes = await asyncio.to_thread(_build_zip_bytes, work_dir)

    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="plu-markdown-{job_id[:8]}.zip"'},
    )


@router.delete("/batch/jobs/{job_id}")
async def delete_batch_job(job_id: str):
    job = JOBS.pop(job_id, None)
    if not job:
        raise HTTPException(status_code=404, detail=f"job_id inconnu : {job_id}")
    work_dir = job.get("work_dir")
    if work_dir:
        shutil.rmtree(work_dir, ignore_errors=True)
    return {"ok": True, "job_id": job_id}
