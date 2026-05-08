"""
router_reglement.py
Endpoints API pour:
- identifier le règlement PLU par INSEE
- vérifier sa taille
- analyser son extractibilité texte
- persister le verdict en base (parcelles.communes)
- lancer un batch asynchrone avec suivi d'avancement
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

import psycopg2
import requests
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from .reglement_qualite import analyser_qualite_reglement

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/identite-fonciere", tags=["Identité Foncière"])

GPU_API = "https://www.geoportail-urbanisme.gouv.fr/api"
WFS_BASE = "https://data.geopf.fr/wfs/ows"
KEYWORDS_OK = ["reglement", "règlement", "regl", "regt"]
KEYWORDS_NOK = ["graphique", "plan", "zonage", "legende", "carte"]
MAX_BATCH_INSEES = 2000

BATCH_JOBS: dict[str, dict] = {}


class ReglementExtractibiliteResponse(BaseModel):
    insee: str
    commune: str | None = None
    gpu_doc_id: str | None = None
    reglement_name: str | None = None
    reglement_url: str | None = None
    reglement_trouve: bool
    reglement_taille_ko: int | None = None
    reglement_taille_depasse_max: bool = False
    reglement_taille_max_mb: int
    extractible: bool
    verdict: str | None = None
    detail: str | None = None
    tokens_estimes: int | None = None
    status_code: int | None = None
    erreur: str | None = None


class ReglementBatchRequest(BaseModel):
    insees: list[str] = Field(..., min_length=1, max_length=MAX_BATCH_INSEES)
    max_pdf_mb: int = Field(default=50, ge=1, le=300)
    skip_if_has_verdict: bool = True
    persist_db: bool = True


class ReglementBatchJobStartResponse(BaseModel):
    job_id: str
    status: str
    total: int


class ReglementBatchJobStatusResponse(BaseModel):
    job_id: str
    status: str
    total: int
    processed: int
    started_at: str
    finished_at: str | None = None
    current_insee: str | None = None
    results: list[ReglementExtractibiliteResponse]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _db_connect():
    direct_url = (os.getenv("SUPABASE_DIRECT_URL") or "").strip()
    if not direct_url:
        raise RuntimeError("SUPABASE_DIRECT_URL manquant")
    return psycopg2.connect(direct_url, sslmode="require")


def _score_reglement_filename(filename: str) -> int:
    name = filename.lower()
    score = 0
    for kw in KEYWORDS_OK:
        if kw in name:
            score += 10
    for kw in KEYWORDS_NOK:
        if kw in name:
            score -= 8
    if name.endswith(".pdf"):
        score += 2
    return score


def _fetch_doc_urba_com_prod(insee: str) -> dict | None:
    resp = requests.get(
        WFS_BASE,
        params={
            "SERVICE": "WFS",
            "VERSION": "2.0.0",
            "REQUEST": "GetFeature",
            "typeNames": "wfs_du:doc_urba_com",
            "outputFormat": "application/json",
            "CQL_FILTER": f"insee='{insee}'",
            "count": "20",
        },
        timeout=20,
    )
    resp.raise_for_status()
    features = resp.json().get("features", [])
    if not features:
        return None
    props = [f.get("properties", {}) for f in features]
    prod = [p for p in props if p.get("gpu_status") == "production"]
    return prod[0] if prod else props[0]


def _find_reglement_name_url(writing_materials: dict) -> tuple[str | None, str | None]:
    scored = []
    for name, url in writing_materials.items():
        scored.append((_score_reglement_filename(name), name, url))
    scored.sort(key=lambda x: x[0], reverse=True)
    if not scored or scored[0][0] <= 0:
        return None, None
    _, best_name, best_url = scored[0]
    return best_name, best_url


def _head_pdf_size_ko(url: str) -> int | None:
    try:
        r = requests.head(url, timeout=12, allow_redirects=True)
        cl = r.headers.get("Content-Length")
        if cl:
            return int(cl) // 1024
        r2 = requests.get(url, stream=True, timeout=12)
        cl2 = r2.headers.get("Content-Length")
        r2.close()
        return int(cl2) // 1024 if cl2 else None
    except Exception:
        return None


def _commune_has_verdict(conn, insee: str) -> bool:
    query = """
        SELECT reglement_verdict
        FROM parcelles.communes
        WHERE code_insee = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(query, (insee,))
        row = cur.fetchone()
    if not row:
        return False
    return row[0] is not None and str(row[0]).strip() != ""


def _persist_result(conn, insee: str, r: ReglementExtractibiliteResponse) -> None:
    query = """
        UPDATE parcelles.communes
        SET
            reglement_verdict = %s,
            reglement_extractible = %s,
            reglement_detail = %s,
            reglement_tokens_estimes = %s,
            reglement_url = %s,
            reglement_nom_fichier = %s,
            reglement_taille_ko = %s,
            reglement_status_code = %s,
            reglement_erreur = %s,
            reglement_analyse_le = now()
        WHERE code_insee = %s
    """
    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                r.verdict,
                r.extractible,
                r.detail,
                r.tokens_estimes,
                r.reglement_url,
                r.reglement_name,
                r.reglement_taille_ko,
                r.status_code,
                r.erreur,
                insee,
            ),
        )


def _analyze_one_insee(insee: str, max_pdf_mb: int) -> ReglementExtractibiliteResponse:
    try:
        props = _fetch_doc_urba_com_prod(insee)
        if not props:
            return ReglementExtractibiliteResponse(
                insee=insee,
                reglement_trouve=False,
                reglement_taille_max_mb=max_pdf_mb,
                extractible=False,
                verdict="ERREUR_ENDPOINT",
                detail=f"Aucun document GPU pour INSEE {insee}",
                status_code=404,
                erreur="Aucun document GPU",
            )

        gpu_doc_id = str(props.get("gpu_doc_id") or "")
        if not gpu_doc_id:
            return ReglementExtractibiliteResponse(
                insee=insee,
                commune=str(props.get("libelle") or insee),
                reglement_trouve=False,
                reglement_taille_max_mb=max_pdf_mb,
                extractible=False,
                verdict="ERREUR_ENDPOINT",
                detail=f"gpu_doc_id absent pour INSEE {insee}",
                status_code=404,
                erreur="gpu_doc_id absent",
            )

        details_resp = requests.get(f"{GPU_API}/document/{gpu_doc_id}/details", timeout=20)
        details_resp.raise_for_status()
        details = details_resp.json()
        writing_materials = details.get("writingMaterials", {}) or {}
        reglement_name, reglement_url = _find_reglement_name_url(writing_materials)

        commune = str(props.get("libelle") or details.get("title") or insee)
        if not reglement_url:
            return ReglementExtractibiliteResponse(
                insee=insee,
                commune=commune,
                gpu_doc_id=gpu_doc_id,
                reglement_name=reglement_name,
                reglement_url=reglement_url,
                reglement_trouve=False,
                reglement_taille_max_mb=max_pdf_mb,
                extractible=False,
                verdict="TROP_COURT",
                detail="Aucun règlement identifié parmi les writingMaterials",
            )

        size_ko = _head_pdf_size_ko(reglement_url)
        too_large = size_ko is not None and size_ko > (max_pdf_mb * 1024)
        if too_large:
            return ReglementExtractibiliteResponse(
                insee=insee,
                commune=commune,
                gpu_doc_id=gpu_doc_id,
                reglement_name=reglement_name,
                reglement_url=reglement_url,
                reglement_trouve=True,
                reglement_taille_ko=size_ko,
                reglement_taille_depasse_max=True,
                reglement_taille_max_mb=max_pdf_mb,
                extractible=False,
                verdict="TROP_VOLUMINEUX",
                detail=f"PDF > {max_pdf_mb} MB (taille={size_ko / 1024:.1f} MB)",
            )

        pdf_resp = requests.get(reglement_url, timeout=90)
        pdf_resp.raise_for_status()
        pdf_bytes = pdf_resp.content
        if pdf_bytes[:4] != b"%PDF":
            return ReglementExtractibiliteResponse(
                insee=insee,
                commune=commune,
                gpu_doc_id=gpu_doc_id,
                reglement_name=reglement_name,
                reglement_url=reglement_url,
                reglement_trouve=True,
                reglement_taille_ko=size_ko,
                reglement_taille_max_mb=max_pdf_mb,
                extractible=False,
                verdict="INVALIDE",
                detail="Fichier téléchargé non reconnu comme PDF",
                status_code=422,
                erreur="Signature PDF invalide",
            )

        q = analyser_qualite_reglement(pdf_bytes)
        return ReglementExtractibiliteResponse(
            insee=insee,
            commune=commune,
            gpu_doc_id=gpu_doc_id,
            reglement_name=reglement_name,
            reglement_url=reglement_url,
            reglement_trouve=True,
            reglement_taille_ko=size_ko or (len(pdf_bytes) // 1024),
            reglement_taille_max_mb=max_pdf_mb,
            extractible=q.utilisable,
            verdict=q.verdict,
            detail=q.detail,
            tokens_estimes=q.tokens_estimes,
        )
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 502
        return ReglementExtractibiliteResponse(
            insee=insee,
            reglement_trouve=False,
            reglement_taille_max_mb=max_pdf_mb,
            extractible=False,
            verdict="ERREUR_ENDPOINT",
            detail=f"Erreur API GPU ({status})",
            status_code=status,
            erreur=str(e),
        )
    except Exception as e:
        logger.error("Erreur analyse INSEE=%s: %s", insee, e, exc_info=True)
        return ReglementExtractibiliteResponse(
            insee=insee,
            reglement_trouve=False,
            reglement_taille_max_mb=max_pdf_mb,
            extractible=False,
            verdict="ERREUR_ENDPOINT",
            detail=f"Erreur analyse: {e}",
            status_code=500,
            erreur=str(e),
        )


@router.post(
    "/urban-documents/reglement-extractibilite/sql/add-columns",
    summary="Ajoute les colonnes SQL de persistance des verdicts",
)
async def add_reglement_columns():
    ddl = """
        ALTER TABLE parcelles.communes
          ADD COLUMN IF NOT EXISTS reglement_verdict text,
          ADD COLUMN IF NOT EXISTS reglement_extractible boolean,
          ADD COLUMN IF NOT EXISTS reglement_detail text,
          ADD COLUMN IF NOT EXISTS reglement_tokens_estimes integer,
          ADD COLUMN IF NOT EXISTS reglement_url text,
          ADD COLUMN IF NOT EXISTS reglement_nom_fichier text,
          ADD COLUMN IF NOT EXISTS reglement_taille_ko integer,
          ADD COLUMN IF NOT EXISTS reglement_analyse_le timestamptz,
          ADD COLUMN IF NOT EXISTS reglement_status_code integer,
          ADD COLUMN IF NOT EXISTS reglement_erreur text
    """
    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur migration colonnes: {e}")


@router.get(
    "/urban-documents/{insee}/reglement-extractibilite",
    response_model=ReglementExtractibiliteResponse,
    summary="Analyser l'extractibilité du règlement PLU pour un INSEE",
)
async def get_reglement_extractibilite(insee: str, max_pdf_mb: int = 50, persist_db: bool = True):
    result = _analyze_one_insee(insee=insee, max_pdf_mb=max_pdf_mb)
    if persist_db:
        try:
            with _db_connect() as conn:
                _persist_result(conn, insee, result)
                conn.commit()
        except Exception as e:
            logger.warning("Persist DB impossible insee=%s: %s", insee, e)
    return result


def _run_batch_job(job_id: str, body: ReglementBatchRequest) -> None:
    job = BATCH_JOBS[job_id]
    job["status"] = "running"
    insees = [str(i).strip() for i in body.insees if str(i).strip()]
    conn = None
    try:
        if body.persist_db:
            conn = _db_connect()
        for insee in insees:
            job["current_insee"] = insee
            if body.skip_if_has_verdict and conn is not None and _commune_has_verdict(conn, insee):
                result = ReglementExtractibiliteResponse(
                    insee=insee,
                    reglement_trouve=False,
                    reglement_taille_max_mb=body.max_pdf_mb,
                    extractible=False,
                    verdict="DEJA_TRAITE",
                    detail="Verdict déjà présent en base",
                )
            else:
                result = _analyze_one_insee(insee=insee, max_pdf_mb=body.max_pdf_mb)
                if conn is not None:
                    _persist_result(conn, insee, result)
                    conn.commit()
            job["results"].append(result)
            job["processed"] += 1
            logger.info(
                "Batch reglement %s: %d/%d - INSEE %s - verdict=%s",
                job_id,
                job["processed"],
                job["total"],
                insee,
                result.verdict,
            )
        job["status"] = "done"
    except Exception as e:
        logger.error("Batch reglement job %s en erreur: %s", job_id, e, exc_info=True)
        job["status"] = "failed"
    finally:
        if conn is not None:
            conn.close()
        job["current_insee"] = None
        job["finished_at"] = _utc_now_iso()


@router.post(
    "/urban-documents/reglement-extractibilite/batch/jobs",
    response_model=ReglementBatchJobStartResponse,
    summary="Lancer un batch asynchrone d'analyse règlement PLU",
)
async def start_batch_job(body: ReglementBatchRequest, background_tasks: BackgroundTasks):
    insees = [str(i).strip() for i in body.insees if str(i).strip()]
    if not insees:
        raise HTTPException(status_code=400, detail="Aucun INSEE fourni")
    job_id = uuid4().hex
    BATCH_JOBS[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "total": len(insees),
        "processed": 0,
        "started_at": _utc_now_iso(),
        "finished_at": None,
        "current_insee": None,
        "results": [],
    }
    background_tasks.add_task(_run_batch_job, job_id, body)
    return ReglementBatchJobStartResponse(job_id=job_id, status="queued", total=len(insees))


@router.get(
    "/urban-documents/reglement-extractibilite/batch/jobs/{job_id}",
    response_model=ReglementBatchJobStatusResponse,
    summary="Consulter l'avancement d'un batch d'analyse règlement PLU",
)
async def get_batch_job(job_id: str):
    job = BATCH_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"job_id inconnu: {job_id}")
    return ReglementBatchJobStatusResponse(
        job_id=job["job_id"],
        status=job["status"],
        total=job["total"],
        processed=job["processed"],
        started_at=job["started_at"],
        finished_at=job["finished_at"],
        current_insee=job["current_insee"],
        results=job["results"],
    )

