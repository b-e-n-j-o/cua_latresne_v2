"""Analyse préliminaire CERFA (PDF) — extraction LLM pour validation UI."""

import uuid
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from services.analyse_cerfa_mistral.GEMINI.orchestrator import analyser_cerfa_complet

router = APIRouter(prefix="/cerfa", tags=["cerfa"])


@router.post("/analyse")
async def analyse_cerfa_endpoint(
    pdf: UploadFile = File(...),
    user_id: str = Form(None),
    user_email: str = Form(None),
):
    """
    Analyse préliminaire d'un CERFA (LLM / Gemini).
    Retourne les informations extraites pour validation UI.
    """
    job_id = str(uuid.uuid4())
    temp_pdf = Path(f"/tmp/cerfa_{job_id}.pdf")

    try:
        with open(temp_pdf, "wb") as f:
            f.write(await pdf.read())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur écriture PDF temporaire: {e}")

    try:
        result = analyser_cerfa_complet(str(temp_pdf))
        return {
            "job_id": job_id,
            **result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if temp_pdf.exists():
            temp_pdf.unlink()
