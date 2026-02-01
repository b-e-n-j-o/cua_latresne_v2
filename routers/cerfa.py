# routers/cerfa.py

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pathlib import Path
import uuid

from services.analyse_cerfa_mistral.GEMINI.orchestrator import (
    analyser_cerfa_complet,
)

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

    # Sauvegarde temporaire du PDF
    try:
        with open(temp_pdf, "wb") as f:
            f.write(await pdf.read())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur écriture PDF temporaire: {e}")

    try:
        # Appel du script métier (synchrone assumé)
        result = analyser_cerfa_complet(str(temp_pdf))

        # Retour direct frontend
        return {
            "job_id": job_id,
            **result
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if temp_pdf.exists():
            temp_pdf.unlink()
