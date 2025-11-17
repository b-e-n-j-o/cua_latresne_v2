# cua_routes.py
# ============================================================
# Routes dédiées au CUA : visualisation HTML + édition DOCX
# ============================================================

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, Response, FileResponse
from pydantic import BaseModel
import base64
import json
from io import BytesIO
import tempfile
import mammoth
import pypandoc

import subprocess
import uuid

# Le client Supabase est injecté depuis main.py
supabase = None

router = APIRouter()

BUCKET_NAME = "visualisation"


# ============================================================
# Utilitaire : nettoie le path
# ============================================================

def get_docx_path(path: str) -> str:
    """Nettoie le path - retire visualisation/ ou public/visualisation/ si présent"""
    path = path.lstrip("/")
    if path.startswith("public/visualisation/"):
        return path[len("public/visualisation/"):]
    if path.startswith("visualisation/"):
        return path[len("visualisation/"):]
    return path


# ============================================================
# 📄 Route : DOCX → HTML
# ============================================================

@router.get("/cua/html")
async def cua_html(t: str):
    try:
        decoded = json.loads(base64.b64decode(t).decode("utf-8"))
        path = get_docx_path(decoded.get("docx"))

        if not path:
            raise HTTPException(400, "Token invalide : aucun chemin DOCX")

        res = supabase.storage.from_(BUCKET_NAME).download(path)
        if not res:
            raise HTTPException(404, f"Fichier introuvable dans bucket {BUCKET_NAME}")

        docx_bytes = BytesIO(res)
        html = mammoth.convert_to_html(docx_bytes).value

        return JSONResponse({"html": html})

    except Exception as e:
        raise HTTPException(500, f"Erreur conversion DOCX -> HTML : {e}")


# ============================================================
# 📄 Route : HTML → DOCX
# ============================================================

class UpdateRequest(BaseModel):
    token: str
    html: str


@router.post("/cua/update")
async def cua_update(req: UpdateRequest):
    try:
        decoded = json.loads(base64.b64decode(req.token).decode("utf-8"))
        path = get_docx_path(decoded.get("docx"))

        if not path:
            raise HTTPException(400, "Token invalide : pas de chemin DOCX")

        # --- HTML → DOCX ---
        with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tmp:
            pypandoc.convert_text(
                req.html,
                "docx",
                format="html",
                outputfile=tmp.name,
                extra_args=["--standalone"]
            )

            tmp.seek(0)
            file_bytes = tmp.read()

        # Upload en overwrite
        supabase.storage.from_(BUCKET_NAME).upload(
            path,
            file_bytes,
            {"upsert": "true"}
        )

        return {"status": "success", "path": path}

    except Exception as e:
        print("⚠️ DEBUG CUA UPDATE ERROR:", repr(e))
        raise HTTPException(500, f"Erreur mise à jour : {e}")



@router.get("/cua/download/docx")
async def download_docx(t: str):
    try:
        decoded = json.loads(base64.b64decode(t).decode("utf-8"))
        path = get_docx_path(decoded.get("docx"))
        if not path:
            raise HTTPException(400, "Token invalide")

        file_bytes = supabase.storage.from_(BUCKET_NAME).download(path)
        if not file_bytes:
            raise HTTPException(404, "Fichier DOCX introuvable")

        return Response(
            content=file_bytes,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": 'attachment; filename="CUA.docx"'}
        )
    except Exception as e:
        raise HTTPException(500, f"Erreur téléchargement DOCX : {e}")


@router.get("/cua/download/pdf")
async def cua_pdf(t: str):
    """
    Convertit le DOCX demandé en PDF et renvoie le fichier au navigateur.
    """
    try:
        decoded = json.loads(base64.b64decode(t).decode("utf-8"))
        path = get_docx_path(decoded.get("docx"))
        if not path:
            raise HTTPException(400, "Token invalide")

        res = supabase.storage.from_(BUCKET_NAME).download(path)
        if not res:
            raise HTTPException(404, "DOCX introuvable")

        tmp_docx = tempfile.NamedTemporaryFile(suffix=".docx", delete=False)
        tmp_docx.write(res)
        tmp_docx.close()

        tmp_pdf_path = tmp_docx.name.replace(".docx", ".pdf")

        subprocess.run([
            "libreoffice", "--headless", "--convert-to", "pdf",
            "--outdir", "/tmp", tmp_docx.name
        ], check=True)

        return FileResponse(
            tmp_pdf_path,
            media_type="application/pdf",
            filename="CUA.pdf"
        )
    except Exception as e:
        raise HTTPException(500, f"Erreur PDF : {e}")
