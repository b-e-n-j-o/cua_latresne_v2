# -*- coding: utf-8 -*-
"""
Routes FastAPI du viewer CUA (DOCX hébergé sur Supabase).

Endpoints publics (chemins inchangés pour le front Kerelia) :
- GET  /cua/html           — DOCX → HTML (mammoth), token base64 { "docx": "…" }
- POST /cua/update         — HTML édité → DOCX (pypandoc) → réupload bucket
- GET  /cua/download/docx  — téléchargement du DOCX

Le client Supabase est injecté au démarrage depuis main (attribut module `supabase`).
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
import base64
import json
from io import BytesIO
import tempfile
import mammoth
import pypandoc

supabase = None

router = APIRouter()

BUCKET_NAME = "visualisation"


def get_docx_path(path: str) -> str:
    """Nettoie le path - retire visualisation/ ou public/visualisation/ si présent"""
    path = path.lstrip("/")
    if path.startswith("public/visualisation/"):
        return path[len("public/visualisation/"):]
    if path.startswith("visualisation/"):
        return path[len("visualisation/"):]
    return path


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
