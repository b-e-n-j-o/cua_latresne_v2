# cua_routes.py
# ============================================================
# Routes dédiées au CUA : visualisation HTML + édition DOCX
# ============================================================

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import base64
import json
from io import BytesIO
import tempfile
import mammoth
import pypandoc

import subprocess
import uuid
from fastapi.responses import FileResponse

# Le client Supabase est injecté depuis main.py
supabase = None

router = APIRouter()


# ============================================================
# Utilitaire : détermine bucket + object_path
# ============================================================

def resolve_bucket_and_path(path: str):
    """
    Décode tous les formats possibles :
      - visualisation/...
      - cua-artifacts/...
      - public/visualisation/...
      - public/cua-artifacts/...
    """

    # On nettoie d'abord
    path = path.lstrip("/")

    # Cas 1 : `visualisation/...`
    if path.startswith("visualisation/"):
        return "visualisation", path[len("visualisation/"):]
    
    # Cas 2 : `public/visualisation/...`
    if path.startswith("public/visualisation/"):
        return "visualisation", path[len("public/visualisation/"):]
    
    # Cas 3 : `cua-artifacts/...`
    if path.startswith("cua-artifacts/"):
        return "cua-artifacts", path[len("cua-artifacts/"):]
    
    # Cas 4 : `public/cua-artifacts/...`
    if path.startswith("public/cua-artifacts/"):
        return "cua-artifacts", path[len("public/cua-artifacts/"):]
    
    raise HTTPException(400, f"Chemin DOCX non supporté : {path}")


# ============================================================
# 📄 Route : DOCX → HTML
# ============================================================

@router.get("/cua/html")
async def cua_html(t: str):
    try:
        decoded = json.loads(base64.b64decode(t).decode("utf-8"))
        path = decoded.get("docx")

        if not path:
            raise HTTPException(400, "Token invalide : aucun chemin DOCX")

        bucket, object_path = resolve_bucket_and_path(path)

        # Téléchargement depuis le bon bucket
        res = supabase.storage.from_(bucket).download(object_path)
        if not res:
            raise HTTPException(404, f"Fichier introuvable dans bucket {bucket}")

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
        path = decoded.get("docx")

        if not path:
            raise HTTPException(400, "Token invalide : pas de chemin DOCX")

        bucket, object_path = resolve_bucket_and_path(path)

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
        supabase.storage.from_(bucket).upload(
            object_path,
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
        # Décoder le token comme pour le PDF
        decoded = json.loads(base64.b64decode(t).decode("utf-8"))
        path = decoded.get("docx")

        if not path:
            raise HTTPException(400, "Token invalide")

        # Télécharger depuis le bucket 'visualisation'
        file_bytes = supabase.storage.from_("visualisation").download(path)
        
        if not file_bytes:
            raise HTTPException(404, "Fichier DOCX introuvable")

        return Response(
            content=file_bytes,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={
                "Content-Disposition": f'attachment; filename="CUA.docx"'
            }
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
        path = decoded.get("docx")

        if not path:
            raise HTTPException(400, "Token invalide")

        # Télécharger le DOCX depuis Supabase
        res = supabase.storage.from_("visualisation").download(path)
        if not res:
            raise HTTPException(404, "DOCX introuvable")

        # Sauvegarde temporaire
        tmp_docx = tempfile.NamedTemporaryFile(suffix=".docx", delete=False)
        tmp_docx.write(res)
        tmp_docx.close()

        tmp_pdf_path = tmp_docx.name.replace(".docx", ".pdf")

        # Conversion via libreoffice
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
