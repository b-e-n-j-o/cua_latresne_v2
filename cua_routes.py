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
async def download_docx(slug: str):
    try:
        # récupérer l'entrée pipeline correspondante
        res = supabase.table("pipelines").select("output_cua").eq("slug", slug).single().execute()
        path = res.data.get("output_cua")

        if not path:
            raise HTTPException(404, "Fichier DOCX introuvable")

        # télécharger les bytes depuis Supabase
        file_bytes = supabase.storage.from_("cua-artifacts").download(path.split("/cua-artifacts/")[1])

        return Response(
            content=file_bytes,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={
                "Content-Disposition": f'attachment; filename="cua_{slug}.docx"'
            }
        )

    except Exception as e:
        raise HTTPException(500, f"Erreur téléchargement DOCX : {e}")


@router.get("/cua/download/pdf")
async def download_pdf(slug: str):
    try:
        # récupérer l'URL du DOCX
        res = supabase.table("pipelines").select("output_cua").eq("slug", slug).single().execute()
        path = res.data.get("output_cua")

        if not path:
            raise HTTPException(404, "DOCX introuvable")

        # télécharger contenu DOCX
        docx_bytes = supabase.storage.from_("cua-artifacts").download(path.split("/cua-artifacts/")[1])

        with tempfile.NamedTemporaryFile(suffix=".docx") as tmp_in:
            tmp_in.write(docx_bytes)
            tmp_in.flush()

            with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp_out:
                pypandoc.convert_file(tmp_in.name, "pdf", outputfile=tmp_out.name)
                pdf_bytes = tmp_out.read()

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="cua_{slug}.pdf"'}
        )

    except Exception as e:
        raise HTTPException(500, f"Erreur génération PDF : {e}")