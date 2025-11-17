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



# Le client Supabase sera injecté depuis main.py
supabase = None

router = APIRouter()


# ============================================================
# 📄 Route 1 : DOCX → HTML (visualisation)
# ============================================================

@router.get("/cua/html")
async def cua_html(t: str):
    try:
        decoded = json.loads(base64.b64decode(t).decode("utf-8"))
        path = decoded.get("docx")

        if not path:
            raise HTTPException(400, "Token invalide : aucun chemin DOCX")

        # Téléchargement dans le bucket ‘visualisation’
        res = supabase.storage.from_("visualisation").download(path)
        if not res:
            raise HTTPException(404, "Fichier introuvable dans Supabase")

        docx_bytes = BytesIO(res)
        html = mammoth.convert_to_html(docx_bytes).value

        return JSONResponse({"html": html})

    except Exception as e:
        raise HTTPException(500, f"Erreur conversion DOCX -> HTML : {e}")


# ============================================================
# 📄 Route 2 : HTML édité → DOCX (sauvegarde)
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

        # --- HTML → DOCX via pypandoc ---
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

        # --- Upload en overwrite ---
        supabase.storage.from_("visualisation").upload(
            path,
            file_bytes,
            {"upsert": 'true'}
        )

        return {"status": "success", "path": path}

    except Exception as e:
        print("⚠️ DEBUG CUA UPDATE ERROR:", repr(e))
        raise HTTPException(500, f"Erreur mise à jour : {e}")
