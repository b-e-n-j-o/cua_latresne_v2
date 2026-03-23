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
from datetime import datetime
import mammoth
import pypandoc

supabase = None

router = APIRouter()

BUCKET_NAME = "visualisation"


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def decode_token(token: str) -> dict:
    """Decode un token base64 (urlsafe ou standard) et renvoie un dict JSON."""
    raw = token.strip()
    # padding base64
    raw += "=" * (-len(raw) % 4)
    try:
        payload = base64.urlsafe_b64decode(raw).decode("utf-8")
    except Exception:
        payload = base64.b64decode(raw).decode("utf-8")
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise ValueError("Le token ne contient pas un objet JSON")
    return decoded


def get_bucket(decoded: dict) -> str:
    bucket = (decoded.get("bucket") or BUCKET_NAME).strip()
    return bucket or BUCKET_NAME


def get_docx_path(path: str) -> str:
    """Nettoie le path - retire visualisation/ ou public/visualisation/ si présent"""
    if not path:
        return ""
    # Cas URL publique complète Supabase
    marker = "/storage/v1/object/public/"
    if marker in path:
        raw = path.split(marker, 1)[1]
        parts = raw.split("/", 1)
        if len(parts) == 2:
            return parts[1].lstrip("/")

    path = path.lstrip("/")
    if path.startswith("public/visualisation/"):
        return path[len("public/visualisation/"):]
    if path.startswith("visualisation/"):
        return path[len("visualisation/"):]
    if path.startswith("public/project-directories/"):
        return path[len("public/project-directories/"):]
    if path.startswith("project-directories/"):
        return path[len("project-directories/"):]
    return path


def resolve_bucket_and_path(decoded: dict) -> tuple[str, str]:
    docx = (decoded.get("docx") or "").strip()
    bucket = get_bucket(decoded)

    marker = "/storage/v1/object/public/"
    if marker in docx:
        raw = docx.split(marker, 1)[1]
        parts = raw.split("/", 1)
        if len(parts) == 2:
            bucket = parts[0].strip() or bucket
            return bucket, parts[1].lstrip("/")

    return bucket, get_docx_path(docx)


@router.get("/cua/html")
async def cua_html(t: str):
    try:
        decoded = decode_token(t)
        bucket, path = resolve_bucket_and_path(decoded)

        if not path:
            raise HTTPException(400, "Token invalide : aucun chemin DOCX")

        res = supabase.storage.from_(bucket).download(path)
        if not res:
            raise HTTPException(404, f"Fichier introuvable dans bucket {bucket}")

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
        decoded = decode_token(req.token)
        bucket, path = resolve_bucket_and_path(decoded)
        file_id = decoded.get("file_id")

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

        supabase.storage.from_(bucket).upload(
            path,
            file_bytes,
            {
                "upsert": "true",
                "content-type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            }
        )

        # Si le token cible un ProjectFile, on met à jour son URL/date pour refléter l'édition
        if file_id:
            try:
                public_url = f"{supabase.supabase_url}/storage/v1/object/public/{bucket}/{path}"
                (
                    supabase.schema("latresne")
                    .table("project_files")
                    .update(
                        {
                            "public_url": public_url,
                            "updated_at": _utc_now_iso(),
                        }
                    )
                    .eq("id", file_id)
                    .execute()
                )
            except Exception:
                # Ne pas bloquer la sauvegarde DOCX si la maj metadata échoue
                pass

        return {"status": "success", "path": path}

    except Exception as e:
        print("⚠️ DEBUG CUA UPDATE ERROR:", repr(e))
        raise HTTPException(500, f"Erreur mise à jour : {e}")


@router.get("/cua/download/docx")
async def download_docx(t: str):
    try:
        decoded = decode_token(t)
        bucket, path = resolve_bucket_and_path(decoded)
        if not path:
            raise HTTPException(400, "Token invalide")

        file_bytes = supabase.storage.from_(bucket).download(path)
        if not file_bytes:
            raise HTTPException(404, "Fichier DOCX introuvable")

        return Response(
            content=file_bytes,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={"Content-Disposition": 'attachment; filename="CUA.docx"'}
        )
    except Exception as e:
        raise HTTPException(500, f"Erreur téléchargement DOCX : {e}")
