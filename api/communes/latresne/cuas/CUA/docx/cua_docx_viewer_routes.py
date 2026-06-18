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
import os
import time
from io import BytesIO
from pathlib import Path
import tempfile
from datetime import datetime, timezone
import httpx
import mammoth
import pypandoc

supabase = None

router = APIRouter()

BUCKET_NAME = "visualisation"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _html_to_docx_bytes(html: str) -> bytes:
    """Convertit le HTML édité en DOCX (fichier temporaire fermé avant lecture)."""
    fd, tmp_path = tempfile.mkstemp(suffix=".docx")
    os.close(fd)
    try:
        pypandoc.convert_text(
            html,
            "docx",
            format="html",
            outputfile=tmp_path,
            extra_args=["--standalone"],
        )
        return Path(tmp_path).read_bytes()
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _storage_service_key() -> str:
    return (os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()


def _download_storage_fresh(bucket: str, path: str) -> bytes:
    """
    Télécharge un objet Storage en contournant le cache CDN Supabase.

    storage.download() et GET /object/... sans paramètre peuvent renvoyer une version
    périmée juste après un upsert ; ?download=1 force la version courante.
    """
    base_url = (getattr(supabase, "supabase_url", None) or os.getenv("SUPABASE_URL", "")).rstrip("/")
    key = _storage_service_key()
    if not base_url or not key:
        raise RuntimeError("SUPABASE_URL et SERVICE_KEY requis pour lire le DOCX")

    clean_path = path.lstrip("/")
    url = f"{base_url}/storage/v1/object/{bucket}/{clean_path}?download=1&t={int(time.time() * 1000)}"
    headers = {
        "Authorization": f"Bearer {key}",
        "apikey": key,
        "Cache-Control": "no-cache, no-store",
    }
    resp = httpx.get(url, headers=headers, timeout=60.0)
    resp.raise_for_status()
    return resp.content


def _upload_storage_docx(bucket: str, path: str, file_bytes: bytes) -> None:
    """Réécrit le DOCX (upsert) et vérifie la taille côté Storage."""
    upload_res = supabase.storage.from_(bucket).upload(
        path,
        file_bytes,
        {
            "content-type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "upsert": "true",
            "cache-control": "no-cache, no-store, must-revalidate",
        },
    )
    if getattr(upload_res, "error", None):
        raise RuntimeError(str(upload_res.error))

    fresh = _download_storage_fresh(bucket, path)
    if len(fresh) != len(file_bytes):
        raise RuntimeError(
            f"Upload non reflété en Storage (envoyé={len(file_bytes)}o, lu={len(fresh)}o)"
        )

def _touch_saved_metadata(
    *,
    decoded: dict,
    bucket: str,
    path: str,
    saved_at: str,
    size_bytes: int,
) -> None:
    """Met à jour project_files et pipelines après sauvegarde."""
    public_url = f"{supabase.supabase_url}/storage/v1/object/public/{bucket}/{path}"
    file_id = decoded.get("file_id")
    slug = decoded.get("slug")

    if file_id:
        try:
            (
                supabase.schema("latresne")
                .table("project_files")
                .update(
                    {
                        "public_url": public_url,
                        "updated_at": saved_at,
                        "size_bytes": size_bytes,
                    }
                )
                .eq("id", file_id)
                .execute()
            )
        except Exception:
            pass

    if slug:
        try:
            from services.auth.pipelines_query import pipelines_schema

            schema = pipelines_schema()
            (
                supabase.schema(schema)
                .table("pipelines")
                .update(
                    {
                        "output_cua": public_url,
                        "updated_at": saved_at,
                    }
                )
                .eq("slug", slug)
                .execute()
            )
        except Exception:
            pass


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

        res = _download_storage_fresh(bucket, path)
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

        saved_at = _utc_now_iso()
        file_bytes = _html_to_docx_bytes(req.html)
        _upload_storage_docx(bucket, path, file_bytes)

        _touch_saved_metadata(
            decoded=decoded,
            bucket=bucket,
            path=path,
            saved_at=saved_at,
            size_bytes=len(file_bytes),
        )

        return {
            "status": "success",
            "path": path,
            "saved_at": saved_at,
            "size_bytes": len(file_bytes),
        }

    except HTTPException:
        raise
    except Exception as e:
        print("⚠️ DEBUG CUA UPDATE ERROR:", repr(e))
        raise HTTPException(500, f"Erreur mise à jour : {e}") from e


@router.get("/cua/download/docx")
async def download_docx(t: str):
    try:
        decoded = decode_token(t)
        bucket, path = resolve_bucket_and_path(decoded)
        if not path:
            raise HTTPException(400, "Token invalide")

        file_bytes = _download_storage_fresh(bucket, path)
        if not file_bytes:
            raise HTTPException(404, "Fichier DOCX introuvable")

        return Response(
            content=file_bytes,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={
                "Content-Disposition": 'attachment; filename="CUA_unite_fonciere.docx"',
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Erreur téléchargement DOCX : {e}") from e
