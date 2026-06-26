# -*- coding: utf-8 -*-
"""
project_directory.py — Dossier logique par projet + fichiers associés
---------------------------------------------------------------------
- ProjectDirectory: 1 dossier logique par pipeline (slug)
- ProjectFile: métadonnées des fichiers (stockés dans Supabase Storage)
"""

from __future__ import annotations

import mimetypes
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from app.deps import SUPABASE_URL
from services.auth.current_user import get_current_user_id
from services.history.project_management import assert_can_view_pipeline

supabase = None

PROJECT_BUCKET = "project-directories"

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def ensure_project_directory(
    supabase_client: Any,
    slug: str,
    user_id: Optional[str] = None,
    created_by: Optional[str] = None,
) -> Dict[str, Any]:
    payload = {
        "project_slug": slug,
        "storage_prefix": f"projects/{slug}",
        "user_id": user_id,
        "created_by": created_by,
        "updated_at": _utc_now_iso(),
    }
    res = (
        supabase_client.schema("latresne")
        .table("project_directories")
        .upsert(payload, on_conflict="project_slug")
        .execute()
    )
    rows = res.data or []
    if not rows:
        raise RuntimeError(f"Echec ensure ProjectDirectory pour {slug}")
    return rows[0]


def register_project_file(
    supabase_client: Any,
    slug: str,
    file_kind: str,
    filename: str,
    storage_path: str,
    public_url: str,
    storage_bucket: str = PROJECT_BUCKET,
    mime_type: Optional[str] = None,
    size_bytes: Optional[int] = None,
    uploaded_by: Optional[str] = None,
    source: str = "user_upload",
) -> Dict[str, Any]:
    directory = ensure_project_directory(supabase_client, slug, user_id=uploaded_by, created_by=uploaded_by)
    payload = {
        "project_directory_id": directory["id"],
        "project_slug": slug,
        "file_kind": file_kind,
        "filename": filename,
        "mime_type": mime_type,
        "size_bytes": size_bytes,
        "storage_bucket": storage_bucket,
        "storage_path": storage_path,
        "public_url": public_url,
        "source": source,
        "uploaded_by": uploaded_by,
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
    }
    res = (
        supabase_client.schema("latresne")
        .table("project_files")
        .insert(payload)
        .execute()
    )
    rows = res.data or []
    if not rows:
        raise RuntimeError(f"Echec insertion ProjectFile pour {slug}")
    return rows[0]


@router.post("/{slug}/directory/ensure")
def api_ensure_project_directory(slug: str, user_id: Optional[str] = None):
    try:
        row = ensure_project_directory(supabase, slug=slug, user_id=user_id, created_by=user_id)
        return {"success": True, "directory": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{slug}/directory")
def get_project_directory(slug: str):
    try:
        res = (
            supabase.schema("latresne")
            .table("project_directories")
            .select("*")
            .eq("project_slug", slug)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            return {"success": False, "error": "Directory introuvable"}
        return {"success": True, "directory": rows[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{slug}/files")
def list_project_files(slug: str, user_id: str = Depends(get_current_user_id)):
    try:
        assert_can_view_pipeline(slug, user_id)
        res = (
            supabase.schema("latresne")
            .table("project_files")
            .select("*")
            .eq("project_slug", slug)
            .order("created_at", desc=True)
            .execute()
        )
        return {"success": True, "files": res.data or []}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/{slug}/files/upload")
async def upload_project_file(
    slug: str,
    file: UploadFile = File(...),
    file_kind: str = Form("attachment"),
    user_id: str = Depends(get_current_user_id),
):
    try:
        assert_can_view_pipeline(slug, user_id)
        ensure_project_directory(supabase, slug=slug, user_id=user_id, created_by=user_id)
        content = await file.read()
        content_type = file.content_type or mimetypes.guess_type(file.filename)[0] or "application/octet-stream"
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_name = (file.filename or "document.bin").replace("/", "_")
        storage_path = f"projects/{slug}/{file_kind}/{timestamp}_{safe_name}"

        supabase.storage.from_(PROJECT_BUCKET).upload(
            path=storage_path,
            file=content,
            file_options={"content-type": content_type, "upsert": "true"},
        )
        public_url = f"{SUPABASE_URL}/storage/v1/object/public/{PROJECT_BUCKET}/{storage_path}"

        file_row = register_project_file(
            supabase,
            slug=slug,
            file_kind=file_kind,
            filename=file.filename or safe_name,
            storage_path=storage_path,
            public_url=public_url,
            storage_bucket=PROJECT_BUCKET,
            mime_type=content_type,
            size_bytes=len(content),
            uploaded_by=user_id,
            source="user_upload",
        )
        return {"success": True, "file": file_row}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete("/{slug}/files/{file_id}")
def delete_project_file(
    slug: str,
    file_id: str,
    user_id: str = Depends(get_current_user_id),
):
    try:
        assert_can_view_pipeline(slug, user_id)
        row_res = (
            supabase.schema("latresne")
            .table("project_files")
            .select("*")
            .eq("id", file_id)
            .eq("project_slug", slug)
            .limit(1)
            .execute()
        )
        rows = row_res.data or []
        if not rows:
            raise HTTPException(status_code=404, detail="Fichier introuvable")
        row = rows[0]

        storage_bucket = row.get("storage_bucket") or PROJECT_BUCKET
        storage_path = row.get("storage_path")
        if storage_path:
            supabase.storage.from_(storage_bucket).remove([storage_path])

        supabase.schema("latresne").table("project_files").delete().eq("id", file_id).execute()
        return {"success": True, "deleted_id": file_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

