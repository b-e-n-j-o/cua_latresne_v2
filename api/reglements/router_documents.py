"""
Router FastAPI — documents officiels GPU par commune.

GET /communes/{slug}/documents          liste des pièces
GET /communes/{slug}/documents/{pk}     métadonnées d'une pièce
GET /communes/{slug}/documents/file     proxy PDF (inline ou attachment)
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.reglements.gpu_pieces import fetch_gpu_file, list_pieces_flat
from api.reglements.router_reglements import require_editor

_CATALOG_DIR = Path(__file__).resolve().parent / "catalogues"
_DOCUMENTS_CATALOG_FILES: dict[str, Path] = {
    "argeles": _CATALOG_DIR / "documents_argeles.json",
    "latresne": _CATALOG_DIR / "documents_latresne.json",
}


class DocumentsCatalog(BaseModel):
    commune_slug: str
    insee: str
    label: str


def _load_documents_catalog(commune_slug: str) -> DocumentsCatalog:
    slug = (commune_slug or "").strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", slug):
        raise HTTPException(status_code=400, detail=f"Slug commune invalide : {commune_slug!r}")
    path = _DOCUMENTS_CATALOG_FILES.get(slug)
    if not path or not path.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"Catalogue documents introuvable pour la commune : {commune_slug}",
        )
    return DocumentsCatalog.model_validate(json.loads(path.read_text(encoding="utf-8")))


router = APIRouter(
    prefix="/communes",
    tags=["documents-officiels"],
    dependencies=[Depends(require_editor)],
)


@router.get("/{commune_slug}/documents")
async def list_documents(
    commune_slug: str,
    search: Optional[str] = Query(None),
) -> list[dict]:
    catalog = _load_documents_catalog(commune_slug)
    try:
        return list_pieces_flat(catalog.insee, search=search)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/{commune_slug}/documents/file")
async def stream_document_file(
    commune_slug: str,
    document_id: str = Query(...),
    file_name: str = Query(...),
    download: bool = Query(False),
):
    _load_documents_catalog(commune_slug)
    try:
        content, content_type = fetch_gpu_file(document_id, file_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Téléchargement GPU impossible : {e}")

    disposition = "attachment" if download else "inline"
    return StreamingResponse(
        iter([content]),
        media_type=content_type,
        headers={
            "Content-Disposition": f'{disposition}; filename="{file_name}"',
            "Content-Length": str(len(content)),
        },
    )


@router.get("/{commune_slug}/documents/{pk}")
async def get_document(commune_slug: str, pk: str) -> dict:
    catalog = _load_documents_catalog(commune_slug)
    try:
        rows = list_pieces_flat(catalog.insee)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    row = next((r for r in rows if r["id"] == pk), None)
    if not row:
        raise HTTPException(status_code=404, detail="Pièce introuvable")
    return row
