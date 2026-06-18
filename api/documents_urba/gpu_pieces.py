"""
gpu_pieces.py — Pièces du dossier d'urbanisme via l'API REST GPU (Géoportail).

Usage métier : lister les PDF officiels d'une commune (PLU, PADD, annexes…)
sans stockage local. Téléchargement à la demande pour affichage.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

API_BASE = "https://www.geoportail-urbanisme.gouv.fr/api"
DEFAULT_TIMEOUT = 60


def _api_get(
    path: str,
    params: dict | None = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    session: requests.Session | None = None,
) -> Any | None:
    url = f"{API_BASE}{path}"
    getter = session.get if session else requests.get
    try:
        r = getter(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning("API GPU %s échec: %s", path, str(e)[:160])
        return None


def find_documents(
    insee: str | None = None,
    partition: str | None = None,
    session: requests.Session | None = None,
) -> list[dict] | None:
    params: dict[str, Any] = {"limit": 100}
    if partition:
        params["partition"] = partition
    elif insee:
        params["partition"] = f"DU_{insee}"
    return _api_get("/document", params=params, session=session)


def get_document_files(document_id: str, session: requests.Session | None = None) -> list[dict] | None:
    return _api_get(f"/document/{document_id}/files", session=session)


def get_document_details(document_id: str, session: requests.Session | None = None) -> dict | None:
    return _api_get(f"/document/{document_id}/details", session=session)


def build_file_download_url(document_id: str, file_name: str) -> str:
    return f"{API_BASE}/document/{document_id}/files/{file_name}"


def build_archive_url(document_id: str) -> str:
    return f"{API_BASE}/document/{document_id}/download"


def _extract_document_id(doc: dict) -> str | None:
    for k in ("id", "_id", "documentId", "documentid"):
        if doc.get(k):
            return str(doc[k])
    return None


def _doc_sort_key(doc: dict) -> tuple:
    update_dt = str(doc.get("updateDate") or "")
    upload_dt = str(doc.get("uploadDate") or "")
    original_name = str(doc.get("originalName") or doc.get("original_name") or "")

    def _to_epoch(value: str) -> float:
        if not value:
            return float("-inf")
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            return float("-inf")

    def _extract_name_date(name: str) -> int:
        parts = name.split("_")
        if parts:
            last = parts[-1]
            if len(last) == 8 and last.isdigit():
                return int(last)
        return -1

    return (
        _to_epoch(update_dt),
        _to_epoch(upload_dt),
        _extract_name_date(original_name),
        original_name,
    )


def _build_pieces_from_files(doc_id: str, files_info: list[dict]) -> list[dict]:
    pieces: list[dict] = []
    for f in files_info:
        fname = f.get("name") or f.get("fileName") or f.get("file")
        if not fname:
            continue
        pieces.append(
            {
                "file_name": fname,
                "title": f.get("title"),
                "categorie": f.get("path"),
                "download_url": build_file_download_url(doc_id, fname),
            }
        )
    return pieces


def list_pieces_dossier(
    insee: str | None = None,
    partition: str | None = None,
    session: requests.Session | None = None,
    latest_only: bool = True,
) -> dict:
    """
    Retourne le dossier GPU d'une commune.

    ``pieces_total`` = nombre de pièces listées (fichiers),
    ``documents_count`` = nombre de livraisons GPU retenues (souvent 1).
    """
    own_session = session is None
    if own_session:
        session = requests.Session()

    out: dict[str, Any] = {
        "insee": insee,
        "partition": partition or (f"DU_{insee}" if insee else None),
        "documents": [],
        "documents_count": 0,
        "pieces_total": 0,
        "error": None,
    }

    try:
        docs = find_documents(insee=insee, partition=partition, session=session)
        if docs is None:
            out["error"] = "Échec de la requête /document."
            return out
        if not docs:
            out["error"] = "Aucun document trouvé pour cette commune/partition."
            return out

        if latest_only and len(docs) > 1:
            docs = [max(docs, key=_doc_sort_key)]

        for doc in docs:
            doc_id = _extract_document_id(doc)
            if not doc_id:
                continue

            files_info = get_document_files(doc_id, session=session)
            if files_info:
                pieces = _build_pieces_from_files(doc_id, files_info)
            else:
                details = get_document_details(doc_id, session=session) or {}
                writing = details.get("writingMaterials") or {}
                pieces = [
                    {
                        "file_name": fname,
                        "title": None,
                        "categorie": None,
                        "download_url": (
                            writing.get(fname) if isinstance(writing, dict) else None
                        )
                        or build_file_download_url(doc_id, fname),
                    }
                    for fname in (details.get("files") or [])
                ]

            out["documents"].append(
                {
                    "document_id": doc_id,
                    "original_name": doc.get("originalName") or doc.get("original_name"),
                    "document_type": doc.get("documentType") or doc.get("type"),
                    "archive_url": build_archive_url(doc_id),
                    "pieces": pieces,
                    "pieces_count": len(pieces),
                }
            )

        out["documents_count"] = len(out["documents"])
        out["pieces_total"] = sum(d["pieces_count"] for d in out["documents"])
        return out

    except Exception as e:
        logger.exception("list_pieces_dossier — erreur inattendue")
        out["error"] = str(e)
        return out
    finally:
        if own_session:
            session.close()


def list_pieces_flat(
    insee: str,
    *,
    search: str | None = None,
    latest_only: bool = True,
) -> list[dict]:
    """Liste aplatie pour l'UI règlements (une ligne = une pièce GPU)."""
    dossier = list_pieces_dossier(insee=insee, latest_only=latest_only)
    if dossier.get("error"):
        raise RuntimeError(dossier["error"])

    rows: list[dict] = []
    q = (search or "").strip().lower()
    for doc in dossier.get("documents", []):
        doc_id = doc["document_id"]
        for piece in doc.get("pieces", []):
            fname = piece["file_name"]
            title = piece.get("title") or fname
            categorie = piece.get("categorie") or "Autres"
            if q and q not in title.lower() and q not in fname.lower() and q not in categorie.lower():
                continue
            rows.append(
                {
                    "id": f"{doc_id}|{fname}",
                    "document_id": doc_id,
                    "file_name": fname,
                    "title": title,
                    "categorie": categorie,
                    "is_pdf": fname.lower().endswith(".pdf"),
                    "document_type": doc.get("document_type"),
                    "original_name": doc.get("original_name"),
                }
            )
    rows.sort(key=lambda r: (r["categorie"], r["title"]))
    return rows


def parse_piece_id(piece_id: str) -> tuple[str, str]:
    if "|" not in piece_id:
        raise ValueError("Identifiant de pièce invalide")
    doc_id, file_name = piece_id.split("|", 1)
    if not doc_id or not file_name:
        raise ValueError("Identifiant de pièce invalide")
    return doc_id, file_name


def _validate_file_name(file_name: str) -> None:
    if not file_name or ".." in file_name or "/" in file_name or "\\" in file_name:
        raise ValueError("Nom de fichier invalide")


def fetch_gpu_file(
    document_id: str,
    file_name: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    session: requests.Session | None = None,
) -> tuple[bytes, str]:
    """Télécharge une pièce GPU en mémoire. Retourne (bytes, content_type)."""
    _validate_file_name(file_name)
    url = build_file_download_url(document_id, file_name)
    own_session = session is None
    if own_session:
        session = requests.Session()
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type") or "application/octet-stream"
        return resp.content, content_type
    finally:
        if own_session:
            session.close()
