#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
`pieces_dossier_urba.py` — Liste complète des pièces d'un dossier d'urbanisme (API GPU).

Complément de `documents_urba.py`. Là où documents_urba donne le PROFIL du
document courant (via WFS doc_urba), ce module donne la LISTE COMPLÈTE DES
PIÈCES (règlement, PADD, OAP, rapports, annexes, plans SUP...) — l'équivalent
de l'onglet "Documents" du Géoportail de l'Urbanisme.

⚠️ Source : API REST interne du GPU (https://www.geoportail-urbanisme.gouv.fr/api/),
PAS le WFS. La spec OpenAPI indique que cette API "répond avant tout à un usage
interne" — donc pas de garantie de stabilité comme le WFS. À monitorer.

Chaînage (spec OpenAPI GPU v6.1.14) :
  1. GET /document?documentType=PLU&partition=DU_<insee>  -> documentId(s)
  2. GET /document/{documentId}/details                   -> profil + files + writingMaterials
     ou GET /document/{documentId}/files                  -> liste des pièces seules
  3. GET /document/{documentId}/files/{fileName}          -> télécharge une pièce (PDF)

Usage CLI :
  python3 pieces_dossier_urba.py --insee 66008
  python3 pieces_dossier_urba.py --insee 66008 --json
  python3 pieces_dossier_urba.py --partition DU_66008
"""

from __future__ import annotations

import json
import logging

import requests

logger = logging.getLogger("pieces_dossier_urba")

API_BASE = "https://www.geoportail-urbanisme.gouv.fr/api"
DEFAULT_TIMEOUT = 60


# ---------- Appels API REST ----------

def _api_get(path: str, params: dict | None = None,
             timeout: int = DEFAULT_TIMEOUT,
             session: requests.Session | None = None):
    """GET JSON sur l'API GPU. Retourne l'objet décodé, ou None si échec."""
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
    document_type: str | None = None,
    session: requests.Session | None = None,
) -> list[dict] | None:
    """
    Liste les documents via /document.
    Filtrage recommandé par partition=DU_<insee> (cible le doc de la commune).
    Retourne la liste brute des documents (chacun avec son id), ou None.
    """
    params = {"limit": 100}
    if partition:
        params["partition"] = partition
    elif insee:
        # à défaut de partition, on cible la partition communale standard
        params["partition"] = f"DU_{insee}"
    if document_type:
        params["documentType[]"] = document_type
    return _api_get("/document", params=params, session=session)


def get_document_details(document_id: str, session: requests.Session | None = None) -> dict | None:
    """GET /document/{id}/details — profil complet + files + writingMaterials."""
    return _api_get(f"/document/{document_id}/details", session=session)


def get_document_files(document_id: str, session: requests.Session | None = None) -> list[dict] | None:
    """GET /document/{id}/files — liste des pièces écrites (DocumentFileInfo)."""
    return _api_get(f"/document/{document_id}/files", session=session)


def build_file_download_url(document_id: str, file_name: str) -> str:
    """URL de téléchargement direct d'une pièce écrite (PDF)."""
    return f"{API_BASE}/document/{document_id}/files/{file_name}"


def build_archive_url(document_id: str) -> str:
    """URL de l'archive ZIP complète du document (format CNIG)."""
    return f"{API_BASE}/document/{document_id}/download"


def build_archive_url_by_partition(partition: str) -> str:
    """URL de l'archive ZIP via partition (alternative sans documentId)."""
    return f"{API_BASE}/document/download-by-partition/{partition}"


# ---------- Orchestration : liste des pièces d'une commune ----------

def _extract_document_id(doc: dict) -> str | None:
    """L'id du document peut s'appeler 'id', '_id' ou 'documentId' selon les versions."""
    for k in ("id", "_id", "documentId", "documentid"):
        if doc.get(k):
            return str(doc[k])
    return None


def _build_pieces_from_files(doc_id: str, files_info: list[dict]) -> list[dict]:
    """
    Transforme la réponse /files (DocumentFileInfo) en pièces enrichies.
    DocumentFileInfo = {name, title, path} où :
      - name  : nom du fichier (ex: 66008_rapport_20220310.pdf)
      - title : titre lisible de la pièce (ex: "Rapport")
      - path  : DOSSIER/CATÉGORIE GPU (ex: "Rapport de présentation") ← classification
    """
    pieces = []
    for f in files_info:
        fname = f.get("name") or f.get("fileName") or f.get("file")
        if not fname:
            continue
        pieces.append({
            "file_name": fname,
            "title": f.get("title"),          # titre lisible de la pièce
            "categorie": f.get("path"),        # dossier GPU (classification native)
            "download_url": build_file_download_url(doc_id, fname),
        })
    return pieces


def _group_by_categorie(pieces: list[dict]) -> dict[str, list[dict]]:
    """Regroupe les pièces par catégorie (champ `path` du GPU)."""
    groups: dict[str, list[dict]] = {}
    for p in pieces:
        cat = p.get("categorie") or "Autres"
        groups.setdefault(cat, []).append(p)
    return groups


def list_pieces_dossier(
    insee: str | None = None,
    partition: str | None = None,
    session: requests.Session | None = None,
) -> dict:
    """
    Liste complète des pièces du dossier d'urbanisme d'une commune, AVEC leur
    classification native GPU (champ `path` -> categorie).

    Utilise /files (DocumentFileInfo: name/title/path) qui porte la catégorie,
    contrairement à /details qui ne renvoie que les noms de fichiers.

    Format :
      {
        "insee", "partition",
        "documents": [
          {
            "document_id", "original_name", "document_type", "archive_url",
            "pieces": [ {file_name, title, categorie, download_url}, ... ],
            "pieces_par_categorie": { "Règlements": [...], "Annexes": [...], ... },
            "pieces_count",
          },
        ],
        "documents_count", "error",
      }
    """
    own_session = session is None
    if own_session:
        session = requests.Session()

    out = {
        "insee": insee, "partition": partition or (f"DU_{insee}" if insee else None),
        "documents": [], "documents_count": 0, "error": None,
    }

    try:
        docs = find_documents(insee=insee, partition=partition, session=session)
        if docs is None:
            out["error"] = "Échec de la requête /document."
            return out
        if not docs:
            out["error"] = "Aucun document trouvé pour cette commune/partition."
            return out

        for doc in docs:
            doc_id = _extract_document_id(doc)
            if not doc_id:
                continue

            # /files porte la classification (name/title/path) — prioritaire
            files_info = get_document_files(doc_id, session=session)
            if files_info:
                pieces = _build_pieces_from_files(doc_id, files_info)
            else:
                # Fallback /details (noms seuls, sans catégorie) si /files échoue
                details = get_document_details(doc_id, session=session) or {}
                writing = details.get("writingMaterials") or {}
                pieces = [
                    {
                        "file_name": fname,
                        "title": None,
                        "categorie": None,
                        "download_url": (
                            writing.get(fname) if isinstance(writing, dict) else None
                        ) or build_file_download_url(doc_id, fname),
                    }
                    for fname in (details.get("files") or [])
                ]

            out["documents"].append({
                "document_id": doc_id,
                "original_name": doc.get("originalName") or doc.get("original_name"),
                "document_type": doc.get("documentType") or doc.get("type"),
                "archive_url": build_archive_url(doc_id),
                "pieces": pieces,
                "pieces_par_categorie": _group_by_categorie(pieces),
                "pieces_count": len(pieces),
            })

        out["documents_count"] = len(out["documents"])
        return out

    except Exception as e:
        logger.exception("list_pieces_dossier — erreur inattendue")
        out["error"] = str(e)
        return out
    finally:
        if own_session:
            session.close()


# ---------- CLI debug ----------

def _cli():
    import argparse
    ap = argparse.ArgumentParser(description="Liste des pièces d'un dossier d'urbanisme (API GPU)")
    ap.add_argument("--insee", help="Code INSEE (ex: 66008)")
    ap.add_argument("--partition", help="Partition directe (ex: DU_66008)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if not args.insee and not args.partition:
        ap.error("Fournir --insee ou --partition")

    logging.basicConfig(level=logging.WARNING)
    result = list_pieces_dossier(insee=args.insee, partition=args.partition)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    print("=" * 74)
    print(f"PIÈCES DU DOSSIER D'URBANISME — {result['partition']}")
    print("=" * 74)
    if result["error"]:
        print(f"⚠️ {result['error']}")
    for doc in result["documents"]:
        print(f"\n▸ {doc['original_name'] or doc['document_id']} ({doc['document_type']})")
        print(f"  Archive ZIP : {doc['archive_url']}")
        print(f"  {doc['pieces_count']} pièce(s) réparties en {len(doc['pieces_par_categorie'])} catégorie(s) :")
        for cat, pieces in doc["pieces_par_categorie"].items():
            print(f"\n    ┌─ {cat} ({len(pieces)})")
            for p in pieces:
                title = f" — {p['title']}" if p.get("title") else ""
                print(f"    │  • {p['file_name']}{title}")
    print(f"\n{result['documents_count']} document(s) au total.")


if __name__ == "__main__":
    _cli()