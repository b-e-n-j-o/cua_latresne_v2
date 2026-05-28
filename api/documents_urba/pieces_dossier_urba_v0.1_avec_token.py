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
import tempfile
from datetime import datetime
from pathlib import Path

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


def _doc_sort_key(doc: dict) -> tuple:
    """
    Clé de tri pour identifier la livraison la plus récente.
    Priorité: updateDate > uploadDate > date dans originalName > originalName.
    """
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
        # Format attendu: 66008_PLU_20251030
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


def list_pieces_dossier(
    insee: str | None = None,
    partition: str | None = None,
    session: requests.Session | None = None,
    latest_only: bool = True,
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

        if latest_only and len(docs) > 1:
            docs = [max(docs, key=_doc_sort_key)]

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


def _import_reglement_qualite_analyzer():
    """
    Import tardif pour éviter de casser l'usage "listing only" si PyMuPDF
    n'est pas installé.
    """
    try:
        from reglement_qualite import analyser_qualite_reglement
        return analyser_qualite_reglement, None
    except Exception as e:
        return None, str(e)


def _import_tiktoken_encoder(model_name: str = "gpt-4o-mini"):
    """Import tardif de tiktoken + encoder pour comptage exact."""
    try:
        import tiktoken
        try:
            enc = tiktoken.encoding_for_model(model_name)
        except Exception:
            enc = tiktoken.get_encoding("cl100k_base")
        return enc, None
    except Exception as e:
        return None, str(e)


def _extract_text_for_token_count(pdf_bytes: bytes) -> tuple[str | None, str | None]:
    """Extraction texte brute d'un PDF en mémoire (PyMuPDF)."""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        texte = "".join((page.get_text() or "") for page in doc)
        return texte, None
    except Exception as e:
        return None, str(e)


def _download_pdf_bytes(
    url: str,
    session: requests.Session,
    timeout: int = DEFAULT_TIMEOUT,
    download_mode: str = "memory",
    tmp_dir: str | None = None,
) -> tuple[bytes | None, str | None]:
    """Télécharge un PDF en RAM (défaut) ou en temporaire disque."""
    try:
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
    except Exception as e:
        return None, f"download_error: {str(e)[:200]}"

    if download_mode == "memory":
        return resp.content, None

    # mode disque (temporaire sur dossier choisi)
    try:
        base_tmp = Path(tmp_dir).resolve() if tmp_dir else (Path(__file__).resolve().parent / "_tmp_pdf_cache")
        base_tmp.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix="gpu_", suffix=".pdf", dir=str(base_tmp), delete=False) as tf:
            tf.write(resp.content)
            tmp_path = tf.name
        pdf_bytes = Path(tmp_path).read_bytes()
        Path(tmp_path).unlink(missing_ok=True)
        return pdf_bytes, None
    except Exception as e:
        return None, f"tmp_error: {str(e)[:200]}"


def analyse_qualite_pieces_pdf(
    dossier: dict,
    session: requests.Session | None = None,
    download_mode: str = "memory",
    tmp_dir: str | None = None,
    use_tiktoken: bool = False,
    tiktoken_model: str = "gpt-4o-mini",
    timeout: int = DEFAULT_TIMEOUT,
) -> dict:
    """
    Analyse chaque pièce PDF du dossier et ajoute un bloc `analyse_qualite_pdf`.
    Renvoie le dossier enrichi.
    """
    analyser_qualite_reglement, import_error = _import_reglement_qualite_analyzer()
    if analyser_qualite_reglement is None:
        dossier["analyse_qualite_pdf"] = {
            "enabled": False,
            "error": f"Analyse indisponible: {import_error}",
        }
        return dossier

    own_session = session is None
    if own_session:
        session = requests.Session()

    encoder = None
    tiktoken_error = None
    if use_tiktoken:
        encoder, tiktoken_error = _import_tiktoken_encoder(tiktoken_model)

    stats = {
        "enabled": True,
        "download_mode": download_mode,
        "tiktoken_enabled": bool(encoder),
        "tiktoken_model": tiktoken_model if use_tiktoken else None,
        "tiktoken_error": tiktoken_error if (use_tiktoken and not encoder) else None,
        "tmp_dir": str(Path(tmp_dir).resolve()) if (download_mode == "disk" and tmp_dir) else (
            str((Path(__file__).resolve().parent / "_tmp_pdf_cache").resolve()) if download_mode == "disk" else None
        ),
        "pdf_scanned": 0,
        "pages_total": 0,
        "llm_friendly": 0,
        "non_llm_friendly": 0,
        "tokens_estimes_total": 0,
        "tokens_tiktoken_total": 0,
        "verdicts": {},
        "errors": 0,
    }

    try:
        total_docs = len(dossier.get("documents", []))
        print(f"\n[scan-qualite] Démarrage analyse qualité PDF sur {total_docs} document(s)...")
        global_pdf_idx = 0

        for doc in dossier.get("documents", []):
            doc_name = doc.get("original_name") or doc.get("document_id") or "document"
            pdf_pieces = [
                p for p in doc.get("pieces", [])
                if (p.get("file_name") or "").lower().endswith(".pdf")
            ]
            doc_pages_total = 0
            doc_tokens_estimes_total = 0
            doc_tokens_tiktoken_total = 0
            doc_pdf_scanned = 0
            print(f"[scan-qualite] Document: {doc_name} | {len(pdf_pieces)} PDF à analyser")

            for piece in doc.get("pieces", []):
                fname = (piece.get("file_name") or "").lower()
                if not fname.endswith(".pdf"):
                    continue

                global_pdf_idx += 1
                stats["pdf_scanned"] += 1
                doc_pdf_scanned += 1
                print(f"[scan-qualite]  - PDF {global_pdf_idx}: {piece.get('file_name')}")
                pdf_bytes, err = _download_pdf_bytes(
                    piece.get("download_url") or "",
                    session=session,
                    timeout=timeout,
                    download_mode=download_mode,
                    tmp_dir=tmp_dir,
                )

                if err or not pdf_bytes:
                    stats["errors"] += 1
                    piece["qualite_pdf"] = {
                        "ok": False,
                        "error": err or "download_error",
                        "llm_friendly": False,
                    }
                    print(f"[scan-qualite]    -> ERREUR téléchargement")
                    continue

                if pdf_bytes[:4] != b"%PDF":
                    stats["errors"] += 1
                    piece["qualite_pdf"] = {
                        "ok": False,
                        "error": "fichier non reconnu comme PDF",
                        "llm_friendly": False,
                    }
                    print(f"[scan-qualite]    -> ERREUR format PDF")
                    continue

                try:
                    q = analyser_qualite_reglement(pdf_bytes)
                    verdict = q.verdict
                    stats["verdicts"][verdict] = stats["verdicts"].get(verdict, 0) + 1
                    if q.utilisable:
                        stats["llm_friendly"] += 1
                    else:
                        stats["non_llm_friendly"] += 1
                    stats["pages_total"] += q.n_pages
                    stats["tokens_estimes_total"] += q.tokens_estimes
                    doc_pages_total += q.n_pages
                    doc_tokens_estimes_total += q.tokens_estimes

                    tiktoken_tokens = None
                    if encoder is not None:
                        texte, text_err = _extract_text_for_token_count(pdf_bytes)
                        if texte is not None:
                            try:
                                tiktoken_tokens = len(encoder.encode(texte))
                                stats["tokens_tiktoken_total"] += tiktoken_tokens
                                doc_tokens_tiktoken_total += tiktoken_tokens
                            except Exception:
                                tiktoken_tokens = None
                        else:
                            # pas bloquant: on garde l'analyse qualité même sans comptage exact
                            logger.warning("Extraction texte tiktoken impossible pour %s: %s", piece.get("file_name"), text_err)

                    piece["qualite_pdf"] = {
                        "ok": True,
                        "verdict": q.verdict,
                        "llm_friendly": q.utilisable,
                        "detail": q.detail,
                        "n_pages": q.n_pages,
                        "pct_pages_textuelles": q.pct_pages_textuelles,
                        "chars_total": q.chars_total,
                        "tokens_estimes": q.tokens_estimes,
                        "tokens_tiktoken": tiktoken_tokens,
                        "n_blocs_image": q.n_blocs_image,
                        "n_blocs_texte": q.n_blocs_texte,
                    }
                    state = "LLM ✅" if q.utilisable else "LLM ❌"
                    tk_str = f", tiktoken={tiktoken_tokens}" if tiktoken_tokens is not None else ""
                    print(f"[scan-qualite]    -> {q.verdict} ({state}), pages={q.n_pages}, tokens_est={q.tokens_estimes}{tk_str}")
                except Exception as e:
                    stats["errors"] += 1
                    piece["qualite_pdf"] = {
                        "ok": False,
                        "error": f"analyse_error: {str(e)[:200]}",
                        "llm_friendly": False,
                    }
                    print(f"[scan-qualite]    -> ERREUR analyse")

            doc["statistiques_pdf"] = {
                "pdf_scanned": doc_pdf_scanned,
                "pages_total": doc_pages_total,
                "tokens_estimes_total": doc_tokens_estimes_total,
                "tokens_tiktoken_total": doc_tokens_tiktoken_total if encoder is not None else None,
            }
    finally:
        if own_session:
            session.close()

    print(
        "[scan-qualite] Terminé | "
        f"pdf={stats['pdf_scanned']} pages={stats['pages_total']} "
        f"tokens_est={stats['tokens_estimes_total']} "
        f"tokens_tk={stats['tokens_tiktoken_total'] if encoder is not None else 'n/a'} "
        f"llm_ok={stats['llm_friendly']} llm_ko={stats['non_llm_friendly']} erreurs={stats['errors']}"
    )
    dossier["analyse_qualite_pdf"] = stats
    return dossier


# ---------- CLI debug ----------

def _cli():
    import argparse
    ap = argparse.ArgumentParser(description="Liste des pièces d'un dossier d'urbanisme (API GPU)")
    ap.add_argument("--insee", help="Code INSEE (ex: 66008)")
    ap.add_argument("--partition", help="Partition directe (ex: DU_66008)")
    ap.add_argument(
        "--all-livraisons",
        action="store_true",
        help="Traite toutes les livraisons (par défaut: uniquement la plus récente)",
    )
    ap.add_argument("--scan-qualite", action="store_true", help="Analyse la qualité extractible des pièces PDF")
    ap.add_argument(
        "--download-mode",
        choices=("memory", "disk"),
        default="memory",
        help="Téléchargement des PDF en RAM (memory) ou temporaire disque (disk)",
    )
    ap.add_argument(
        "--tmp-dir",
        help="Dossier temporaire quand --download-mode=disk (par défaut: _tmp_pdf_cache à côté du script)",
    )
    ap.add_argument(
        "--use-tiktoken",
        action="store_true",
        help="Calcule aussi le nombre de tokens exact avec tiktoken (si installé)",
    )
    ap.add_argument(
        "--tiktoken-model",
        default="gpt-4o-mini",
        help="Modèle tiktoken (défaut: gpt-4o-mini, fallback cl100k_base)",
    )
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    if not args.insee and not args.partition:
        ap.error("Fournir --insee ou --partition")

    logging.basicConfig(level=logging.WARNING)
    result = list_pieces_dossier(
        insee=args.insee,
        partition=args.partition,
        latest_only=not args.all_livraisons,
    )
    if args.scan_qualite and not result.get("error"):
        result = analyse_qualite_pieces_pdf(
            result,
            download_mode=args.download_mode,
            tmp_dir=args.tmp_dir,
            use_tiktoken=args.use_tiktoken,
            tiktoken_model=args.tiktoken_model,
        )

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
                qual = p.get("qualite_pdf")
                if qual and qual.get("ok"):
                    vf = "LLM ✅" if qual.get("llm_friendly") else "LLM ❌"
                    verdict = qual.get("verdict", "?")
                    print(f"    │  • {p['file_name']}{title} [{verdict} | {vf}]")
                elif qual and not qual.get("ok"):
                    print(f"    │  • {p['file_name']}{title} [ERREUR ANALYSE]")
                else:
                    print(f"    │  • {p['file_name']}{title}")
        dstat = doc.get("statistiques_pdf")
        if dstat:
            tk_total = dstat.get("tokens_tiktoken_total")
            tk_str = str(tk_total) if tk_total is not None else "n/a"
            print(
                f"  Stats PDF doc : {dstat.get('pdf_scanned', 0)} PDF | "
                f"{dstat.get('pages_total', 0)} pages | "
                f"tokens_est={dstat.get('tokens_estimes_total', 0)} | "
                f"tokens_tiktoken={tk_str}"
            )
    print(f"\n{result['documents_count']} document(s) au total.")

    qa = result.get("analyse_qualite_pdf")
    if qa:
        print("\n" + "=" * 74)
        print("SYNTHÈSE QUALITÉ PDF")
        print("=" * 74)
        print(f"Mode téléchargement : {qa.get('download_mode')}")
        if qa.get("download_mode") == "disk":
            print(f"Dossier temporaire  : {qa.get('tmp_dir')}")
        print(f"PDF scannés         : {qa.get('pdf_scanned')}")
        print(f"Pages totales       : {qa.get('pages_total')}")
        print(f"LLM friendly        : {qa.get('llm_friendly')}")
        print(f"Non LLM friendly    : {qa.get('non_llm_friendly')}")
        print(f"Tokens estimés      : {qa.get('tokens_estimes_total')}")
        if qa.get("tiktoken_enabled"):
            print(f"Tokens tiktoken     : {qa.get('tokens_tiktoken_total')} ({qa.get('tiktoken_model')})")
        elif qa.get("tiktoken_model"):
            print(f"Tokens tiktoken     : indisponible ({qa.get('tiktoken_error')})")
        print(f"Erreurs             : {qa.get('errors')}")
        if qa.get("verdicts"):
            print("\nRépartition verdicts :")
            for verdict, count in sorted(qa["verdicts"].items()):
                print(f"  - {verdict:<10} : {count}")


if __name__ == "__main__":
    _cli()