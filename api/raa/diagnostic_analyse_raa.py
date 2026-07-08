#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnostic pas-à-pas de l'analyse RAA (téléchargement PDF → appel Vertex/Gemini).

Usage (depuis cua_latresne_v4/) :

    # Via un RAA déjà en base (recommandé)
    python -m api.raa.diagnostic_analyse_raa --commune argeles --raa-id 42

    # Via une URL PDF directe (sans base)
    python -m api.raa.diagnostic_analyse_raa --commune argeles \\
        --pdf-url "https://www.pyrenees-orientales.gouv.fr/contenu/telechargement/..."

    # Lister les RAA en erreur
    python -m api.raa.diagnostic_analyse_raa --commune argeles --list-erreurs

Étapes testées :
  1. Variables d'environnement (GEMINI_API_KEY, GEMINI_MODEL)
  2. Téléchargement du PDF (requests, comme service_analyse_raa)
  3. Appel Gemini inline (Part.from_bytes, comme _call_gemini)
  4. (optionnel) Pipeline complet analyser_raa(persist=False)
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
load_dotenv(_ROOT / ".env")

from api._env import DB_CONFIG, GEMINI_API_KEY, GEMINI_MODEL  # noqa: E402
from api.raa.raa_config import RAA_COMMUNES, get_raa_config  # noqa: E402
from api.raa.service_analyse_raa import (  # noqa: E402
    DOWNLOAD_TIMEOUT,
    HEADERS,
    INLINE_PDF_MAX_BYTES,
    _call_gemini,
    _download_pdf,
    analyser_raa,
    get_client,
)


def _banner(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def _ok(msg: str) -> None:
    print(f"  ✓ {msg}")


def _fail(msg: str) -> None:
    print(f"  ✗ {msg}")


def _check_env() -> bool:
    _banner("1. Environnement")
    ok = True
    key = GEMINI_API_KEY or ""
    if key:
        _ok(f"GEMINI_API_KEY présente ({len(key)} car., début={key[:6]}…)")
    else:
        _fail("GEMINI_API_KEY absente (ni GOOGLE_API_KEY)")
        ok = False
    _ok(f"GEMINI_MODEL = {GEMINI_MODEL}")
    for var in ("GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_LOCATION"):
        import os
        val = os.environ.get(var)
        if val:
            _ok(f"{var} = {val}")
        else:
            print(f"  · {var} non défini (optionnel avec clé Vertex)")
    return ok


def _fetch_raa(conn, schema: str, raa_id: int) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, titre, pdf_url, taille_mo, statut
            FROM {schema}.raa WHERE id=%s;
            """,
            (raa_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "titre": row[1],
        "pdf_url": row[2],
        "taille_mo": row[3],
        "statut": row[4],
    }


def _list_erreurs(conn, schema: str, limit: int = 20) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT r.id, r.titre, r.taille_mo, r.statut, a.erreur, r.pdf_url
            FROM {schema}.raa r
            LEFT JOIN LATERAL (
                SELECT erreur FROM {schema}.raa_analyse aa
                WHERE aa.raa_id = r.id ORDER BY aa.created_at DESC LIMIT 1
            ) a ON TRUE
            WHERE COALESCE(r.masque, false) = false
              AND r.statut = 'erreur'
            ORDER BY r.date_publication DESC NULLS LAST
            LIMIT %s;
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [
        {
            "id": r[0], "titre": r[1], "taille_mo": r[2],
            "statut": r[3], "erreur": r[4], "pdf_url": r[5],
        }
        for r in rows
    ]


def _test_download(pdf_url: str) -> Path | None:
    _banner("2. Téléchargement PDF")
    print(f"  URL : {pdf_url[:100]}…" if len(pdf_url) > 100 else f"  URL : {pdf_url}")
    print(f"  Timeout : {DOWNLOAD_TIMEOUT}s | User-Agent : {HEADERS['User-Agent']}")
    t0 = time.time()
    try:
        path = _download_pdf(pdf_url)
        elapsed = time.time() - t0
        size = path.stat().st_size
        mo = size / (1024 * 1024)
        _ok(f"Téléchargé en {elapsed:.1f}s — {mo:.2f} Mo ({size:,} octets)")
        if size > INLINE_PDF_MAX_BYTES:
            _fail(f"PDF > limite inline Gemini ({INLINE_PDF_MAX_BYTES // (1024*1024)} Mo)")
        return path
    except Exception as e:
        elapsed = time.time() - t0
        _fail(f"Échec après {elapsed:.1f}s : {type(e).__name__}: {e}")
        print("\n  Traceback :")
        traceback.print_exc()
        print(
            "\n  → Si l'erreur est ici : le serveur préfecture coupe la connexion.\n"
            "    Causes fréquentes : IP cloud (Render), PDF volumineux, pas de retry."
        )
        return None


def _test_gemini(pdf_path: Path, commune_slug: str) -> bool:
    _banner("3. Appel Vertex / Gemini (inline PDF)")
    cfg = get_raa_config(commune_slug)
    if not cfg:
        _fail(f"Commune inconnue : {commune_slug}")
        return False

    size = pdf_path.stat().st_size
    print(f"  Modèle : {GEMINI_MODEL}")
    print(f"  PDF   : {size:,} octets ({size / (1024*1024):.2f} Mo)")
    print(f"  Prompt : {len(cfg.analyse_prompt)} car. (analyse) + system")

    try:
        client = get_client()
        _ok("Client genai initialisé (vertexai=True)")
    except Exception as e:
        _fail(f"Impossible de créer le client : {e}")
        return False

    t0 = time.time()
    try:
        res = _call_gemini(client, pdf_path, cfg)
        elapsed = time.time() - t0
        analyse = res["analyse"]
        _ok(f"Réponse en {elapsed:.1f}s")
        _ok(
            f"Tokens in={res['tokens_in']:,} out={res['tokens_out']:,} | "
            f"alerte={analyse.get('niveau_alerte')} | "
            f"arrêtés={analyse.get('nb_arretes_total')}"
        )
        return True
    except Exception as e:
        elapsed = time.time() - t0
        _fail(f"Échec après {elapsed:.1f}s : {type(e).__name__}: {e}")
        print("\n  Traceback :")
        traceback.print_exc()
        print(
            "\n  → Si l'erreur est ici : Vertex ferme la connexion pendant l'upload/réponse.\n"
            "    Causes fréquentes : PDF trop gros pour le timeout HTTP, quota, clé Vertex\n"
            "    invalide, modèle indisponible, ou payload inline > ~20 Mo instable."
        )
        return False


def _test_pipeline(conn, raa_id: int, commune_slug: str) -> bool:
    _banner("4. Pipeline complet (analyser_raa, persist=False)")
    t0 = time.time()
    try:
        res = analyser_raa(conn, raa_id, commune_slug, persist=False)
        elapsed = time.time() - t0
        if res.get("statut") == "analyse" and not res.get("erreur"):
            _ok(f"OK en {elapsed:.1f}s — {res.get('niveau_alerte')}")
            return True
        _fail(f"statut={res.get('statut')} | {res.get('erreur')}")
        return False
    except Exception as e:
        _fail(f"{type(e).__name__}: {e}")
        traceback.print_exc()
        return False


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Diagnostic analyse RAA (PDF + Vertex).")
    p.add_argument(
        "--commune", default="argeles", choices=sorted(RAA_COMMUNES.keys()),
    )
    p.add_argument("--raa-id", type=int, help="ID du recueil en base.")
    p.add_argument("--pdf-url", help="URL PDF directe (ignore la base).")
    p.add_argument(
        "--list-erreurs", action="store_true",
        help="Lister les RAA en statut erreur puis quitter.",
    )
    p.add_argument(
        "--full", action="store_true",
        help="Lancer aussi analyser_raa(persist=False) si étapes 2-3 OK.",
    )
    p.add_argument("--skip-gemini", action="store_true", help="Ne tester que le téléchargement.")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    cfg = get_raa_config(args.commune)
    if not cfg:
        print(f"Commune inconnue : {args.commune}")
        return 1

    print(f"Diagnostic RAA — {cfg.commune_label} ({cfg.schema})")

    if not _check_env():
        return 1

    conn = psycopg2.connect(**DB_CONFIG)

    if args.list_erreurs:
        _banner("RAA en erreur")
        for r in _list_erreurs(conn, cfg.schema):
            err = (r["erreur"] or "?")[:80]
            print(
                f"  #{r['id']:4d}  {r['taille_mo'] or 0:5.1f} Mo  "
                f"{(r['titre'] or '')[:50]:50s}  {err}"
            )
        conn.close()
        return 0

    pdf_url = args.pdf_url
    raa_id = args.raa_id

    if not pdf_url and raa_id:
        row = _fetch_raa(conn, cfg.schema, raa_id)
        if not row:
            print(f"RAA #{raa_id} introuvable dans {cfg.schema}.raa")
            conn.close()
            return 1
        print(f"\nRAA #{row['id']} — {row['titre']} ({row['taille_mo']} Mo, statut={row['statut']})")
        pdf_url = row["pdf_url"]

    if not pdf_url:
        print("Indiquez --raa-id N ou --pdf-url URL (ou --list-erreurs).")
        conn.close()
        return 1

    pdf_path = _test_download(pdf_url)
    if not pdf_path:
        conn.close()
        return 2

    gemini_ok = True
    if not args.skip_gemini:
        gemini_ok = _test_gemini(pdf_path, args.commune)

    if args.full and gemini_ok and raa_id:
        _test_pipeline(conn, raa_id, args.commune)

    if pdf_path.exists():
        pdf_path.unlink()

    conn.close()
    print()
    if gemini_ok or args.skip_gemini:
        print("Diagnostic terminé.")
        return 0
    print("Diagnostic terminé avec échec Gemini.")
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
