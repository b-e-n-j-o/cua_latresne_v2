#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test API locale (main.py) : envoi d'un PDF à /analyze-cerfa puis polling /status/{job_id}.

⚠️ Script volontairement simple : modifie uniquement les constantes ci-dessous.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


# ============================================================
# ✅ CONFIG À MODIFIER À LA MAIN
# ============================================================
BASE_URL = "http://127.0.0.1:8000"
CODE_INSEE = "33234"
USER_EMAIL = "benjamin.benoit21@hotmail.fr"
USER_ID: Optional[str] = None  # optionnel

# 👉 Mets ici ton PDF (tu modifieras ce chemin à la main)
PDF_PATH = "/Users/benjaminbenoit/Downloads/cerfa_CU_13410-2025-10-27.pdf"

# Polling
TIMEOUT_HTTP_S = 160.0
POLL_INTERVAL_S = 30.0
MAX_WAIT_S = 60 * 30


def _print_json(obj: Any) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=False, default=str))


def _health_check(base_url: str, timeout_s: float) -> None:
    url = f"{base_url.rstrip('/')}/health"
    try:
        r = requests.get(url, timeout=timeout_s)
    except Exception as e:
        raise RuntimeError(f"Impossible de joindre l'API sur {url}: {e}") from e

    if r.status_code != 200:
        raise RuntimeError(f"/health KO ({r.status_code}) -> {r.text}")


def _post_analyze_cerfa(
    base_url: str,
    pdf_path: Path,
    code_insee: Optional[str],
    user_id: Optional[str],
    user_email: Optional[str],
    timeout_s: float,
) -> str:
    url = f"{base_url.rstrip('/')}/analyze-cerfa"
    data: Dict[str, str] = {}
    if code_insee:
        data["code_insee"] = code_insee
    if user_id:
        data["user_id"] = user_id
    if user_email:
        data["user_email"] = user_email

    with pdf_path.open("rb") as f:
        files = {
            "pdf": (
                pdf_path.name,
                f,
                "application/pdf",
            )
        }
        r = requests.post(url, data=data, files=files, timeout=timeout_s)

    if r.status_code != 200:
        raise RuntimeError(f"POST /analyze-cerfa KO ({r.status_code}) -> {r.text}")

    payload = r.json()
    if not payload.get("success") or not payload.get("job_id"):
        raise RuntimeError(f"Réponse inattendue /analyze-cerfa: {payload}")

    return str(payload["job_id"])


def _get_status(base_url: str, job_id: str, timeout_s: float) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/status/{job_id}"
    r = requests.get(url, timeout=timeout_s)
    if r.status_code != 200:
        raise RuntimeError(f"GET /status/{job_id} KO ({r.status_code}) -> {r.text}")
    return r.json()


def main() -> int:
    pdf_path = Path(PDF_PATH).expanduser().resolve()
    if not pdf_path.exists():
        print(f"❌ PDF introuvable: {pdf_path}\n👉 Modifie PDF_PATH dans ce script.", file=sys.stderr)
        return 2
    if pdf_path.suffix.lower() != ".pdf":
        print(f"⚠️ Le fichier ne semble pas être un PDF: {pdf_path.name}", file=sys.stderr)

    base_url: str = str(BASE_URL).rstrip("/")

    try:
        print(f"🔎 Health check: {base_url}/health")
        _health_check(base_url, timeout_s=TIMEOUT_HTTP_S)
        print("✅ API OK\n")

        print("📤 Envoi PDF -> /analyze-cerfa")
        job_id = _post_analyze_cerfa(
            base_url=base_url,
            pdf_path=pdf_path,
            code_insee=CODE_INSEE,
            user_id=USER_ID,
            user_email=USER_EMAIL,
            timeout_s=TIMEOUT_HTTP_S,
        )
        print(f"✅ job_id = {job_id}\n")

        start = time.time()
        last_log_len = 0

        while True:
            elapsed = time.time() - start
            if elapsed > MAX_WAIT_S:
                print(f"⏳ Timeout: {MAX_WAIT_S}s dépassées. Dernier status:")
                _print_json(_get_status(base_url, job_id, timeout_s=TIMEOUT_HTTP_S))
                return 1

            status = _get_status(base_url, job_id, timeout_s=TIMEOUT_HTTP_S)

            s = status.get("status")
            step = status.get("current_step")
            logs: List[str] = status.get("logs") or []

            # Affiche uniquement les nouveaux logs depuis le dernier poll
            if len(logs) > last_log_len:
                new_lines = logs[last_log_len:]
                for line in new_lines:
                    print(line)
                last_log_len = len(logs)

            if s in {"success", "error", "timeout"}:
                print("\n==============================")
                print(f"🏁 FIN job {job_id} | status={s} | step={step}")
                if status.get("error"):
                    print(f"💥 error: {status.get('error')}")
                if status.get("slug"):
                    print(f"🔗 slug: {status.get('slug')}")
                print("==============================\n")
                return 0 if s == "success" else 1

            time.sleep(POLL_INTERVAL_S)

    except KeyboardInterrupt:
        print("\n🛑 Interrompu par l'utilisateur.")
        return 130
    except Exception as e:
        print(f"❌ Erreur: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


