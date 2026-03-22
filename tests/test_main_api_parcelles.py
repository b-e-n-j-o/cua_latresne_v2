#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test API locale : POST /analyze-parcelles puis polling /status/{job_id}.

Le flux PDF CERFA complet n'existe plus côté API : l'analyse PDF se fait via
POST /cerfa/analyse ; le pipeline CUA se lance uniquement depuis des parcelles.

⚠️ Modifie les constantes ci-dessous.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict, List, Optional

import requests


BASE_URL = "http://127.0.0.1:8000"
CODE_INSEE = "33234"
COMMUNE_NOM: Optional[str] = "Latresne"
USER_EMAIL = "benjamin.benoit21@hotmail.fr"
USER_ID: Optional[str] = None

PARCELLES: List[Dict[str, str]] = [
    {"section": "AC", "numero": "0242"},
]

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


def _post_analyze_parcelles(
    base_url: str,
    parcelles: List[Dict[str, str]],
    code_insee: str,
    commune_nom: Optional[str],
    user_id: Optional[str],
    user_email: Optional[str],
    timeout_s: float,
) -> str:
    url = f"{base_url.rstrip('/')}/analyze-parcelles"
    payload: Dict[str, Any] = {
        "parcelles": parcelles,
        "code_insee": code_insee,
    }
    if commune_nom:
        payload["commune_nom"] = commune_nom
    if user_id:
        payload["user_id"] = user_id
    if user_email:
        payload["user_email"] = user_email

    r = requests.post(url, json=payload, timeout=timeout_s)

    if r.status_code != 200:
        raise RuntimeError(f"POST /analyze-parcelles KO ({r.status_code}) -> {r.text}")

    data = r.json()
    if not data.get("success") or not data.get("job_id"):
        raise RuntimeError(f"Réponse inattendue /analyze-parcelles: {data}")

    return str(data["job_id"])


def _get_status(base_url: str, job_id: str, timeout_s: float) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/status/{job_id}"
    r = requests.get(url, timeout=timeout_s)
    if r.status_code != 200:
        raise RuntimeError(f"GET /status/{job_id} KO ({r.status_code}) -> {r.text}")
    return r.json()


def main() -> int:
    base_url: str = str(BASE_URL).rstrip("/")

    try:
        print(f"🔎 Health check: {base_url}/health")
        _health_check(base_url, timeout_s=TIMEOUT_HTTP_S)
        print("✅ API OK\n")

        print("📤 POST /analyze-parcelles")
        job_id = _post_analyze_parcelles(
            base_url=base_url,
            parcelles=PARCELLES,
            code_insee=CODE_INSEE,
            commune_nom=COMMUNE_NOM,
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
