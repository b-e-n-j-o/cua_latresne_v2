# -*- coding: utf-8 -*-
"""
Déclenchement quotidien de la veille cadastrale (cron-job.org → Render).

    POST /api/tasks/trigger-veille
    GET  /api/tasks/trigger-veille/status

Auth : header X-Cron-Token (ou x-internal-token) = INTERNAL_TOKEN.
Répond tout de suite (200) ; le --apply tourne dans un thread + process isolé
(un redémarrage uvicorn ne doit pas tuer le --apply en cours).
"""

from __future__ import annotations

import hmac
import json
import logging
import os
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Header, HTTPException

from api.veille_sig._env import BACKEND_ROOT

logger = logging.getLogger("cadastre_veille_cron")

router = APIRouter(tags=["cadastre-veille-cron"])

ETL_COMMUNES_JSON = (
    BACKEND_ROOT / "services" / "ingestion_cadastre" / "config" / "etl_communes.json"
)
LOG_DIR = BACKEND_ROOT / "api" / "veille_sig" / "cadastre" / "reports"
# Téléchargement Etalab + diff 15 k parcelles : plusieurs minutes par commune.
_COMMUNE_TIMEOUT_S = 45 * 60

_lock = threading.Lock()
_JOB: dict[str, Any] = {
    "job_id": None,
    "status": "idle",
    "started_at": None,
    "finished_at": None,
    "communes": [],
    "error": None,
    "log": [],
}


def _internal_token() -> str:
    return (
        os.getenv("INTERNAL_TOKEN")
        or os.getenv("KERELIA_INTERNAL_AGENT_TOKEN")
        or os.getenv("INTERNAL_AGENT_TOKEN")
        or ""
    ).strip()


def _require_cron_token(
    x_cron_token: str | None,
    x_internal_token: str | None,
) -> None:
    expected = _internal_token()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="INTERNAL_TOKEN absent côté serveur — déclenchement refusé.",
        )
    provided = (x_cron_token or x_internal_token or "").strip()
    if not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="Non autorisé")


def _load_communes() -> list[dict[str, str]]:
    if not ETL_COMMUNES_JSON.is_file():
        raise FileNotFoundError(f"Config introuvable : {ETL_COMMUNES_JSON}")
    data = json.loads(ETL_COMMUNES_JSON.read_text(encoding="utf-8"))
    if not isinstance(data, list) or not data:
        raise ValueError(f"{ETL_COMMUNES_JSON} doit être un tableau JSON non vide")
    out = []
    for i, row in enumerate(data):
        schema = (row.get("schema") or "").strip()
        insee = (row.get("insee") or "").strip()
        if not schema or not insee:
            raise ValueError(f"Ligne {i}: schema et insee requis — {row}")
        if schema == "mios":
            continue
        out.append(
            {
                "schema": schema,
                "insee": insee,
                "label": (row.get("label") or schema).strip(),
            }
        )
    return out


def _append_log(message: str) -> None:
    with _lock:
        _JOB["log"].append(message)
    logger.info(message)


def _tail(path, n: int = 40) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(lines[-n:])


def _run_apply(job_id: str, commune: dict[str, str]) -> tuple[int, str]:
    """Lance --apply dans sa propre session (survit à un SIGTERM uvicorn)."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"cron_{job_id}_{commune['schema']}.log"
    cmd = [
        sys.executable,
        "-m",
        "api.veille_sig.cadastre.sync_or_add_parcelles",
        "--insee",
        commune["insee"],
        "--schema",
        commune["schema"],
        "--label",
        commune["label"],
        "--apply",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = (
        f"{BACKEND_ROOT}{os.pathsep}{env.get('PYTHONPATH', '')}"
    ).rstrip(os.pathsep)
    with log_path.open("w", encoding="utf-8") as logf:
        logf.write(f"$ {' '.join(cmd)}\n")
        logf.flush()
        proc = subprocess.Popen(
            cmd,
            cwd=str(BACKEND_ROOT),
            env=env,
            stdout=logf,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            rc = proc.wait(timeout=_COMMUNE_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=30)
            return 124, f"timeout {_COMMUNE_TIMEOUT_S}s — voir {log_path.name}"
    if rc != 0:
        return rc, _tail(log_path)
    return rc, log_path.name


def _run_veille_cadastrale(job_id: str) -> None:
    with _lock:
        if _JOB.get("job_id") != job_id:
            return
        _JOB["status"] = "running"
        _JOB["started_at"] = datetime.now(timezone.utc).isoformat()
        _JOB["error"] = None
        _JOB["log"] = []

    try:
        communes = _load_communes()
        with _lock:
            _JOB["communes"] = [
                {"schema": c["schema"], "insee": c["insee"], "status": "pending"}
                for c in communes
            ]
        _append_log(f"Veille cadastrale — {len(communes)} commune(s)")

        failures: list[str] = []
        for i, commune in enumerate(communes):
            label = f"{commune['label']} ({commune['insee']})"
            _append_log(f"[{i + 1}/{len(communes)}] {label} — apply")
            rc, detail = _run_apply(job_id, commune)
            if rc != 0:
                msg = f"{label} : exit {rc} — {detail}"
                failures.append(msg)
                _append_log(f"❌ {msg}")
                with _lock:
                    _JOB["communes"][i]["status"] = "error"
            else:
                _append_log(f"✅ {label} ({detail})")
                with _lock:
                    _JOB["communes"][i]["status"] = "done"

        with _lock:
            if failures:
                _JOB["status"] = "error"
                _JOB["error"] = f"{len(failures)} échec(s)"
            else:
                _JOB["status"] = "done"
                _JOB["error"] = None
    except Exception as exc:
        logger.exception("Veille cadastrale cron échouée")
        with _lock:
            _JOB["status"] = "error"
            _JOB["error"] = str(exc)
            _JOB["log"].append(f"❌ {exc}")
    finally:
        with _lock:
            _JOB["finished_at"] = datetime.now(timezone.utc).isoformat()


def _enqueue(job_id: str) -> None:
    threading.Thread(
        target=_run_veille_cadastrale,
        args=(job_id,),
        name=f"cadastre-veille-{job_id}",
        daemon=False,
    ).start()


def _job_public() -> dict[str, Any]:
    with _lock:
        return {
            "job_id": _JOB["job_id"],
            "status": _JOB["status"],
            "started_at": _JOB["started_at"],
            "finished_at": _JOB["finished_at"],
            "communes": list(_JOB["communes"]),
            "error": _JOB["error"],
            "log": list(_JOB["log"][-40:]),
        }


@router.post("/api/tasks/trigger-veille")
def trigger_veille(
    x_cron_token: str | None = Header(default=None, alias="X-Cron-Token"),
    x_internal_token: str | None = Header(default=None, alias="x-internal-token"),
):
    """cron-job.org : POST + header X-Cron-Token = INTERNAL_TOKEN Render."""
    _require_cron_token(x_cron_token, x_internal_token)

    with _lock:
        if _JOB["status"] == "running":
            snapshot = {
                "job_id": _JOB["job_id"],
                "status": _JOB["status"],
                "started_at": _JOB["started_at"],
                "finished_at": _JOB["finished_at"],
                "communes": list(_JOB["communes"]),
                "error": _JOB["error"],
                "log": list(_JOB["log"][-40:]),
            }
            return {
                "status": "already_running",
                "message": "Une veille cadastrale est déjà en cours.",
                "job": snapshot,
            }
        job_id = str(uuid.uuid4())[:8]
        _JOB.update(
            {
                "job_id": job_id,
                "status": "pending",
                "started_at": None,
                "finished_at": None,
                "communes": [],
                "error": None,
                "log": [],
            }
        )

    _enqueue(job_id)
    logger.info("[cadastre-veille cron job=%s] enqueued", job_id)
    return {
        "status": "success",
        "message": "Tâche de veille cadastrale démarrée en arrière-plan.",
        "job_id": job_id,
        "status_url": "/api/tasks/trigger-veille/status",
    }


@router.get("/api/tasks/trigger-veille/status")
def trigger_veille_status(
    x_cron_token: str | None = Header(default=None, alias="X-Cron-Token"),
    x_internal_token: str | None = Header(default=None, alias="x-internal-token"),
):
    _require_cron_token(x_cron_token, x_internal_token)
    return _job_public()
