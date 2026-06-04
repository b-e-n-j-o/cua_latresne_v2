"""
Pipeline ETL commune : parcelles → BAN → enrichissement (+ Slack).
Utilisé par run_etl_commune.py (CLI) et router_etl_commune.py (Render).
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests

from services.ingestion_cadastre.env_loader import load_project_env
from services.ingestion_cadastre.etl_slack import notify_etl_complete
from services.ingestion_cadastre.etl_stats import fetch_post_etl_stats

log = logging.getLogger(__name__)

HERE = Path(__file__).resolve().parent
DEFAULT_BACKEND_URL = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("BACKEND_URL") or "https://api.kerelia.fr"
POLL_INTERVAL_S = 5
POLL_TIMEOUT_S = 3600

SYNC_ETALAB_SCRIPT = Path(
    os.getenv("SYNC_ETALAB_SCRIPT", str(HERE / "sync_or_add_parcelles.py"))
)


@dataclass
class EtlConfig:
    schema: str
    insee: str
    parcelles_mode: str = "etalab"
    backend_url: str = DEFAULT_BACKEND_URL
    internal_token: Optional[str] = None
    force_parcelles: bool = False
    skip_ban: bool = False
    skip_enrich: bool = False
    dry_run: bool = False
    no_slack: bool = False


def sanitize_schema(schema: str) -> str:
    s = schema.strip().lower()
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", s):
        raise ValueError("Schéma invalide.")
    return s


def sanitize_insee(code: str) -> str:
    c = code.strip().upper()
    if not re.fullmatch(r"[0-9A-Z]{5}", c):
        raise ValueError("Code INSEE invalide.")
    return c


def internal_auth_headers(token: str | None = None) -> dict[str, str]:
    t = (token or os.getenv("INTERNAL_TOKEN") or "").strip()
    if not t:
        return {}
    return {"x-internal-token": t}


def run_script(script: Path, args: list[str], dry_run: bool) -> None:
    if not script.is_file():
        raise FileNotFoundError(f"Script introuvable : {script}")
    cmd = [sys.executable, str(script), *args]
    if dry_run and script.name == "ingest_ban_adresse_et_lien_parcelles.py":
        cmd.append("--dry-run")
    elif dry_run and script.name == "enrichir_parcelles_adresses.py":
        cmd.append("--dry-run")
    log.info("→ %s", " ".join(cmd))
    result = subprocess.run(cmd, cwd=str(HERE), check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Échec {script.name} (code {result.returncode})")


def trigger_parcelles_sync_api(
    backend_url: str,
    insee: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    internal_token: str | None = None,
) -> str:
    headers = internal_auth_headers(internal_token)
    url = f"{backend_url.rstrip('/')}/admin/parcelles/sync"
    payload = {"communes": [insee], "dry_run": dry_run, "force": force}
    log.info("API parcelles sync : POST %s", url)
    r = requests.post(url, json=payload, headers=headers, timeout=120)
    r.raise_for_status()
    job_id = r.json().get("job_id")
    if not job_id:
        raise RuntimeError(f"Réponse API sans job_id : {r.json()}")
    if dry_run:
        return job_id
    status_url = f"{backend_url.rstrip('/')}/admin/parcelles/sync/status/{job_id}"
    deadline = time.time() + POLL_TIMEOUT_S
    while time.time() < deadline:
        sr = requests.get(status_url, headers=headers, timeout=60)
        sr.raise_for_status()
        job = sr.json()
        status = job.get("status")
        if status == "error":
            raise RuntimeError(f"Job sync en erreur : {job}")
        if status in ("done", "completed", "finished"):
            return job_id
        time.sleep(POLL_INTERVAL_S)
    raise TimeoutError(f"Timeout sync parcelles job {job_id}")


def load_parcelles_diff_json(insee: str, schema: str) -> Optional[dict[str, Any]]:
    path = HERE / f"diff_parcelles_{insee}_{schema}_parcelles.json"
    if not path.is_file():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def step_parcelles(
    cfg: EtlConfig,
    *,
    defer_slack: bool = True,
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    schema, insee = cfg.schema, cfg.insee
    log.info("=== [1/3] Parcelles (mode=%s) ===", cfg.parcelles_mode)

    if cfg.parcelles_mode == "skip":
        return None, "Parcelles : étape ignorée"

    if cfg.parcelles_mode == "api":
        trigger_parcelles_sync_api(
            cfg.backend_url,
            insee,
            force=cfg.force_parcelles,
            dry_run=cfg.dry_run,
            internal_token=cfg.internal_token,
        )
        if cfg.dry_run:
            return None, "Parcelles : sync API dry-run"
        run_script(
            HERE / "sync_parcelles_vers_schema.py",
            ["--schema", schema, "--insee", insee],
            cfg.dry_run,
        )
        return None, f"Parcelles : sync API + copie → {schema}.parcelles"

    if cfg.parcelles_mode == "etalab":
        if not SYNC_ETALAB_SCRIPT.is_file():
            raise FileNotFoundError(f"sync_or_add_parcelles introuvable : {SYNC_ETALAB_SCRIPT}")
        args = ["--insee", insee, "--schema", schema]
        args.append("--dry-run" if cfg.dry_run else "--insert")
        if defer_slack:
            args.append("--no-slack")
        run_script(SYNC_ETALAB_SCRIPT, args, cfg.dry_run)
        return load_parcelles_diff_json(insee, schema), None

    if cfg.parcelles_mode == "schema-only":
        run_script(
            HERE / "sync_parcelles_vers_schema.py",
            ["--schema", schema, "--insee", insee],
            cfg.dry_run,
        )
        return None, f"Parcelles : copie nationale → {schema}.parcelles"

    raise ValueError(f"Mode parcelles inconnu : {cfg.parcelles_mode}")


def execute_etl(cfg: EtlConfig) -> dict[str, Any]:
    """Exécute le pipeline complet. Lève en cas d'échec."""
    load_project_env()
    cfg.schema = sanitize_schema(cfg.schema)
    cfg.insee = sanitize_insee(cfg.insee)

    commune_label = cfg.schema.replace("_", " ").title()
    log.info("=== ETL commune : %s (INSEE %s) ===", cfg.schema, cfg.insee)
    t0 = time.time()

    parcelles_result, parcelles_note = step_parcelles(cfg)
    if not cfg.skip_ban:
        log.info("=== [2/3] BAN ===")
        run_script(
            HERE / "ingest_ban_adresse_et_lien_parcelles.py",
            ["--schema", cfg.schema],
            cfg.dry_run,
        )
    if not cfg.skip_enrich:
        log.info("=== [3/3] Enrichissement ===")
        run_script(
            HERE / "enrichir_parcelles_adresses.py",
            ["--schema", cfg.schema],
            cfg.dry_run,
        )

    elapsed = time.time() - t0
    log.info("=== ETL terminé en %.0f s ===", elapsed)

    if not cfg.no_slack and not cfg.dry_run:
        ban_stats = enrich_stats = None
        try:
            ban_stats, enrich_stats = fetch_post_etl_stats(
                cfg.schema,
                include_ban=not cfg.skip_ban,
                include_enrich=not cfg.skip_enrich,
            )
        except Exception as e:
            log.warning("Stats Slack : %s", e)
        if parcelles_result:
            commune_label = parcelles_result.get("commune") or commune_label
        notify_etl_complete(
            schema=cfg.schema,
            insee=cfg.insee,
            commune_label=commune_label,
            parcelles_result=parcelles_result,
            parcelles_note=parcelles_note,
            ban_stats=ban_stats,
            enrich_stats=enrich_stats,
            elapsed_s=elapsed,
            dry_run=False,
        )

    return {
        "schema": cfg.schema,
        "insee": cfg.insee,
        "elapsed_s": round(elapsed, 1),
        "parcelles_mode": cfg.parcelles_mode,
    }
