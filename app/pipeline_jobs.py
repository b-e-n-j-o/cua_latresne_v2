"""Exécution asynchrone du pipeline parcelles (sous-processus) et mise à jour de JOBS."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

from app.state import JOBS

PROJECT_ROOT = Path(__file__).resolve().parent.parent


async def run_pipeline_from_parcelles_async(
    job_id: str,
    parcelles: list,
    code_insee: str,
    commune_nom: str | None,
    env: dict | None = None,
    out_dir: str | None = None,
    demandeur: dict | None = None,
):
    """Exécute le script pipeline_from_parcelles.py et met à jour JOBS pour suivi via polling."""
    try:
        print(
            f"🟢 [run_pipeline_from_parcelles] START job_id={job_id}, "
            f"parcelles={len(parcelles)}, insee={code_insee}, out_dir={out_dir}"
        )

        pipeline_script = (
            PROJECT_ROOT
            / "api"
            / "communes"
            / "latresne"
            / "cuas"
            / "INTERSECTIONS"
            / "pipeline_from_parcelles.py"
        )

        job = JOBS.get(job_id, {})
        job.update({
            "status": "running",
            "start_time": datetime.now().isoformat(),
            "filename": f"{len(parcelles)} parcelle(s)",
            "current_step": "unite_fonciere",
            "logs": [],
        })
        JOBS[job_id] = job

        if env is None:
            env = os.environ.copy()

        # Garde-fou Render/Supabase : en host pooler, forcer transaction mode (6543)
        # pour éviter MaxClientsInSessionMode si l'instance a conservé un ancien env.
        supabase_host = str(env.get("SUPABASE_HOST") or "").strip().strip('"').strip("'")
        supabase_port = str(env.get("SUPABASE_PORT") or "").strip().strip('"').strip("'")
        if "pooler.supabase.com" in supabase_host.lower():
            if not supabase_port or supabase_port == "5432":
                env["SUPABASE_PORT"] = "6543"
                print(
                    f"⚠️ [JOB {job_id}] SUPABASE_PORT absent/5432 sur pooler, "
                    "forçage automatique vers 6543."
                )

        print(
            f"🧪 [JOB {job_id}] DB env utilisé: host={supabase_host or '<missing>'}, "
            f"port={env.get('SUPABASE_PORT', '<missing>')}"
        )

        parcelles_json = json.dumps(parcelles)
        cmd = [
            "python3",
            str(pipeline_script),
            "--parcelles", parcelles_json,
            "--code-insee", code_insee,
        ]
        if commune_nom:
            cmd += ["--commune-nom", commune_nom]
        if out_dir:
            cmd += ["--out-dir", out_dir]
        if env.get("USER_ID"):
            cmd += ["--user-id", env["USER_ID"]]
        if env.get("USER_EMAIL"):
            cmd += ["--user-email", env["USER_EMAIL"]]
        if demandeur:
            demandeur_json = json.dumps(demandeur)
            cmd += ["--demandeur", demandeur_json]

        print(f"🚀 [JOB {job_id}] Lancement du pipeline parcelles : {' '.join(cmd[:5])}...")
        print(f"🔥 Pipeline démarré pour job {job_id}")

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(PROJECT_ROOT),
            env=env,
        )

        while True:
            if process.stdout.at_eof():
                break

            line_bytes = await process.stdout.readline()
            if not line_bytes:
                break

            line = line_bytes.decode().rstrip()
            print(f"[{job_id}] {line}")

            JOBS[job_id]["logs"].append(line)

            if "Vérification unité foncière" in line or "unite_fonciere" in line:
                JOBS[job_id]["current_step"] = "unite_fonciere"
            elif "Analyse des intersections" in line or "intersections" in line:
                JOBS[job_id]["current_step"] = "intersections"
            elif "Génération cartes" in line or "Génération CUA" in line:
                JOBS[job_id]["current_step"] = "generation_cua"
            elif "CUA DOCX généré" in line:
                JOBS[job_id]["current_step"] = "cua_pret"

            if "💥" in line:
                JOBS[job_id]["status"] = "error"
                JOBS[job_id]["error"] = line
                JOBS[job_id]["current_step"] = "error"

        returncode = await process.wait()
        print(f"🔥 Pipeline terminé pour job {job_id} (returncode={returncode})")
        JOBS[job_id]["returncode"] = returncode
        JOBS[job_id]["end_time"] = datetime.now().isoformat()

        if returncode == 0:
            JOBS[job_id]["status"] = "success"
            JOBS[job_id]["current_step"] = "done"

            out_dir = JOBS[job_id].get("out_dir")

            if out_dir:
                sub_result_file = Path(out_dir) / "sub_orchestrator_result.json"
                if sub_result_file.exists():
                    sub_result = json.loads(sub_result_file.read_text(encoding="utf-8"))
                    JOBS[job_id]["result_enhanced"] = sub_result
                    print(f"✅ [JOB {job_id}] Résultat enrichi avec sub_orchestrator_result.json")

                    slug = sub_result.get("slug")
                    if slug:
                        JOBS[job_id]["slug"] = slug
                        print(f"🎯 PATCH: slug injecté → {slug}")
                    else:
                        print("⚠️ PATCH: slug absent dans sub_orchestrator_result.json")
            else:
                print(f"⚠️ Aucun out_dir trouvé pour job {job_id}, fallback vers out_pipeline/")
                out_dirs = list((PROJECT_ROOT / "out_pipeline").glob("*"))
                if out_dirs:
                    latest_out = max(out_dirs, key=os.path.getmtime)
                    sub_result_file = latest_out / "sub_orchestrator_result.json"
                    if sub_result_file.exists():
                        sub_result = json.loads(sub_result_file.read_text(encoding="utf-8"))
                        JOBS[job_id]["result_enhanced"] = sub_result
                        slug = sub_result.get("slug")
                        if slug:
                            JOBS[job_id]["slug"] = slug
        else:
            JOBS[job_id]["status"] = "error"
            JOBS[job_id]["current_step"] = "error"

        print(f"🏁 [JOB {job_id}] Terminé avec statut : {JOBS[job_id]['status']}")

    except Exception as e:
        print(f"❌ Exception non bloquante: {e}")
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = str(e)
        JOBS[job_id]["current_step"] = "error"
        JOBS[job_id]["end_time"] = datetime.now().isoformat()
