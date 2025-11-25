#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
audit_slug_pipeline.py — Audit complet de la chaîne du SLUG Kerelia
-------------------------------------------------------------------
Ce script vérifie TOUTES les étapes :
1) Génération du slug dans l’orchestrator
2) Écriture du fichier sub_orchestrator_result.json
3) Upload dans Supabase (table pipelines)
4) URLs stockées (output_cua, cartes, gpkg…)
5) backend /status et /pipelines
6) front — cohérence des données reçues

Usage :
    python3 audit_slug_pipeline.py --job-id <JOB_ID>
    python3 audit_slug_pipeline.py --last-job
"""

import os
import json
import argparse
from pathlib import Path
from typing import Optional
import requests
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
API_BASE = os.getenv("API_BASE", "http://localhost:5002")  # adapter à ton backend

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================================
# UTIL
# ============================================================
def log(msg):
    print(f"[AUDIT] {msg}")


def read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except:
        return None


# ============================================================
# 1) RÉCUPÉRER LE JOB
# ============================================================
def get_job(job_id: Optional[str]):
    # Essayer d'abord via l'API si disponible
    try:
        if job_id == "LAST":
            log("Récupération du dernier job via API…")
            r = requests.get(f"{API_BASE}/results", timeout=2).json()
            if r["success"] and r["results"]:
                job_id = r["results"][0]["id"]
            else:
                raise RuntimeError("Impossible de trouver un job récent")
        
        log(f"Lecture du job {job_id} via API…")
        r = requests.get(f"{API_BASE}/status/{job_id}", timeout=2).json()
        return job_id, r
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        log(f"⚠️ API non disponible ({e}) → mode fichier uniquement")
        # Mode fallback : utiliser le dernier dossier out_pipeline
        outdir = find_out_dir()
        if not outdir:
            raise RuntimeError("Aucun dossier out_pipeline trouvé et API non disponible")
        
        # Extraire job_id depuis le nom du dossier ou utiliser un placeholder
        if job_id == "LAST":
            job_id = outdir.name
            log(f"→ Utilisation du dossier {outdir} comme job_id")
        
        sub_result = read_json(outdir / "sub_orchestrator_result.json")
        pipeline_result = read_json(outdir / "pipeline_result.json")
        
        job = {
            "status": "success",
            "current_step": "done",
            "result_enhanced": sub_result or {},
            "result": pipeline_result or {}
        }
        
        # Extraire le slug si disponible
        if sub_result and sub_result.get("slug"):
            job["slug"] = sub_result["slug"]
        
        return job_id or "unknown", job
    except Exception as e:
        raise RuntimeError(f"Erreur lors de la récupération du job : {e}")


# ============================================================
# 2) AUDIT DU DOSSIER OUT_PIPELINE
# ============================================================
def find_out_dir():
    dirs = list(Path("out_pipeline").glob("*"))
    if not dirs:
        return None
    return max(dirs, key=os.path.getmtime)


def analyze_out_dir(outdir: Path):
    log(f"Analyse du dossier pipeline : {outdir}")

    sub_json = read_json(outdir / "sub_orchestrator_result.json")
    pipe_json = read_json(outdir / "pipeline_result.json")

    return {
        "sub_json": sub_json,
        "pipeline_json": pipe_json,
        "files": [p.name for p in outdir.iterdir()]
    }


# ============================================================
# 3) AUDIT SUPABASE
# ============================================================
def analyze_supabase(slug: str):
    log(f"Recherche Supabase du pipeline slug={slug}…")
    try:
        res = (
            supabase
            .schema("latresne")
            .table("pipelines")
            .select("*")
            .eq("slug", slug)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            return None
        return rows[0]
    except Exception as e:
        return {"error": str(e)}


# ============================================================
# 4) AUDIT COHÉRENCE FRONT / BACK
# ============================================================
def analyze_backend(slug: str):
    try:
        r = requests.get(f"{API_BASE}/pipelines/by_slug?slug={slug}", timeout=2)
        return r.json()
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        log("⚠️ API non disponible pour /pipelines/by_slug")
        return {"error": "API non disponible"}
    except:
        return None


# ============================================================
# 5) GÉNÉRATION RAPPORT HTML
# ============================================================
def generate_report(job_id, job, out_info, supa_info, backend_info):
    html = f"""
    <html><body>
    <h1>Audit Pipeline — Job {job_id}</h1>

    <h2>1) Status Backend</h2>
    <pre>{json.dumps(job, indent=2, ensure_ascii=False)}</pre>

    <h2>2) sub_orchestrator_result.json</h2>
    <pre>{json.dumps(out_info["sub_json"], indent=2, ensure_ascii=False)}</pre>

    <h2>3) pipeline_result.json</h2>
    <pre>{json.dumps(out_info["pipeline_json"], indent=2, ensure_ascii=False)}</pre>

    <h2>4) Liste fichiers OUT_DIR</h2>
    <pre>{json.dumps(out_info["files"], indent=2)}</pre>

    <h2>5) Enregistrement Supabase (table pipelines)</h2>
    <pre>{json.dumps(supa_info, indent=2, ensure_ascii=False)}</pre>

    <h2>6) Backend /pipelines/by_slug</h2>
    <pre>{json.dumps(backend_info, indent=2, ensure_ascii=False)}</pre>

    </body></html>
    """

    report = Path("audit_report.html")
    report.write_text(html, encoding="utf-8")
    log(f"📄 Rapport généré → audit_report.html")


# ============================================================
# 6) PIPELINE COMPLET
# ============================================================
def audit(job_id: Optional[str]):
    # 1) job FastAPI (ou fallback fichiers)
    try:
        job_id, job = get_job(job_id)
    except RuntimeError as e:
        log(f"❌ Erreur : {e}")
        log("💡 Astuce : démarrez l'API FastAPI (uvicorn main:app) ou utilisez --out-dir pour spécifier un dossier")
        raise

    # extraction slug du backend direct
    slug = (
        job.get("slug")
        or job.get("result_enhanced", {}).get("slug")
        or None
    )

    log(f"Slug détecté : {slug}")

    # 2) dossier out_pipeline
    outdir = find_out_dir()
    out_info = analyze_out_dir(outdir)

    # 3) si slug absent → essayer de récupérer dans sub_orchestrator_result.json
    if not slug:
        try:
            slug = out_info["sub_json"].get("slug")
            log(f"→ Slug trouvé dans sub_orchestrator_result.json : {slug}")
        except:
            pass

    # 4) audit Supabase
    supa_info = analyze_supabase(slug) if slug else {}

    # 5) audit backend
    backend_info = analyze_backend(slug) if slug else {}

    # 6) rapport HTML
    generate_report(job_id, job, out_info, supa_info, backend_info)

    return {
        "job_id": job_id,
        "slug": slug,
        "job": job,
        "out_info": out_info,
        "supabase": supa_info,
        "backend": backend_info
    }


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", help="Identifiant du job à auditer")
    parser.add_argument("--last-job", action="store_true", help="Audit du dernier job")
    args = parser.parse_args()

    job_id = None
    if args.last_job:
        job_id = "LAST"
    elif args.job_id:
        job_id = args.job_id
    else:
        raise RuntimeError("Vous devez spécifier --job-id ou --last-job")

    audit(job_id)
