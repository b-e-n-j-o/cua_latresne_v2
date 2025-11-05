from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from pathlib import Path
from datetime import datetime
import subprocess
import uuid
import json
import os

from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="Kerelia CUA API", version="2.1")



app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://kerelia.fr"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dictionnaire global pour le suivi des jobs
JOBS = {}

# ============================================================
# 🔧 Fonction d’exécution du pipeline (tâche asynchrone)
# ============================================================

def run_pipeline(job_id: str, pdf_path: Path, code_insee: str | None):
    """Exécute le pipeline complet en tâche de fond, avec logs live + sauvegarde."""
    BASE_DIR = Path(__file__).resolve().parent
    ORCHESTRATOR = BASE_DIR / "orchestrator_global.py"

    # Prépare les infos du job
    out = {
        "status": "running",
        "start_time": datetime.now().isoformat(),
        "pdf": pdf_path.name,
        "code_insee": code_insee,
        "logs": [],  # on conserve les lignes de logs ici
    }
    JOBS[job_id] = out

    try:
        # Commande du pipeline global
        cmd = ["python3", str(ORCHESTRATOR), "--pdf", str(pdf_path)]
        if code_insee:
            cmd.extend(["--code-insee", code_insee])

        print(f"🚀 [JOB {job_id}] Lancement du pipeline : {' '.join(cmd)}")

        # Exécution avec affichage progressif
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # affichage ligne par ligne
        )

        for line in process.stdout:
            print(f"[{job_id}] {line}", end="")  # affichage live dans le terminal
            out["logs"].append(line.strip())

        process.wait(timeout=1800)
        out["returncode"] = process.returncode

        # On vérifie la sortie pipeline
        out_dirs = list((BASE_DIR / "out_pipeline").glob("*"))
        if out_dirs:
            latest_out = max(out_dirs, key=os.path.getmtime)
            result_file = latest_out / "pipeline_result.json"

            if result_file.exists():
                result_json = json.loads(result_file.read_text(encoding="utf-8"))
                out["result"] = result_json
                out["status"] = "success" if process.returncode == 0 else "error"
            else:
                out["status"] = "error"
                out["error"] = "Pipeline terminé mais aucun résultat trouvé."
        else:
            out["status"] = "error"
            out["error"] = "Aucun dossier out_pipeline trouvé."

    except subprocess.TimeoutExpired:
        out["status"] = "timeout"
        out["error"] = "⏱️ Pipeline > 30 min"
        out["logs"].append("⚠️ Pipeline arrêté pour dépassement de temps.")
    except Exception as e:
        out["status"] = "error"
        out["error"] = str(e)
        out["logs"].append(f"❌ Erreur interne : {e}")
    finally:
        if pdf_path.exists():
            pdf_path.unlink()
        out["end_time"] = datetime.now().isoformat()
        JOBS[job_id] = out
        print(f"✅ [JOB {job_id}] Terminé avec statut : {out['status']}")

# ============================================================
# 🚀 Endpoint principal : lancement du pipeline
# ============================================================

@app.post("/analyze-cerfa")
async def analyze_cerfa(
    background_tasks: BackgroundTasks,
    pdf: UploadFile = File(...),
    code_insee: str = Form(None),
):
    """Lance le pipeline complet (CERFA → UF → Intersections → CUA)."""
    job_id = str(uuid.uuid4())
    temp_pdf = Path(f"/tmp/cerfa_{job_id}.pdf")

    with open(temp_pdf, "wb") as f:
        f.write(await pdf.read())

    JOBS[job_id] = {
        "status": "queued",
        "created_at": datetime.now().isoformat(),
        "filename": pdf.filename,
    }

    background_tasks.add_task(run_pipeline, job_id, temp_pdf, code_insee)

    return {"success": True, "job_id": job_id}


# ============================================================
# 🔍 Endpoint de suivi : état du job
# ============================================================

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    """Retourne l’état d’un job et ses résultats éventuels."""
    job = JOBS.get(job_id)
    if not job:
        return {"success": False, "error": "Job introuvable"}
    return job


# ============================================================
# 🗂️ Endpoint : derniers résultats
# ============================================================

@app.get("/results")
async def list_results(limit: int = 10):
    """
    Retourne les N derniers jobs terminés (success, error ou timeout).
    Utile pour afficher l’historique des CUA dans ton interface.
    """
    # Filtrer les jobs terminés
    finished_jobs = [
        {"id": job_id, **data}
        for job_id, data in JOBS.items()
        if data.get("status") in {"success", "error", "timeout"}
    ]

    # Trier par date de fin (desc)
    finished_jobs.sort(key=lambda j: j.get("end_time", ""), reverse=True)

    # Limiter le nombre de résultats
    return {
        "success": True,
        "count": len(finished_jobs),
        "results": finished_jobs[:limit],
    }


# ============================================================
# ✅ Endpoint de test / santé
# ============================================================

@app.get("/health")
async def health_check():
    """Vérifie que l’API est en ligne."""
    return {"status": "ok", "message": "Kerelia API opérationnelle 🚀"}


# ============================================================
# 🧾 ENDPOINT 2 — DERNIERS PIPELINES (table latresne.pipelines)
# ============================================================

@app.get("/pipelines/latest")
def get_latest_pipelines(limit: int = 10):
    """
    Récupère les derniers pipelines enregistrés pour Latresne depuis Supabase.
    """
    try:
        response = (
            supabase
            .table("latresne.pipelines")
            .select("*")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )

        pipelines = response.data or []
        return {
            "success": True,
            "count": len(pipelines),
            "pipelines": pipelines
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================