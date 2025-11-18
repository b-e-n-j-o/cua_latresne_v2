from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr
from pathlib import Path
from datetime import datetime
import uuid
import json
import os
import base64
import asyncio
import mammoth
from io import BytesIO

from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client
from dotenv import load_dotenv
from CERFA_ANALYSE.auth_utils import get_user_insee_list
from utils.email_utils import send_internal_email

from admin_routes import router as admin_router
from cua_routes import router as cua_router
import tempfile
import pypandoc

from fastapi import WebSocket, WebSocketDisconnect
from websocket_manager import ws_manager
from CERFA_ANALYSE.pre_analyse_cerfa import pre_analyse_cerfa
from CERFA_ANALYSE.analyse_gemini import analyse_cerfa


from websocket_endpoints import router as ws_router


# ============================================================
# 🔧 CONFIGURATION
# ============================================================
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# ✅ Un seul client global (cible les schémas via .schema())
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

import cua_routes
cua_routes.supabase = supabase

app = FastAPI(title="Kerelia CUA API", version="2.1")
# ============================================================
# 📩 Modèle Lead + endpoint de capture
# ============================================================

app.include_router(ws_router)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://kerelia.fr",
        "https://*.vercel.app",
        "http://localhost:5173",
        "http://localhost:3000",
        "https://www.kerelia.fr",
        "https://www.kerelia.fr/*"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dictionnaire global pour le suivi des jobs
JOBS = {}

app.include_router(admin_router)
app.include_router(cua_router)
# ============================================================
# 🔧 Fonction d'exécution du pipeline (tâche asynchrone)
# ============================================================

async def run_pipeline(job_id: str, pdf_path: Path, code_insee: str | None, env: dict | None = None):
    """Exécute le pipeline global et diffuse les logs en temps réel via WebSocket."""
    
    BASE_DIR = Path(__file__).resolve().parent
    ORCHESTRATOR = BASE_DIR / "orchestrator_global.py"

    JOBS[job_id] = {
        "status": "running",
        "start_time": datetime.now().isoformat(),
        "filename": pdf_path.name,
        "current_step": "queued",
        "logs": []
    }

    if env is None:
        env = os.environ.copy()

    cmd = ["python3", str(ORCHESTRATOR), "--pdf", str(pdf_path)]
    if code_insee:
        cmd += ["--code-insee", code_insee]

    print(f"🚀 [JOB {job_id}] Lancement du pipeline : {' '.join(cmd)}")
    print(f"👤 [JOB {job_id}] USER_ID={env.get('USER_ID')} USER_EMAIL={env.get('USER_EMAIL')}")

    # 📌 On lance le pipeline en mode asyncio
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(BASE_DIR),
        env=env
    )

    # 📌 Lecture ligne par ligne
    while True:
        if process.stdout.at_eof():
            break

        line_bytes = await process.stdout.readline()
        if not line_bytes:
            break

        line = line_bytes.decode().rstrip()
        print(f"[{job_id}] {line}")

        JOBS[job_id]["logs"].append(line)

        # 🟩 BROADCAST à TOUS LES CLIENTS WebSocket
        await ws_manager.broadcast(job_id, {
            "event": "log",
            "message": line
        })

        # 🟦 Détection d'étapes
        if "Analyse du CERFA" in line:
            JOBS[job_id]["current_step"] = "analyse_cerfa"
            await ws_manager.broadcast(job_id, {
                "step": "analyse_cerfa",
                "event": "step"
            })
        elif "Unité foncière" in line:
            JOBS[job_id]["current_step"] = "verification_unite_fonciere"
            await ws_manager.broadcast(job_id, {
                "step": "verification_unite_fonciere",
                "event": "step"
            })
        elif "Rapport d'intersection" in line:
            JOBS[job_id]["current_step"] = "intersections"
            await ws_manager.broadcast(job_id, {
                "step": "intersections",
                "event": "step"
            })
        elif "CUA" in line or "carte" in line:
            JOBS[job_id]["current_step"] = "generation_cua"
            await ws_manager.broadcast(job_id, {
                "step": "generation_cua",
                "event": "step"
            })

        # 🔥 Détection des erreurs explicites
        if "Utilisateur non autorisé à analyser la commune" in line:
            JOBS[job_id]["status"] = "error"
            JOBS[job_id]["error"] = line
            JOBS[job_id]["current_step"] = "error"
        if line.startswith("❌") or line.startswith("💥"):
            JOBS[job_id]["status"] = "error"
            JOBS[job_id]["error"] = line
            JOBS[job_id]["current_step"] = "error"

    # 📌 Fin du pipeline
    returncode = await process.wait()
    JOBS[job_id]["returncode"] = returncode
    JOBS[job_id]["end_time"] = datetime.now().isoformat()

    # On vérifie la sortie pipeline
    out_dirs = list((BASE_DIR / "out_pipeline").glob("*"))
    if out_dirs:
        latest_out = max(out_dirs, key=os.path.getmtime)
        result_file = latest_out / "pipeline_result.json"

        if result_file.exists():
            result_json = json.loads(result_file.read_text(encoding="utf-8"))
            JOBS[job_id]["result"] = result_json
            JOBS[job_id]["status"] = "success" if returncode == 0 else "error"
            
            # ✅ Intégration du résultat du sous-orchestrateur (cartes + CUA)
            sub_result_file = latest_out / "sub_orchestrator_result.json"
            if sub_result_file.exists():
                JOBS[job_id]["current_step"] = "generation_cua"
                print(f"🧾 [JOB {job_id}] Étape : génération du certificat CUA")
                sub_result = json.loads(sub_result_file.read_text(encoding="utf-8"))
                JOBS[job_id]["result_enhanced"] = sub_result
                print(f"✅ [JOB {job_id}] Résultat enrichi avec sub_orchestrator_result.json")
        else:
            JOBS[job_id]["status"] = "error"
            JOBS[job_id]["error"] = "Pipeline terminé mais aucun résultat trouvé."
    else:
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = "Aucun dossier out_pipeline trouvé."

    if JOBS[job_id].get("status") == "success":
        JOBS[job_id]["current_step"] = "done"
        await ws_manager.broadcast(job_id, {
            "status": "success",
            "event": "done"
        })
    else:
        JOBS[job_id]["current_step"] = "error"
        await ws_manager.broadcast(job_id, {
            "status": "error",
            "event": "error",
            "error": JOBS[job_id].get("error", "Erreur inconnue")
        })

    # Nettoyage
    if pdf_path.exists():
        pdf_path.unlink()

    print(f"🏁 [JOB {job_id}] Terminé avec statut : {JOBS[job_id]['status']}")

# ============================================================
# 🚀 Endpoint principal : lancement du pipeline
# ============================================================

@app.post("/analyze-cerfa")
async def analyze_cerfa(
    pdf: UploadFile = File(...),
    code_insee: str = Form(None),
    user_id: str = Form(None),
    user_email: str = Form(None),
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
        "user_id": user_id,
        "user_email": user_email,
    }

# ============================================================
# 🔐 Vérification des droits utilisateur
# ============================================================
    user_insee_list = get_user_insee_list(user_id) if user_id else []
    print(f"👤 Droits utilisateur {user_email}: {user_insee_list or 'toutes communes'}")

    if user_insee_list and code_insee and code_insee not in user_insee_list:
        raise HTTPException(
            status_code=403,
            detail=(
                "Accès refusé : vous n'êtes autorisé qu'à analyser "
                f"les communes suivantes : {', '.join(user_insee_list)}"
            )
        )

    # 🧠 Transmission au sous-processus (via variables d'environnement)
    env = os.environ.copy()
    if user_id:
        env["USER_ID"] = user_id
    if user_email:
        env["USER_EMAIL"] = user_email

    # 🔥 Lancement du pipeline en tâche asynchrone
    asyncio.create_task(run_pipeline(job_id, temp_pdf, code_insee, env))

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
            .schema("latresne")
            .table("pipelines")
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
# 🔍 ENDPOINT 3 — RETROUVER UN PIPELINE PAR SLUG
# ============================================================

@app.get("/pipelines/by_slug")
def get_pipeline_by_slug(slug: str):
    """
    Retrouve un pipeline spécifique à partir de son slug unique.
    Utile pour afficher les détails d'un CUA depuis le lien court.
    """
    try:
        response = (
            supabase
            .schema("latresne")
            .table("pipelines")
            .select("*")
            .eq("slug", slug)
            .limit(1)
            .execute()
        )

        rows = response.data or []
        if not rows:
            return {
                "success": False,
                "error": "Slug introuvable"
            }
        
        return {
            "success": True,
            "pipeline": rows[0]
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================
# 👤 ENDPOINT 4 — PIPELINES D'UN UTILISATEUR
# ============================================================

@app.get("/pipelines/by_user")
def get_pipelines_by_user(user_id: str, limit: int = 15):
    """
    Récupère les pipelines d'un utilisateur spécifique.
    Utile pour afficher l'historique personnel dans l'interface.
    """
    try:
        response = (
            supabase
            .schema("latresne")
            .table("pipelines")
            .select("*")
            .eq("user_id", user_id)
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

# ============================================================
# 🧠 ENDPOINT DEBUG — TEST SUPABASE (latresne + public)
# ============================================================

@app.get("/debug/supabase")
def debug_supabase():
    """
    Vérifie la connectivité à Supabase et l'accès aux schémas latresne + public.
    Retourne un petit résumé des lignes accessibles dans les tables clés.
    """
    try:
        print("🧩 [DEBUG] Vérification connexion Supabase...")
        
        # Test 1 : latresne.pipelines
        res_latresne = (
            supabase
            .schema("latresne")
            .table("pipelines")
            .select("id, slug, created_at")
            .limit(3)
            .execute()
        )
        nb_latresne = len(res_latresne.data or [])
        print(f"✅ [DEBUG] latresne.pipelines OK — {nb_latresne} ligne(s) visibles")

        # Test 2 : public.shortlinks
        res_public = (
            supabase
            .schema("public")
            .table("shortlinks")
            .select("slug, target_url, created_at")
            .limit(3)
            .execute()
        )
        nb_public = len(res_public.data or [])
        print(f"✅ [DEBUG] public.shortlinks OK — {nb_public} ligne(s) visibles")

        return {
            "status": "ok",
            "latresne": {
                "rows": nb_latresne,
                "examples": res_latresne.data
            },
            "public": {
                "rows": nb_public,
                "examples": res_public.data
            }
        }

    except Exception as e:
        print(f"💥 [DEBUG] Erreur connexion Supabase : {e}")
        return {
            "status": "error",
            "details": str(e)
        }




@app.post("/auth/generate-reset-token")
def generate_reset_token(email: str = Body(..., embed=True)):
    """
    Génère un token de réinitialisation Supabase à la demande.
    Utilisé pour permettre qu'un lien d'email n'expire jamais.
    """
    import requests

    url = f"{SUPABASE_URL}/auth/v1/admin/generate_link"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "type": "recovery",
        "email": email
    }

    r = requests.post(url, headers=headers, json=payload)
    data = r.json()

    if "action_link" not in data:
        print("❌ Erreur generate_link :", data)
        raise HTTPException(500, "Impossible de générer un token Supabase")

    # Lien final vers la page officielle Supabase pour définir un mot de passe
    return {"reset_url": data["action_link"]}


class Lead(BaseModel):
    email: EmailStr
    need: str
    profile: str | None = None
    commune: str
    parcelle: str | None = None
    message: str | None = None


@app.post("/lead")
async def receive_lead(payload: dict):
    try:
        supabase.table("leads").insert({
            "profile": payload.get("profile"),
            "email": payload.get("email"),
            "commune": payload.get("commune"),
            "parcelle": payload.get("parcelle"),
            "message": payload.get("message"),
        }).execute()

        # 👉 Appel à ton module SendGrid
        send_internal_email(payload)

        return {"status": "ok"}

    except Exception as e:
        print("❌ Erreur /lead:", e)
        raise HTTPException(status_code=500, detail="Erreur serveur")

