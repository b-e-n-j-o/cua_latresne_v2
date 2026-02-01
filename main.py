from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any
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
from utils.email_utils import send_internal_email, send_password_reset_email

from admin_routes import router as admin_router
from cua_routes import router as cua_router
import tempfile
import pypandoc
import requests
import google.generativeai as genai


from rag.rag_routes import router as rag_router
from rag.rag_routes_plu import router as rag_plu_router
from rag.rag_routes_meta import router as rag_meta_router
from rag.cag_plu_routes import router as cag_plu_router
from rag.rag_routes_parallel import router as rag_parallel_router



# from api.plui import router as plui_router
# from api.plui_tiles import router as plui_tiles_router
from api.communes import router as communes_router
from api.departements import router as departements_router
from api.tiles_generic import router as tiles_router
from api.parcelle_et_voisins import router as parcelle_router
from api.parcelle_geometrie import router as parcelle_geometrie_router
from api.topography_consolidated import router as topo_router
from api.generate_dpe import router as dpe_router

from api.plu.fetch_plu import router as plu_router
from api.plu.chat import router as chat_router
from api.tiles_mbtiles import router as mbtiles_router
from api.tiles_mbtiles_parcelles import router as tiles_parcelles
from api.identite_parcelle.zonage_plui import router as zonage_plui_router



from api.identite_parcelle.route_identite_parcelle import router as identite_parcelle_router

from api.latresne.tiles_latresne import router as latresne_router
from api.latresne.tiles_mbtiles import router as latresne_mbtiles_router
from api.latresne.patrimoine import router as patrimoine_router

from routers.cerfa import router as cerfa_router
from services.history.centroid_history import router as centroid_history_router
from services.history.suivi import router as suivi_router






import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)




# ============================================================
# 🔧 CONFIGURATION
# ============================================================
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# ✅ Un seul client global (cible les schémas via .schema())
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

import cua_routes
import services.history.centroid_history as centroid_history_module
import services.history.suivi as suivi_module
cua_routes.supabase = supabase
centroid_history_module.supabase = supabase
suivi_module.supabase = supabase

app = FastAPI(title="Kerelia CUA API", version="2.1")
# ============================================================
# 📩 Modèle Lead + endpoint de capture
# ============================================================


@app.on_event("startup")
async def log_routes():
    print("\n=== ROUTES DISPONIBLES ===")
    for route in app.routes:
        print(route.path, route.methods)
    print("=========================\n")




app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "https://www.kerelia.fr",
        "https://kerelia.fr",
        # Ajoutez vos domaines Vercel si besoin
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dictionnaire global pour le suivi des jobs
JOBS = {}

app.include_router(admin_router)
app.include_router(cua_router)
app.include_router(rag_router)
app.include_router(rag_plu_router)
app.include_router(rag_meta_router)
app.include_router(cag_plu_router)
app.include_router(rag_parallel_router)

# app.include_router(plui_router)
# app.include_router(plui_tiles_router)
app.include_router(communes_router)
app.include_router(departements_router)
app.include_router(mbtiles_router)
app.include_router(tiles_router)
app.include_router(parcelle_router)
app.include_router(topo_router)
app.include_router(dpe_router)
app.include_router(plu_router)
app.include_router(chat_router)
app.include_router(identite_parcelle_router)
app.include_router(zonage_plui_router)

# endpoint pour les couches latresne
app.include_router(latresne_router)
app.include_router(latresne_mbtiles_router)
app.include_router(patrimoine_router)
app.include_router(parcelle_geometrie_router)
app.include_router(tiles_parcelles)

app.include_router(cerfa_router)
app.include_router(centroid_history_router)
app.include_router(suivi_router)




# ============================================================
# 🔧 Fonction d'exécution du pipeline (tâche asynchrone)
# ============================================================

async def run_pipeline(job_id: str, pdf_path: Path, code_insee: str | None, env: dict | None = None, out_dir: str | None = None):
    """Exécute le pipeline global et met à jour JOBS pour suivi via polling."""
    try:
        print(f"🟢 [run_pipeline] START job_id={job_id}, pdf={pdf_path}, insee={code_insee}, out_dir={out_dir}")
        
        BASE_DIR = Path(__file__).resolve().parent
        ORCHESTRATOR = BASE_DIR / "orchestrator_global.py"

        # 🔴 NE PLUS RÉÉCRIRE ENTIÈREMENT JOBS[job_id] (préserve out_dir, user_id, etc.)
        job = JOBS.get(job_id, {})
        job.update({
            "status": "running",
            "start_time": datetime.now().isoformat(),
            "filename": pdf_path.name,
            "current_step": "analyse_cerfa",
            "logs": [],
        })
        JOBS[job_id] = job

        if env is None:
            env = os.environ.copy()

        cmd = ["python3", str(ORCHESTRATOR), "--pdf", str(pdf_path)]
        if code_insee:
            cmd += ["--code-insee", code_insee]
        if out_dir:
            cmd += ["--out-dir", out_dir]  # ← NOUVEAU : passer le OUT_DIR à l'orchestrateur

        print(f"🚀 [JOB {job_id}] Lancement du pipeline : {' '.join(cmd)}")
        print(f"👤 [JOB {job_id}] USER_ID={env.get('USER_ID')} USER_EMAIL={env.get('USER_EMAIL')}")
        print(f"🔥 Pipeline démarré pour job {job_id}")

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

            # 🟦 Détection d'étapes (version robuste avec plusieurs mots-clés)
            # 1) Fin analyse CERFA → passage UF
            if "cerfa_result.json" in line:
                JOBS[job_id]["current_step"] = "unite_fonciere"
            # 2) Début génération carte
            elif "Génération carte depuis WKT" in line:
                JOBS[job_id]["current_step"] = "cartes"
            # 3) CUA généré
            elif "CUA DOCX généré" in line:
                JOBS[job_id]["current_step"] = "cua_pret"
            # Détections de fallback (anciennes)
            elif "Analyse du CERFA" in line or "📄 Analyse du CERFA" in line:
                JOBS[job_id]["current_step"] = "analyse_cerfa"
            elif "Unité foncière" in line or "rapport_unite_fonciere" in line:
                JOBS[job_id]["current_step"] = "unite_fonciere"
            elif "Rapport d'intersection" in line or "Analyse des intersections" in line:
                JOBS[job_id]["current_step"] = "intersections"
            elif "Génération CUA" in line or "Génération du CUA" in line:
                JOBS[job_id]["current_step"] = "generation_cua"

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
        print(f"🔥 Pipeline terminé pour job {job_id} (returncode={returncode})")
        JOBS[job_id]["returncode"] = returncode
        JOBS[job_id]["end_time"] = datetime.now().isoformat()

        # Vérification du returncode pour définir le statut et current_step
        if returncode == 0:
            JOBS[job_id]["status"] = "success"
            JOBS[job_id]["current_step"] = "done"
        else:
            JOBS[job_id]["status"] = "error"
            JOBS[job_id]["current_step"] = "error"

        # On vérifie la sortie pipeline pour enrichir les résultats (sans écraser le statut)
        out_dir = JOBS[job_id].get("out_dir")
        
        if out_dir:
            result_file = Path(out_dir) / "pipeline_result.json"
            sub_result_file = Path(out_dir) / "sub_orchestrator_result.json"

            if result_file.exists():
                result_json = json.loads(result_file.read_text(encoding="utf-8"))
                JOBS[job_id]["result"] = result_json
                
            # ✅ Intégration du résultat du sous-orchestrateur (cartes + CUA)
            if sub_result_file.exists():
                print(f"🧾 [JOB {job_id}] Étape : génération du certificat CUA")
                sub_result = json.loads(sub_result_file.read_text(encoding="utf-8"))
                JOBS[job_id]["result_enhanced"] = sub_result
                print(f"✅ [JOB {job_id}] Résultat enrichi avec sub_orchestrator_result.json")

                # 🎯 PATCH : injecter le slug dans le suivi du job
                slug = sub_result.get("slug")
                if slug:
                    JOBS[job_id]["slug"] = slug
                    print(f"🎯 PATCH: slug injecté → {slug}")
                else:
                    print("⚠️ PATCH: slug absent dans sub_orchestrator_result.json")
            else:
                # On ajoute un warning mais on ne change pas le statut basé sur returncode
                if JOBS[job_id]["status"] == "success":
                    print(f"⚠️ [JOB {job_id}] Pipeline réussi mais sub_orchestrator_result.json introuvable.")
        else:
            # Fallback : chercher dans out_pipeline (ancien comportement)
            print(f"⚠️ Aucun out_dir trouvé pour job {job_id}, fallback vers out_pipeline/")
            out_dirs = list((BASE_DIR / "out_pipeline").glob("*"))
            if out_dirs:
                latest_out = max(out_dirs, key=os.path.getmtime)
                result_file = latest_out / "pipeline_result.json"
                sub_result_file = latest_out / "sub_orchestrator_result.json"

                if result_file.exists():
                    result_json = json.loads(result_file.read_text(encoding="utf-8"))
                    JOBS[job_id]["result"] = result_json
                
                if sub_result_file.exists():
                    sub_result = json.loads(sub_result_file.read_text(encoding="utf-8"))
                    JOBS[job_id]["result_enhanced"] = sub_result
                    slug = sub_result.get("slug")
                    if slug:
                        JOBS[job_id]["slug"] = slug
            else:
                # On ajoute un warning mais on ne change pas le statut basé sur returncode
                if JOBS[job_id]["status"] == "success":
                    print(f"⚠️ [JOB {job_id}] Pipeline réussi mais aucun dossier out_pipeline trouvé.")
                else:
                    JOBS[job_id]["error"] = "Aucun dossier out_pipeline trouvé."

        # Nettoyage
        if pdf_path.exists():
            pdf_path.unlink()

        print(f"🏁 [JOB {job_id}] Terminé avec statut : {JOBS[job_id]['status']}")
    
    except Exception as e:
        print(f"❌ Exception non bloquante: {e}")
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = str(e)
        JOBS[job_id]["current_step"] = "error"
        JOBS[job_id]["end_time"] = datetime.now().isoformat()
        return

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
    print(f"Appel à analyze-cerfa")
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

    # 📁 Dossier unique pour ce job
    out_dir = f"/tmp/out_pipeline_{job_id}"
    JOBS[job_id]["out_dir"] = out_dir

    # 🔥 Lancement du pipeline en tâche asynchrone
    asyncio.create_task(
        run_pipeline(job_id, temp_pdf, code_insee, env, out_dir=out_dir)
    )

    return {"success": True, "job_id": job_id}


# ============================================================
# 🚀 Endpoint : analyse depuis une liste de parcelles
# ============================================================

class ParcelleRequest(BaseModel):
    parcelles: List[Dict[str, str]]  # [{"section": "AC", "numero": "0242"}, ...]
    code_insee: str
    commune_nom: Optional[str] = None
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    demandeur: Optional[Dict[str, Any]] = None  # Données du demandeur depuis ValidationView


class ParcelleWithCerfaDataRequest(BaseModel):
    """
    Requête étendue : parcelles + données CERFA complètes issues du front (LLM + corrections user).
    - parcelles : liste de sections/numéros (après modifications utilisateur)
    - code_insee / commune_nom : contexte géographique
    - cerfa_data : données structurées (info_generales + parcelles_detectees) provenant du front
    """
    parcelles: List[Dict[str, str]]
    code_insee: str
    commune_nom: Optional[str] = None
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    cerfa_data: Dict[str, Any]


async def run_pipeline_from_parcelles_async(
    job_id: str,
    parcelles: list,
    code_insee: str,
    commune_nom: str | None,
    env: dict | None = None,
    out_dir: str | None = None,
    demandeur: dict | None = None
):
    """Exécute le pipeline depuis les parcelles et met à jour JOBS pour suivi via polling."""
    try:
        print(f"🟢 [run_pipeline_from_parcelles] START job_id={job_id}, parcelles={len(parcelles)}, insee={code_insee}, out_dir={out_dir}")
        
        BASE_DIR = Path(__file__).resolve().parent
        PIPELINE_SCRIPT = BASE_DIR / "pipeline_from_parcelles.py"

        # 🔴 NE PLUS RÉÉCRIRE ENTIÈREMENT JOBS[job_id] (préserve out_dir, user_id, etc.)
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

        # Construire la commande
        parcelles_json = json.dumps(parcelles)
        cmd = [
            "python3",
            str(PIPELINE_SCRIPT),
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

            # 🟦 Détection d'étapes
            if "Vérification unité foncière" in line or "unite_fonciere" in line:
                JOBS[job_id]["current_step"] = "unite_fonciere"
            elif "Analyse des intersections" in line or "intersections" in line:
                JOBS[job_id]["current_step"] = "intersections"
            elif "Génération cartes" in line or "Génération CUA" in line:
                JOBS[job_id]["current_step"] = "generation_cua"
            elif "CUA DOCX généré" in line:
                JOBS[job_id]["current_step"] = "cua_pret"

            # 🔥 Détection des erreurs
            if "💥" in line:
                JOBS[job_id]["status"] = "error"
                JOBS[job_id]["error"] = line
                JOBS[job_id]["current_step"] = "error"

        # 📌 Fin du pipeline
        returncode = await process.wait()
        print(f"🔥 Pipeline terminé pour job {job_id} (returncode={returncode})")
        JOBS[job_id]["returncode"] = returncode
        JOBS[job_id]["end_time"] = datetime.now().isoformat()

        # Vérification du returncode
        if returncode == 0:
            JOBS[job_id]["status"] = "success"
            JOBS[job_id]["current_step"] = "done"
            
            # Récupération du résultat depuis le dossier de sortie
            out_dir = JOBS[job_id].get("out_dir")
            
            if out_dir:
                sub_result_file = Path(out_dir) / "sub_orchestrator_result.json"
                if sub_result_file.exists():
                    sub_result = json.loads(sub_result_file.read_text(encoding="utf-8"))
                    JOBS[job_id]["result_enhanced"] = sub_result
                    print(f"✅ [JOB {job_id}] Résultat enrichi avec sub_orchestrator_result.json")

                    # 🎯 PATCH : injecter le slug dans le suivi du job
                    slug = sub_result.get("slug")
                    if slug:
                        JOBS[job_id]["slug"] = slug
                        print(f"🎯 PATCH: slug injecté → {slug}")
                    else:
                        print("⚠️ PATCH: slug absent dans sub_orchestrator_result.json")
            else:
                # Fallback : chercher dans out_pipeline (ancien comportement)
                print(f"⚠️ Aucun out_dir trouvé pour job {job_id}, fallback vers out_pipeline/")
                out_dirs = list((BASE_DIR / "out_pipeline").glob("*"))
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
        return


@app.post("/analyze-parcelles")
async def analyze_parcelles(req: ParcelleRequest):
    """Lance le pipeline complet depuis une liste de parcelles (sans CERFA)."""
    print(f"Appel à analyze-parcelles")
    
    # Validation
    if not req.parcelles or len(req.parcelles) == 0:
        raise HTTPException(status_code=400, detail="Liste de parcelles vide")
    
    if len(req.parcelles) > 20:
        raise HTTPException(status_code=400, detail="Trop de parcelles (max 20)")
    
    # Validation format parcelles
    for p in req.parcelles:
        if not isinstance(p, dict) or "section" not in p or "numero" not in p:
            raise HTTPException(
                status_code=400,
                detail=f"Format de parcelle invalide. Attendu: {{'section': 'AC', 'numero': '0242'}}"
            )
    
    job_id = str(uuid.uuid4())
    
    JOBS[job_id] = {
        "status": "queued",
        "created_at": datetime.now().isoformat(),
        "filename": f"{len(req.parcelles)} parcelle(s)",
        "user_id": req.user_id,
        "user_email": req.user_email,
    }
    
    # ============================================================
    # 🔐 Vérification des droits utilisateur
    # ============================================================
    user_insee_list = get_user_insee_list(req.user_id) if req.user_id else []
    print(f"👤 Droits utilisateur {req.user_email}: {user_insee_list or 'toutes communes'}")
    
    if user_insee_list and req.code_insee and req.code_insee not in user_insee_list:
        raise HTTPException(
            status_code=403,
            detail=(
                "Accès refusé : vous n'êtes autorisé qu'à analyser "
                f"les communes suivantes : {', '.join(user_insee_list)}"
            )
        )
    
    # 🧠 Transmission au sous-processus (via variables d'environnement)
    env = os.environ.copy()
    if req.user_id:
        env["USER_ID"] = req.user_id
    if req.user_email:
        env["USER_EMAIL"] = req.user_email
    
    # 📁 Dossier unique pour ce job
    out_dir = f"/tmp/out_pipeline_{job_id}"
    JOBS[job_id]["out_dir"] = out_dir
    
    # 🔥 Lancement du pipeline en tâche asynchrone
    asyncio.create_task(
        run_pipeline_from_parcelles_async(
            job_id,
            req.parcelles,
            req.code_insee,
            req.commune_nom,
            env,
            out_dir=out_dir,
            demandeur=req.demandeur
        )
    )
    
    return {"success": True, "job_id": job_id}


@app.post("/analyze-parcelles-with-json-data")
async def analyze_parcelles_with_json_data(req: ParcelleWithCerfaDataRequest):
    """
    Variante avancée qui prend :
    - une liste de parcelles (section/numero) potentiellement modifiées par l'utilisateur
    - les données CERFA complètes (issues de l'analyse LLM + corrections front)
    
    Objectif : utiliser ces données pour le header CUA (demandeur, adresse terrain, numéro CU, etc.)
    plutôt que de reconstruire un CERFA minimal côté backend.
    """
    print("Appel à analyze-parcelles-with-json-data")

    # Validation parcelles
    if not req.parcelles or len(req.parcelles) == 0:
        raise HTTPException(status_code=400, detail="Liste de parcelles vide")
    if len(req.parcelles) > 20:
        raise HTTPException(status_code=400, detail="Trop de parcelles (max 20)")
    for p in req.parcelles:
        if not isinstance(p, dict) or "section" not in p or "numero" not in p:
            raise HTTPException(
                status_code=400,
                detail="Format de parcelle invalide. Attendu: {'section': 'AC', 'numero': '0242'}",
            )

    job_id = str(uuid.uuid4())

    JOBS[job_id] = {
        "status": "queued",
        "created_at": datetime.now().isoformat(),
        "filename": f"{len(req.parcelles)} parcelle(s)",
        "user_id": req.user_id,
        "user_email": req.user_email,
    }

    # 🔐 Vérification des droits utilisateur
    user_insee_list = get_user_insee_list(req.user_id) if req.user_id else []
    print(f"👤 Droits utilisateur {req.user_email}: {user_insee_list or 'toutes communes'}")

    if user_insee_list and req.code_insee and req.code_insee not in user_insee_list:
        raise HTTPException(
            status_code=403,
            detail=(
                "Accès refusé : vous n'êtes autorisé qu'à analyser "
                f"les communes suivantes : {', '.join(user_insee_list)}"
            ),
        )

    # 🧠 Transmission au sous-processus (via variables d'environnement)
    env = os.environ.copy()
    if req.user_id:
        env["USER_ID"] = req.user_id
    if req.user_email:
        env["USER_EMAIL"] = req.user_email

    # 📁 Dossier unique pour ce job
    out_dir = f"/tmp/out_pipeline_{job_id}"
    JOBS[job_id]["out_dir"] = out_dir

    # 🧱 Construction du cerfa_result.json à partir des données front
    cerfa_dir = Path(out_dir)
    cerfa_dir.mkdir(parents=True, exist_ok=True)

    # 🔍 Log des clés reçues côté backend pour tracer le pont front → back
    cerfa_payload = req.cerfa_data or {}
    print(
        "[CUA] Backend /analyze-parcelles-with-json-data cerfa_data keys:",
        list(cerfa_payload.keys())
    )

    # Ici, cerfa_payload est déjà un objet "data" complet au format attendu par le CUA
    data: Dict[str, Any] = cerfa_payload

    cerfa_json: Dict[str, Any] = {
        "meta": {
            "source": "frontend_cerfa",
            "generated_at": datetime.now().isoformat(),
            "commune_insee": data.get("commune_insee"),
            "commune_nom": data.get("commune_nom"),
        },
        "data": data,
        "errors": [],
    }

    cerfa_out = cerfa_dir / "cerfa_result.json"
    cerfa_out.write_text(json.dumps(cerfa_json, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"✅ cerfa_result.json (depuis cerfa_data front) écrit dans {cerfa_out}")

    # 🔥 Lancement du pipeline en tâche asynchrone (réutilise run_pipeline_from_parcelles_async)
    asyncio.create_task(
        run_pipeline_from_parcelles_async(
            job_id,
            req.parcelles,
            req.code_insee,
            req.commune_nom,
            env,
            out_dir=out_dir,
            demandeur=data.get("demandeur") or {},
        )
    )

    return {"success": True, "job_id": job_id}


# ============================================================
# 🔍 Endpoint de suivi : état du job
# ============================================================

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    """Retourne l'état d'un job et ses résultats éventuels."""
    job = JOBS.get(job_id)
    print(f"🟣 DEBUG /status/{job_id} → job =", json.dumps(job, indent=2, default=str) if job else None)
    if not job:
        return {"success": False, "error": "Job introuvable"}
    
    # 🎯 PATCH : Charger les résultats depuis le out_dir du job si pas encore chargés
    out_dir = job.get("out_dir")
    if out_dir and "result_enhanced" not in job:
        result_file = Path(out_dir) / "pipeline_result.json"
        sub_result_file = Path(out_dir) / "sub_orchestrator_result.json"
        
        if result_file.exists():
            try:
                job["result"] = json.loads(result_file.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"⚠️ Erreur lecture pipeline_result.json : {e}")
        
        if sub_result_file.exists():
            try:
                sub_result = json.loads(sub_result_file.read_text(encoding="utf-8"))
                job["result_enhanced"] = sub_result
                # Extraire le slug si disponible
                slug = sub_result.get("slug")
                if slug:
                    job["slug"] = slug
            except Exception as e:
                print(f"⚠️ Erreur lecture sub_orchestrator_result.json : {e}")
    
    # 🎯 PATCH : si slug absent mais présent dans result_enhanced → on le remonte
    if "slug" not in job:
        try:
            slug = job.get("result_enhanced", {}).get("slug")
            if slug:
                job["slug"] = slug
        except:
            pass
    
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




# ============================================================
# 🧠 Fonctions utilitaires pour l'analyse IA
# ============================================================

def get_dossier_from_slug(slug: str):
    """
    Récupère un pipeline (dossier CUA) depuis Supabase via son slug.
    Retourne l'objet JSON ou None si introuvable.
    """
    resp = (
        supabase
        .schema("latresne")
        .table("pipelines")
        .select("*")
        .eq("slug", slug)
        .limit(1)
        .execute()
    )

    rows = resp.data or []
    if not rows:
        return None
    return rows[0]


def extract_docx_text(docx_bytes: bytes) -> str:
    """
    Extrait le texte brut d'un fichier DOCX.
    Utilise mammoth pour convertir le DOCX en texte.
    """
    try:
        result = mammoth.extract_raw_text(BytesIO(docx_bytes))
        return result.value
    except Exception as e:
        print(f"⚠️ Erreur extraction DOCX: {e}")
        return ""


class AISummaryRequest(BaseModel):
    slug: str


@app.post("/cua/ai_summary")
async def ai_summary(req: AISummaryRequest):
    try:
        slug = req.slug

        # 1️⃣ Charger le dossier depuis Supabase
        dossier = get_dossier_from_slug(slug)
        if not dossier:
            return {"success": False, "error": "Dossier introuvable"}

        docx_url = dossier.get("output_cua")
        intersections_json = dossier.get("intersections")

        if not docx_url:
            return {"success": False, "error": "Le CUA n'est pas encore généré"}

        # 2️⃣ Télécharger le DOCX
        data = requests.get(docx_url).content
        text = extract_docx_text(data)

        # 3️⃣ Construire prompt IA
        prompt = f"""
        Tu es un expert en urbanisme et en relecture de documents.
        Voici un certificat d'urbanisme généré automatiquement.

        === CONTENU DOCX ===
        {text}

        === COUCHES INTERSECTÉES ===
        {json.dumps(intersections_json, indent=2)}

        Tâches :
        1) Détecte incohérences, erreurs, duplications, typos ou défauts de génération, ou encore éléments pas cohérents ou clair avec la réglementaiton. Sois le plus exhaustif possible.
        2) Signale tout élément étrange ou potentiellement faux.
        3) Fais des propositions de modifications pour améliorer le CUA en fonction des incohérences et erreurs détectées.
        Réponds de façon structurée, concise et fiable.
        Réponds directement l'analyse, sans préambule.
        N'ecris pas de ** ou * dans la réponse.
        """

        # 4️⃣ Appel Gemini Flash Lite
        gemini_api_key = os.getenv("GEMINI_API_KEY")
        if not gemini_api_key:
            return {"success": False, "error": "GEMINI_API_KEY manquante dans les variables d'environnement"}
        
        genai.configure(api_key=gemini_api_key)
        model = genai.GenerativeModel("gemini-2.5-flash-lite")
        response = model.generate_content(prompt)
        
        summary = response.text

        return {"success": True, "summary": summary}

    except Exception as e:
        return {"success": False, "error": str(e)}


class PasswordResetRequest(BaseModel):
    email: EmailStr


@app.post("/auth/send-password-reset")
def send_password_reset(req: PasswordResetRequest):
    """
    Endpoint appelé par le FRONT pour envoyer un email
    de réinitialisation custom Kerelia.
    """

    email = req.email

    # 1) Générer le lien Supabase de récupération
    url = f"{SUPABASE_URL}/auth/v1/admin/generate_link"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "type": "recovery",
        "email": email,
        "redirect_to": "https://kerelia.fr/update-password"
    }

    r = requests.post(url, headers=headers, json=payload)
    data = r.json()

    if "action_link" not in data:
        print("❌ Erreur generate_link :", data)
        raise HTTPException(500, "Impossible de générer un lien de réinitialisation")

    reset_url = data["action_link"]

    # 2) Envoyer l’email custom Kerelia
    try:
        send_password_reset_email(email, reset_url)
    except Exception as e:
        print("❌ Erreur envoi email custom :", e)
        raise HTTPException(status_code=500, detail="Erreur durant l’envoi email")

    return {"success": True}
