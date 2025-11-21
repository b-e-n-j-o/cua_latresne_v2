#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
orchestrator_global.py — Pipeline global KERELIA (phase 2)
-----------------------------------------------------------
1️⃣ Analyse du CERFA via Gemini (analyse_gemini.py)
2️⃣ Vérification unité foncière via WFS IGN (verification_unite_fonciere.py)
3️⃣ Intersections avec couches urbanistiques (intersections.py)
-----------------------------------------------------------
Étapes suivantes prévues :
4️⃣ Génération cartes 2D / 3D
5️⃣ Génération certificat d'urbanisme DOCX
"""

import subprocess
import json
import logging
import sys
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from CERFA_ANALYSE.auth_utils import is_authorized_for_insee

# ✨ Nouveau : imports directs des modules internes
from CERFA_ANALYSE.analyse_gemini import analyse_cerfa
from CERFA_ANALYSE.verification_unite_fonciere import verifier_unite_fonciere
from INTERSECTIONS.intersections import calculate_intersection, CATALOGUE
from CUA.sub_orchestrator_cua import generer_visualisations_et_cua_depuis_wkt
from sqlalchemy import create_engine, text

# ============================================================
# CONFIG
# ============================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("orchestrator_global")

# Configuration Supabase pour upload final
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = "visualisation"

CERFA_ANALYSE_SCRIPT = "./CERFA_ANALYSE/analyse_gemini.py"
VERIF_UF_SCRIPT = "./CERFA_ANALYSE/verification_unite_fonciere.py"
INTERSECTIONS_SCRIPT = "./INTERSECTIONS/intersections.py"
SUB_ORCHESTRATOR_CUA = "./CUA/sub_orchestrator_cua.py"

# Variable globale pour OUT_DIR (sera initialisée dans run_global_pipeline)
OUT_DIR = None

# ============================================================
# UTILS
# ============================================================
def run_subprocess(cmd, desc):
    """Exécute une commande subprocess et logge les erreurs proprement."""
    logger.info(f"\n🚀 Étape : {desc}")
    try:
        subprocess.run(cmd, check=True, cwd=Path(__file__).parent)
    except subprocess.CalledProcessError as e:
        logger.error(f"💥 Échec lors de {desc}: {e}")
        sys.exit(1)


def fail_pipeline(reason: str):
    """Enregistre l'erreur dans la DB puis stoppe le pipeline."""
    try:
        slug = os.getenv("PIPELINE_SLUG")
        supabase = None
        if SUPABASE_URL and SUPABASE_KEY:
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

        if slug and supabase:
            supabase.schema("latresne").table("pipelines").update({
                "status": "error",
                "error": reason
            }).eq("slug", slug).execute()
    except Exception as e:
        logger.error(f"Impossible d'enregistrer l'erreur dans pipelines: {e}")

    logger.error(reason)
    sys.exit(1)


def _analyse_intersections_depuis_wkt(wkt_path: str, out_dir: Path):
    """
    Fonction helper pour analyser les intersections depuis un fichier WKT.
    Reproduit la logique du CLI de intersections.py sans subprocess.
    """
    # Configuration DB (identique à intersections.py)
    SUPABASE_HOST = os.getenv('SUPABASE_HOST')
    SUPABASE_DB = os.getenv('SUPABASE_DB')
    SUPABASE_USER = os.getenv('SUPABASE_USER')
    SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')
    SUPABASE_PORT = os.getenv('SUPABASE_PORT')
    
    DATABASE_URL = f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
    engine = create_engine(DATABASE_URL)
    
    # Lecture du WKT
    with open(wkt_path, "r", encoding="utf-8") as f:
        parcelle_wkt = f.read()
    
    logger.info(f"📐 Analyse des intersections depuis : {wkt_path}")
    
    # Calcul de la surface SIG
    with engine.connect() as conn:
        area_parcelle_sig = float(conn.execute(
            text("SELECT ST_Area(ST_GeomFromText(:wkt, 2154))"),
            {"wkt": parcelle_wkt}
        ).scalar())
    
    rapport = {
        "parcelle": "UF 0000",
        "surface_m2": round(area_parcelle_sig, 2),
        "intersections": {}
    }
    
    # Analyse pour chaque table du catalogue
    for table, config in CATALOGUE.items():
        logger.info(f"→ {table}")
        # Nouveau format intersections v10
        objets, surface_totale_sig, metadata = calculate_intersection(parcelle_wkt, table)
        
        if objets:
            logger.info(f"  ✅ {len(objets)} objet(s) | {surface_totale_sig:.2f} m²")
            # Calcul du pourcentage SIG
            pct_sig = 0.0
            if area_parcelle_sig > 0:
                pct_sig = round(surface_totale_sig / area_parcelle_sig * 100, 4)
            
            # Nouveau JSON conforme à intersections v10
            rapport["intersections"][table] = {
                "nom": config["nom"],
                "type": config["type"],
                "pct_sig": pct_sig,
                "objets": objets
            }
        else:
            logger.info(f"  ❌ Aucune intersection")
            rapport["intersections"][table] = {
                "nom": config["nom"],
                "type": config["type"],
                "pct_sig": 0.0,
                "objets": []
            }
    
    # Sauvegarde des rapports
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = out_dir / f"rapport_intersections_{timestamp}.json"
    out_html = out_dir / f"rapport_intersections_{timestamp}.html"
    
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(rapport, f, indent=2, ensure_ascii=False)
    
    html = generate_html(rapport)
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)
    
    logger.info(f"✅ Rapports d'intersections exportés ({out_json}, {out_html})")
    return str(out_json)


def generate_html(rapport):
    """Génère le HTML du rapport d'intersections (copié depuis intersections.py)."""
    parcelle = rapport['parcelle']
    area = rapport['surface_m2']
    results = rapport['intersections']
    
    # Grouper par type
    by_type = {}
    for table, data in results.items():
        t = data['type']
        if t not in by_type:
            by_type[t] = []
        by_type[t].append((table, data))
    
    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<title>Rapport {parcelle}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 20px; }}
h1 {{ color: #333; }}
.info {{ background: #f0f0f0; padding: 10px; margin-bottom: 20px; }}
.type-section {{ margin-bottom: 30px; }}
.type-header {{ background: #2c5aa0; color: white; padding: 10px; }}
.couche {{ margin: 10px 0; padding: 10px; border: 1px solid #ddd; }}
.couche h3 {{ margin: 0 0 10px 0; color: #2c5aa0; }}
table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
th {{ background: #f5f5f5; }}
.no-intersect {{ color: #999; }}
</style>
</head>
<body>
<h1>Rapport d'intersection</h1>
<div class="info">
<strong>Parcelle:</strong> {parcelle}<br>
<strong>Surface:</strong> {area:,.2f} m²
</div>
"""
    
    for type_name in sorted(by_type.keys()):
        items = by_type[type_name]
        intersected = [(t, d) for t, d in items if d['objets']]
        
        html += f"""
<div class="type-section">
<div class="type-header">
<h2>{type_name.upper()} ({len(intersected)}/{len(items)} intersections)</h2>
</div>
"""
        
        for table, data in items:
            if data['objets']:
                html += f"""
<div class="couche">
<h3>✓ {data['nom']}</h3>
<p><strong>Part concernée:</strong> {data['pct_sig']:.4f}% de la surface cadastrale indicative</p>
"""
                # Headers (exclure les colonnes de surfaces)
                obj_keys = [k for k in data['objets'][0].keys() 
                           if not k.lower().startswith("surface") 
                           and not k.lower().endswith("_m2")]
                
                # Afficher le tableau seulement s'il y a des colonnes après filtrage
                if obj_keys:
                    html += "<table>\n<tr>\n"
                    for key in obj_keys:
                        html += f"<th>{key}</th>"
                    html += "</tr>\n"
                    
                    # Rows (exclure les colonnes de surfaces)
                    for obj in data['objets']:
                        html += "<tr>"
                        for key in obj_keys:
                            html += f"<td>{obj.get(key, '')}</td>"
                        html += "</tr>\n"
                    
                    html += "</table>\n"
                
                html += "</table></div>\n"
            else:
                html += f"""<div class="couche no-intersect"><h3>✗ {data['nom']}</h3><p>Aucune intersection</p></div>\n"""
        
        html += "</div>\n"
    
    html += "</body></html>"
    return html

# ============================================================
# PIPELINE PRINCIPAL (IMPORTABLE)
# ============================================================
def run_global_pipeline(
    pdf_path: str,
    code_insee: str | None = None,
    user_id: str | None = None,
    user_email: str | None = None,
    notify_step=None,
    out_dir: str | None = None  # ← NOUVEAU : accepter un OUT_DIR existant
):
    """
    Pipeline importable : analyse CERFA → UF → intersections → CUA
    Utilisée par FastAPI et WebSocket.
    notify_step(event) : callback appelée à chaque étape.
    out_dir : chemin vers un dossier de sortie existant (optionnel).
    """
    global OUT_DIR
    
    # Initialisation du dossier de sortie
    if out_dir:
        # ✅ Utiliser le OUT_DIR fourni (créé par WebSocket)
        OUT_DIR = Path(out_dir)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Utilisation du OUT_DIR fourni : {OUT_DIR}")
    else:
        # Créer un nouveau OUT_DIR avec timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        OUT_DIR = Path("./out_pipeline") / timestamp
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Création d'un nouveau OUT_DIR : {OUT_DIR}")
    
    # Petit wrapper pour notifier le front si fourni
    def emit(evt, payload=None):
        if notify_step:
            try:
                notify_step({"event": evt, "payload": payload})
            except Exception:
                pass

    emit("start", {"pdf": pdf_path})

    # 1️⃣ Analyse CERFA
    logger.info("=== Analyse du CERFA ===")
    emit("analyse_cerfa:start")
    cerfa_out = OUT_DIR / "cerfa_result.json"

    # ✅ Skip si déjà analysé
    if cerfa_out.exists():
        logger.info("✅ CERFA déjà analysé, chargement du JSON existant")
        with open(cerfa_out, "r", encoding="utf-8") as f:
            cerfa_json = json.load(f)
    else:
        logger.info("📄 Analyse du CERFA en cours...")
        cerfa_json = analyse_cerfa(str(pdf_path), out_json=str(cerfa_out))

    emit("analyse_cerfa:done", cerfa_json)

    insee = cerfa_json["data"].get("commune_insee") or code_insee
    if not insee:
        raise RuntimeError("Code INSEE introuvable")

    # Vérification autorisation utilisateur
    if user_id:
        if not is_authorized_for_insee(user_id, insee):
            raise RuntimeError(f"⛔ Utilisateur non autorisé à analyser la commune {insee}")

    # 2️⃣ Vérification unité foncière
    logger.info("=== Unité foncière ===")
    emit("uf:start")
    uf_json_path = OUT_DIR / "rapport_unite_fonciere.json"
    uf_json = verifier_unite_fonciere(
        str(cerfa_out), insee, str(OUT_DIR)
    )
    Path(uf_json_path).write_text(json.dumps(uf_json, indent=2, ensure_ascii=False), encoding="utf-8")
    emit("uf:done", uf_json)

    if not uf_json.get("success"):
        raise RuntimeError("Unité foncière non valide")

    wkt_path = uf_json.get("geom_wkt_path")
    if not wkt_path or not Path(wkt_path).exists():
        raise RuntimeError("Geom WKT manquant")

    # 3️⃣ Intersections
    logger.info("=== Rapport d'intersection ===")
    emit("intersections:start")
    intersections_json_path = _analyse_intersections_depuis_wkt(wkt_path, OUT_DIR)
    emit("intersections:done", {"path": intersections_json_path})

    # 4️⃣ Génération cartes + CUA
    logger.info("=== Génération CUA ===")
    emit("cua:start")
    cua_result = generer_visualisations_et_cua_depuis_wkt(
        wkt_path=wkt_path,
        out_dir=str(OUT_DIR),
        commune="latresne",
        code_insee=insee
    )
    emit("cua:done", cua_result)

    # -------------------------------
    # RETOUR GLOBAL
    # -------------------------------
    result = {
        "cerfa_result": str(cerfa_out),
        "uf_result": str(uf_json_path),
        "geom_wkt": wkt_path,
        "intersections": intersections_json_path
    }

    # Intégration du résultat global du sous-orchestrateur
    cua_docx = OUT_DIR / "CUA_unite_fonciere.docx"
    if cua_docx.exists():
        result["cua_docx"] = str(cua_docx)

    result_path = OUT_DIR / "pipeline_result.json"
    result_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info(f"\n🎉 PIPELINE TERMINÉ AVEC SUCCÈS 🎉")
    logger.info(f"📦 Résumé enregistré dans : {result_path}")

    # ============================================================
    # 📤 UPLOAD FINAL : pipeline_result.json vers Supabase
    # ============================================================
    logger.info("\n📤 Upload final des résultats JSON vers Supabase...")
    
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Récupérer le slug depuis sub_orchestrator_result.json
        sub_result_file = OUT_DIR / "sub_orchestrator_result.json"
        slug = None
        if sub_result_file.exists():
            sub_result = json.loads(sub_result_file.read_text(encoding="utf-8"))
            slug = sub_result.get("slug")
        
        if not slug:
            logger.warning("⚠️ Slug introuvable — impossible d'uploader les résultats JSON.")
        else:
            # Fichiers potentiels à uploader
            result_files = [
                OUT_DIR / "pipeline_result.json",
                OUT_DIR / "sub_orchestrator_result.json"
            ]
            
            for file_path in result_files:
                if file_path.exists():
                    remote_path = f"{slug}/{file_path.name}"
                    try:
                        with open(file_path, "rb") as f:
                            supabase.storage.from_(SUPABASE_BUCKET).upload(
                                remote_path, f.read(), {"upsert": "true"}
                            )
                        remote_url = (
                            f"{SUPABASE_URL}/storage/v1/object/public/"
                            f"{SUPABASE_BUCKET}/{remote_path}"
                        )
                        logger.info(f"✅ {file_path.name} uploadé vers Supabase : {remote_url}")
                    except Exception as e:
                        logger.error(f"💥 Erreur upload {file_path.name} : {e}")
                else:
                    logger.warning(f"⚠️ Fichier {file_path.name} non trouvé pour upload.")
            
            # ============================================================
            # 👤 MISE À JOUR : user_id / user_email dans la table pipelines
            # ============================================================
            try:
                if slug and (user_id or user_email):
                    logger.info(f"👤 Mise à jour des infos utilisateur pour le pipeline {slug}...")
                    update_data = {}
                    if user_id:
                        update_data["user_id"] = user_id
                    if user_email:
                        update_data["user_email"] = user_email

                    supabase.schema("latresne").table("pipelines").update(update_data).eq("slug", slug).execute()
                    logger.info(f"✅ user_id / user_email mis à jour : {user_id or 'None'} / {user_email or 'None'}")
                else:
                    logger.info("⚠️ Aucun USER_ID ou USER_EMAIL trouvé — pas de mise à jour utilisateur.")
            except Exception as e:
                logger.error(f"💥 Erreur lors de la mise à jour des infos utilisateur : {e}")
            
            # ============================================================
            # 🧠 MISE À JOUR : pipeline_result_url dans la table pipelines
            # ============================================================
            try:
                if (OUT_DIR / "pipeline_result.json").exists():
                    result_url = (
                        f"{SUPABASE_URL}/storage/v1/object/public/"
                        f"{SUPABASE_BUCKET}/{slug}/pipeline_result.json"
                    )

                    logger.info("🧩 Mise à jour du champ pipeline_result_url dans la base...")
                    supabase.schema("latresne").table("pipelines").update({
                        "pipeline_result_url": result_url
                    }).eq("slug", slug).execute()
                    logger.info(f"✅ pipeline_result_url mis à jour : {result_url}")
            except Exception as e:
                logger.error(f"💥 Erreur lors de la mise à jour du pipeline_result_url : {e}")
    
    except Exception as e:
        logger.error(f"💥 Erreur lors de l'upload final : {e}")

    return {
        "cerfa": cerfa_json,
        "uf": uf_json,
        "cua": cua_result
    }


# ============================================================
# PIPELINE PRINCIPAL (CLI - COMPATIBILITÉ)
# ============================================================
def orchestrer_pipeline(pdf_path: str, code_insee: str, out_dir: str | None = None):
    """
    Orchestration complète du process CERFA → UF → Intersections (CLI)
    Wrapper pour compatibilité avec l'ancienne CLI.
    """
    user_id = os.getenv("USER_ID")
    user_email = os.getenv("USER_EMAIL")
    
    try:
        run_global_pipeline(
            pdf_path=pdf_path,
            code_insee=code_insee,
            user_id=user_id,
            user_email=user_email,
            notify_step=None,
            out_dir=out_dir  # ← NOUVEAU : transmettre out_dir
        )
    except Exception as e:
        fail_pipeline(str(e))

# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Orchestrator global — KERELIA (phase 2)")
    ap.add_argument("--pdf", required=True, help="Chemin vers le CERFA PDF")
    ap.add_argument("--code-insee", default=None, help="Code INSEE (fallback si non trouvé)")
    ap.add_argument("--out-dir", default=None, help="Dossier de sortie existant (optionnel)")
    args = ap.parse_args()

    orchestrer_pipeline(args.pdf, args.code_insee, args.out_dir)
