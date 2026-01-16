"""
Endpoint API pour génération topographie 3D
À ajouter dans votre fichier FastAPI principal
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
import subprocess
import os
import json
import tempfile
import traceback
import logging
import re
import shutil
from pathlib import Path

# Configuration du logger
logger = logging.getLogger(__name__)

router = APIRouter()

class TopographyRequest(BaseModel):
    code_insee: str
    section: str
    numero: str

@router.post("/topographie-3d")
async def generate_topography_3d(request: TopographyRequest):
    """
    Génère une visualisation 3D de la topographie d'une parcelle.
    Retourne directement le fichier HTML Plotly.
    """
    logger.info(f"🔵 Début génération topographie 3D - INSEE: {request.code_insee}, Section: {request.section}, Numéro: {request.numero}")
    
    # Créer un dossier temporaire pour cette génération
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Construire l'ID parcelle
            id_parcelle = f"{request.section} {request.numero}"
            logger.info(f"📋 ID Parcelle: {id_parcelle}")
            
            # Appeler le script Python map_3d.py
            # Chercher le script dans plusieurs emplacements possibles
            possible_paths = [
                "/opt/kerelia/scripts/map_3d.py",  # Production
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "CUA", "map_3d.py"),  # Relatif au projet
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "api", "map_3d.py"),  # Dans api/
            ]
            
            script_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    script_path = path
                    logger.info(f"✅ Script trouvé: {script_path}")
                    break
            
            if not script_path:
                error_msg = f"❌ Script map_3d.py introuvable. Chemins testés: {possible_paths}"
                logger.error(error_msg)
                raise HTTPException(
                    status_code=500,
                    detail=error_msg
                )
            
            cmd = [
                "python3",
                script_path,
                "--code_insee", request.code_insee,
                "--id_parcelle", id_parcelle,
                "--output", temp_dir,
                "--exaggeration", "1.0"
            ]
            
            logger.info(f"🚀 Exécution commande: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120  # 2 minutes max
            )
            
            logger.info(f"📊 Code retour: {result.returncode}")
            if result.stdout:
                logger.info(f"📤 stdout: {result.stdout[:500]}")  # Limiter à 500 caractères
            if result.stderr:
                logger.warning(f"⚠️ stderr: {result.stderr[:500]}")
            
            if result.returncode != 0:
                error_detail = f"Erreur génération (code {result.returncode}): {result.stderr}"
                logger.error(f"❌ {error_detail}")
                raise HTTPException(
                    status_code=500,
                    detail=error_detail
                )
            
            # Parser le résultat JSON (extraire seulement la partie JSON)
            try:
                json_match = re.search(r'\{[\s\S]*\}', result.stdout)
                if not json_match:
                    raise ValueError("Pas de JSON trouvé dans stdout")
                output_data = json.loads(json_match.group())
                logger.info(f"✅ JSON parsé avec succès: {list(output_data.keys())}")
            except (json.JSONDecodeError, ValueError) as json_err:
                error_detail = f"Erreur parsing JSON: {str(json_err)}\nstdout: {result.stdout[:500]}"
                logger.error(f"❌ {error_detail}")
                raise HTTPException(
                    status_code=500,
                    detail=error_detail
                )
            
            if output_data.get("error"):
                error_detail = output_data["error"]
                logger.error(f"❌ Erreur dans output_data: {error_detail}")
                raise HTTPException(
                    status_code=500,
                    detail=error_detail
                )
            
            html_path = output_data.get("path")
            logger.info(f"📄 Chemin HTML: {html_path}")
            
            if not html_path:
                error_detail = "Chemin HTML manquant dans la réponse"
                logger.error(f"❌ {error_detail}")
                raise HTTPException(
                    status_code=500,
                    detail=error_detail
                )
            
            if not os.path.exists(html_path):
                error_detail = f"Fichier HTML introuvable: {html_path}"
                logger.error(f"❌ {error_detail}")
                raise HTTPException(
                    status_code=500,
                    detail=error_detail
                )
            
            logger.info(f"✅ Fichier HTML généré avec succès: {html_path}")
            
            # Copier le fichier vers un fichier temporaire persistant
            # (car temp_dir sera supprimé à la sortie du context manager)
            permanent_temp = tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.html',
                delete=False  # Important : ne pas supprimer automatiquement
            )
            permanent_temp.close()
            
            shutil.copy(html_path, permanent_temp.name)
            logger.info(f"📋 Fichier copié vers: {permanent_temp.name}")
            
            # Retourner le fichier permanent
            return FileResponse(
                permanent_temp.name,
                media_type="text/html",
                filename=f"topo_3d_{request.section}_{request.numero}.html"
            )
            
        except subprocess.TimeoutExpired:
            error_detail = "Timeout: génération trop longue (>2min)"
            logger.error(f"❌ {error_detail}")
            raise HTTPException(
                status_code=504,
                detail=error_detail
            )
        except HTTPException:
            # Re-raise les HTTPException sans modification
            raise
        except Exception as e:
            # Log détaillé avec traceback pour toutes les autres erreurs
            error_traceback = traceback.format_exc()
            error_detail = f"Erreur serveur: {str(e)}"
            logger.error(f"❌ Erreur détaillée:\n{error_traceback}")
            print(f"❌ Erreur détaillée: {error_traceback}")
            raise HTTPException(
                status_code=500,
                detail=error_detail
            )

