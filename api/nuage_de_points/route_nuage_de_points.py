"""
Route API pour génération nuage de points LIDAR
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
import logging

from .nuage_de_points import (
    generer_nuage_parcelle,
    convertir_laz_to_json,
    NuageDePointsException
)

router = APIRouter(prefix="/api/lidar", tags=["LIDAR"])
logger = logging.getLogger(__name__)

@router.get("/parcelle/{insee}/{section}/{numero}")
async def get_nuage_parcelle(
    insee: str,
    section: str,
    numero: str
):
    """
    Génère et retourne le nuage de points LIDAR d'une parcelle
    
    Returns:
        - metadata: informations parcelle + statistiques nuage
        - laz_url: endpoint pour télécharger le fichier LAZ
    """
    try:
        result = await generer_nuage_parcelle(insee, section, numero)
        
        return {
            "success": True,
            "parcelle": {
                "insee": insee,
                "section": section,
                "numero": numero,
                "commune": result["commune"],
                "surface_m2": result["surface"]
            },
            "nuage": {
                "nb_points": result["nb_points"],
                "altitude_min": result["alt_min"],
                "altitude_max": result["alt_max"],
                "classes": result["classes"]
            },
            "laz_url": f"/api/lidar/download/{result['file_id']}"
        }
        
    except NuageDePointsException as e:
        logger.error(f"Erreur génération nuage {insee} {section} {numero}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Erreur serveur: {e}")
        raise HTTPException(status_code=500, detail="Erreur serveur")

@router.get("/download/{file_id}")
async def download_laz(file_id: str):
    """Télécharge le fichier LAZ généré"""
    try:
        file_path = Path(f"./output_lidar/{file_id}.laz")
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Fichier non trouvé")
        
        return FileResponse(
            path=file_path,
            media_type="application/octet-stream",
            filename=f"{file_id}.laz"
        )
        
    except Exception as e:
        logger.error(f"Erreur téléchargement {file_id}: {e}")
        raise HTTPException(status_code=500, detail="Erreur téléchargement")

@router.get("/parcelle/{insee}/{section}/{numero}/json")
async def get_nuage_json(
    insee: str,
    section: str,
    numero: str
):
    """Retourne le nuage de points en JSON pour visualisation web"""
    try:
        file_path = Path(f"./output_lidar/parcelle_{insee}_{section}_{numero}.laz")
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Nuage non généré")
        
        points_json = convertir_laz_to_json(file_path)
        return points_json
        
    except Exception as e:
        logger.error(f"Erreur conversion JSON: {e}")
        raise HTTPException(status_code=500, detail="Erreur conversion")