"""
route_identite_parcelle.py
Endpoint API pour l'identité parcellaire
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List

from .identite_parcelle import analyser_identite_parcelle

router = APIRouter(prefix="/api/identite-parcelle", tags=["Identité Parcellaire"])

# ------------------------------------------------------------
# Models
# ------------------------------------------------------------

class ParcelleRequest(BaseModel):
    commune: str
    section: str
    numero: str
    insee: str

class IntersectionResult(BaseModel):
    table: str
    display_name: str
    elements: List[str] = []

class IdentiteResponse(BaseModel):
    success: bool
    parcelle: str
    commune: str
    insee: str
    nb_intersections: int
    intersections: List[IntersectionResult]
    error: str | None = None

# ------------------------------------------------------------
# Endpoint
# ------------------------------------------------------------

@router.post("/intersect", response_model=IdentiteResponse)
async def intersect_parcelle(payload: ParcelleRequest):
    """
    Calcule les intersections entre une parcelle et toutes les couches
    du schéma 'carto' en base de données avec leurs éléments discriminants.
    
    Workflow:
    1. Récupère la géométrie de la parcelle depuis l'IGN (EPSG:2154)
    2. Teste l'intersection avec chaque table du schéma 'carto'
    3. Extrait les valeurs des attributs discriminants pour chaque couche
    4. Retourne les couches et leurs éléments intersectés
    """
    try:
        result = analyser_identite_parcelle(
            section=payload.section,
            numero=payload.numero,
            insee=payload.insee,
            commune=payload.commune
        )
        
        return IdentiteResponse(
            success=True,
            **result,
            error=None
        )
    
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    
    except Exception as e:
        return IdentiteResponse(
            success=False,
            parcelle=f"{payload.section} {payload.numero}",
            commune=payload.commune,
            insee=payload.insee,
            nb_intersections=0,
            intersections=[],
            error=str(e)
        )