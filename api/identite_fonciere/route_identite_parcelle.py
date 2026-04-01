"""
route_identite_parcelle.py
Endpoint API pour l'identité parcellaire
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel
from typing import List, Dict, Any
from pathlib import Path

from .identite_fonciere import (
    CATALOGUE,
    analyser_identite_fonciere,
    analyser_identite_parcelle,
)
from .carte_identite_fonciere import generate_identite_fonciere_map_html
from .sse_identite_fonciere import iter_identite_fonciere_sse_chunks, sse_error_chunk
from .pdf.rapport_identite_fonciere import generate_rapport_pdf

router = APIRouter(prefix="/api/identite-parcelle", tags=["Identité Parcellaire"])
router_fonciere = APIRouter(prefix="/api/identite-fonciere", tags=["Identité Foncière"])

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
    article: str | None = None
    attribut_discriminant: str | None = None
    elements: List[Dict[str, Any]] = []

class IdentiteResponse(BaseModel):
    success: bool
    parcelle: str
    commune: str
    insee: str
    nb_intersections: int
    intersections: List[IntersectionResult]
    error: str | None = None


class IdentiteFonciereRequest(BaseModel):
    commune: str
    insee: str | None = None
    srid: int | None = None
    geometry: dict


class IdentiteFonciereMapRequest(IdentiteFonciereRequest):
    intersections: List[IntersectionResult] | None = None


class RapportFonciereRequest(BaseModel):
    """PDF : intersections déjà calculées (recommandé) ou géométrie seule pour relancer l’analyse."""

    commune: str
    insee: str | None = None
    srid: int | None = None
    geometry: dict | None = None
    intersections: List[IntersectionResult] | None = None
    output_dir: str | None = None
    # Référence cadastrale dans le PDF si pas d’UF (ex. section + numéro)
    parcelle: str | None = None


class IdentiteFonciereMapResponse(BaseModel):
    success: bool
    html: str
    metadata: Dict[str, Any]
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


@router_fonciere.post("/intersect", response_model=IdentiteResponse)
async def intersect_fonciere(payload: IdentiteFonciereRequest):
    """
    Calcule les intersections à partir d'une géométrie GeoJSON
    représentant l'unité foncière.
    """
    try:
        result = analyser_identite_fonciere(
            geometry=payload.geometry,
            commune=payload.commune,
            insee=payload.insee,
            srid=payload.srid
        )

        return IdentiteResponse(
            success=True,
            **result,
            error=None
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        return IdentiteResponse(
            success=False,
            parcelle="UNITE_FONCIERE",
            commune=payload.commune,
            insee=payload.insee or "",
            nb_intersections=0,
            intersections=[],
            error=str(e)
        )


@router_fonciere.post("/intersect/stream")
async def intersect_fonciere_stream(payload: IdentiteFonciereRequest):
    """
    Même analyse que POST /intersect, en SSE : événements `init`, `layer_done` par couche du catalogue,
    puis `complete` avec le même corps que la réponse JSON classique.
    """

    def gen():
        try:
            for chunk in iter_identite_fonciere_sse_chunks(
                payload.geometry,
                payload.commune,
                payload.insee,
                payload.srid,
            ):
                yield chunk
        except ValueError as e:
            yield sse_error_chunk(str(e))
        except Exception as e:
            yield sse_error_chunk(str(e))

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router_fonciere.post("/map", response_model=IdentiteFonciereMapResponse)
async def map_fonciere(payload: IdentiteFonciereMapRequest):
    """
    Génère une carte 2D HTML minimale (Folium) :
    - unité foncière analysée
    - couches intersectées
    - légende simplifiée
    """
    try:
        res = generate_identite_fonciere_map_html(
            geometry=payload.geometry,
            commune=payload.commune,
            insee=payload.insee,
            srid=payload.srid,
            intersections=[i.model_dump() for i in payload.intersections] if payload.intersections else None,
        )
        return IdentiteFonciereMapResponse(
            success=True,
            html=res["html"],
            metadata=res["metadata"],
            intersections=res["intersections"],
            error=None,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        return IdentiteFonciereMapResponse(
            success=False,
            html="",
            metadata={},
            intersections=[],
            error=str(e),
        )


@router_fonciere.post("/rapport")
async def rapport_fonciere(payload: RapportFonciereRequest):
    """
    Génère le rapport PDF d'identité foncière.
    Si intersections fournies dans le payload : utilisées directement (évite de recalculer).
    Sinon : lance l'analyse complète puis génère le PDF.
    """
    try:
        if payload.intersections:
            ref_parcelle = payload.parcelle or "UNITE_FONCIERE"
            result = {
                "parcelle": ref_parcelle,
                "commune": payload.commune,
                "insee": payload.insee or "",
                "nb_intersections": len(payload.intersections),
                "intersections": [i.model_dump() for i in payload.intersections],
            }
        elif payload.geometry is not None:
            result = analyser_identite_fonciere(
                geometry=payload.geometry,
                commune=payload.commune,
                insee=payload.insee,
                srid=payload.srid,
            )
        else:
            raise HTTPException(
                status_code=400,
                detail="Fournir `intersections` ou `geometry` pour générer le rapport.",
            )

        output_dir = payload.output_dir or "./rapports_identite"
        pdf_path = generate_rapport_pdf(
            result,
            output_dir=output_dir,
            catalogue=CATALOGUE,
        )

        return FileResponse(
            pdf_path,
            media_type="application/pdf",
            filename=Path(pdf_path).name,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))