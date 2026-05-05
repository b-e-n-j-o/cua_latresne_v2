"""
Router FastAPI pour la visualisation 3D MNT par reference parcellaire.

Endpoints:
- POST /mnt/visualisation/html : retourne la page HTML Plotly
- GET  /mnt/health             : healthcheck
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from api.mnt.parcelle_to_mnt import (
    build_emprise_mnt,
    export_plotly_3d,
    fetch_mnt_from_geometry,
    fetch_parcelle_geometry,
    fetch_parcelles_contigues,
)

router = APIRouter()


class MntVisualisationRequest(BaseModel):
    code_insee: str
    section: str
    numero: str
    exaggeration: float = Field(default=1.5, ge=0.1, le=20.0)
    include_voisins: bool = Field(
        default=True,
        description="Inclut les parcelles contiguës pour le terrain environnant (MNT élargi).",
    )


@router.post("/visualisation/html")
async def get_mnt_visualisation_html(body: MntVisualisationRequest):
    """
    Pipeline MNT:
    1) Parcelle cible depuis latresne.parcelles_latresne (Lambert 93)
    2) Parcelles contiguës optionnelles (ST_Touches)
    3) Clip MNT sur union cible + voisins ; contour jaune = parcelle cible
    4) Generation HTML Plotly
    Retourne directement le HTML.
    """
    try:
        geom_cible = fetch_parcelle_geometry(body.code_insee, body.section, body.numero)
        if body.include_voisins:
            voisins, n_voisins = fetch_parcelles_contigues(geom_cible, body.code_insee)
            emprise = build_emprise_mnt(geom_cible, voisins)
        else:
            emprise = geom_cible
            n_voisins = 0

        mnt, transform, resolution = fetch_mnt_from_geometry(emprise)

        with tempfile.TemporaryDirectory(prefix="kerelia_mnt_") as tmp_dir:
            result = export_plotly_3d(
                geometry_target=geom_cible,
                mnt=mnt,
                transform=transform,
                resolution=resolution,
                code_insee=body.code_insee,
                section=body.section,
                numero=body.numero,
                output_dir=tmp_dir,
                exaggeration=body.exaggeration,
                n_voisins=n_voisins,
            )
            html_path = Path(result["path"])
            html_content = html_path.read_text(encoding="utf-8")

        return HTMLResponse(
            content=html_content,
            headers={
                "X-Surface-M2": str(round(float(result["surface_m2"]), 1)),
                "X-Resolution-M": str(round(float(result["resolution_m"]), 3)),
                "X-N-Voisins": str(int(result.get("n_voisins", 0))),
                "Access-Control-Expose-Headers": "X-Surface-M2,X-Resolution-M,X-N-Voisins",
            },
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health():
    return {"status": "ok"}
