"""
Map PLU — endpoint GET /session/{session_id}/map.

Sert les données cartographiques GeoJSON d'une session existante
sans passer par le LLM. Utilisé par le frontend pour :
  - afficher la carte au chargement d'une session depuis la sidebar
  - rafraîchir la carte sans nouveau tour de conversation

Cartographie décorrélée du LLM : show_map=true si la session a des refs parcellaires.
Les géométries ne transitent jamais par Gemini — uniquement ce endpoint :
  - Frontend → GET /session/{id}/map → GeoJSON pour MapLibre
"""

import logging

from fastapi import APIRouter, HTTPException

from .._env import DB_CONFIG

try:
    from ..tools.carto import build_carto_payload
    from ..tools.utils.parcel_geom import refs_from_session
except ImportError:
    from tools.carto import build_carto_payload
    from tools.utils.parcel_geom import refs_from_session

from .sessions import session_get

logger = logging.getLogger("plu_api")
router = APIRouter()


@router.get("/session/{session_id}/map")
def get_session_map(session_id: str, buffer_m: float = 100.0):
    """
    Données cartographiques GeoJSON (EPSG:4326) d'une session existante.

    Retourne :
      - parcelle  : GeoJSON Feature (contour unité foncière)
      - zones       : GeoJSON FeatureCollection (zonage PLU)
      - prescriptions : surfaciques / linéaires / ponctuelles
      - servitudes  : assiettes surfaciques SUP (sup_assiette_s)

    Paramètres :
      - buffer_m  : buffer autour de l'unité foncière en mètres (défaut: 100)
    """
    session = session_get(session_id)
    if not session:
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id} introuvable.",
        )

    refs = refs_from_session(session)
    if not refs:
        raise HTTPException(
            status_code=422,
            detail="La session ne contient aucune référence cadastrale.",
        )

    logger.info(f"map fetch — session={session_id} refs={refs} buffer={buffer_m}m")

    result = build_carto_payload(DB_CONFIG, buffer_m=buffer_m, **refs)

    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])

    return result
