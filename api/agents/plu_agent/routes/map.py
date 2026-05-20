"""
Map PLU — endpoint GET /session/{session_id}/map.

Sert les données cartographiques GeoJSON d'une session existante
sans passer par le LLM. Utilisé par le frontend pour :
  - afficher la carte au chargement d'une session depuis la sidebar
  - rafraîchir la carte sans nouveau tour de conversation

Le LLM déclenche l'affichage via get_map_data (tool call) ; show_map=true dans la réponse chat.
Les géométries ne transitent jamais par Gemini — uniquement ce endpoint :
  - Frontend → GET /session/{id}/map → GeoJSON pour MapLibre
"""

import logging

from fastapi import APIRouter, HTTPException

from .._env import DB_CONFIG

try:
    from ..tools import get_map_data
except ImportError:
    from tools import get_map_data

from .sessions import session_get

logger = logging.getLogger("plu_api")
router = APIRouter()


@router.get("/session/{session_id}/map")
def get_session_map(session_id: str, buffer_m: float = 100.0):
    """
    Données cartographiques GeoJSON (EPSG:4326) d'une session existante.

    Retourne :
      - parcelle  : GeoJSON Feature (contour de la parcelle)
      - zones     : GeoJSON FeatureCollection (zones PLU intersectantes,
                    clippées au buffer, avec couleurs par typezone CNIG)

    Paramètres :
      - buffer_m  : buffer autour de la parcelle en mètres (défaut: 100)

    Utilisé par PluMapPanel au chargement d'une session depuis la sidebar,
    ou lors d'un refresh explicite de la carte.
    """
    session = session_get(session_id)
    if not session:
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id} introuvable.",
        )

    section = session.get("section")
    numero  = session.get("numero")
    idu     = session.get("idu")
    geojson = session.get("geojson")

    if not any([section and numero, idu, geojson]):
        raise HTTPException(
            status_code=422,
            detail="La session ne contient aucune référence géographique (section+numero, idu ou geojson).",
        )

    logger.info(
        f"map fetch — session={session_id} "
        f"section={section} numero={numero} idu={idu} buffer={buffer_m}m"
    )

    result = get_map_data(
        DB_CONFIG,
        section=section,
        numero=numero,
        idu=idu,
        geojson=geojson,
        buffer_m=buffer_m,
    )

    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])

    return result