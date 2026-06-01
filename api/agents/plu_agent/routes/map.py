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
from ..commune_profile import CommuneProfile

try:
    from ..cartography.carto import build_carto_payload
    from ..tools.utils.parcel_geom import refs_from_session, resolve_session_refs
except ImportError:
    from cartography.carto import build_carto_payload
    from tools.utils.parcel_geom import refs_from_session, resolve_session_refs

from .sessions import messages_get, session_get, session_persist_refs

logger = logging.getLogger("plu_api")


def register(router: APIRouter, profile: CommuneProfile, bind) -> None:
    @router.get("/session/{session_id}/map")
    @bind
    def get_session_map(session_id: str, buffer_m: float = 100.0):
        """
        Données cartographiques GeoJSON (EPSG:4326) d'une session existante.

        Paramètres :
          - buffer_m  : buffer (m) pour le zonage PLU uniquement (défaut: 100).
            Prescriptions, servitudes et informations : intersection stricte parcelle.
        """
        session = session_get(session_id)
        if not session:
            raise HTTPException(
                status_code=404,
                detail=f"Session {session_id} introuvable.",
            )

        refs = refs_from_session(session)
        if not refs:
            messages = messages_get(session_id)
            refs = resolve_session_refs(session, messages)
            if refs and not refs_from_session(session):
                session_persist_refs(session_id, **refs)
                session = session_get(session_id) or session
                logger.info(
                    "map fetch — refs reconstituées depuis l'historique session=%s",
                    session_id,
                )

        if not refs:
            raise HTTPException(
                status_code=422,
                detail=(
                    "La session ne contient aucune référence cadastrale. "
                    "Indiquez une parcelle (section + numéro ou IDU) dans votre message."
                ),
            )

        logger.info(
            f"map fetch — commune={profile.slug} session={session_id} "
            f"refs={refs} buffer={buffer_m}m"
        )

        result = build_carto_payload(DB_CONFIG, buffer_m=buffer_m, **refs)

        if result.get("error"):
            logger.warning("map fetch failed — %s", result["error"])
            raise HTTPException(status_code=400, detail=result["error"])

        return result
