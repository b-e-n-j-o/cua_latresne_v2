#!/usr/bin/env python3
"""
api.py — montage FastAPI de l'agent PLU multi-communes.

Responsabilité
--------------
Créer un `APIRouter` **par commune** à partir d'un `CommuneProfile` :
  - préfixe URL (`/api/plu/argeles`, `/api/plu/latresne`, …) ;
  - enregistrement des routes chat / sessions / carte / health (`register_routes`).

Exports utilisés par `main.py`
------------------------------
  - `argeles_router` : production Argelès (alias `router` pour compatibilité) ;
  - `latresne_router` : même code, autre profil et schéma SQL ;
  - `create_plu_router(profile)` : factory pour une nouvelle commune.

Ce module ne définit pas la logique LLM ni SQL : il ne fait qu'assembler des
routeurs déjà paramétrés par le profil passé en argument.

Standalone (dev)
----------------
    uvicorn api.agents.plu_agent.api:app --reload --port 8001

Monte les deux communes sur une mini-app locale (`create_standalone_app`).
"""

import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .commune_profile import CommuneProfile
from .communes import ARGELES_PROFILE, FRANCE_PROFILE, LATRESNE_PROFILE, get_commune_profile
from .routes import register_routes


def create_plu_router(profile: CommuneProfile) -> APIRouter:
    """
    Construit un routeur complet pour une commune (sessions, chat, map, health).

    Chaque endpoint est enveloppé par `bind_commune_profile` pour activer
    `commune_context` avant tout accès SQL ou LLM.
    """
    router = APIRouter(prefix=profile.api_prefix, tags=list(profile.api_tags))
    register_routes(router, profile)
    return router


argeles_router = create_plu_router(ARGELES_PROFILE)
latresne_router = create_plu_router(LATRESNE_PROFILE)
france_router = create_plu_router(FRANCE_PROFILE)

# Compatibilité imports existants (main.py historique, scripts)
router = argeles_router
API_PREFIX = ARGELES_PROFILE.api_prefix
API_TAGS = list(ARGELES_PROFILE.api_tags)


def create_standalone_app() -> FastAPI:
    """App de développement avec toutes les communes du registre."""
    app = FastAPI(
        title="Agent PLU multi-communes",
        description="LLM outillé PLU — un préfixe HTTP par client / schéma SQL",
        version="2.1.0",
    )
    for slug in ("argeles", "latresne", "france"):
        app.include_router(create_plu_router(get_commune_profile(slug)))
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app


app = create_standalone_app()

if __name__ == "__main__":
    uvicorn.run("api.agents.plu_agent.api:app", host="0.0.0.0", port=8001, reload=True)
