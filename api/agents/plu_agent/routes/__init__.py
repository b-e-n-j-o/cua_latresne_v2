"""
routes/__init__.py — agrégation des endpoints PLU pour un profil commune.

Responsabilité
--------------
Appelé une fois par `api.create_plu_router(profile)` pour attacher au même
`APIRouter` tous les sous-modules métier, **déjà liés** au bon profil :

  - `system`   → GET /healthz, GET /tools
  - `sessions` → CRUD sessions + messages (tables `{schema}.plu_*`)
  - `chat`     → POST /chat/{id} (boucle Gemini + tools)
  - `map`      → GET /session/{id}/map (GeoJSON, hors LLM)

Chaque module expose `register(router, profile, bind)` au lieu d'un routeur
global unique : deux communes = deux routeurs = deux jeux d'endpoints isolés,
sans mélange de schéma SQL entre clients.

Ne pas importer ce package pour une seule route : passer par `create_plu_router`.
"""

from fastapi import APIRouter

from ..commune_profile import CommuneProfile
from .profile_guard import bind_commune_profile


def register_routes(router: APIRouter, profile: CommuneProfile) -> None:
    """
    Enregistre toutes les routes PLU sur `router` pour la commune donnée.

    `profile` est figé à la construction du routeur ; `bind` propage ce profil
    dans `commune_context` à chaque requête entrante.
    """
    from . import chat, map, sessions, system

    bind = bind_commune_profile(profile)
    system.register(router, profile, bind)
    sessions.register(router, profile, bind)
    chat.register(router, profile, bind)
    map.register(router, profile, bind)
