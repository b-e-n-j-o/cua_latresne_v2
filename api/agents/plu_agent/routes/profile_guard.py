"""
profile_guard.py — activation du profil commune sur chaque endpoint.

Responsabilité
--------------
Garantir que `commune_context.get_current_profile()` et `q("table")` ciblent
la bonne commune pour **toute** la durée d'une requête HTTP :
  - entrée : `set_current_profile(profile)` ;
  - sortie (finally) : `reset_current_profile(token)`.

Utilisation
-----------
Dans `routes/__init__.py`, on construit une fois :
    bind = bind_commune_profile(profile)
Puis chaque handler :
    @router.post("/session")
    @bind
    def create_session(...): ...

Sans ce garde, les tools lèveraient « Aucun CommuneProfile actif » ou liraient
le mauvais schéma sous charge concurrente (deux communes en parallèle).
"""

from __future__ import annotations

from functools import wraps
from typing import Callable, TypeVar

from ..commune_context import reset_current_profile, set_current_profile
from ..commune_profile import CommuneProfile

F = TypeVar("F", bound=Callable)


def bind_commune_profile(profile: CommuneProfile) -> Callable[[F], F]:
    """
    Décorateur de route : fixe le profil commune pour l'appel du handler.

    Le `profile` est capturé à l'enregistrement du routeur (une instance par
    commune dans `api.create_plu_router`), pas lu depuis l'URL à chaque fois.
    """
    def decorator(fn: F) -> F:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            token = set_current_profile(profile)
            try:
                return fn(*args, **kwargs)
            finally:
                reset_current_profile(token)

        return wrapper  # type: ignore[return-value]

    return decorator
