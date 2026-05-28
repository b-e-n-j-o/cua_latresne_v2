"""
communes/ — registre des profils PLU par client.

Responsabilité
--------------
Centraliser les instances `CommuneProfile` et les exposer par slug :
  - `COMMUNE_PROFILES` : dict slug → profil ;
  - `get_commune_profile(slug)` : résolution pour factory / admin ;
  - `list_commune_slugs()` : liste des communes déployées.

Structure du package
--------------------
  - `argeles.py`   → ARGELES_PROFILE (production actuelle)
  - `latresne.py`  → LATRESNE_PROFILE (squelette, schéma `latresne`)
  - `prompts/`     → fichiers Markdown des prompts système (un par commune)

Ajouter une commune
-------------------
  1. Créer `communes/<slug>.py` avec un `CommuneProfile`.
  2. Ajouter l'entrée dans `COMMUNE_PROFILES` ci-dessous.
  3. Monter le routeur dans `api.py` / `main.py` via `create_plu_router(profile)`.
  4. Côté frontend : `communeConfig.ts` + route `/slug/chat`.
"""

from ..commune_profile import CommuneProfile
from .argeles import ARGELES_PROFILE
from .france import FRANCE_PROFILE
from .latresne import LATRESNE_PROFILE

COMMUNE_PROFILES: dict[str, CommuneProfile] = {
    ARGELES_PROFILE.slug: ARGELES_PROFILE,
    FRANCE_PROFILE.slug: FRANCE_PROFILE,
    LATRESNE_PROFILE.slug: LATRESNE_PROFILE,
}


def get_commune_profile(slug: str) -> CommuneProfile:
    """Retourne le profil d'une commune ou lève `KeyError` si slug inconnu."""
    key = (slug or "").strip().lower()
    profile = COMMUNE_PROFILES.get(key)
    if not profile:
        known = ", ".join(sorted(COMMUNE_PROFILES))
        raise KeyError(f"Commune PLU inconnue : {slug!r}. Connues : {known}.")
    return profile


def list_commune_slugs() -> list[str]:
    """Slugs des communes enregistrées (ordre alphabétique)."""
    return sorted(COMMUNE_PROFILES)
