"""
commune_context.py — profil commune actif pendant une requête.

Responsabilité
--------------
Propager le `CommuneProfile` de l'endpoint HTTP vers tout le code appelé en
cascade (sessions, chat, tools PostGIS) **sans** le passer en argument à chaque
fonction.

Mécanisme : `ContextVar` (compatible async FastAPI — une valeur par requête).

API principale
--------------
  - `set_current_profile` / `reset_current_profile` : posés par `profile_guard`
    autour de chaque endpoint.
  - `get_current_profile()` : lu dans les routes et la persistance SQL.
  - `q("parcelles")` → `"latresne.parcelles"` si la requête est sur `/api/plu/latresne`.
  - `current_schema()` : défaut `argeles` hors HTTP (tests CLI, scripts).

À ne pas confondre
------------------
  - `commune_profile.py` : définition statique des profils.
  - `communes/*.py` : instances concrètes (Argelès, Latresne).
  - Ce fichier : **quel** profil est actif **maintenant**.
"""

from __future__ import annotations

from contextvars import ContextVar

from .commune_profile import CommuneProfile

_profile_ctx: ContextVar[CommuneProfile | None] = ContextVar(
    "plu_commune_profile", default=None
)


def set_current_profile(profile: CommuneProfile):
    """Active un profil ; retourne un token pour `reset_current_profile`."""
    return _profile_ctx.set(profile)


def reset_current_profile(token) -> None:
    """Restaure le profil précédent en fin d'endpoint (finally)."""
    _profile_ctx.reset(token)


def get_current_profile() -> CommuneProfile:
    """Profil obligatoire — lève si l'endpoint a oublié le garde `@bind`."""
    profile = _profile_ctx.get()
    if profile is None:
        raise RuntimeError(
            "Aucun CommuneProfile actif — la route doit appeler set_current_profile()."
        )
    return profile


def get_current_profile_optional() -> CommuneProfile | None:
    """Profil actif ou None (diagnostic, code optionnel)."""
    return _profile_ctx.get()


def current_schema(default: str = "argeles") -> str:
    """
    Nom du schéma SQL pour les requêtes outillées.

    Hors requête HTTP (tests unitaires, `python plu_agent.py`), retourne `argeles`
    par défaut pour ne pas casser les scripts existants.
    """
    profile = _profile_ctx.get()
    return profile.schema if profile else default


def q(table: str, *, schema: str | None = None) -> str:
    """
    Qualifie une table pour le SQL dynamique : `{schema}.{table}`.

    Exemple dans un tool :
        f\"FROM {q('zonage_plu')} z\"  →  FROM latresne.zonage_plu z

    Préférer cette helper à un schéma codé en dur `argeles.`.
    """
    sch = schema or current_schema()
    return f"{sch}.{table}"
