"""
commune_profile.py — modèle de configuration d'une commune (client PLU).

Responsabilité
--------------
Définir **ce qui diffère** entre communes sans dupliquer le code métier :
  - schéma PostgreSQL (`argeles`, `latresne`, …) ;
  - préfixe HTTP (`/api/plu/{slug}`) ;
  - prompt système Gemini ;
  - catalogue de couches (`catalog` ← JSON dans `communes/catalogs/`) ;
  - liste des tools exposés au modèle.

Ce module ne fait **aucune requête SQL** ni appel LLM : il ne contient que des
dataclasses et des helpers (ex. `load_prompt`, `table()`).

Relations
---------
  - Instancié une fois par commune dans `communes/argeles.py`, `latresne.py`.
  - Enregistré dans `communes/__init__.py` (registre global).
  - Lu à l'exécution via `commune_context` (requête HTTP) ou passé à
    `api.create_plu_router(profile)` (montage FastAPI).

Pour ajouter une commune : créer `communes/<slug>.py` + entrée dans le registre.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .layer_catalog import LayerCatalog, load_commune_catalog


@dataclass(frozen=True)
class CommuneProfile:
    """
    Configuration complète d'un déploiement agent PLU pour une commune.

    Une instance = un client (utilisateurs, sessions et données isolés par schéma).
    Immutable (`frozen`) pour être partagée entre threads/async sans mutation.
    """

    slug: str
    """Identifiant URL et registre (ex. `argeles`, `latresne`)."""

    schema: str
    """Schéma PostgreSQL : toutes les tables métier sont `{schema}.<table>`."""
    label: str
    insee: str | None
    api_prefix: str
    api_tags: tuple[str, ...]
    system_prompt: str
    catalog: LayerCatalog = field(repr=False)
    # Tools LLM exposés à Gemini pour cette commune (dispatch filtre par profil).
    llm_tool_names: tuple[str, ...] = (
        "get_parcelle",
        "get_contexte_parcelle",
        "search_articles_urbanisme",
        "get_article_urbanisme_by_num",
    )
    gemini_model: str | None = None  # None → variable d'env globale

    def table(self, name: str) -> str:
        """Nom qualifié `schema.table` (parcelles, zonage_plu, …)."""
        return f"{self.schema}.{name}"

    def sessions_table(self) -> str:
        """Table de persistance des conversations (`plu_sessions`)."""
        return self.table("plu_sessions")

    def messages_table(self) -> str:
        """Historique des tours user/model (`plu_messages`)."""
        return self.table("plu_messages")

    def layer_enabled(self, layer_id: str) -> bool:
        """Couche active dans le catalogue JSON fusionné."""
        return self.catalog.is_enabled(layer_id)


def build_commune_profile(
    *,
    slug: str,
    schema: str,
    label: str,
    api_prefix: str,
    api_tags: tuple[str, ...],
    system_prompt: str,
    insee: str | None = None,
    catalog: LayerCatalog | None = None,
    llm_tool_names: tuple[str, ...] = (
        "get_parcelle",
        "get_contexte_parcelle",
        "search_articles_urbanisme",
        "get_article_urbanisme_by_num",
    ),
    gemini_model: str | None = None,
) -> CommuneProfile:
    """Fabrique un profil avec catalogue JSON chargé pour `slug`."""
    return CommuneProfile(
        slug=slug,
        schema=schema,
        label=label,
        insee=insee,
        api_prefix=api_prefix,
        api_tags=api_tags,
        system_prompt=system_prompt,
        catalog=catalog or load_commune_catalog(slug),
        llm_tool_names=llm_tool_names,
        gemini_model=gemini_model,
    )


def load_prompt(path: str, *, default: str = "") -> str:
    """
    Charge le prompt système depuis `communes/prompts/<commune>_system.md`.

    Sépare le texte LLM du code Python pour faciliter les révisions métier.
    """
    try:
        from pathlib import Path

        text = Path(path).read_text(encoding="utf-8").strip()
        return text or default
    except OSError:
        return default
