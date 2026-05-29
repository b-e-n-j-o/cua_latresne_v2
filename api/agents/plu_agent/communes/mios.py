"""
communes/mios.py — profil PLU Mios.

Catalogue : `catalogs/default.json` + `catalogs/mios.json` (vide = socle GPU uniquement).
"""

from pathlib import Path

from ..commune_profile import build_commune_profile, load_prompt

_PROMPTS = Path(__file__).resolve().parent / "prompts"

MIOS_PROFILE = build_commune_profile(
    slug="mios",
    schema="mios",
    label="Mios",
    insee="33284",
    api_prefix="/api/plu/mios",
    api_tags=("plu-agent-mios",),
    system_prompt=load_prompt(
        _PROMPTS / "mios_system.md",
        default="Expert PLU Mios.",
    ),
    llm_tool_names=(
        "get_parcelle",
        "get_contexte_parcelle",
        "get_reglement_zone",
        "search_articles_urbanisme",
        "get_article_urbanisme_by_num",
    ),
)
