"""
communes/argeles.py — profil PLU Argelès-sur-Mer.

Catalogue de couches : `catalogs/default.json` + `catalogs/argeles.json` (surcharges).
"""

from pathlib import Path

from ..commune_profile import build_commune_profile, load_prompt

_PROMPTS = Path(__file__).resolve().parent / "prompts"

ARGELES_PROFILE = build_commune_profile(
    slug="argeles",
    schema="argeles",
    label="Argelès-sur-Mer",
    insee="66008",
    api_prefix="/api/plu/argeles",
    api_tags=("plu-agent-argeles",),
    system_prompt=load_prompt(
        _PROMPTS / "argeles_system.md",
        default="Expert PLU Argelès-sur-Mer.",
    ),
    llm_tool_names=(
        "get_parcelle",
        "get_contexte_parcelle",
        "get_reglement_zone",
        "get_ppr_reglement",
        "get_pprif_reglement",
        "search_articles_urbanisme",
        "get_article_urbanisme_by_num",
    ),
)
