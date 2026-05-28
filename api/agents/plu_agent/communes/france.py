"""
communes/france.py — profil PLU France entière (fallback GPU live).
"""

from pathlib import Path

from ..commune_profile import build_commune_profile, load_prompt

_PROMPTS = Path(__file__).resolve().parent / "prompts"

FRANCE_PROFILE = build_commune_profile(
    slug="france",
    schema="france",
    label="France entière",
    insee=None,
    api_prefix="/api/plu/france",
    api_tags=("plu-agent-france",),
    system_prompt=load_prompt(
        _PROMPTS / "france_system.md",
        default="Expert urbanisme France entière (GPU live).",
    ),
    llm_tool_names=(
        "resolve_commune_insee",
        "get_geoportail_contexte_live",
    ),
)

