"""
communes/latresne.py — profil PLU Latresne.

Catalogue : `catalogs/default.json` + `catalogs/latresne.json`
(pprt, désactivation de couches, renommages de tables, etc.).
"""

from pathlib import Path

from ..commune_profile import build_commune_profile, load_prompt

_PROMPTS = Path(__file__).resolve().parent / "prompts"

LATRESNE_PROFILE = build_commune_profile(
    slug="latresne",
    schema="latresne",
    label="Latresne",
    insee=None,
    api_prefix="/api/plu/latresne",
    api_tags=("plu-agent-latresne",),
    system_prompt=load_prompt(
        _PROMPTS / "latresne_system.md",
        default="Expert PLU Latresne.",
    ),
)
