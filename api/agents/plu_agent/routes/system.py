"""Endpoints système — health check et catalogue des tools."""

from fastapi import APIRouter

from .._env import GEMINI_MODEL
from ..commune_profile import CommuneProfile

try:
    from ..tools import TOOL_DECLARATIONS
except ImportError:
    from tools import TOOL_DECLARATIONS


def register(router: APIRouter, profile: CommuneProfile, bind) -> None:
    @router.get("/healthz")
    @bind
    def health():
        enabled = [L.id for L in profile.catalog.enabled_layers()]
        return {
            "status": "ok",
            "commune": profile.slug,
            "schema": profile.schema,
            "model": profile.gemini_model or GEMINI_MODEL,
            "layers_enabled": enabled,
        }

    @router.get("/tools")
    @bind
    def list_tools():
        names = set(profile.llm_tool_names)
        return {
            "commune": profile.slug,
            "tools": [
                {"name": fd.name, "description": fd.description}
                for fd in TOOL_DECLARATIONS.function_declarations
                if fd.name in names
            ],
        }
