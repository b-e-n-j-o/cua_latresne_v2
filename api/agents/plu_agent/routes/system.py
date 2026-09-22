"""Endpoints système — health check, ping LLM et catalogue des tools."""

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from .._env import GEMINI_MODEL, MISTRAL_CHAT_MODEL, PLU_LLM_PROVIDER
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
            "provider": PLU_LLM_PROVIDER,
            "model": (
                MISTRAL_CHAT_MODEL
                if PLU_LLM_PROVIDER == "mistral"
                else (profile.gemini_model or GEMINI_MODEL)
            ),
            "layers_enabled": enabled,
        }

    @router.get("/llm-health")
    @bind
    def llm_health(stack: str = Query("vertex")):
        """
        Ping LLM + embeddings. ``stack=vertex`` (défaut) ou ``stack=mistral``.
        """
        stack = (stack or "vertex").strip().lower()
        if stack not in ("vertex", "mistral"):
            raise HTTPException(status_code=400, detail="stack doit être vertex ou mistral")
        if stack == "mistral":
            from .chat_mistral import probe_mistral

            result = probe_mistral()
        else:
            from .chat import probe_llm

            result = probe_llm()
        result.setdefault("commune", profile.slug)
        return JSONResponse(content=result, status_code=200 if result.get("ok") else 503)

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
