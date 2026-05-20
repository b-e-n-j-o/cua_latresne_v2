"""Endpoints système — health check et catalogue des tools."""

from fastapi import APIRouter

from .._env import GEMINI_MODEL

try:
    from ..tools import TOOL_DECLARATIONS
except ImportError:
    from tools import TOOL_DECLARATIONS

router = APIRouter()


@router.get("/healthz")
def health():
    return {"status": "ok", "model": GEMINI_MODEL}


@router.get("/tools")
def list_tools():
    return {
        "tools": [
            {"name": fd.name, "description": fd.description}
            for fd in TOOL_DECLARATIONS.function_declarations
        ]
    }
