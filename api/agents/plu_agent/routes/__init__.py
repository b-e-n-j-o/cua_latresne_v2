"""Agrégation des routeurs FastAPI de l'agent PLU."""

from fastapi import APIRouter

from .chat import router as chat_router
from .sessions import router as sessions_router
from .system import router as system_router


def register_routes(router: APIRouter) -> None:
    router.include_router(system_router)
    router.include_router(sessions_router)
    router.include_router(chat_router)
