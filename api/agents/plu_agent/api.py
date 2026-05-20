#!/usr/bin/env python3
"""
api.py — point d'entrée FastAPI, monté dans main.py.

Logique métier par domaine :
    routes/chat.py      → prompt LLM, boucle Gemini, POST /chat/{id}
    routes/sessions.py  → SQL Supabase, POST/GET session(s)
    routes/system.py    → GET /healthz, GET /tools
    tools.py            → tools PostGIS exposés au LLM
    _env.py             → variables d'environnement (DB, Gemini)

Lancer en standalone :
    python api.py
    uvicorn api.agents.plu_agent.api:app --reload --port 8001
"""

import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ._env import API_PREFIX, API_TAGS
from .routes import register_routes

router = APIRouter(prefix=API_PREFIX, tags=API_TAGS)
register_routes(router)

app = FastAPI(
    title="Agent PLU Argelès-sur-Mer",
    description="LLM outillé pour l'analyse réglementaire PLU via PostGIS — sessions Supabase",
    version="2.0.0",
)
app.include_router(router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)
