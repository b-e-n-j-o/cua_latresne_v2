"""Charge le .env du backend Kerelia (local) ou les variables Render (prod)."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
BACKEND_ROOT = HERE.parent.parent
ENV_BACKEND = BACKEND_ROOT / ".env"


def load_project_env() -> None:
    """
    Local : cua_latresne_v4/.env
    Render : variables d'environnement du service (dashboard), sans fichier .env
    """
    if ENV_BACKEND.is_file():
        load_dotenv(ENV_BACKEND, override=True)
    elif not os.getenv("SUPABASE_HOST"):
        raise RuntimeError(
            f"Ni {ENV_BACKEND} ni SUPABASE_HOST en environnement (Render: configurer le dashboard)"
        )
