"""Variables d'environnement partagées (DB + Gemini) — pas de logique métier."""

import os


def _require_env(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise RuntimeError(
            f"Variable d'environnement manquante pour plu_agent : {name}. "
            f"Ajoutez-la dans le dashboard Render (Environment)."
        )
    return value


DB_CONFIG = {
    "host":            _require_env("SUPABASE_HOST"),
    "port":            int(os.environ.get("SUPABASE_PORT", 5432)),
    "dbname":          _require_env("SUPABASE_DB"),
    "user":            _require_env("SUPABASE_USER"),
    "password":        _require_env("SUPABASE_PASSWORD"),
    "sslmode":         "require",
    "connect_timeout": 15,
}

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
GEMINI_MODEL   = os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-lite")

API_PREFIX = "/api/plu/argeles"
API_TAGS   = ["plu-agent-argeles"]
