"""Variables d'environnement partagées (DB + Gemini) — pas de logique métier."""

import os

DB_CONFIG = {
    "host":            os.environ["SUPABASE_HOST"],
    "port":            int(os.environ.get("SUPABASE_PORT", 5432)),
    "dbname":          os.environ["SUPABASE_DB"],
    "user":            os.environ["SUPABASE_USER"],
    "password":        os.environ["SUPABASE_PASSWORD"],
    "sslmode":         "require",
    "connect_timeout": 15,
}

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
GEMINI_MODEL   = os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-lite")

API_PREFIX = "/api/plu/argeles"
API_TAGS   = ["plu-agent-argeles"]
