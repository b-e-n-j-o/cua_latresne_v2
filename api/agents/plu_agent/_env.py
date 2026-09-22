"""
_env.py — configuration d'infrastructure partagée (toutes communes).

Responsabilité
--------------
Centraliser ce qui est **commun** à tous les clients PLU sur le même déploiement :
  - connexion Supabase / PostGIS (`DB_CONFIG`) ;
  - clés et modèle Gemini (`GEMINI_API_KEY`, `GEMINI_MODEL`).

Ce qui n'est **pas** ici (voir `commune_profile` / `communes/`) :
  - schéma SQL (`argeles` vs `latresne`) ;
  - préfixe HTTP `/api/plu/{slug}` ;
  - prompt système, couches carto, liste de tools.

Les profils par commune peuvent surcharger `gemini_model` plus tard ; par défaut
toutes les communes partagent le modèle défini dans l'environnement Render.
"""

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
GEMINI_MODEL   = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash")

# Stack Mistral (chat GLM 5.2 + embeddings). Clé BEN en priorité (palier GLM).
MISTRAL_API_KEY = (
    (os.environ.get("MISTRAL_API_KEY_BEN") or "").strip()
    or (os.environ.get("MISTRAL_API_KEY") or "").strip()
)
MISTRAL_CHAT_MODEL = os.environ.get("MISTRAL_CHAT_MODEL", "zai-glm-5-2")
MISTRAL_EMBED_MODEL = os.environ.get("MISTRAL_EMBED_MODEL", "mistral-embed")
MISTRAL_REASONING_EFFORT = os.environ.get("MISTRAL_REASONING_EFFORT", "high")
MISTRAL_EMBED_DIM = int(os.environ.get("MISTRAL_EMBED_DIM", "1024"))
# Défaut chat PLU : mistral (l'UI n'envoie pas encore provider). Vertex si PLU_LLM_PROVIDER=gemini.
PLU_LLM_PROVIDER = (os.environ.get("PLU_LLM_PROVIDER") or "mistral").strip().lower()

# Préfixe HTTP par commune : voir communes/ + api.create_plu_router()
