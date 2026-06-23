"""Configuration DB partagée (re-export plu_agent)."""

from api.agents.plu_agent._env import DB_CONFIG, GEMINI_API_KEY, GEMINI_MODEL

__all__ = ["DB_CONFIG", "GEMINI_API_KEY", "GEMINI_MODEL"]
