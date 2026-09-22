"""
Client Vertex unique pour l'agent PLU (chat + embeddings).

Pas de Gemini Developer (`generativelanguage.googleapis.com`) :
toujours `vertexai=True` → `aiplatform.googleapis.com`.
"""

from __future__ import annotations

import os

from google import genai

try:
    from ._env import GEMINI_API_KEY
except ImportError:
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")


def build_vertex_client() -> genai.Client:
    if GEMINI_API_KEY:
        return genai.Client(vertexai=True, api_key=GEMINI_API_KEY)
    return genai.Client(vertexai=True)
