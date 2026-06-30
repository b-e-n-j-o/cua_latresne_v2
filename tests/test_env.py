# -*- coding: utf-8 -*-
"""Chargement des variables d'env pour les tests (séparé du .env applicatif)."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

# Racine backend cua_latresne_v4
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Fichier gitignoré — copier depuis .env.test.example
TEST_CREDENTIALS_ENV = PROJECT_ROOT / ".env.test.local"
# Variante optionnelle dans tests/
TEST_CREDENTIALS_ENV_ALT = PROJECT_ROOT / "tests" / ".env.test.local"


def load_app_env() -> None:
    """Config app (SUPABASE_URL, clés…) — .env principal."""
    load_dotenv(PROJECT_ROOT / ".env")


def load_test_credentials_env() -> bool:
    """
    Identifiants smoke tests uniquement (AUTH_TEST_*).
    Ne mélange pas avec le .env de l'app.
    Retourne True si un fichier dédié a été trouvé.
    """
    for path in (TEST_CREDENTIALS_ENV, TEST_CREDENTIALS_ENV_ALT):
        if path.is_file():
            load_dotenv(path)
            return True
    return False


def load_all_test_env() -> None:
    """App d'abord, puis creds tests (sans écraser l'env shell existant)."""
    load_app_env()
    load_test_credentials_env()
