# -*- coding: utf-8 -*-
"""Fixtures pytest — smoke auth E2E.

Auth E2E smoke tests.

Usage :
    pytest tests/smoke -v
    pytest tests/smoke --prod -v

Config :
    .env.test.example   — identifiants tests uniquement (gitignoré)
    .env.test.local     — identifiants tests uniquement (gitignoré)

Ce script est utilisé pour les tests smoke auth E2E, les tests sont exécutés sur le backend local et sur le backend déployé.
"""

from __future__ import annotations

import os

import pytest

from tests.smoke.auth_e2e import DEFAULT_API_BASE, PROD_API_BASE, supabase_sign_in
from tests.test_env import TEST_CREDENTIALS_ENV, load_all_test_env

load_all_test_env()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--prod",
        action="store_true",
        default=False,
        help=f"Cible l'API déployée ({PROD_API_BASE})",
    )


@pytest.fixture(scope="session")
def api_base(request: pytest.FixtureRequest) -> str:
    if request.config.getoption("--prod"):
        return PROD_API_BASE
    return os.getenv("AUTH_TEST_API_BASE", DEFAULT_API_BASE).rstrip("/")


@pytest.fixture(scope="session")
def auth_credentials() -> tuple[str, str]:
    email = os.getenv("AUTH_TEST_EMAIL", "").strip()
    password = os.getenv("AUTH_TEST_PASSWORD", "").strip()
    if not email or not password:
        pytest.skip(
            "AUTH_TEST_EMAIL / AUTH_TEST_PASSWORD manquants — "
            f"créer {TEST_CREDENTIALS_ENV.name} depuis .env.test.example "
            "(ou exporter les variables en shell / secrets CI)."
        )
    return email, password


@pytest.fixture(scope="session")
def expect_superadmin() -> bool:
    raw = os.getenv("AUTH_TEST_EXPECT_SUPERADMIN", "").strip().lower()
    return raw in ("1", "true", "yes", "on")


@pytest.fixture(scope="session")
def commune_slug() -> str | None:
    slug = os.getenv("AUTH_TEST_COMMUNE_SLUG", "").strip()
    return slug or None


@pytest.fixture(scope="session")
def auth_session(auth_credentials: tuple[str, str]) -> dict:
    email, password = auth_credentials
    return supabase_sign_in(email, password)
