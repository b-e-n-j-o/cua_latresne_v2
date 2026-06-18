# -*- coding: utf-8 -*-
"""Contrôle d'accès multi-communes."""

from services.auth.commune_access import (
    assert_authorized_for_commune_slug,
    assert_authorized_for_insee,
    get_authorized_commune_slugs,
    get_authorized_insee_codes,
    get_user_commune_access,
    is_authorized_for_commune_slug,
    is_authorized_for_insee,
)

__all__ = [
    "assert_authorized_for_commune_slug",
    "assert_authorized_for_insee",
    "get_authorized_commune_slugs",
    "get_authorized_insee_codes",
    "get_user_commune_access",
    "is_authorized_for_commune_slug",
    "is_authorized_for_insee",
]
