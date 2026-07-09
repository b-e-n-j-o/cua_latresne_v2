# -*- coding: utf-8 -*-
"""Servitudes Argelès — wrapper vers api.modules_communs.servitudes."""

from __future__ import annotations

from sqlalchemy.engine import Engine

try:
    from api.cuas.argeles.db import SCHEMA, get_engine
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from db import SCHEMA, get_engine

from api.modules_communs.servitudes import (
    ARGELES_SERVITUDES_CONFIG,
    REGLEMENTS_SCHEMA,
    REGLEMENTS_TABLE,
    I4_VARIANTES_TABLE,
    compute_servitudes_reglementation as _compute_servitudes_reglementation,
)

__all__ = [
    "SCHEMA",
    "REGLEMENTS_SCHEMA",
    "REGLEMENTS_TABLE",
    "I4_VARIANTES_TABLE",
    "ARGELES_SERVITUDES_CONFIG",
    "compute_servitudes_reglementation",
]


def compute_servitudes_reglementation(
    uf_wkt: str,
    *,
    engine=None,
    schema: str = SCHEMA,
    surface_sig: float = 0.0,
    min_pct_sig: float | None = None,
    reglements_schema: str = REGLEMENTS_SCHEMA,
    reglement_table: str = REGLEMENTS_TABLE,
    i4_table: str = I4_VARIANTES_TABLE,
) -> dict:
    """
    Retourne les servitudes intersectées enrichies de leur réglementation.
    Géométrie : {schema}.servitudes. Laius : public.servitudes_reglements (+ i4).
    """
    del schema, reglements_schema, reglement_table, i4_table  # API stable, config centralisée
    engine = engine or get_engine()
    kwargs: dict = {
        "engine": engine,
        "config": ARGELES_SERVITUDES_CONFIG,
        "surface_sig": surface_sig,
    }
    if min_pct_sig is not None:
        kwargs["min_pct_sig"] = min_pct_sig
    return _compute_servitudes_reglementation(uf_wkt, **kwargs)
