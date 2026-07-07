# -*- coding: utf-8 -*-
"""Servitudes Latresne — wrapper vers api.modules_communs.servitudes."""

from __future__ import annotations

from sqlalchemy.engine import Engine

from api.modules_communs.servitudes import (
    LATRESNE_SERVITUDES_CONFIG,
    compute_servitudes_reglementation as _compute_servitudes_reglementation,
)

SCHEMA = LATRESNE_SERVITUDES_CONFIG.geo_schema
SERVITUDES_TABLE = LATRESNE_SERVITUDES_CONFIG.servitudes_table


def compute_servitudes_reglementation(
    uf_wkt: str,
    *,
    engine: Engine,
) -> dict:
    return _compute_servitudes_reglementation(
        uf_wkt,
        engine=engine,
        config=LATRESNE_SERVITUDES_CONFIG,
    )
