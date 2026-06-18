"""Colonnes JSONB par couche SIG sur parcelles + assemblage API sig_resume."""

from __future__ import annotations

import math
from decimal import Decimal
from typing import Any

LAYER_COL_PREFIX = "sig_"
LEGACY_RESUME_COLUMN = "sig_resume"


def sanitize_for_json(value: Any) -> Any:
    """Rend un objet sérialisable JSON (NaN/Inf → null)."""
    if isinstance(value, dict):
        return {k: sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_for_json(v) for v in value]
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, Decimal):
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    return value


def layer_column(layer_key: str, cfg: dict | None = None) -> str:
    """Nom de colonne PostgreSQL pour une couche (ex. hauteurs → sig_hauteurs)."""
    if cfg and cfg.get("column"):
        return str(cfg["column"])
    return f"{LAYER_COL_PREFIX}{layer_key}"


def layer_key_from_column(column: str) -> str | None:
    if not column.startswith(LAYER_COL_PREFIX) or column == LEGACY_RESUME_COLUMN:
        return None
    return column[len(LAYER_COL_PREFIX) :]


def assemble_sig_resume(
    *,
    section: str | None,
    numero: str | None,
    idu: str | None,
    contenance,
    layers: dict,
    legacy: dict | None = None,
) -> dict | None:
    """Reconstruit le payload sig_resume attendu par le front."""
    if layers:
        payload: dict = {
            "section": section,
            "numero": numero,
            "idu": idu,
            "layers": layers,
        }
        if contenance is not None:
            try:
                c = float(contenance)
                if math.isfinite(c):
                    payload["contenance_m2"] = round(c, 2)
            except (TypeError, ValueError):
                pass
        return sanitize_for_json(payload)
    return sanitize_for_json(legacy) if legacy is not None else None
