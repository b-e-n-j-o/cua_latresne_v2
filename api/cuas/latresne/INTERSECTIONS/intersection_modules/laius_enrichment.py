# -*- coding: utf-8 -*-
"""
Enrichissement des intersections avec les laius textuels en base
(plu_laius, pprmvt_laius) — jointure par code_zone.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SCHEMA = "latresne"
SRID = 2154

MIN_INTERSECTION_AREA_M2 = 0.01


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


def _table_exists(engine: Engine, schema: str, table: str) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = :schema AND table_name = :table
                    )
                    """
                ),
                {"schema": schema, "table": table},
            ).scalar()
        )


def _load_laius(engine: Engine, schema: str, table: str, key_col: str = "code_zone") -> dict[str, str]:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
    key_col = _safe_ident(key_col)
    sql = text(
        f"""
        SELECT {key_col}, reglementation
        FROM {schema}.{table}
        WHERE {key_col} IS NOT NULL
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    out: dict[str, str] = {}
    for row in rows:
        key = str(row[key_col]).strip().upper()
        if not key:
            continue
        regl = row.get("reglementation")
        out[key] = (str(regl).strip() if regl else "")
    return out


def _normalize_zone_code(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    val = str(raw).strip().upper()
    return val or None


def enrich_layer_from_laius(
    layer: dict[str, Any],
    laius: dict[str, str],
    *,
    zone_key: str,
) -> int:
    """Injecte reglementation depuis laius dans chaque objet. Retourne le nb enrichi."""
    if not layer or not laius:
        return 0

    count = 0
    for obj in layer.get("objets") or []:
        code = _normalize_zone_code(obj.get(zone_key))
        if not code:
            continue
        regl = laius.get(code)
        if regl:
            obj["reglementation"] = regl
            count += 1
    return count


def enrich_zonage_plu(rapport: dict, engine: Engine, schema: str = SCHEMA) -> dict:
    if not _table_exists(engine, schema, "plu_laius"):
        return {"status": "table_absente", "enriched": 0, "table": "plu_laius"}

    laius = _load_laius(engine, schema, "plu_laius")
    dispositions_generales = laius.get("DG") or None

    layer = (rapport.get("intersections") or {}).get("zonage_plu")
    n = 0
    if layer and layer.get("objets"):
        n = enrich_layer_from_laius(layer, laius, zone_key="zonage_reglement")

    return {
        "status": "concernee" if (n or dispositions_generales) else "non_concernee",
        "enriched": n,
        "table": "plu_laius",
        "dispositions_generales": dispositions_generales,
    }


def enrich_pprmvt(rapport: dict, engine: Engine, schema: str = SCHEMA) -> dict:
    layer = (rapport.get("intersections") or {}).get("pprmvt_latresne")
    if not layer or not layer.get("objets"):
        return {"status": "non_concernee", "enriched": 0}

    if not _table_exists(engine, schema, "pprmvt_laius"):
        return {"status": "table_absente", "enriched": 0, "table": "pprmvt_laius"}

    laius = _load_laius(engine, schema, "pprmvt_laius")
    n = enrich_layer_from_laius(layer, laius, zone_key="etiquette")
    return {
        "status": "concernee" if n else "non_concernee",
        "enriched": n,
        "table": "pprmvt_laius",
    }


def enrich_laius_reglementaires(rapport: dict, engine: Engine, schema: str = SCHEMA) -> dict:
    """Enrichit zonage PLU et PPRMVT depuis les tables laius."""
    return {
        "zonage_plu": enrich_zonage_plu(rapport, engine, schema),
        "pprmvt": enrich_pprmvt(rapport, engine, schema),
    }
