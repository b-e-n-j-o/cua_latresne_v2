# -*- coding: utf-8 -*-
"""
Module métier dédié : servitudes d'utilité publique (SUP).

Intersecte les couches SUP avec l'UF, puis récupère la réglementation
depuis servitudes_reglements (jointure sur suptype).

Cas particulier i4 (variable=true) : la réglementation détaillée est
résolue via servitudes_reglements_i4 en fonction de gen_type et gen_tension
de l'entité intersectée.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from sqlalchemy import text

try:
    from api.cuas.argeles.db import GEOM_COL, SCHEMA, SRID, get_engine
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from db import GEOM_COL, SCHEMA, SRID, get_engine


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TENSION_RE = re.compile(r"(\d+(?:[.,]\d+)?)")

MIN_INTERSECTION_AREA_M2 = 0.01
MIN_INTERSECTION_LENGTH_M = 0.01

# Couches SUP à interroger — assiettes uniquement (gen_type/gen_tension sur assiette_s).
SUP_SOURCE_TABLES: tuple[dict[str, Any], ...] = (
    {
        "table": "sup_assiette_s",
        "nom": "Servitudes d'utilité publique (assiettes surfaciques)",
        "geom_col": "geometry",
        "geom_type": "surfacique",
        "attrs": ["gid", "id", "nomsuplitt", "suptype", "nomreg", "nom_servitude", "gen_type", "gen_tension"],
        "entity_id": "gid",
    },
    {
        "table": "sup_assiette_l",
        "nom": "Servitudes (assiettes linéaires)",
        "geom_col": "geometry",
        "geom_type": "lineaire",
        "attrs": ["gid", "nomsuplitt", "suptype", "nomreg", "nom_servitude"],
        "entity_id": "gid",
    },
    {
        "table": "sup_assiette_p",
        "nom": "Servitudes (assiettes ponctuelles)",
        "geom_col": "geometry",
        "geom_type": "ponctuel",
        "attrs": ["gid", "nomsuplitt", "suptype", "nomreg", "nom_servitude"],
        "entity_id": "gid",
    },
)


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


def _table_exists(engine, schema: str, table: str) -> bool:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
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


def _column_exists(engine, schema: str, table: str, column: str) -> bool:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = :schema
                          AND table_name = :table
                          AND column_name = :column
                    )
                    """
                ),
                {"schema": schema, "table": table, "column": column},
            ).scalar()
        )


def _parse_tension_kv(raw: Any) -> Optional[float]:
    if raw is None or str(raw).strip() == "":
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    m = _TENSION_RE.search(str(raw))
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def _normalize_gen_type(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    val = str(raw).strip()
    if not val:
        return None
    low = val.lower()
    if "souter" in low:
        return "Souterrain"
    if "aer" in low or "aér" in low:
        return "Aérien"
    return val


def _match_i4_reglement(
    gen_type: Optional[str],
    tension: Optional[float],
    i4_rows: list[dict],
) -> Optional[dict]:
    """Sélectionne la ligne servitudes_reglements_i4 correspondante."""
    gen_type = _normalize_gen_type(gen_type)
    if not gen_type:
        return None

    candidates = [r for r in i4_rows if r.get("gen_type") == gen_type]
    if not candidates:
        return None

    if tension is None:
        open_rows = [r for r in candidates if r.get("tension_min") is None and r.get("tension_max") is None]
        return open_rows[0] if open_rows else None

    for row in candidates:
        t_min = row.get("tension_min")
        t_max = row.get("tension_max")
        if t_min is not None and tension < float(t_min):
            continue
        if t_max is not None and tension >= float(t_max):
            continue
        return row
    return None


def _load_reglements(engine, schema: str, reglement_table: str) -> dict[str, dict]:
    schema = _safe_ident(schema)
    reglement_table = _safe_ident(reglement_table)
    sql = text(
        f"""
        SELECT suptype, libelle, reglementation, base_legale, url_fiche_gpu, variable
        FROM {schema}.{reglement_table}
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return {str(r["suptype"]).strip().lower(): dict(r) for r in rows if r.get("suptype")}


def _load_reglements_i4(engine, schema: str, i4_table: str) -> list[dict]:
    schema = _safe_ident(schema)
    i4_table = _safe_ident(i4_table)
    sql = text(
        f"""
        SELECT id, gen_type, tension_min, tension_max, libelle_var, complement
        FROM {schema}.{i4_table}
        ORDER BY id
        """
    )
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(sql).mappings().all()]


def _intersect_sup_table(
    engine,
    schema: str,
    uf_wkt: str,
    source: dict,
) -> list[dict]:
    table = _safe_ident(source["table"])
    geom_col = _safe_ident(source.get("geom_col", GEOM_COL))
    geom_type = source.get("geom_type", "surfacique")
    attrs = [_safe_ident(a) for a in source.get("attrs", []) if _column_exists(engine, schema, table, a)]
    if not attrs:
        return []

    select_cols = ", ".join(f"t.{a}" for a in attrs)
    schema = _safe_ident(schema)

    if geom_type == "surfacique":
        filter_clause = (
            f"AND ST_Area(ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom)) "
            f"> {MIN_INTERSECTION_AREA_M2}"
        )
        metric_expr = f"ST_Area(ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom))"
    elif geom_type == "lineaire":
        filter_clause = (
            f"AND ST_Length(ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom)) "
            f"> {MIN_INTERSECTION_LENGTH_M}"
        )
        metric_expr = f"ST_Length(ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom))"
    else:
        filter_clause = ""
        metric_expr = "NULL"

    geom_valid = f"ST_MakeValid(t.{geom_col})"
    distance_expr = (
        f"ST_Distance(ST_MakeValid(uf.geom), {geom_valid}) AS distance_m, "
        f"ST_Distance(ST_Centroid(ST_MakeValid(uf.geom)), ST_Centroid({geom_valid})) "
        f"AS distance_centroide_m"
    )

    sql = text(
        f"""
        WITH uf AS (
            SELECT ST_GeomFromText(:wkt, {SRID}) AS geom
        )
        SELECT DISTINCT ON (ST_AsBinary({geom_valid}), t.suptype)
               {select_cols},
               {metric_expr} AS metric,
               {distance_expr}
        FROM {schema}.{table} t
        CROSS JOIN uf
        WHERE t.{geom_col} IS NOT NULL
          AND ST_Intersects({geom_valid}, uf.geom)
          {filter_clause}
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, {"wkt": uf_wkt}).mappings().all()

    out = []
    for row in rows:
        obj = {a: row[a] for a in attrs}
        metric = row.get("metric")
        if metric is not None:
            obj["metric"] = round(float(metric), 2)
        for dist_key in ("distance_m", "distance_centroide_m"):
            dist = row.get(dist_key)
            if dist is not None:
                obj[dist_key] = round(float(dist), 1)
        out.append(obj)
    return out


def _distance_monument_m(entity: dict) -> Optional[float]:
    """Distance indicative : bord à bord, ou entre centroïdes si assiette intersectée."""
    dist = entity.get("distance_m")
    if dist is not None and float(dist) >= 0.01:
        return float(dist)
    centroide = entity.get("distance_centroide_m")
    if centroide is not None:
        return float(centroide)
    return float(dist) if dist is not None else None


def _is_ac1_suptype(suptype: Any) -> bool:
    return str(suptype or "").strip().lower() == "ac1"


def _build_reglementation_text(base_reg: dict, i4_row: Optional[dict]) -> str:
    parts = []
    base = (base_reg.get("reglementation") or "").strip()
    if base:
        parts.append(base)
    if i4_row:
        complement = (i4_row.get("complement") or "").strip()
        if complement and complement not in base:
            parts.append(complement)
    return "\n\n".join(parts).strip()


def _servitude_dedup_key(entry: dict) -> tuple:
    """Clé de dédup : une entrée par suptype (réglementation fixe), variante i4, ou monument AC1."""
    suptype = entry["suptype"].lower()
    if _is_ac1_suptype(suptype):
        return (suptype, "ac1", entry.get("entity_id"))
    if entry.get("i4"):
        return (suptype, "i4", entry["i4"].get("id"))
    if entry.get("i4_non_resolu"):
        return (suptype, "generic")
    return (suptype,)


def _filter_servitudes_redundant_generic(servitudes: list[dict]) -> list[dict]:
    """
    Si une servitude variable (ex. I4) a une variante i4 résolue, on retire les entrées
    génériques non résolues (générateur / ligne sans gen_type — doublon visuel).
    """
    enriched_suptypes = {
        s["suptype"].lower()
        for s in servitudes
        if s.get("i4")
    }
    if not enriched_suptypes:
        return servitudes
    return [
        s for s in servitudes
        if not (s["suptype"].lower() in enriched_suptypes and s.get("i4_non_resolu"))
    ]


def _resolve_servitude_entry(
    entity: dict,
    source: dict,
    reglements: dict[str, dict],
    i4_rows: list[dict],
) -> Optional[dict]:
    suptype_raw = entity.get("suptype")
    if not suptype_raw or not str(suptype_raw).strip():
        return None

    suptype = str(suptype_raw).strip()
    reg = reglements.get(suptype.lower())
    if not reg:
        return None

    i4_row = None
    if reg.get("variable"):
        i4_row = _match_i4_reglement(
            entity.get("gen_type"),
            _parse_tension_kv(entity.get("gen_tension")),
            i4_rows,
        )

    libelle = reg.get("libelle") or entity.get("nomsuplitt") or entity.get("nomgen") or suptype
    reglementation = _build_reglementation_text(reg, i4_row)

    entry = {
        "source_table": source["table"],
        "source_nom": source.get("nom", source["table"]),
        "entity_id": entity.get(source.get("entity_id", "gid")) or entity.get("id"),
        "suptype": suptype,
        "nomsuplitt": entity.get("nomsuplitt") or entity.get("nomgen"),
        "nomreg": entity.get("nomreg"),
        "nom_servitude": entity.get("nom_servitude"),
        "libelle": libelle,
        "reglementation": reglementation,
        "base_legale": reg.get("base_legale"),
        "url_fiche_gpu": reg.get("url_fiche_gpu"),
        "variable": bool(reg.get("variable")),
    }
    if entity.get("metric") is not None:
        entry["metric"] = entity["metric"]
    dist = _distance_monument_m(entity)
    if dist is not None:
        entry["distance_m"] = round(dist, 1)
    if i4_row:
        entry["i4"] = {
            "id": i4_row.get("id"),
            "gen_type": i4_row.get("gen_type"),
            "libelle_var": i4_row.get("libelle_var"),
            "tension_min": i4_row.get("tension_min"),
            "tension_max": i4_row.get("tension_max"),
        }
        if i4_row.get("libelle_var"):
            entry["libelle"] = f"{libelle} — {i4_row['libelle_var']}"
    elif reg.get("variable"):
        entry["i4_non_resolu"] = True

    return entry


def _aggregate_ac1_servitudes(servitudes: list[dict]) -> list[dict]:
    """Regroupe les AC1 intersectés : liste des monuments (nomsuplitt) + une seule réglementation."""
    ac1_entries = [s for s in servitudes if _is_ac1_suptype(s.get("suptype"))]
    if not ac1_entries:
        return servitudes

    others = [s for s in servitudes if not _is_ac1_suptype(s.get("suptype"))]
    monuments_map: dict[str, dict] = {}

    for entry in ac1_entries:
        nom = (entry.get("nomsuplitt") or "").strip()
        if not nom:
            continue
        key = nom.casefold()
        dist = entry.get("distance_m")
        existing = monuments_map.get(key)
        if existing is None or (
            dist is not None
            and (existing.get("distance_m") is None or dist < existing["distance_m"])
        ):
            monuments_map[key] = {"nom": nom, "distance_m": dist}

    if not monuments_map:
        return servitudes

    monuments = sorted(
        monuments_map.values(),
        key=lambda m: m.get("distance_m") if m.get("distance_m") is not None else float("inf"),
    )
    base = ac1_entries[0]
    aggregated = {
        k: v
        for k, v in base.items()
        if k not in ("entity_id", "nomsuplitt", "metric", "i4", "i4_non_resolu")
    }
    aggregated["libelle"] = base.get("libelle") or "Servitude AC1 — Monuments historiques"
    aggregated["monuments"] = monuments
    return others + [aggregated]


def compute_servitudes_reglementation(
    uf_wkt: str,
    *,
    engine=None,
    schema: str = SCHEMA,
    reglement_table: str = "servitudes_reglements",
    i4_table: str = "servitudes_reglements_i4",
) -> dict:
    """
    Retourne les servitudes intersectées enrichies de leur réglementation.
    """
    engine = engine or get_engine()
    schema = _safe_ident(schema)
    reglement_table = _safe_ident(reglement_table)
    i4_table = _safe_ident(i4_table)

    missing_tables = [
        t
        for t in (reglement_table, i4_table)
        if not _table_exists(engine, schema, t)
    ]
    if missing_tables:
        return {
            "status": "table_absente",
            "diagnostic_metier": "Module non exécutable : table(s) de réglementation manquante(s)",
            "tables_manquantes": missing_tables,
            "servitudes": [],
        }

    reglements = _load_reglements(engine, schema, reglement_table)
    i4_rows = _load_reglements_i4(engine, schema, i4_table)

    servitudes: list[dict] = []
    seen_keys: set[tuple] = set()

    for source in SUP_SOURCE_TABLES:
        table = source["table"]
        if not _table_exists(engine, schema, table):
            continue
        try:
            entities = _intersect_sup_table(engine, schema, uf_wkt, source)
        except Exception:
            continue

        for entity in entities:
            entry = _resolve_servitude_entry(entity, source, reglements, i4_rows)
            if not entry:
                continue

            dedup_key = _servitude_dedup_key(entry)
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            servitudes.append(entry)

    servitudes = _filter_servitudes_redundant_generic(servitudes)
    servitudes = _aggregate_ac1_servitudes(servitudes)

    if not servitudes:
        return {
            "status": "non_concernee",
            "diagnostic_metier": "RAS : aucune servitude d'utilité publique réglementée sur l'UF",
            "servitudes": [],
        }

    return {
        "status": "concernee",
        "diagnostic_metier": f"{len(servitudes)} servitude(s) d'utilité publique identifiée(s)",
        "servitudes": servitudes,
    }
