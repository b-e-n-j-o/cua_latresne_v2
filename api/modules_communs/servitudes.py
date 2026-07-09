# -*- coding: utf-8 -*-
"""
Cœur métier servitudes SUP — commune-agnostique.

Intersecte {schema}.servitudes avec l'UF, enrichit via public.servitudes_reglements
et public.servitudes_reglements_i4 (I4 variable, agrégation par libelle_var).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TENSION_RE = re.compile(r"(\d+(?:[.,]\d+)?)")

REGLEMENTS_SCHEMA = "public"
REGLEMENTS_TABLE = "servitudes_reglements"
I4_VARIANTES_TABLE = "servitudes_reglements_i4"
SRID = 2154
MIN_INTERSECTION_AREA_M2 = 0.01
DEFAULT_MIN_PCT_SIG = 1.0

AGGREGATE_SUM_METRIC_SUPTYPES = frozenset({"EL3", "I6"})

ATTRS_BY_SUPTYPE: dict[str, list[str]] = {
    "ac1": [
        "nomsuplitt", "typeass", "nature_protection",
        "precision_protection", "statut_proprietaire",
    ],
    "i1": ["gml_id", "transporteur", "cat_fluide"],
    "as1": ["nom_captage", "perimetre_protection", "ins_pro__1"],
    "i4": ["tension", "type"],
    "pt3": ["nom_sup", "type"],
}

# Suptypes couverts par la table unifiée {schema}.servitudes (couches solo à retirer des catalogues).
LATRESNE_SERVITUDES_SUPTYPES = frozenset(
    {"PT3", "EL3", "AC1", "I1", "I4", "AS1", "I6", "A4"}
)
ARGELES_SERVITUDES_SUPTYPES = frozenset(
    {"T1", "PM8", "AC1", "I4", "A7", "PM1", "EL9", "AC3", "AC2"}
)

# Clés catalogue Latresne à retirer quand le suptype est dans LATRESNE_SERVITUDES_SUPTYPES.
LATRESNE_SOLO_LAYER_KEYS_IN_SERVITUDES_TABLE = frozenset({
    "a4",
    "ac1",
    "i1_hydrocarbure_chimique_gaz",
    "i6",
    "pt3",
    "as1_captages_eau",
    "el3_garonne",
    "i4_cables_haute_tension",
})

# Argelès : pas de couches solo SUP dans le catalogue PLU (tout passe par default.servitudes).
ARGELES_SOLO_LAYER_KEYS_IN_SERVITUDES_TABLE: frozenset[str] = frozenset()


@dataclass(frozen=True)
class ServitudesConfig:
    """Configuration géographique par commune."""

    geo_schema: str
    servitudes_table: str = "servitudes"
    geom_column: str = "geom_2154"
    entity_id_column: str = "id"
    i4_type_field: str = "type"
    i4_tension_field: str = "tension"
    excluded_suptypes: frozenset[str] = field(
        default_factory=lambda: frozenset({"PM1", "PM1_DETAILLEE", "PM1_DETAILLEE_GIRONDE"})
    )


LATRESNE_SERVITUDES_CONFIG = ServitudesConfig(
    geo_schema="latresne",
    excluded_suptypes=frozenset({"PM1", "PM1_DETAILLEE", "PM1_DETAILLEE_GIRONDE"}),
)

ARGELES_SERVITUDES_CONFIG = ServitudesConfig(
    geo_schema="argeles",
    excluded_suptypes=frozenset(),
)


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


def _load_reglements(engine: Engine) -> dict[str, dict]:
    schema = _safe_ident(REGLEMENTS_SCHEMA)
    table = _safe_ident(REGLEMENTS_TABLE)
    sql = text(
        f"""
        SELECT suptype, libelle, reglementation, base_legale, url_fiche_gpu, variable
        FROM {schema}.{table}
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return {
        str(r["suptype"]).strip().upper(): dict(r)
        for r in rows
        if r.get("suptype")
    }


def _norm_suptype(suptype: Any) -> str:
    return str(suptype or "").strip().upper()


def _is_ac1(suptype: str) -> bool:
    return _norm_suptype(suptype) == "AC1"


def _is_i4(suptype: str) -> bool:
    return _norm_suptype(suptype) == "I4"


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


def _load_i4_variantes(engine: Engine) -> list[dict]:
    if not _table_exists(engine, REGLEMENTS_SCHEMA, I4_VARIANTES_TABLE):
        return []
    schema = _safe_ident(REGLEMENTS_SCHEMA)
    table = _safe_ident(I4_VARIANTES_TABLE)
    sql = text(
        f"""
        SELECT gen_type, tension_min, tension_max, libelle_var, complement
        FROM {schema}.{table}
        ORDER BY id
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return [dict(r) for r in rows]


def _match_i4_variante(
    gen_type: Optional[str],
    tension: Optional[float],
    i4_variantes: list[dict],
) -> Optional[dict]:
    gen_type = _normalize_gen_type(gen_type)
    if not gen_type:
        return None

    candidates = [r for r in i4_variantes if r.get("gen_type") == gen_type]
    if not candidates:
        return None

    if tension is None:
        open_rows = [
            r for r in candidates
            if r.get("tension_min") is None and r.get("tension_max") is None
        ]
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


def _parse_tension(raw: Any) -> Optional[float]:
    if raw is None or str(raw).strip() == "":
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    m = _TENSION_RE.search(str(raw))
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def _excluded_suptypes_sql(excluded: frozenset[str]) -> str:
    if not excluded:
        return ""
    quoted = ", ".join(f"'{_safe_ident(s)}'" for s in sorted(excluded))
    return f"AND UPPER(TRIM(s.suptype)) NOT IN ({quoted})"


def _intersect_servitudes(
    engine: Engine,
    uf_wkt: str,
    config: ServitudesConfig,
    *,
    surface_sig: float = 0.0,
    min_pct_sig: float = DEFAULT_MIN_PCT_SIG,
) -> list[dict]:
    schema = _safe_ident(config.geo_schema)
    table = _safe_ident(config.servitudes_table)
    geom_col = _safe_ident(config.geom_column)
    entity_id = _safe_ident(config.entity_id_column)
    excluded_sql = _excluded_suptypes_sql(config.excluded_suptypes)

    sql = text(
        f"""
        WITH uf AS (
            SELECT ST_MakeValid(ST_GeomFromText(:wkt, {SRID})) AS geom
        )
        SELECT
            s.{entity_id} AS id,
            s.suptype,
            s.source_table,
            s.nomsuplitt,
            s.typeass,
            s.nature_protection,
            s.precision_protection,
            s.statut_proprietaire,
            s.nom_sup,
            s.{_safe_ident(config.i4_type_field)} AS type,
            s.gml_id,
            s.transporteur,
            s.cat_fluide,
            s.nom_captage,
            s.perimetre_protection,
            s.ins_pro__1,
            s.{_safe_ident(config.i4_tension_field)} AS tension,
            ROUND(CAST(ST_Area(
                ST_Intersection(ST_MakeValid(s.{geom_col}), uf.geom)
            ) AS numeric), 2) AS area_m2,
            ROUND(CAST(ST_Distance(
                ST_MakeValid(uf.geom), ST_MakeValid(s.{geom_col})
            ) AS numeric), 1) AS distance_m
        FROM {schema}.{table} s
        CROSS JOIN uf
        WHERE s.{geom_col} IS NOT NULL
          AND s.suptype IS NOT NULL
          {excluded_sql}
          AND ST_Intersects(ST_MakeValid(s.{geom_col}), uf.geom)
          AND ST_Area(ST_Intersection(ST_MakeValid(s.{geom_col}), uf.geom))
              > {MIN_INTERSECTION_AREA_M2}
          AND (
              :min_pct_sig <= 0
              OR :surface_sig <= 0
              OR (
                  ST_Area(ST_Intersection(ST_MakeValid(s.{geom_col}), uf.geom))
                  / :surface_sig * 100
              ) > :min_pct_sig
          )
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "wkt": uf_wkt,
                "surface_sig": float(surface_sig or 0),
                "min_pct_sig": float(min_pct_sig),
            },
        ).mappings().all()

    out = []
    for row in rows:
        obj = dict(row)
        suptype = str(obj.get("suptype") or "").strip().upper()
        area = obj.pop("area_m2", None)
        if area is not None:
            obj["metric"] = round(float(area), 2)
            if surface_sig > 0:
                obj["pct_sig"] = round(float(area) / surface_sig * 100, 4)
        dist = obj.get("distance_m")
        if dist is not None:
            obj["distance_m"] = round(float(dist), 1)
        obj["suptype"] = suptype
        out.append(obj)
    return out


def _entity_attrs(entity: dict) -> dict[str, Any]:
    suptype = str(entity.get("suptype") or "").upper()
    keys = ATTRS_BY_SUPTYPE.get(suptype.lower(), [])
    attrs = {}
    for k in keys:
        val = entity.get(k)
        if val is not None and str(val).strip():
            attrs[k] = val
    return attrs


def _servitude_dedup_key(entry: dict) -> tuple:
    suptype = entry["suptype"].upper()
    entity_id = entry.get("entity_id")
    return (suptype, entity_id)


def _resolve_servitude_entry(
    entity: dict,
    reglements: dict[str, dict],
    i4_variantes: list[dict],
    config: ServitudesConfig,
) -> Optional[dict]:
    suptype = str(entity.get("suptype") or "").strip().upper()
    if not suptype:
        return None

    reg = reglements.get(suptype)
    if not reg:
        return None

    libelle = (reg.get("libelle") or entity.get("nomsuplitt") or suptype).strip()
    reglementation = (reg.get("reglementation") or "").strip()

    entry: dict[str, Any] = {
        "entity_id": entity.get("id"),
        "suptype": suptype,
        "libelle": libelle,
        "reglementation": reglementation,
        "base_legale": reg.get("base_legale"),
        "url_fiche_gpu": reg.get("url_fiche_gpu"),
        "variable": bool(reg.get("variable")),
        **_entity_attrs(entity),
    }
    if entity.get("metric") is not None:
        entry["metric"] = entity["metric"]
    if entity.get("pct_sig") is not None:
        entry["pct_sig"] = entity["pct_sig"]
    if entity.get("distance_m") is not None:
        entry["distance_m"] = entity["distance_m"]

    if suptype == "I4":
        tension = _parse_tension(entity.get("tension"))
        if tension is not None:
            entry["tension_kv"] = tension

        if entry["variable"] and i4_variantes:
            variante = _match_i4_variante(entity.get("type"), tension, i4_variantes)
            if variante:
                entry["i4_variante"] = {
                    "libelle_var": variante.get("libelle_var"),
                    "gen_type": variante.get("gen_type"),
                    "tension_min": variante.get("tension_min"),
                    "tension_max": variante.get("tension_max"),
                    "complement": variante.get("complement"),
                }
            else:
                entry["i4_non_resolu"] = True

    return entry


def _max_pct_sig(entries: list[dict]) -> Optional[float]:
    vals = []
    for entry in entries:
        pct = entry.get("pct_sig")
        if pct is None:
            continue
        try:
            vals.append(float(pct))
        except (TypeError, ValueError):
            continue
    return max(vals) if vals else None


def _aggregate_ac1_entries(entries: list[dict]) -> Optional[dict]:
    monuments_map: dict[str, dict] = {}

    for entry in entries:
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
        return None

    monuments = sorted(
        monuments_map.values(),
        key=lambda m: m.get("distance_m") if m.get("distance_m") is not None else float("inf"),
    )
    base = entries[0]
    aggregated = {
        k: v
        for k, v in base.items()
        if k not in ("entity_id", "nomsuplitt", "metric")
    }
    aggregated["libelle"] = base.get("libelle") or "Servitude AC1 — Monuments historiques"
    aggregated["monuments"] = monuments
    aggregated["nb_fragments"] = len(entries)
    pct_max = _max_pct_sig(entries)
    if pct_max is not None:
        aggregated["pct_sig"] = pct_max
    return aggregated


def _aggregate_sum_metric_entries(entries: list[dict]) -> dict:
    base = entries[0]
    total_metric = round(sum(float(e.get("metric") or 0) for e in entries), 2)
    aggregated = {
        k: v
        for k, v in base.items()
        if k not in ("entity_id", "metric", "distance_m")
    }
    aggregated["metric"] = total_metric
    aggregated["nb_fragments"] = len(entries)
    pct_max = _max_pct_sig(entries)
    if pct_max is not None:
        aggregated["pct_sig"] = pct_max
    return aggregated


def _aggregate_i4_entries(entries: list[dict]) -> Optional[dict]:
    if not entries:
        return None

    variantes_map: dict[str, dict] = {}
    nb_non_resolus = 0

    for entry in entries:
        variante = entry.get("i4_variante")
        if not variante:
            nb_non_resolus += 1
            continue
        libelle_var = (variante.get("libelle_var") or "").strip()
        if not libelle_var:
            nb_non_resolus += 1
            continue

        key = libelle_var.casefold()
        if key not in variantes_map:
            variantes_map[key] = {
                "libelle_var": libelle_var,
                "gen_type": variante.get("gen_type"),
                "tension_min": variante.get("tension_min"),
                "tension_max": variante.get("tension_max"),
                "complement": (variante.get("complement") or "").strip(),
                "nb_fragments": 0,
                "metric": 0.0,
            }
        slot = variantes_map[key]
        slot["nb_fragments"] += 1
        slot["metric"] = round(slot["metric"] + float(entry.get("metric") or 0), 2)

    base = entries[0]
    aggregated = {
        k: v
        for k, v in base.items()
        if k not in (
            "entity_id",
            "metric",
            "distance_m",
            "i4_variante",
            "tension_kv",
            "type",
            "tension",
            "i4_non_resolu",
        )
    }
    aggregated["libelle"] = base.get("libelle")
    aggregated["reglementation"] = (base.get("reglementation") or "").strip()
    aggregated["variantes"] = sorted(
        variantes_map.values(),
        key=lambda v: (v.get("gen_type") or "", v.get("libelle_var") or ""),
    )
    aggregated["nb_fragments"] = len(entries)
    aggregated["metric"] = round(sum(float(e.get("metric") or 0) for e in entries), 2)
    pct_max = _max_pct_sig(entries)
    if pct_max is not None:
        aggregated["pct_sig"] = pct_max
    if nb_non_resolus:
        aggregated["i4_non_resolu"] = True
        aggregated["nb_non_resolus"] = nb_non_resolus
    return aggregated


def _aggregate_servitudes(servitudes: list[dict]) -> list[dict]:
    by_suptype: dict[str, list[dict]] = {}
    for entry in servitudes:
        st = _norm_suptype(entry.get("suptype"))
        by_suptype.setdefault(st, []).append(entry)

    result: list[dict] = []
    for suptype in sorted(by_suptype.keys()):
        entries = by_suptype[suptype]
        if _is_ac1(suptype):
            aggregated = _aggregate_ac1_entries(entries)
            if aggregated:
                result.append(aggregated)
            else:
                result.extend(entries)
        elif suptype in AGGREGATE_SUM_METRIC_SUPTYPES:
            result.append(_aggregate_sum_metric_entries(entries))
        elif _is_i4(suptype) and any(e.get("variable") for e in entries):
            aggregated = _aggregate_i4_entries(entries)
            if aggregated:
                result.append(aggregated)
            else:
                result.extend(entries)
        else:
            result.extend(entries)

    return result


def compute_servitudes_reglementation(
    uf_wkt: str,
    *,
    engine: Engine,
    config: ServitudesConfig,
    surface_sig: float = 0.0,
    min_pct_sig: float = DEFAULT_MIN_PCT_SIG,
) -> dict:
    """Retourne les servitudes intersectées enrichies de leur réglementation textuelle."""
    geo_schema = _safe_ident(config.geo_schema)
    servitudes_table = _safe_ident(config.servitudes_table)

    missing = []
    if not _table_exists(engine, geo_schema, servitudes_table):
        missing.append(f"{geo_schema}.{servitudes_table}")
    if not _table_exists(engine, REGLEMENTS_SCHEMA, REGLEMENTS_TABLE):
        missing.append(f"{REGLEMENTS_SCHEMA}.{REGLEMENTS_TABLE}")

    if missing:
        return {
            "status": "table_absente",
            "diagnostic_metier": "Module non exécutable : table(s) manquante(s)",
            "tables_manquantes": missing,
            "servitudes": [],
        }

    reglements = _load_reglements(engine)
    i4_variantes = _load_i4_variantes(engine)
    entities = _intersect_servitudes(
        engine,
        uf_wkt,
        config,
        surface_sig=surface_sig,
        min_pct_sig=min_pct_sig,
    )

    servitudes: list[dict] = []
    seen_keys: set[tuple] = set()

    for entity in entities:
        entry = _resolve_servitude_entry(entity, reglements, i4_variantes, config)
        if not entry:
            continue
        dedup_key = _servitude_dedup_key(entry)
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)
        servitudes.append(entry)

    servitudes = _aggregate_servitudes(servitudes)

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
