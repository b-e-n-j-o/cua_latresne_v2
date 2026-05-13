"""
Page PDF « Zonage PLU » : carto + légende % + tableau + laius.

Logique isolée du reste de `plu_visuels.py` : les requêtes PostGIS ciblent
``{db_schema}.{table}`` selon le catalogue actif (GPU `zonage_plu` vs Latresne `plu_latresne`),
au lieu de ``latresne.plu_latresne`` en dur.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
import psycopg2
from shapely.geometry import shape
from shapely.ops import unary_union

from .plu_visuels import (
    MIN_PCT_ZONAGE_URBAIN,
    PLU_LATRESNE_TABLE,
    _color_map_from_plu_gdf,
    _color_from_typezone,
    _db_params,
    _merge_color_map_for_stats,
    fetch_parcelles_uf_for_schema,
    parcelle_gdf_from_geojson,
    render_combined_plu_visual,
)

logger = logging.getLogger(__name__)

_SQL_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

# Couches susceptibles d’alimenter la page « Zonage PLU » (carte + filtre + résumé couverture)
ZONAGE_PAGE_LAYER_KEYS = frozenset({"plu_latresne", "zonage_plu"})


def _sql_ident(name: str) -> str:
    n = (name or "").strip().lower()
    if not _SQL_IDENT_RE.match(n):
        raise ValueError(f"identifiant SQL invalide pour page zonage PLU : {name!r}")
    return '"' + n.replace('"', '""') + '"'


def _fqn(schema: str, table: str) -> str:
    return f"{_sql_ident(schema)}.{_sql_ident(table)}"


@dataclass(frozen=True)
class PluZonagePageConfig:
    """Cible PostGIS pour la page zonage du rapport PDF."""

    db_schema: str
    table: str
    variant: str  # "legacy" | "geoportail"


def resolve_plu_zonage_page_config(
    db_schema: str,
    catalogue: Optional[Dict[str, Any]],
) -> PluZonagePageConfig:
    """
    Choisit la table PLU surfacique utilisée pour la page PDF.

    - Schéma ``latresne`` + entrée ``plu_latresne`` au catalogue → flux legacy (geom_3857, laius).
    - Sinon, si ``zonage_plu`` est au catalogue (GPU) → ``{db_schema}.zonage_plu`` (geom_2154 → 3857).
    - Sinon ``plu_latresne`` si présent au catalogue.
    - Repli : ``latresne`` / ``plu_latresne``.
    """
    sch = (db_schema or "latresne").strip().lower()
    if not _SQL_IDENT_RE.match(sch):
        raise ValueError(f"db_schema invalide pour page zonage : {db_schema!r}")
    cat = catalogue or {}
    if sch == "latresne" and "plu_latresne" in cat:
        return PluZonagePageConfig(db_schema=sch, table="plu_latresne", variant="legacy")
    if "zonage_plu" in cat:
        return PluZonagePageConfig(db_schema=sch, table="zonage_plu", variant="geoportail")
    if "plu_latresne" in cat:
        return PluZonagePageConfig(db_schema=sch, table="plu_latresne", variant="legacy")
    return PluZonagePageConfig(db_schema="latresne", table="plu_latresne", variant="legacy")


def _zonage_plu_element_stats_key(el: dict) -> str:
    """
    Clé alignée sur le GROUP BY des stats (``typezone``) pour le filtre surface ≥ %.
    À ne pas confondre avec ``zone_key_from_intersection_element`` (affichage : ``libelle`` d’abord).
    """
    v_tz = el.get("typezone")
    if v_tz is not None and str(v_tz).strip():
        return str(v_tz).strip()
    for k in ("idzone", "libelle", "gml_id"):
        v = el.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def zone_key_from_intersection_element(el: dict, table: str) -> str:
    """
    Clé de zonage pour résumé couverture, en-tête PDF et colonne « zonage » du tableau.

    Pour ``zonage_plu`` (GPU), on affiche en priorité le ``libelle``, puis ``typezone``,
    puis les replis ``idzone`` / ``gml_id``. Le filtre ≥ % utilise
    :func:`_zonage_plu_element_stats_key` (toujours basé sur ``typezone`` côté SQL).
    """
    t = (table or "").strip()
    if t == "zonage_plu":
        for k in ("libelle", "typezone", "idzone", "gml_id"):
            v = el.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
        return ""
    z = el.get("zonage_reglement") if "zonage_reglement" in el else el.get("Zonage")
    return str(z).strip() if z is not None else ""


def fetch_plu_zonage_context_gdf(
    parcelle_gdf: gpd.GeoDataFrame,
    buffer_m: float,
    cfg: PluZonagePageConfig,
) -> gpd.GeoDataFrame:
    """Entités PLU dans un buffer Web Mercator (même colonnes logiques pour le rendu)."""
    parc_3857 = parcelle_gdf.to_crs(epsg=3857)
    parc_geom = unary_union(parc_3857.geometry)
    buffer_geom = parc_geom.buffer(buffer_m)
    wkt_buffer = buffer_geom.wkt
    fq = _fqn(cfg.db_schema, cfg.table)

    if cfg.variant == "legacy":
        sql = f"""
            SELECT
                id,
                libelle,
                libelong,
                typezone,
                zonage_reglement,
                ST_AsGeoJSON(
                    ST_Intersection(geom_3857, ST_GeomFromText(%s, 3857))
                ) AS geom_json
            FROM {fq} t
            WHERE ST_Intersects(geom_3857, ST_GeomFromText(%s, 3857))
              AND geom_invalid IS NOT TRUE
            ORDER BY zonage_reglement;
        """
    else:
        sql = f"""
            SELECT
                gml_id AS id,
                libelle,
                libelong,
                typezone,
                COALESCE(
                    NULLIF(TRIM(g.idzone), ''),
                    NULLIF(TRIM(g.libelle), ''),
                    g.gml_id::text
                ) AS zonage_reglement,
                ST_AsGeoJSON(
                    ST_Intersection(ST_Transform(g.geom_2154, 3857), ST_GeomFromText(%s, 3857))
                ) AS geom_json
            FROM {fq} g
            WHERE g.geom_2154 IS NOT NULL
              AND ST_Intersects(ST_Transform(g.geom_2154, 3857), ST_GeomFromText(%s, 3857))
            ORDER BY zonage_reglement;
        """

    conn = psycopg2.connect(**_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (wkt_buffer, wkt_buffer))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    if not rows:
        return gpd.GeoDataFrame(columns=["zonage_reglement", "geometry"], crs="EPSG:3857")

    records: List[Dict[str, Any]] = []
    for row in rows:
        d = dict(zip(cols, row))
        geom_json = d.pop("geom_json", None)
        if not geom_json:
            continue
        geom = shape(json.loads(geom_json))
        if geom.is_empty:
            continue
        d["geometry"] = geom
        records.append(d)

    gdf = gpd.GeoDataFrame(records, crs="EPSG:3857")
    return gdf


def compute_plu_zonage_pct_stats(
    parcelle_gdf: gpd.GeoDataFrame,
    cfg: PluZonagePageConfig,
) -> Dict[str, float]:
    """
    Surfaces d’intersection UF × zonage en %.

    - **legacy** : une ligne par ``zonage_reglement`` (clés = libellé réglementaire local).
    - **geoportail** : surfaces **agrégées par** ``typezone`` (code CNIG), somme des polygones
      partageant le même type ; clé ``Non renseigné`` si ``typezone`` vide.
    """
    parc_3857 = parcelle_gdf.to_crs(epsg=3857)
    parc_geom = unary_union(parc_3857.geometry)
    total_area = parc_geom.area
    if total_area <= 0:
        return {}
    parc_wkt = parc_geom.wkt
    fq = _fqn(cfg.db_schema, cfg.table)

    if cfg.variant == "legacy":
        sql = f"""
            SELECT
                zonage_reglement,
                ST_Area(
                    ST_Intersection(geom_3857, ST_GeomFromText(%s, 3857))
                ) AS area_m2
            FROM {fq} t
            WHERE ST_Intersects(geom_3857, ST_GeomFromText(%s, 3857))
              AND geom_invalid IS NOT TRUE;
        """
    else:
        sql = f"""
            SELECT
                COALESCE(
                    NULLIF(TRIM(g.typezone::text), ''),
                    'Non renseigné'
                ) AS typezone_agg,
                SUM(
                    ST_Area(
                        ST_Intersection(
                            ST_Transform(g.geom_2154, 3857),
                            ST_GeomFromText(%s, 3857)
                        )
                    )
                )::double precision AS area_m2
            FROM {fq} g
            WHERE g.geom_2154 IS NOT NULL
              AND ST_Intersects(ST_Transform(g.geom_2154, 3857), ST_GeomFromText(%s, 3857))
            GROUP BY typezone_agg;
        """

    stats: Dict[str, float] = {}
    conn = psycopg2.connect(**_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (parc_wkt, parc_wkt))
            for row in cur.fetchall():
                zone, area = row[0], row[1]
                z = zone or "Non renseigné"
                stats[str(z).strip()] = stats.get(str(z).strip(), 0.0) + float(area or 0.0)
    finally:
        conn.close()

    return {z: (a / total_area) * 100 for z, a in stats.items() if a > 0}


def filter_zonage_page_layer_for_report(
    layer: dict[str, Any],
    pct_stats: dict[str, float],
    cfg: PluZonagePageConfig,
    min_pct: float = MIN_PCT_ZONAGE_URBAIN,
) -> dict[str, Any]:
    if (layer.get("table") or "").strip() != cfg.table:
        return layer
    if not pct_stats:
        return layer

    allowed = {k for k, v in pct_stats.items() if isinstance(v, (int, float)) and float(v) >= min_pct}
    out = dict(layer)
    elems = [e for e in (layer.get("elements") or []) if isinstance(e, dict)]

    if not allowed:
        out["elements"] = []
        out["_plu_all_zonages_below_min_pct"] = True
        return out

    def _elem_pct_key(e: dict) -> str:
        if (cfg.table or "").strip() == "zonage_plu":
            return _zonage_plu_element_stats_key(e)
        return zone_key_from_intersection_element(e, cfg.table)

    kept = [e for e in elems if _elem_pct_key(e) in allowed]
    out["elements"] = kept
    if not kept and elems:
        out["_plu_all_zonages_below_min_pct"] = True
    return out


def zonage_page_table_rows_from_intersections(
    intersections: List[dict[str, Any]],
    cfg: PluZonagePageConfig,
) -> List[dict[str, str]]:
    """Lignes tableau (zonage_reglement, libelle, libelle_description) pour la page PDF."""
    for layer in intersections:
        if (layer.get("table") or "").strip() != cfg.table:
            continue
        if layer.get("_plu_all_zonages_below_min_pct"):
            return []
        order: List[str] = []
        acc: Dict[str, Dict[str, str]] = {}
        for el in layer.get("elements") or []:
            if not isinstance(el, dict):
                continue
            z = zone_key_from_intersection_element(el, cfg.table)
            if not z:
                continue
            lib_raw = el.get("libelle")
            lib = str(lib_raw).strip() if lib_raw is not None else ""
            desc_raw = el.get("libelle_description")
            if desc_raw is None or (isinstance(desc_raw, str) and not str(desc_raw).strip()):
                desc_raw = el.get("libelong")
            desc = str(desc_raw).strip() if desc_raw is not None else ""
            if z not in acc:
                order.append(z)
                acc[z] = {
                    "zonage_reglement": z,
                    "libelle": lib,
                    "libelle_description": desc,
                }
            else:
                if lib and not acc[z]["libelle"]:
                    acc[z]["libelle"] = lib
                if desc and not acc[z]["libelle_description"]:
                    acc[z]["libelle_description"] = desc
        rows = [acc[k] for k in order]
        rows.sort(key=lambda r: r["zonage_reglement"].lower())
        return rows
    return []


def fetch_zonage_laius_for_page(
    zonages: list[str],
    cfg: PluZonagePageConfig,
) -> dict[str, str]:
    """Textes laius : uniquement flux legacy ``plu_latresne`` avec colonne ``laius_reglement``."""
    if cfg.variant != "legacy" or cfg.table != PLU_LATRESNE_TABLE:
        return {}
    cleaned = sorted({str(z).strip() for z in zonages if z is not None and str(z).strip()})
    if not cleaned:
        return {}

    fq = _fqn(cfg.db_schema, cfg.table)
    conn = None
    try:
        conn = psycopg2.connect(**_db_params())
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT ON (zonage_reglement)
                    zonage_reglement,
                    laius_reglement
                FROM {fq} t
                WHERE zonage_reglement = ANY(%s)
                  AND laius_reglement IS NOT NULL
                  AND TRIM(COALESCE(laius_reglement::text, '')) <> ''
                  AND geom_invalid IS NOT TRUE
                ORDER BY zonage_reglement, id;
                """,
                (cleaned,),
            )
            rows = cur.fetchall()
    except Exception as exc:
        logger.warning("fetch_zonage_laius_for_page : %s", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()

    out: dict[str, str] = {}
    for z, laius in rows:
        if z is None or laius is None:
            continue
        ks = str(z).strip()
        txt = str(laius).strip()
        if ks and txt:
            out[ks] = txt
    return out


def generate_plu_zonage_page_visuals_from_uf_geometry(
    geometry: dict[str, Any],
    out_dir: str,
    cfg: PluZonagePageConfig,
    *,
    srid: Optional[int] = None,
    buffer_m: float = 300.0,
    dpi: int = 180,
    insee: str = "",
    parcelles_cadastrales: Optional[list[dict[str, Any]]] = None,
) -> tuple[str, str, dict[str, float], List[dict[str, Any]]]:
    """
    PNG carte + légende et stats % pour la page « Zonage PLU », selon ``cfg``.
    """
    from pathlib import Path

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    sub = out / "plu_visuels_assets"
    sub.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256(
        json.dumps(geometry, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]
    tag = f"uf_{h}_{cfg.db_schema}_{cfg.table}"
    map_path = str(sub / f"plu_map_{tag}.png")

    parcelle_gdf = parcelle_gdf_from_geojson(geometry, srid)
    plu_gdf = fetch_plu_zonage_context_gdf(parcelle_gdf, buffer_m=buffer_m, cfg=cfg)
    pct_stats = compute_plu_zonage_pct_stats(parcelle_gdf, cfg)

    parcelles_pc_gdf, parcelles_detail = fetch_parcelles_uf_for_schema(
        cfg.db_schema,
        insee,
        parcelle_gdf,
        parcelles_cadastrales,
    )

    color_map = _merge_color_map_for_stats(
        _color_map_from_plu_gdf(plu_gdf),
        pct_stats,
        plu_gdf,
    )

    legend_color_map: Optional[dict[str, str]] = None
    legend_label_map: Optional[dict[str, str]] = None
    if cfg.variant == "geoportail":
        # Stats agrégées par typezone : couleur et libellé sont directement indexés par le typezone.
        # (Pas de dépendance au buffer carto par zonage_reglement.)
        legend_color_map = {k: _color_from_typezone(k) for k in pct_stats.keys()}
        legend_label_map = {k: str(k).strip() for k in pct_stats.keys()}

    geop = cfg.variant == "geoportail"
    render_combined_plu_visual(
        parcelle_gdf,
        plu_gdf,
        color_map,
        pct_stats,
        map_path,
        dpi=dpi,
        pct_min_affiche=MIN_PCT_ZONAGE_URBAIN,
        parcelles_cadastrales_gdf=parcelles_pc_gdf if not parcelles_pc_gdf.empty else None,
        map_zonage_label_column=("typezone" if geop else "zonage_reglement"),
        legend_panel_title=("Type de zone (part UF)" if geop else None),
        legend_color_map=legend_color_map,
        legend_label_map=legend_label_map,
    )
    return map_path, map_path, pct_stats, parcelles_detail
