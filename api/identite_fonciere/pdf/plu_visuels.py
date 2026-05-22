"""
generate_plu_visuals.py
=======================
Génère **une seule image PNG** pour une parcelle / UF :
  **Carte** (fond satellite + contour parcelle + PLU buffer 50 m) **à gauche**, **légende**
  « Zonage réglementaire » (répartition % par **`zonage_reglement`**, seuil ≥ 1 % par défaut)
  **à droite** — format adapté au PDF A4 pleine largeur.

  Les **couleurs** des zones PLU suivent la colonne **`typezone`** (repères type CNIG / GPU) :
  N… vert, A jaune, U rouge, AU… rouge atténué, etc. La répartition textuelle reste en
  **`zonage_reglement`**.

  (Les deux premiers chemins retournés pointent vers le **même PNG** pour compatibilité
  avec l’API historique qui distinguait carte et second visuel.)

La géométrie parcelle est lue uniquement dans latresne.parcelles (PostGIS),
comme l’export GeoJSON (ST_Transform(geom_2154, 4326)).

Usage standalone :
    python generate_plu_visuals.py [--insee 33234] [--section AL] [--numero 0074] [--out_dir .]

Intégration programmatique :
    from plu_visuels import generate_plu_visuals, generate_plu_visuals_from_uf_geometry
    map_path, map_png_compat, pct_stats = generate_plu_visuals(...)
    # UF + PDF : generate_plu_visuals_from_uf_geometry renvoie aussi parcelles_uf_detail (liste dicts).
    # Le PDF page PLU peut enrichir avec plu_zonage_table_rows_from_intersections (tableau libellés).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import warnings
from pathlib import Path
from typing import Any, List, Optional, Tuple

import contextily as ctx
import geopandas as gpd
import matplotlib
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib import gridspec
import psycopg2
from matplotlib.colors import to_rgba
from shapely.geometry import shape
from shapely.ops import unary_union

matplotlib.use("Agg")
warnings.filterwarnings("ignore")

# Seuil d’affichage rapport / PDF / légende (pas la carto buffer 50 m)
MIN_PCT_ZONAGE_URBAIN = float(os.getenv("PLU_ZONAGE_MIN_PCT", "1.0"))

# Table PLU Latresne (identique au catalogue / header)
zonage_plu_TABLE = "zonage_plu"

# PNG : carte carrée + panneau droit (légende zonages)
# largeur_totale / hauteur — secours si lecture PIL indisponible côté PDF
PLU_MAP_COVER_ASPECT_WH = 1.0 + 0.34
PLU_MAP_SQUARE_SIDE_IN = 6.2  # côté du carré carte (pouces)
PLU_MAP_RIGHT_PANEL_RATIO = 0.34  # largeur panneau droit / côté carte

# ---------------------------------------------------------------------------
# Couleurs PLU : nomenclature d’appui (CNIG / typezone GPU), pas palette arbitraire
# ---------------------------------------------------------------------------
# Ancienne palette discrète conservée pour compat. éventuelle ; le rendu utilise
# `_color_from_typezone` + colonne `typezone` de `latresne.zonage_plu`.
PLU_PALETTE = [
    "#2D6A4F", "#52B788", "#B7E4C7", "#74C69D",
    "#E76F51", "#F4A261", "#E9C46A", "#264653",
    "#457B9D", "#A8DADC", "#1D3557", "#F1FAEE",
    "#6D6875", "#B5838D", "#E5989B", "#FFCDB2",
]

# ---------------------------------------------------------------------------
# Config DB depuis variables d'environnement (avec fallbacks)
# ---------------------------------------------------------------------------

def _db_params() -> dict:
    """Même principe que les scripts ETL / extraire_geojson_des_parcelles (variables d’environnement)."""
    host = (os.getenv("SUPABASE_HOST") or "").strip() or "aws-0-eu-west-3.pooler.supabase.com"
    port = (os.getenv("SUPABASE_PORT") or "5432").strip()
    if "pooler.supabase.com" in host.lower() and port == "5432":
        port = "6543"
    return {
        "host": host,
        "port": int(port),
        "dbname": os.getenv("SUPABASE_DB", "postgres"),
        "user": os.getenv("SUPABASE_USER", ""),
        "password": os.getenv("SUPABASE_PASSWORD", ""),
        "connect_timeout": 15,
        "sslmode": "require",
    }


# ---------------------------------------------------------------------------
# GeoJSON UF / parcelle → GeoDataFrame (aligné sur header.identite_fonciere)
# ---------------------------------------------------------------------------

def _first_xy_pair(coords: Any) -> Optional[tuple[float, float]]:
    if isinstance(coords, list):
        if (
            len(coords) >= 2
            and isinstance(coords[0], (int, float))
            and isinstance(coords[1], (int, float))
        ):
            return (float(coords[0]), float(coords[1]))
        for item in coords:
            got = _first_xy_pair(item)
            if got:
                return got
    return None


def _detect_input_srid(parcelle_geometry: dict[str, Any], explicit_srid: Optional[int] = None) -> int:
    if explicit_srid in (4326, 2154, 3857):
        return explicit_srid
    pair = _first_xy_pair(parcelle_geometry.get("coordinates"))
    if not pair:
        return 4326
    x, y = pair
    if -180 <= x <= 180 and -90 <= y <= 90:
        return 4326
    if abs(x) <= 20037508 and abs(y) <= 20037508:
        return 3857
    if 0 <= x <= 1300000 and 5800000 <= y <= 7300000:
        return 2154
    return 4326


def parcelle_gdf_from_geojson(
    geometry: dict[str, Any],
    srid: Optional[int] = None,
) -> gpd.GeoDataFrame:
    """Construit un GeoDataFrame EPSG:4326 à partir d’un GeoJSON de parcelle / UF."""
    if not isinstance(geometry, dict) or "type" not in geometry:
        raise ValueError("geometry GeoJSON invalide")
    g = shape(geometry)
    if g.is_empty:
        raise ValueError("Géométrie vide")
    det = _detect_input_srid(geometry, srid)
    if det == 2154:
        return gpd.GeoDataFrame([{"geometry": g}], crs="EPSG:2154").to_crs(4326)
    if det == 3857:
        return gpd.GeoDataFrame([{"geometry": g}], crs="EPSG:3857").to_crs(4326)
    return gpd.GeoDataFrame([{"geometry": g}], crs="EPSG:4326")


def zonages_urbains_pour_rapport(
    pct_stats: dict[str, float],
    min_pct: float = MIN_PCT_ZONAGE_URBAIN,
) -> set[str]:
    """Zonages `zonage_reglement` à faire figurer au-delà du seuil (% surface UF)."""
    return {k for k, v in pct_stats.items() if v >= min_pct}


def filter_zonage_plu_layer_for_report(
    layer: dict[str, Any],
    pct_stats: dict[str, float],
    min_pct: float = MIN_PCT_ZONAGE_URBAIN,
) -> dict[str, Any]:
    """
    Filtre la couche catalogue `zonage_plu` : ne garde que les éléments dont le zonage
    représente >= min_pct % de la surface d’étude. Met des drapeaux si tout est sous le seuil.
    """
    from ..identite_fonciere import get_catalogue, get_identite_db_schema
    from .plu_zonage_rapport import filter_zonage_page_layer_for_report, resolve_plu_zonage_page_config

    cfg = resolve_plu_zonage_page_config(get_identite_db_schema(), get_catalogue())
    return filter_zonage_page_layer_for_report(layer, pct_stats, cfg, min_pct=min_pct)


def plu_zonage_table_rows_from_intersections(
    intersections: List[dict[str, Any]],
) -> List[dict[str, str]]:
    """
    Lignes pour le tableau PDF (page PLU après la carte) : une ligne par `zonage_reglement`
    distinct parmi les éléments déjà filtrés (≥ seuil % surface UF, comme le corps du rapport).

    Chaque dict contient : ``zonage_reglement``, ``libelle``, ``libelle_description``.
    Délègue à ``plu_zonage_rapport.zonage_page_table_rows_from_intersections``.
    """
    from ..identite_fonciere import get_catalogue, get_identite_db_schema
    from .plu_zonage_rapport import resolve_plu_zonage_page_config, zonage_page_table_rows_from_intersections

    cfg = resolve_plu_zonage_page_config(get_identite_db_schema(), get_catalogue())
    return zonage_page_table_rows_from_intersections(intersections, cfg)


# ---------------------------------------------------------------------------
# Textes « laius » réglementaires (zonage_plu)
# ---------------------------------------------------------------------------


def fetch_laius_reglement_par_zonages(zonages: list[str]) -> dict[str, str]:
    """Rétrocompat : laius SQL uniquement pour flux legacy ``zonage_plu`` (voir ``plu_zonage_rapport``)."""
    from ..identite_fonciere import get_catalogue, get_identite_db_schema
    from .plu_zonage_rapport import fetch_zonage_laius_for_page, resolve_plu_zonage_page_config

    cfg = resolve_plu_zonage_page_config(get_identite_db_schema(), get_catalogue())
    return fetch_zonage_laius_for_page(zonages, cfg)


# ---------------------------------------------------------------------------
# 1. Parcelle : latresne.parcelles uniquement (équivalent export GeoJSON)
# ---------------------------------------------------------------------------

def _normalize_parcelle_ids(section: str, numero: str) -> tuple[str, str]:
    return section.upper().strip(), str(numero).strip().zfill(4)


def fetch_parcelle_geojson_db(insee: str, section: str, numero: str) -> dict:
    """
    Retourne la géométrie GeoJSON (dict) de la parcelle cible, comme
    `ST_AsGeoJSON(ST_Transform(geom_2154, 4326))` dans extraire_geojson_des_parcelles.py.
    """
    sec, num = _normalize_parcelle_ids(section, numero)
    conn = psycopg2.connect(**_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geom_json
                FROM latresne.parcelles
                WHERE code_insee = %s
                  AND UPPER(TRIM(section)) = %s
                  AND LPAD(TRIM(numero), 4, '0') = %s
                  AND geom_2154 IS NOT NULL
                LIMIT 1
                """,
                (insee.strip(), sec, num),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row or not row[0]:
        raise ValueError(
            f"Parcelle introuvable ou geom_2154 nulle en base : "
            f"code_insee={insee!r} section={sec!r} numero={num!r}"
        )
    return json.loads(row[0])


def fetch_parcelle(insee: str, section: str, numero: str) -> gpd.GeoDataFrame:
    """GeoDataFrame EPSG:4326 à partir du GeoJSON parcelle en base."""
    sec, num = _normalize_parcelle_ids(section, numero)
    print(f"  ↳ Parcelle en base latresne.parcelles : {insee} {sec} {num} …")
    gj = fetch_parcelle_geojson_db(insee, section, numero)
    geom = shape(gj)
    if geom.is_empty:
        raise ValueError(f"Géométrie parcelle vide : {insee} {sec} {num}")
    gdf = gpd.GeoDataFrame([{"geometry": geom}], crs="EPSG:4326")
    print("  ✓ GeoJSON parcelle chargé depuis PostGIS")
    return gdf


def _parcelles_cadastre_keys(
    parcelles_cadastrales: Optional[list[dict[str, Any]]],
) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    if not parcelles_cadastrales:
        return pairs
    for p in parcelles_cadastrales:
        if not isinstance(p, dict):
            continue
        sec, num = _normalize_parcelle_ids(str(p.get("section", "")), str(p.get("numero", "")))
        if sec or num:
            pairs.append((sec, num))
    return pairs


def fetch_parcelles_uf_for_schema(
    db_schema: str,
    insee: str,
    uf_gdf_4326: gpd.GeoDataFrame,
    parcelles_cadastrales: Optional[list[dict[str, Any]]],
) -> Tuple[gpd.GeoDataFrame, List[dict[str, Any]]]:
    """
    Parcelles cadastrales de l’UF dans ``{db_schema}.parcelles`` ou ``…parcelles`` (legacy Latresne).
    Même logique que l’ancien nom ``fetch_parcelles_uf``, avec schéma explicite.
    """
    from ..identite_fonciere import _parcelles_table_for_schema

    sch = (db_schema or "latresne").strip().lower()
    if not re.match(r"^[a-z_][a-z0-9_]*$", sch):
        return gpd.GeoDataFrame(), []

    insee_clean = (insee or "").strip()
    if not insee_clean:
        return gpd.GeoDataFrame(), []

    tbl = _parcelles_table_for_schema(sch)
    if not re.match(r"^[a-z_][a-z0-9_]*$", tbl):
        return gpd.GeoDataFrame(), []
    fq = f'"{sch}"."{tbl}"'

    conn = psycopg2.connect(**_db_params())
    rows: list[tuple[Any, ...]] = []
    cols: list[str] = []
    try:
        with conn.cursor() as cur:
            keys = _parcelles_cadastre_keys(parcelles_cadastrales)
            if keys:
                key_sql: list[str] = []
                params: list[Any] = [insee_clean]
                for sec, num in keys:
                    key_sql.append("(UPPER(TRIM(section)) || '|' || LPAD(TRIM(numero), 4, '0')) = %s")
                    params.append(f"{sec}|{num}")
                where_keys = " OR ".join(key_sql)
                cur.execute(
                    f"""
                    SELECT
                        section,
                        numero,
                        idu,
                        contenance,
                        ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geom_json
                    FROM {fq}
                    WHERE code_insee = %s
                      AND geom_2154 IS NOT NULL
                      AND ({where_keys})
                    ORDER BY UPPER(TRIM(section)), LPAD(TRIM(numero), 4, '0');
                    """,
                    params,
                )
            else:
                uf_3857 = uf_gdf_4326.to_crs(epsg=3857)
                uf_union = unary_union(uf_3857.geometry)
                wkt = uf_union.wkt
                if sch == "latresne" and tbl == "parcelles":
                    cur.execute(
                        f"""
                        SELECT
                            section,
                            numero,
                            idu,
                            contenance,
                            ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geom_json
                        FROM {fq}
                        WHERE code_insee = %s
                          AND geom_3857 IS NOT NULL
                          AND ST_Intersects(geom_3857, ST_GeomFromText(%s, 3857))
                        ORDER BY UPPER(TRIM(section)), LPAD(TRIM(numero), 4, '0');
                        """,
                        (insee_clean, wkt),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT
                            section,
                            numero,
                            idu,
                            contenance,
                            ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geom_json
                        FROM {fq}
                        WHERE code_insee = %s
                          AND geom_2154 IS NOT NULL
                          AND ST_Intersects(
                              ST_Transform(geom_2154, 3857),
                              ST_GeomFromText(%s, 3857)
                          )
                        ORDER BY UPPER(TRIM(section)), LPAD(TRIM(numero), 4, '0');
                        """,
                        (insee_clean, wkt),
                    )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
    except Exception as exc:
        print(f"  ⚠ fetch_parcelles_uf_for_schema({sch}) : {exc}")
        return gpd.GeoDataFrame(), []
    finally:
        conn.close()

    if not rows or not cols:
        return gpd.GeoDataFrame(), []

    records: list[dict[str, Any]] = []
    for row in rows:
        d = dict(zip(cols, row))
        gj = d.pop("geom_json", None)
        if not gj:
            continue
        try:
            d["geometry"] = shape(json.loads(gj))
        except Exception:
            continue
        records.append(d)

    if not records:
        return gpd.GeoDataFrame(), []

    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")

    contenances: list[float] = []
    for _, row in gdf.iterrows():
        c = row.get("contenance")
        try:
            if c is not None and str(c).strip() not in ("", "nan"):
                contenances.append(float(c))
            else:
                contenances.append(0.0)
        except (TypeError, ValueError):
            contenances.append(0.0)

    total_c = sum(contenances)
    if total_c <= 0:
        try:
            g2154 = gdf.to_crs(epsg=2154)
            contenances = [
                float(geom.area) if geom is not None and not geom.is_empty else 0.0
                for geom in g2154.geometry
            ]
            total_c = sum(contenances)
        except Exception:
            total_c = 0.0

    detail_out: List[dict[str, Any]] = []
    for i, (_, row) in enumerate(gdf.iterrows()):
        sec = str(row.get("section") or "").strip()
        num = str(row.get("numero") or "").strip()
        ref = f"{sec} {num}".strip() or "—"
        idu = row.get("idu")
        cm2 = contenances[i] if i < len(contenances) else 0.0
        pct = (100.0 * cm2 / total_c) if total_c > 0 else 0.0
        detail_out.append(
            {
                "ref": ref,
                "section": sec,
                "numero": num,
                "idu": str(idu).strip() if idu is not None else "",
                "contenance_m2": round(cm2, 2),
                "pct_uf": round(pct, 2),
            }
        )

    return gdf, detail_out


def fetch_parcelles_uf(
    insee: str,
    uf_gdf_4326: gpd.GeoDataFrame,
    parcelles_cadastrales: Optional[list[dict[str, Any]]],
) -> Tuple[gpd.GeoDataFrame, List[dict[str, Any]]]:
    """Rétrocompat : équivalent à ``fetch_parcelles_uf_for_schema(\"latresne\", ...)``."""
    return fetch_parcelles_uf_for_schema(
        "latresne",
        insee,
        uf_gdf_4326,
        parcelles_cadastrales,
    )


# ---------------------------------------------------------------------------
# 2. Fetch entités PLU depuis PostGIS (intersection + buffer 300 m)
# ---------------------------------------------------------------------------

def fetch_plu_context(parcelle_gdf: gpd.GeoDataFrame, buffer_m: float = 50.0) -> gpd.GeoDataFrame:
    """
    Récupère les entités PLU intersectant la parcelle + un buffer de `buffer_m` mètres.
    Retourne un GeoDataFrame en EPSG:3857.
    """
    # Reprojection en 3857 pour le buffer métrique
    parc_3857 = parcelle_gdf.to_crs(epsg=3857)
    parc_geom = unary_union(parc_3857.geometry)
    buffer_geom = parc_geom.buffer(buffer_m)

    # WKT pour la requête SQL (en EPSG:3857 — la colonne geom_3857 est indexée)
    wkt_buffer = buffer_geom.wkt

    sql = f"""
        SELECT
            id,
            libelle,
            libelong,
            typezone,
            zonage_reglement,
            ST_AsGeoJSON(
                ST_Intersection(geom_3857, ST_GeomFromText('{wkt_buffer}', 3857))
            ) AS geom_json
        FROM latresne.zonage_plu
        WHERE ST_Intersects(geom_3857, ST_GeomFromText('{wkt_buffer}', 3857))
          AND geom_invalid IS NOT TRUE
        ORDER BY zonage_reglement;
    """

    print(f"  ↳ Requête PLU (buffer {buffer_m} m) …")
    conn = psycopg2.connect(**_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    if not rows:
        print("  ⚠ Aucune entité PLU trouvée dans le périmètre.")
        return gpd.GeoDataFrame(columns=["zonage_reglement", "geometry"], crs="EPSG:3857")

    records = []
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
    print(f"  ✓ {len(gdf)} entité(s) PLU récupérée(s)")
    return gdf


# ---------------------------------------------------------------------------
# 3. Calcul des intersections réelles (pour la légende %)
# ---------------------------------------------------------------------------

def compute_intersection_stats(
    parcelle_gdf: gpd.GeoDataFrame,
    plu_gdf: gpd.GeoDataFrame,
) -> dict[str, float]:
    """
    Calcule la surface d'intersection (m²) entre la parcelle et chaque zone PLU.
    Retourne un dict {zonage_reglement: pct} où pct somme à 100.
    """
    parc_3857 = parcelle_gdf.to_crs(epsg=3857)
    parc_geom = unary_union(parc_3857.geometry)
    total_area = parc_geom.area

    if total_area <= 0 or plu_gdf.empty:
        return {}

    # Re-fetch uniquement les intersections strictes (pas le buffer)
    parc_wkt = parc_geom.wkt
    stats: dict[str, float] = {}

    conn = psycopg2.connect(**_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    zonage_reglement,
                    ST_Area(
                        ST_Intersection(geom_3857, ST_GeomFromText('{parc_wkt}', 3857))
                    ) AS area_m2
                FROM latresne.zonage_plu
                WHERE ST_Intersects(geom_3857, ST_GeomFromText('{parc_wkt}', 3857))
                  AND geom_invalid IS NOT TRUE;
            """)
            for zone, area in cur.fetchall():
                zone = zone or "Non renseigné"
                stats[zone] = stats.get(zone, 0.0) + (area or 0.0)
    finally:
        conn.close()

    # Convertir en pourcentages
    pct = {z: (a / total_area) * 100 for z, a in stats.items() if a > 0}
    return pct


# ---------------------------------------------------------------------------
# 4. Couleurs par zone (typezone → couleur CNIG d’appui)
# ---------------------------------------------------------------------------

def _safe_str_typezone(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, float) and str(v) == "nan":
        return None
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return None
    return s


def _color_from_typezone(typezone: Any) -> str:
    """
    Couleur d’appui selon `typezone` (PLU / GPU), logique CNIG généralisable :
    - préfixe **N** → zone naturelle (vert)
    - **A** (seul) → zone agricole (jaune)
    - **U** (seul) → zone urbaine (rouge franc)
    - préfixe **AU** (ex. AUc) → à urbaniser / variantes (rouge atténué)
    - autre préfixe **U**… (hors U seul) → variante rouge
    - défaut → rouge très atténué
    """
    t = _safe_str_typezone(typezone)
    if not t:
        return "#9CA3AF"
    u = t.upper()
    if u.startswith("N"):
        return "#2D6A4F"
    if u == "A":
        return "#E9C46A"
    if u == "U":
        return "#C1121F"
    if u.startswith("AU"):
        return "#E07A7A"
    if u.startswith("U"):
        return "#D4574A"
    return "#D4A5A5"


def _zonage_reglement_to_typezone(plu_gdf: gpd.GeoDataFrame) -> dict[str, str]:
    """Un `typezone` représentatif par `zonage_reglement` (1er non nul)."""
    out: dict[str, str] = {}
    if plu_gdf.empty or "zonage_reglement" not in plu_gdf.columns:
        return out
    if "typezone" not in plu_gdf.columns:
        return out
    for z in plu_gdf["zonage_reglement"].dropna().unique():
        sub = plu_gdf[plu_gdf["zonage_reglement"] == z]
        ser = sub["typezone"]
        tz = None
        for v in ser:
            tz = _safe_str_typezone(v)
            if tz:
                break
        if tz:
            out[str(z).strip()] = tz
    return out


def _color_map_from_plu_gdf(plu_gdf: gpd.GeoDataFrame) -> dict[str, str]:
    """Couleur par `zonage_reglement`, dérivée du `typezone` des entités PLU."""
    tz_by_z = _zonage_reglement_to_typezone(plu_gdf)
    return {z: _color_from_typezone(tz) for z, tz in tz_by_z.items()}


def _merge_color_map_for_stats(
    color_map: dict[str, str],
    pct_stats: dict[str, float],
    plu_gdf: gpd.GeoDataFrame,
) -> dict[str, str]:
    """Complète les clés présentes dans les % (intersection) mais absentes du buffer carto."""
    out = dict(color_map)
    tz_by_z = _zonage_reglement_to_typezone(plu_gdf)
    for k in pct_stats:
        ks = str(k).strip()
        if ks not in out:
            out[ks] = _color_from_typezone(tz_by_z.get(ks))
    return out


# ---------------------------------------------------------------------------
# 5. Carte + légende (une seule image PNG)
# ---------------------------------------------------------------------------


def _populate_map_axis(
    ax,
    parcelle_gdf: gpd.GeoDataFrame,
    plu_gdf: gpd.GeoDataFrame,
    color_map: dict[str, str],
    *,
    parcelles_cadastrales_gdf: Optional[gpd.GeoDataFrame] = None,
    zonage_label_column: str = "zonage_reglement",
) -> None:
    """Carte satellite + PLU + périmètre UF ; optionnellement limites fines entre parcelles cadastrales."""
    parc_3857 = parcelle_gdf.to_crs(epsg=3857)
    parc_geom = unary_union(parc_3857.geometry)
    minx, miny, maxx, maxy = parc_geom.bounds
    pad = 80  # m (Web Mercator)

    if not plu_gdf.empty:
        for zone in plu_gdf["zonage_reglement"].unique():
            subset = plu_gdf[plu_gdf["zonage_reglement"] == zone]
            color = color_map.get(str(zone), "#888888")
            rgba = list(to_rgba(color))
            rgba[3] = 0.40
            try:
                subset.plot(
                    ax=ax,
                    facecolor=rgba,
                    edgecolor=color,
                    linewidth=1.2,
                    zorder=2,
                )
            except Exception:
                pass

    # Parcelles cadastrales : limites internes (jaune fin) si > 1 ; libellés section/numéro dès qu’on a les géométries
    if parcelles_cadastrales_gdf is not None and not parcelles_cadastrales_gdf.empty:
        try:
            ppc = parcelles_cadastrales_gdf.to_crs(epsg=3857)
            if len(ppc) > 1:
                ppc.plot(
                    ax=ax,
                    facecolor="none",
                    edgecolor="#FFE135",
                    linewidth=0.65,
                    zorder=3,
                )
            for _, prow in ppc.iterrows():
                g = prow.geometry
                if g is None or g.is_empty:
                    continue
                try:
                    c = g.centroid
                except Exception:
                    continue
                sec = str(prow.get("section", "") or "").strip()
                num = str(prow.get("numero", "") or "").strip()
                lbl = f"{sec} {num}".strip()
                if not lbl:
                    continue
                ax.text(
                    c.x,
                    c.y,
                    lbl,
                    fontsize=5.2,
                    ha="center",
                    va="center",
                    color="#1a1a1a",
                    fontweight="bold",
                    path_effects=[pe.withStroke(linewidth=1.0, foreground="white")],
                    zorder=6,
                    clip_on=True,
                )
        except Exception:
            pass

    parc_3857.plot(
        ax=ax,
        facecolor="none",
        edgecolor="#FFD600",
        linewidth=2.5,
        zorder=4,
    )

    ax.set_xlim(minx - pad, maxx + pad)
    ax.set_ylim(miny - pad, maxy + pad)
    ax.set_aspect("equal", adjustable="box")

    crs_str = parc_3857.crs.to_string()
    try:
        ctx.add_basemap(
            ax,
            crs=crs_str,
            source=ctx.providers.Esri.WorldImagery,
            zoom="auto",
            attribution=False,
            zorder=0,
        )
    except Exception as e:
        print(f"  ⚠ Esri WorldImagery indisponible ({e}), tentative OSM…")
        try:
            ctx.add_basemap(
                ax,
                crs=crs_str,
                source=ctx.providers.OpenStreetMap.Mapnik,
                zoom="auto",
                attribution=False,
                zorder=0,
            )
        except Exception as e2:
            print(f"  ⚠ OSM aussi indisponible ({e2})")

    if not plu_gdf.empty:
        for _, row in plu_gdf.iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty or geom.area < 50:
                continue
            try:
                cx_, cy_ = geom.centroid.x, geom.centroid.y
                col = zonage_label_column if zonage_label_column in plu_gdf.columns else "zonage_reglement"
                lbl = row.get(col) if hasattr(row, "get") else row[col]
                ax.text(
                    cx_,
                    cy_,
                    str(lbl),
                    fontsize=6.5,
                    ha="center",
                    va="center",
                    color="white",
                    fontweight="bold",
                    path_effects=[pe.withStroke(linewidth=1.5, foreground="black")],
                    zorder=5,
                    clip_on=True,
                )
            except Exception:
                pass

    ax.set_axis_off()


def _draw_zonage_legend_panel(
    ax_leg,
    pct_stats: dict[str, float],
    color_map: dict[str, str],
    pct_min_affiche: float,
    *,
    zonage_to_typezone: Optional[dict[str, str]] = None,
    title: Optional[str] = None,
    label_map: Optional[dict[str, str]] = None,
) -> None:
    """Légende des zonages : pourcentages et couleurs selon type CNIG."""
    ax_leg.cla()
    ax_leg.axis("off")
    ax_leg.set_xlim(0, 1)
    ax_leg.set_ylim(0, 1)

    if not pct_stats:
        ax_leg.text(0.5, 0.5, "Aucune intersection\ncalculable", ha="center", va="center", fontsize=8)
        return

    filtre = {k: v for k, v in pct_stats.items() if v >= pct_min_affiche}
    if not filtre:
        ax_leg.text(
            0.5,
            0.5,
            f"Aucun zonage ≥ {pct_min_affiche:g} %",
            ha="center",
            va="center",
            fontsize=8,
        )
        return

    labels = list(filtre.keys())
    values = [filtre[l] for l in labels]
    z2t = zonage_to_typezone or {}
    lm = label_map or {}
    legend_labels: list[str] = []
    for l, v in zip(labels, values):
        lk = str(l).strip()
        tz = z2t.get(lk)
        display = lm.get(lk) or lk
        legend_labels.append(f"{display}  {v:.1f} %")

    handles = [
        mpatches.Patch(
            facecolor=color_map.get(str(l), "#888888"),
            edgecolor="white",
            linewidth=0.75,
        )
        for l in labels
    ]

    ax_leg.legend(
        handles,
        legend_labels,
        loc="center",
        fontsize=10,
        framealpha=0.9,
        edgecolor="#cccccc",
        facecolor="#fafafa",
        title=title or "Zonage réglementaire",
        title_fontsize=9,
    )


def render_combined_plu_visual(
    parcelle_gdf: gpd.GeoDataFrame,
    plu_gdf: gpd.GeoDataFrame,
    color_map: dict[str, str],
    pct_stats: dict[str, float],
    out_path: str,
    dpi: int = 180,
    *,
    pct_min_affiche: float = MIN_PCT_ZONAGE_URBAIN,
    parcelles_cadastrales_gdf: Optional[gpd.GeoDataFrame] = None,
    map_zonage_label_column: str = "zonage_reglement",
    legend_panel_title: Optional[str] = None,
    legend_color_map: Optional[dict[str, str]] = None,
    legend_label_map: Optional[dict[str, str]] = None,
) -> str:
    """
    Une seule figure : carte carrée à gauche, légende « Zonage réglementaire » à droite (PNG).
    """
    side = float(PLU_MAP_SQUARE_SIDE_IN)
    right_ratio = float(PLU_MAP_RIGHT_PANEL_RATIO)
    fig_w = side * (1.0 + right_ratio)
    fig_h = side
    fig = plt.figure(figsize=(fig_w, fig_h), facecolor="white")
    gs = gridspec.GridSpec(
        1,
        2,
        figure=fig,
        width_ratios=[1.0, right_ratio],
        wspace=0.06,
        left=0.04,
        right=0.98,
        bottom=0.07,
        top=0.97,
    )
    ax_map = fig.add_subplot(gs[0, 0])
    ax_leg = fig.add_subplot(gs[0, 1])

    _populate_map_axis(
        ax_map,
        parcelle_gdf,
        plu_gdf,
        color_map,
        parcelles_cadastrales_gdf=parcelles_cadastrales_gdf,
        zonage_label_column=map_zonage_label_column,
    )
    _draw_zonage_legend_panel(
        ax_leg,
        pct_stats,
        legend_color_map or color_map,
        pct_min_affiche,
        zonage_to_typezone=_zonage_reglement_to_typezone(plu_gdf),
        title=legend_panel_title,
        label_map=legend_label_map,
    )

    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor="white", pad_inches=0.12)
    plt.close(fig)
    print(f"  ✓ Visuel combiné carte + légende enregistré : {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------

def generate_plu_visuals_from_uf_geometry(
    geometry: dict[str, Any],
    out_dir: str,
    *,
    srid: Optional[int] = None,
    buffer_m: float = 300.0,
    dpi: int = 180,
    insee: str = "",
    parcelles_cadastrales: Optional[list[dict[str, Any]]] = None,
) -> tuple[str, str, dict[str, float], List[dict[str, Any]]]:
    """
    Image unique (carte + légende zonages) à partir du GeoJSON d’étude (UF), même logique SQL que la parcelle DB.
    Retourne (chemin PNG, doublon compat. API, stats % par zonage, détail parcelles cadastrales pour le PDF).
    """
    from ..identite_fonciere import get_catalogue, get_identite_db_schema
    from .plu_zonage_rapport import (
        generate_plu_zonage_page_visuals_from_uf_geometry,
        resolve_plu_zonage_page_config,
    )

    cfg = resolve_plu_zonage_page_config(get_identite_db_schema(), get_catalogue())
    return generate_plu_zonage_page_visuals_from_uf_geometry(
        geometry,
        out_dir,
        cfg,
        srid=srid,
        buffer_m=buffer_m,
        dpi=dpi,
        insee=insee,
        parcelles_cadastrales=parcelles_cadastrales,
    )


def generate_plu_visuals(
    insee: str = "33234",
    section: str = "AL",
    numero: str = "0074",
    out_dir: str = ".",
    buffer_m: float =300.0,
    dpi: int = 180,
) -> tuple[str, str, dict[str, float]]:
    """
    Génère une image PNG (carte + légende zonages côte à côte).

    Returns:
        (map_png_path, map_png_path_compat, pct_stats) — les deux premiers chemins sont identiques.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    tag = f"{insee}_{section}_{numero}"
    map_path = str(out / f"plu_map_{tag}.png")

    # 1. Parcelle
    print("\n[1/4] Récupération de la parcelle (latresne.parcelles)…")
    parcelle_gdf = fetch_parcelle(insee, section, numero)

    # 2. PLU contexte
    print(f"\n[2/4] Récupération du PLU (buffer {buffer_m} m)…")
    plu_gdf = fetch_plu_context(parcelle_gdf, buffer_m=buffer_m)

    # 3. Stats d'intersection (pour la légende %)
    print("\n[3/4] Calcul des intersections…")
    pct_stats = compute_intersection_stats(parcelle_gdf, plu_gdf)
    print(f"  ↳ Zonages intersectés : {pct_stats}")

    # 4. Couleurs (typezone CNIG → par zonage_reglement)
    color_map = _merge_color_map_for_stats(
        _color_map_from_plu_gdf(plu_gdf),
        pct_stats,
        plu_gdf,
    )

    # 5. Rendu
    print("\n[4/4] Rendu carte + légende…")
    render_combined_plu_visual(
        parcelle_gdf,
        plu_gdf,
        color_map,
        pct_stats,
        map_path,
        dpi=dpi,
        pct_min_affiche=MIN_PCT_ZONAGE_URBAIN,
    )

    print(f"\n✅ Visuel combiné enregistré :\n  • {map_path}")
    return map_path, map_path, pct_stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Génère les visuels PLU d'une parcelle.")
    parser.add_argument("--insee", default="33234")
    parser.add_argument("--section", default="AL")
    parser.add_argument("--numero", default="0074")
    parser.add_argument("--out_dir", default=".")
    parser.add_argument("--buffer", type=float, default=300.0)
    parser.add_argument("--dpi", type=int, default=180)
    args = parser.parse_args()

    # Charger le .env (dossier du script ou racine projet cua_latresne_v4)
    _here = Path(__file__).resolve().parent
    for env_file in (_here / ".env", _here.parents[3] / ".env"):
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())

    generate_plu_visuals(
        insee=args.insee,
        section=args.section,
        numero=args.numero,
        out_dir=args.out_dir,
        buffer_m=args.buffer,
        dpi=args.dpi,
    )