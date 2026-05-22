"""
Carte regroupée des couches « servitude » du catalogue identité foncière + page PDF dédiée.

- Carte : fond satellite, UF, parcelles cadastrales de l’UF, tracés des entités (buffer).
- Légende : une couleur par couche représentée sur la carte.
- Tableau récapitulatif : une ligne par couche intersectant l’UF, avec % de surface UF couverte.
- Détail : pour chaque type (clé catalogue / vue), tableau d’attributs aligné sur le corps du rapport
  (colonnes ``keep`` / ``clean_attributes``), comme l’article 4.

Schéma PostGIS : ``{db_schema}.*`` (argument, ContextVar ``get_identite_db_schema``, ou env
``IDENTITE_FONCIERE_DB_SCHEMA``). Parcelles UF : ``{db_schema}.parcelles`` ou
``latresne.parcelles`` .

Le PPRI (`pm1_detaillee_gironde`) est exclu de cette carte (page PPRI dédiée).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import contextily as ctx
import geopandas as gpd
import matplotlib
import pandas as pd
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import psycopg2
from matplotlib import gridspec
from matplotlib.colors import to_rgba
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import HRFlowable, Image, Paragraph, Spacer, Table, TableStyle
from shapely.geometry import shape
from shapely.ops import unary_union
from xml.sax.saxutils import escape as xml_escape

from .plu_visuels import (
    PLU_MAP_COVER_ASPECT_WH,
    PLU_MAP_RIGHT_PANEL_RATIO,
    PLU_MAP_SQUARE_SIDE_IN,
    fetch_parcelles_uf_for_schema,
    parcelle_gdf_from_geojson,
)

matplotlib.use("Agg")
warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)

BUFFER_SERVITUDES_M = 300.0

# PPRI : page dédiée `section_ppri` — pas de doublon sur cette carte
SERVITUDE_MAP_EXCLUDE_TABLES = frozenset({"pm1_detaillee_gironde"})

# Couleurs distinctes (ordre stable par clé de table)
SERVITUDE_LAYER_COLORS = [
    "#2563EB",
    "#DC2626",
    "#059669",
    "#D97706",
    "#7C3AED",
    "#DB2777",
    "#0D9488",
    "#CA8A04",
    "#4F46E5",
    "#0EA5E9",
]


def _catalogue_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    for rel in (
        ("api", "identite_fonciere", "catalogues", "catalogue_identite_fonciere_latresne.json"),
        ("api", "identite_fonciere", "catalogues", "catalogue_identite_fonciere.json"),
        ("catalogues", "catalogue_identite_fonciere.json"),
        ("CATALOGUES", "catalogue_identite_fonciere.json"),
    ):
        p = root.joinpath(*rel)
        if p.is_file():
            return p
    return root / "api" / "identite_fonciere" / "catalogues" / "catalogue_identite_fonciere_latresne.json"


def load_catalogue_identite_fonciere() -> Dict[str, Any]:
    p = _catalogue_path()
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def servitude_catalog_entries(
    catalogue: Optional[Dict[str, Any]] = None,
) -> List[Tuple[str, str]]:
    """
    (clé table, nom d'affichage catalogue) pour type == servitude, hors exclusions.
    """
    if catalogue is None:
        try:
            from ..identite_fonciere import get_catalogue

            catalogue = get_catalogue()
        except Exception:
            catalogue = load_catalogue_identite_fonciere()
    cat = catalogue
    out: List[Tuple[str, str]] = []
    for table_key, cfg in cat.items():
        if not isinstance(cfg, dict) or cfg.get("type") != "servitude":
            continue
        if table_key in SERVITUDE_MAP_EXCLUDE_TABLES:
            continue
        if not re.match(r"^[a-z_][a-z0-9_]*$", str(table_key)):
            logger.warning("Clé catalogue ignorée (identifiant invalide) : %s", table_key)
            continue
        nom = (cfg.get("nom_affiche") or cfg.get("nom") or table_key).strip()
        out.append((str(table_key), nom))
    return sorted(out, key=lambda x: x[0].lower())


def _db_params() -> dict:
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


def _resolve_servitudes_db_schema(db_schema: Optional[str] = None) -> str:
    """
    Schéma PostGIS des couches servitudes / parcelles.

    Priorité : argument explicite, puis ``get_identite_db_schema()`` (ContextVar requête),
    puis variable d'environnement ``IDENTITE_FONCIERE_DB_SCHEMA``, défaut ``latresne``.
    """
    raw = (db_schema or "").strip().lower()
    if raw and re.match(r"^[a-z_][a-z0-9_]*$", raw):
        return raw
    try:
        from ..identite_fonciere import get_identite_db_schema

        got = (get_identite_db_schema() or "").strip().lower()
        if got and re.match(r"^[a-z_][a-z0-9_]*$", got):
            return got
    except Exception:
        pass
    env = (os.getenv("IDENTITE_FONCIERE_DB_SCHEMA") or "latresne").strip().lower() or "latresne"
    return env if re.match(r"^[a-z_][a-z0-9_]*$", env) else "latresne"


def _find_geom_column(conn, table_name: str, schema: str) -> Optional[str]:
    if not re.match(r"^[a-z_][a-z0-9_]*$", table_name):
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table_name),
        )
        rows = cur.fetchall()
    by_name = {r[0]: r[1] for r in rows}
    for c in ("geom_2154", "geom"):
        if c in by_name:
            return c
    for col, udt in by_name.items():
        if udt == "geometry":
            return col
    return None


def _color_map_for_tables(table_keys: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for i, k in enumerate(sorted(table_keys, key=lambda x: x.lower())):
        out[k] = SERVITUDE_LAYER_COLORS[i % len(SERVITUDE_LAYER_COLORS)]
    return out


def _sql_ident_quoted(name: str) -> Optional[str]:
    n = (name or "").strip().lower()
    if not n or not re.match(r"^[a-z_][a-z0-9_]*$", n):
        return None
    return '"' + n.replace('"', '""') + '"'


def compute_servitude_layer_uf_overlap_pct(
    parcelle_gdf: gpd.GeoDataFrame,
    table_key: str,
    geom_col: str,
    schema: str,
) -> float:
    """
    Part de la surface de l’UF (Lambert-93) couverte par l’union des intersections
    avec les entités d’une couche servitude (surfacique : extract sur polygones).
    """
    sch_q = _sql_ident_quoted(schema)
    tbl_q = _sql_ident_quoted(table_key)
    if not sch_q or not tbl_q:
        return 0.0
    gc = (geom_col or "").strip()
    if not gc or not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", gc):
        return 0.0

    uf_2154 = parcelle_gdf.to_crs(epsg=2154)
    uf_geom = unary_union(uf_2154.geometry)
    if uf_geom.is_empty:
        return 0.0
    uf_wkt = uf_geom.wkt
    fq = f"{sch_q}.{tbl_q}"

    sql = f"""
        WITH uf AS (SELECT ST_GeomFromText(%s, 2154) AS g),
        ix AS (
            SELECT ST_CollectionExtract(
                ST_Force2D(ST_Intersection(ST_MakeValid(t.{gc}), uf.g)),
                3
            ) AS g2
            FROM {fq} t, uf
            WHERE t.{gc} IS NOT NULL AND ST_Intersects(t.{gc}, uf.g)
        )
        SELECT
            ST_Area((SELECT g FROM uf))::double precision AS au,
            COALESCE(
                ST_Area(
                    ST_Intersection(
                        (SELECT g FROM uf),
                        (SELECT ST_UnaryUnion(ST_Collect(g2)) FROM ix
                         WHERE g2 IS NOT NULL AND NOT ST_IsEmpty(g2))
                    )
                ),
                0.0
            )::double precision AS ai
    """
    conn = psycopg2.connect(**_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (uf_wkt,))
            row = cur.fetchone()
        if not row:
            return 0.0
        au, ai = float(row[0] or 0.0), float(row[1] or 0.0)
        if au <= 0:
            return 0.0
        return min(100.0, max(0.0, (ai / au) * 100.0))
    except Exception as exc:
        logger.warning("Servitudes : surface UF pour %s : %s", table_key, exc)
        return 0.0
    finally:
        conn.close()


def count_intersections_uf(
    parcelle_gdf: gpd.GeoDataFrame,
    table_key: str,
    geom_col: str,
    schema: str,
) -> int:
    uf_2154 = parcelle_gdf.to_crs(epsg=2154)
    uf_geom = unary_union(uf_2154.geometry)
    wkt = uf_geom.wkt
    fq = f'"{schema}"."{table_key}"'
    sql = f"""
        SELECT COUNT(*)::bigint
        FROM {fq} t
        WHERE t.{geom_col} IS NOT NULL
          AND ST_Intersects(t.{geom_col}, ST_GeomFromText(%s, 2154))
    """
    conn = psycopg2.connect(**_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (wkt,))
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0
    finally:
        conn.close()


def fetch_servitudes_in_buffer_gdf(
    parcelle_gdf: gpd.GeoDataFrame,
    buffer_m: float,
    table_key: str,
    geom_col: str,
    schema: str,
) -> gpd.GeoDataFrame:
    """Entités d'une couche découpées au buffer, EPSG:3857, colonne `layer_key`."""
    wkt_4326 = unary_union(parcelle_gdf.geometry).wkt
    fq = f'"{schema}"."{table_key}"'
    sql = f"""
        WITH uf AS (
            SELECT ST_Transform(ST_GeomFromText(%s, 4326), 2154) AS geom
        ),
        buf AS (
            SELECT ST_Buffer(uf.geom, %s) AS geom FROM uf
        )
        SELECT
            ST_AsGeoJSON(
                ST_Transform(
                    ST_Intersection(t.{geom_col}, buf.geom),
                    3857
                )
            ) AS geom_json
        FROM {fq} t, buf
        WHERE t.{geom_col} IS NOT NULL
          AND ST_Intersects(t.{geom_col}, buf.geom)
    """
    conn = psycopg2.connect(**_db_params())
    rows: List[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (wkt_4326, buffer_m))
            rows = [r[0] for r in cur.fetchall() if r[0]]
    finally:
        conn.close()

    recs: List[dict] = []
    for gj in rows:
        try:
            geom = shape(json.loads(gj))
        except Exception:
            continue
        if geom.is_empty:
            continue
        recs.append({"layer_key": table_key, "geometry": geom})
    if not recs:
        return gpd.GeoDataFrame(columns=["layer_key", "geometry"], crs="EPSG:3857")
    return gpd.GeoDataFrame(recs, crs="EPSG:3857")


def _plot_layer_geoms(ax, sub: gpd.GeoDataFrame, color: str) -> None:
    if sub.empty:
        return
    for gt in sub.geometry.geom_type.unique():
        part = sub.loc[sub.geometry.geom_type == gt]
        try:
            if gt in ("Polygon", "MultiPolygon"):
                rgba = list(to_rgba(color))
                rgba[3] = 0.38
                part.plot(
                    ax=ax,
                    facecolor=rgba,
                    edgecolor=color,
                    linewidth=1.15,
                    zorder=2,
                )
            elif gt in ("LineString", "MultiLineString"):
                part.plot(ax=ax, color=color, linewidth=2.0, zorder=2)
            elif gt in ("Point", "MultiPoint"):
                part.plot(ax=ax, color=color, markersize=22, zorder=2, alpha=0.9)
        except Exception:
            pass


def _populate_servitudes_map_axis(
    ax,
    parcelle_gdf: gpd.GeoDataFrame,
    servitudes_gdf: gpd.GeoDataFrame,
    color_by_layer: Dict[str, str],
    *,
    parcelles_cadastrales_gdf: Optional[gpd.GeoDataFrame] = None,
) -> None:
    parc_3857 = parcelle_gdf.to_crs(epsg=3857)
    parc_geom = unary_union(parc_3857.geometry)
    minx, miny, maxx, maxy = parc_geom.bounds
    pad = 80.0

    if not servitudes_gdf.empty and "layer_key" in servitudes_gdf.columns:
        for lk, sub in servitudes_gdf.groupby("layer_key"):
            key = str(lk)
            col = color_by_layer.get(key, "#888888")
            _plot_layer_geoms(ax, sub, col)

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
    except Exception:
        try:
            ctx.add_basemap(
                ax,
                crs=crs_str,
                source=ctx.providers.OpenStreetMap.Mapnik,
                zoom="auto",
                attribution=False,
                zorder=0,
            )
        except Exception:
            pass

    ax.set_axis_off()


def _legend_servitudes_panel(
    ax_leg,
    layers_in_map: List[str],
    display_names: Dict[str, str],
    color_by_layer: Dict[str, str],
) -> None:
    ax_leg.cla()
    ax_leg.axis("off")
    ax_leg.set_xlim(0, 1)
    ax_leg.set_ylim(0, 1)
    if not layers_in_map:
        ax_leg.text(0.5, 0.5, "Aucune entité dans le périmètre", ha="center", va="center", fontsize=8)
        return
    labels = [display_names.get(k, k)[:52] for k in layers_in_map]
    handles = [
        mpatches.Patch(
            facecolor=color_by_layer.get(k, "#888888"),
            edgecolor="white",
            linewidth=0.75,
        )
        for k in layers_in_map
    ]
    ax_leg.legend(
        handles,
        labels,
        loc="center",
        fontsize=8,
        framealpha=0.92,
        edgecolor="#cccccc",
        facecolor="#fafafa",
        title="Servitudes",
        title_fontsize=9,
    )


def render_servitudes_combined_png(
    parcelle_gdf: gpd.GeoDataFrame,
    servitudes_gdf: gpd.GeoDataFrame,
    color_by_layer: Dict[str, str],
    display_names: Dict[str, str],
    layers_in_legend: List[str],
    out_path: str,
    dpi: int = 180,
    parcelles_cadastrales_gdf: Optional[gpd.GeoDataFrame] = None,
) -> str:
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
    _populate_servitudes_map_axis(
        ax_map,
        parcelle_gdf,
        servitudes_gdf,
        color_by_layer,
        parcelles_cadastrales_gdf=parcelles_cadastrales_gdf,
    )
    _legend_servitudes_panel(ax_leg, layers_in_legend, display_names, color_by_layer)
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor="white", pad_inches=0.12)
    plt.close(fig)
    return out_path


def _image_size_pt(png_path: Path, target_width_pt: float) -> Tuple[float, float]:
    try:
        from PIL import Image as PILImage

        with PILImage.open(png_path) as im:
            pw, ph = im.size
        if pw > 0 and ph > 0:
            w = max(float(target_width_pt), 1.0)
            return w, w * (float(ph) / float(pw))
    except Exception:
        pass
    w = max(float(target_width_pt), 1.0)
    return w, w / float(PLU_MAP_COVER_ASPECT_WH)


def _flowables_servitude_layer_attributes(
    layer: Dict[str, Any],
    inner_w: float,
    c_border: Any,
    ps_attr_key: ParagraphStyle,
    ps_attr_val: ParagraphStyle,
    ps_muted: ParagraphStyle,
) -> List[Any]:
    """Tableau d’attributs aligné sur le corps du rapport (article 4), sans bandeau type."""
    from .rapport_identite_fonciere import (
        PDF_LINEAR_LAYER_ATTR_MAX_ROWS,
        _aggregate_elements_for_pdf,
        _build_attr_label_map,
        _format_attr_value,
        _is_linear_geom_layer,
        _normalize_layer_elements,
        _pdf_column_keys,
        _pdf_keep_effectif_vide,
        _pdf_keep_only_reglementation,
    )

    out: List[Any] = []
    display_name = layer.get("display_name") or layer.get("table") or "Couche"

    elements = _normalize_layer_elements(layer)
    filtered_elements = _aggregate_elements_for_pdf(layer, elements)
    attr_trunc_note: Optional[str] = None

    if _pdf_keep_effectif_vide(layer):
        out.append(
            Paragraph(
                "Intersection détectée (attributs non configurés pour le tableau : <i>keep</i> vide).",
                ps_muted,
            )
        )
        return out
    if _pdf_keep_only_reglementation(layer):
        cnt = len(elements)
        rows = [
            [
                Paragraph("<b>Couche</b>", ps_attr_key),
                Paragraph("<b>Nombre d’entités intersectées</b>", ps_attr_key),
            ],
            [
                Paragraph(xml_escape(str(display_name)), ps_attr_val),
                Paragraph(str(cnt), ps_attr_val),
            ],
        ]
        tbl = Table(rows, colWidths=[inner_w * 0.68, inner_w * 0.32], repeatRows=1)
        tbl.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E8F5EE")),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCDDCC")),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7FCF9")]),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )
        out.append(Spacer(1, 4))
        out.append(tbl)
        return out
    if not filtered_elements:
        out.append(
            Paragraph(
                "Intersection détectée (données attributaires non disponibles).",
                ps_muted,
            )
        )
        return out

    table_elements = filtered_elements
    n_tab = len(filtered_elements)
    if _is_linear_geom_layer(layer) and n_tab > PDF_LINEAR_LAYER_ATTR_MAX_ROWS:
        table_elements = filtered_elements[:PDF_LINEAR_LAYER_ATTR_MAX_ROWS]
        omitted = n_tab - PDF_LINEAR_LAYER_ATTR_MAX_ROWS
        attr_trunc_note = (
            f"… et {omitted} autre(s) entité(s) non affichée(s) "
            f"(couche linéaire : aperçu limité à {PDF_LINEAR_LAYER_ATTR_MAX_ROWS} ligne(s))."
        )

    label_map = _build_attr_label_map(layer)
    catalog_keys = _pdf_column_keys(layer)
    if catalog_keys:
        all_keys = catalog_keys
    else:
        all_keys = []
        for el in filtered_elements:
            for k in el.keys():
                if k not in all_keys:
                    all_keys.append(k)

    if not all_keys:
        out.append(
            Paragraph(
                "Intersection détectée (données attributaires non disponibles pour le tableau).",
                ps_muted,
            )
        )
        return out

    attr_rows: List[List[Any]] = []
    header_cells = [Paragraph(f"<b>{xml_escape(label_map.get(k, k))}</b>", ps_attr_key) for k in all_keys]
    attr_rows.append(header_cells)
    for el in table_elements:
        row_cells = []
        for k in all_keys:
            val = el.get(k)
            row_cells.append(Paragraph(_format_attr_value(val), ps_attr_val))
        attr_rows.append(row_cells)

    col_w = inner_w / max(len(all_keys), 1)
    col_widths = [col_w] * len(all_keys)
    repeat_hdr = 1 if len(attr_rows) > 1 else 0
    out.append(Spacer(1, 4))
    attr_tbl = Table(attr_rows, colWidths=col_widths, repeatRows=repeat_hdr)
    attr_tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E8F5EE")),
                ("GRID", (0, 0), (-1, -1), 0.5, c_border),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7FCF9")]),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    out.append(attr_tbl)
    if attr_trunc_note:
        out.append(Spacer(1, 4))
        out.append(Paragraph(xml_escape(attr_trunc_note), ps_muted))
    return out


def build_servitudes_section_flowables(
    map_png_path: str,
    *,
    table_width: float,
    layer_keys: List[str],
    display_names: Dict[str, str],
    c_kerelia_light: Any,
    c_border: Any,
    layer_uf_overlap_pct: Optional[Dict[str, float]] = None,
    intersection_layers: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Any]:
    """Récap + carte + tableau synthèse (avec % UF) + détail attributaire par couche intersectée."""
    pp = Path(map_png_path)
    if not pp.is_file():
        return []

    pct_map = layer_uf_overlap_pct or {}
    inter_by_table = intersection_layers or {}

    base = getSampleStyleSheet()
    ps_kicker = ParagraphStyle(
        "ServKicker",
        parent=base["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#6b7f72"),
        fontName="Helvetica-Bold",
        spaceAfter=6,
        leading=10,
    )
    ps_title = ParagraphStyle(
        "ServTitle",
        parent=base["Normal"],
        fontSize=17,
        textColor=colors.HexColor("#1e4d2f"),
        fontName="Helvetica-Bold",
        spaceAfter=8,
        leading=22,
    )
    ps_lbl = ParagraphStyle(
        "ServLbl",
        parent=base["Normal"],
        fontSize=9,
        textColor=colors.HexColor("#5a5a5a"),
        fontName="Helvetica-Bold",
        leading=12,
    )
    ps_val = ParagraphStyle(
        "ServVal",
        parent=base["Normal"],
        fontSize=9.5,
        textColor=colors.HexColor("#1a1a1a"),
        fontName="Helvetica",
        leading=12,
    )
    ps_layer_hdr = ParagraphStyle(
        "ServLayerHdr",
        parent=ps_lbl,
        fontSize=10,
        textColor=colors.HexColor("#1e4d2f"),
        spaceBefore=4,
    )
    ps_attr_key = ParagraphStyle(
        "ServAttrKey",
        parent=base["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#1a1a1a"),
        fontName="Helvetica-Bold",
        leading=10,
    )
    ps_attr_val = ParagraphStyle(
        "ServAttrVal",
        parent=base["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#1a1a1a"),
        fontName="Helvetica",
        leading=10,
    )
    ps_muted = ParagraphStyle(
        "ServMuted",
        parent=base["Normal"],
        fontSize=8.5,
        textColor=colors.HexColor("#666666"),
        fontName="Helvetica-Oblique",
        leading=11,
    )

    tw = max(float(table_width), 120.0)
    content_w = max(tw * 0.98, 1.0)
    img_w, img_h = _image_size_pt(pp, content_w)

    flow: List[Any] = []
    flow.append(Spacer(1, 0.35 * cm))
    flow.append(Paragraph("SERVITUDES", ps_kicker))
    flow.append(
        Paragraph(
            "Servitudes d’utilité publique",
            ps_title,
        )
    )
    flow.append(Spacer(1, 10))
    flow.append(HRFlowable(width="100%", thickness=1, color=c_kerelia_light))
    flow.append(Spacer(1, 10))
    flow.append(Image(str(pp), width=img_w, height=img_h))

    if layer_keys:
        flow.append(Spacer(1, 12))
        flow.append(
            Paragraph(
                xml_escape("Servitudes intersectant l’unité foncière"),
                ps_lbl,
            )
        )
        flow.append(Spacer(1, 4))
        hdr = [
            Paragraph(xml_escape("Servitude"), ps_lbl),
            Paragraph(xml_escape("% surface UF"), ps_lbl),
        ]
        trows: List[List[Any]] = [hdr]
        for table_key in layer_keys:
            nom = display_names.get(table_key, table_key)
            p_raw = pct_map.get(table_key)
            if p_raw is None:
                pct_txt = "—"
            else:
                pct_txt = f"{float(p_raw):.1f} %"
            trows.append(
                [
                    Paragraph(xml_escape(nom), ps_val),
                    Paragraph(xml_escape(pct_txt), ps_val),
                ]
            )
        col_w0 = tw * 0.78
        tbl = Table(trows, colWidths=[col_w0, tw - col_w0])
        tbl.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.5, c_border),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F0F7F4")),
                ]
            )
        )
        flow.append(tbl)

        if inter_by_table:
            flow.append(Spacer(1, 14))
            flow.append(
                Paragraph(
                    xml_escape("Détail par type de servitude (données intersectées)"),
                    ps_lbl,
                )
            )
            flow.append(Spacer(1, 6))
            for table_key in layer_keys:
                ly = inter_by_table.get(table_key)
                if not isinstance(ly, dict):
                    continue
                nom = display_names.get(table_key, table_key)
                p_raw = pct_map.get(table_key)
                pct_suffix = (
                    f" — {float(p_raw):.1f} % de la surface de l’unité foncière"
                    if p_raw is not None
                    else ""
                )
                flow.append(
                    Paragraph(
                        f"<b>{xml_escape(str(nom))}</b>{xml_escape(pct_suffix)}",
                        ps_layer_hdr,
                    )
                )
                flow.extend(
                    _flowables_servitude_layer_attributes(
                        ly,
                        content_w,
                        c_border,
                        ps_attr_key,
                        ps_attr_val,
                        ps_muted,
                    )
                )

    return flow


def generate_servitudes_visuals_from_uf_geometry(
    geometry: dict[str, Any],
    out_dir: str,
    *,
    srid: Optional[int] = None,
    buffer_m: float = BUFFER_SERVITUDES_M,
    dpi: int = 180,
    insee: str = "",
    parcelles_cadastrales: Optional[list[dict[str, Any]]] = None,
    catalogue: Optional[Dict[str, Any]] = None,
    db_schema: Optional[str] = None,
) -> Optional[Tuple[str, List[str], Dict[str, str], Dict[str, float]]]:
    """
    Retourne (chemin PNG, clés des couches intersectant l’UF triées par libellé, display_names,
    pourcentages de surface UF par clé) si au moins une couche servitude intersecte l’UF ; sinon None.
    """
    entries = servitude_catalog_entries(catalogue)
    if not entries:
        return None

    schema = _resolve_servitudes_db_schema(db_schema)
    parcelle_gdf = parcelle_gdf_from_geojson(geometry, srid)
    display_names = {k: v for k, v in entries}

    uf_counts: Dict[str, int] = {}
    geom_cols: Dict[str, str] = {}

    conn = psycopg2.connect(**_db_params())
    try:
        for table_key, _nom in entries:
            gcol = _find_geom_column(conn, table_key, schema)
            if not gcol:
                logger.warning("Servitudes : pas de géométrie pour %s", table_key)
                continue
            geom_cols[table_key] = gcol
    finally:
        conn.close()

    for table_key in geom_cols:
        try:
            uf_counts[table_key] = count_intersections_uf(
                parcelle_gdf, table_key, geom_cols[table_key], schema
            )
        except Exception as exc:
            logger.warning("Servitudes : comptage UF %s : %s", table_key, exc)
            uf_counts[table_key] = 0

    intersecting = {k: n for k, n in uf_counts.items() if n > 0}
    if not intersecting:
        return None

    layer_uf_pct: Dict[str, float] = {}
    for table_key in intersecting:
        try:
            layer_uf_pct[table_key] = compute_servitude_layer_uf_overlap_pct(
                parcelle_gdf,
                table_key,
                geom_cols[table_key],
                schema,
            )
        except Exception as exc:
            logger.warning("Servitudes : pourcentage surface UF %s : %s", table_key, exc)
            layer_uf_pct[table_key] = 0.0

    color_by_layer = _color_map_for_tables([e[0] for e in entries])

    gdfs: List[gpd.GeoDataFrame] = []
    for table_key in intersecting:
        try:
            gdf_one = fetch_servitudes_in_buffer_gdf(
                parcelle_gdf,
                buffer_m,
                table_key,
                geom_cols[table_key],
                schema,
            )
            if not gdf_one.empty:
                gdfs.append(gdf_one)
        except Exception as exc:
            logger.warning("Servitudes : fetch buffer %s : %s", table_key, exc)

    if not gdfs:
        return None

    servitudes_all = gpd.GeoDataFrame(
        pd.concat(gdfs, ignore_index=True),
        crs=gdfs[0].crs,
    )
    if servitudes_all.empty:
        return None

    layers_in_map = sorted(
        servitudes_all["layer_key"].astype(str).unique().tolist(),
        key=lambda x: x.lower(),
    )
    layers_in_legend = [k for k in layers_in_map if k in color_by_layer]

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    sub = out / "servitudes_visuels_assets"
    sub.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256(
        json.dumps(geometry, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]
    map_path = str(sub / f"servitudes_map_{h}.png")

    parcelles_pc_gdf, _detail = fetch_parcelles_uf_for_schema(
        schema,
        insee,
        parcelle_gdf,
        parcelles_cadastrales,
    )

    render_servitudes_combined_png(
        parcelle_gdf,
        servitudes_all,
        color_by_layer,
        display_names,
        layers_in_legend,
        map_path,
        dpi=dpi,
        parcelles_cadastrales_gdf=parcelles_pc_gdf if not parcelles_pc_gdf.empty else None,
    )

    layer_keys_ordered = sorted(
        intersecting.keys(),
        key=lambda k: display_names.get(k, k).lower(),
    )
    return map_path, layer_keys_ordered, display_names, layer_uf_pct


def build_servitudes_flowables_for_report(
    *,
    map_png_path: str,
    layer_keys: List[str],
    display_names: Dict[str, str],
    table_width: float,
    c_kerelia_light: Any,
    c_border: Any,
    layer_uf_overlap_pct: Optional[Dict[str, float]] = None,
    intersection_layers: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Any]:
    return build_servitudes_section_flowables(
        map_png_path,
        table_width=table_width,
        layer_keys=layer_keys,
        display_names=display_names,
        c_kerelia_light=c_kerelia_light,
        c_border=c_border,
        layer_uf_overlap_pct=layer_uf_overlap_pct,
        intersection_layers=intersection_layers,
    )
