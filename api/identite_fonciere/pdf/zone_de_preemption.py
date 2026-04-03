"""
Page PDF dédiée aux zones de préemption (`latresne.preemption`).

Carte satellite + UF + parcelles + zones (buffer), légende, puis texte(s) de
`laius_reglement` (distincts, une seule occurrence chacun) rendu(s) en Markdown
via `laius_reglement_to_flowables` (titres #/##, **gras**, listes).

Pour généraliser ce schéma à d’autres couches, les paramètres typiques seraient :
  - schéma + nom de table (identifiants SQL sûrs) ;
  - colonne géométrie préférée (ex. geom_3857 / geom_2154) ;
  - buffer cartographique (m) ;
  - titre de section + sous-titre / kicker ;
  - libellé de légende ;
  - couleur unique (ou palette / colonne de style) ;
  - colonne laius Markdown (défaut `laius_reglement`) ;
  - mode de fusion du texte : un bloc par valeur distincte vs concaténation ;
  - condition d’affichage : intersection stricte avec l’UF.

Ce module reste volontairement ciblé préemption ; les constantes en tête de fichier
servent de configuration lisible.
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
    fetch_parcelles_latresne_uf,
    parcelle_gdf_from_geojson,
)

matplotlib.use("Agg")
warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)

# --- Configuration couche (réutilisable comme modèle) ---
PREEMPTION_SCHEMA = "latresne"
PREEMPTION_TABLE = "preemption"
PREEMPTION_BUFFER_M = 300.0
PREEMPTION_MAP_COLOR = "#6D28D9"
PREEMPTION_SECTION_KICKER = "ARTICLE 9 — DROITS DE PRÉEMPTION"
PREEMPTION_SECTION_TITLE = "Zones de préemption"
PREEMPTION_LEGEND_LABEL = "Zones de préemption"
PREEMPTION_LAIUS_REGLEMENT_COLUMN = "laius_reglement"

# Intersection « réelle » avec l’UF : ST_Intersects est vrai au simple contact de bord ;
# on exige une aire > seuil (m², EPSG:2154) et on ne garde que des géométries de dimension 2.
PREEMPTION_MIN_INTERSECTION_AREA_M2 = 0.05
# Part minimale de la surface d’étude recouverte pour afficher la section (aligné PLU/PPRI).
PREEMPTION_UF_MIN_INTERSECTION_PCT = 1.0

CATALOGUE_TABLE_KEY = "preemption"


def _catalogue_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    for rel in (
        ("catalogues", "catalogue_identite_fonciere.json"),
        ("CATALOGUES", "catalogue_identite_fonciere.json"),
    ):
        p = root.joinpath(*rel)
        if p.is_file():
            return p
    return root / "catalogues" / "catalogue_identite_fonciere.json"


def catalogue_display_name(table_key: str = CATALOGUE_TABLE_KEY) -> str:
    try:
        with open(_catalogue_path(), encoding="utf-8") as f:
            cat = json.load(f)
        cfg = cat.get(table_key) or {}
        return str(cfg.get("nom_affiche") or cfg.get("nom") or PREEMPTION_SECTION_TITLE).strip()
    except Exception:
        return PREEMPTION_SECTION_TITLE


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


def _db_schema() -> str:
    return os.getenv("IDENTITE_FONCIERE_DB_SCHEMA", PREEMPTION_SCHEMA).strip() or PREEMPTION_SCHEMA


def _fq(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def _column_exists(conn, schema: str, table: str, col: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            LIMIT 1
            """,
            (schema, table, col),
        )
        return cur.fetchone() is not None


def pick_preemption_geom_column(conn, schema: str, table: str) -> Optional[str]:
    """Préfère `geom_3857` (index GIST courant), sinon `geom_2154` / `geom`."""
    if not re.match(r"^[a-z_][a-z0-9_]*$", table):
        return None
    for c in ("geom_3857", "geom_2154", "geom"):
        if _column_exists(conn, schema, table, c):
            return c
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND udt_name = 'geometry'
            LIMIT 1
            """,
            (schema, table),
        )
        row = cur.fetchone()
        return row[0] if row else None


def compute_preemption_uf_overlap_pct(
    parcelle_gdf: gpd.GeoDataFrame,
    geom_col: str,
    schema: str,
    table: str,
    *,
    min_area_m2: float = PREEMPTION_MIN_INTERSECTION_AREA_M2,
) -> float:
    """
    Pourcentage de la surface de l’UF (2154) couverte par l’union des intersections
    surfaciques avec la couche préemption. Les seuls contacts linéaires / ponctuels
    (aire nulle ou dimension ≠ 2) sont exclus.
    """
    fq = _fq(schema, table)
    wkt_2154 = unary_union(parcelle_gdf.to_crs(epsg=2154).geometry).wkt

    if geom_col == "geom_3857":
        g2154 = "ST_Transform(t.geom_3857, 2154)"
        geom_ok = "t.geom_3857 IS NOT NULL"
    else:
        g2154 = f"t.{geom_col}"
        geom_ok = f"t.{geom_col} IS NOT NULL"

    sql = f"""
        WITH uf AS (SELECT ST_GeomFromText(%s, 2154) AS g),
        au AS (SELECT ST_Area(g)::float8 AS a FROM uf),
        ints AS (
            SELECT ST_Intersection({g2154}, uf.g) AS ig
            FROM {fq} t, uf
            WHERE {geom_ok}
              AND ST_Intersects({g2154}, uf.g)
        ),
        surf AS (
            SELECT ig FROM ints
            WHERE ig IS NOT NULL
              AND NOT ST_IsEmpty(ig)
              AND ST_Dimension(ig) = 2
              AND ST_Area(ig) > %s
        )
        SELECT
            CASE
                WHEN COALESCE((SELECT a FROM au), 0) <= 0 THEN 0::float8
                ELSE LEAST(
                    100.0,
                    COALESCE(
                        (SELECT ST_Area(ST_UnaryUnion(ST_Collect(ig)))::float8 FROM surf),
                        0
                    ) / (SELECT a FROM au) * 100.0
                )
            END AS pct
    """
    conn = psycopg2.connect(**_db_params())
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (wkt_2154, min_area_m2))
            row = cur.fetchone()
            if not row or row[0] is None:
                return 0.0
            return float(row[0])
    finally:
        conn.close()


def fetch_preemption_geoms_buffer_3857(
    parcelle_gdf: gpd.GeoDataFrame,
    buffer_m: float,
    geom_col: str,
    schema: str,
    table: str,
) -> gpd.GeoDataFrame:
    fq = _fq(schema, table)
    wkt_4326 = unary_union(parcelle_gdf.geometry).wkt

    if geom_col == "geom_3857":
        sql = f"""
            WITH uf AS (
                SELECT ST_Transform(ST_GeomFromText(%s, 4326), 3857) AS g
            ),
            buf AS (SELECT ST_Buffer(uf.g, %s) AS g FROM uf)
            SELECT ST_AsGeoJSON(ST_Intersection(t.geom_3857, buf.g)) AS geom_json
            FROM {fq} t, buf
            WHERE t.geom_3857 IS NOT NULL
              AND ST_Intersects(t.geom_3857, buf.g)
        """
        params: Tuple[Any, ...] = (wkt_4326, buffer_m)
    else:
        sql = f"""
            WITH uf AS (
                SELECT ST_Transform(ST_GeomFromText(%s, 4326), 2154) AS geom
            ),
            buf AS (SELECT ST_Buffer(uf.geom, %s) AS geom FROM uf)
            SELECT ST_AsGeoJSON(
                ST_Transform(ST_Intersection(t.{geom_col}, buf.geom), 3857)
            ) AS geom_json
            FROM {fq} t, buf
            WHERE t.{geom_col} IS NOT NULL
              AND ST_Intersects(t.{geom_col}, buf.geom)
        """
        params = (wkt_4326, buffer_m)

    conn = psycopg2.connect(**_db_params())
    rows: List[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
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
        recs.append({"geometry": geom})
    if not recs:
        return gpd.GeoDataFrame(columns=["geometry"], crs="EPSG:3857")
    return gpd.GeoDataFrame(recs, crs="EPSG:3857")


def fetch_distinct_reglementation_uf(
    parcelle_gdf: gpd.GeoDataFrame,
    geom_col: str,
    schema: str,
    table: str,
    reg_col: str = PREEMPTION_LAIUS_REGLEMENT_COLUMN,
    *,
    min_area_m2: float = PREEMPTION_MIN_INTERSECTION_AREA_M2,
) -> List[str]:
    if not re.match(r"^[a-z_][a-z0-9_]*$", reg_col):
        return []
    fq = _fq(schema, table)
    wkt_2154 = unary_union(parcelle_gdf.to_crs(epsg=2154).geometry).wkt

    if geom_col == "geom_3857":
        g2154 = "ST_Transform(t.geom_3857, 2154)"
        geom_ok = "t.geom_3857 IS NOT NULL"
    else:
        g2154 = f"t.{geom_col}"
        geom_ok = f"t.{geom_col} IS NOT NULL"

    sql = f"""
        WITH uf AS (SELECT ST_GeomFromText(%s, 2154) AS g)
        SELECT DISTINCT t.{reg_col}
        FROM {fq} t, uf
        WHERE {geom_ok}
          AND ST_Intersects({g2154}, uf.g)
          AND t.{reg_col} IS NOT NULL
          AND TRIM(COALESCE(t.{reg_col}::text, '')) <> ''
          AND ST_Dimension(ST_Intersection({g2154}, uf.g)) = 2
          AND ST_Area(ST_Intersection({g2154}, uf.g)) > %s
        ORDER BY 1
    """
    conn = psycopg2.connect(**_db_params())
    out: List[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (wkt_2154, min_area_m2))
            for (txt,) in cur.fetchall():
                if txt is None:
                    continue
                s = str(txt).strip()
                if s:
                    out.append(s)
    finally:
        conn.close()
    return out


def _plot_zones(ax, zones_gdf: gpd.GeoDataFrame, color: str) -> None:
    if zones_gdf.empty:
        return
    rgba = list(to_rgba(color))
    rgba[3] = 0.38
    for gt in zones_gdf.geometry.geom_type.unique():
        part = zones_gdf.loc[zones_gdf.geometry.geom_type == gt]
        try:
            if gt in ("Polygon", "MultiPolygon"):
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
                part.plot(ax=ax, color=color, markersize=20, zorder=2, alpha=0.9)
        except Exception:
            pass


def _populate_preemption_map_axis(
    ax,
    parcelle_gdf: gpd.GeoDataFrame,
    zones_gdf: gpd.GeoDataFrame,
    map_color: str,
    *,
    parcelles_cadastrales_gdf: Optional[gpd.GeoDataFrame] = None,
) -> None:
    _plot_zones(ax, zones_gdf, map_color)

    parc_3857 = parcelle_gdf.to_crs(epsg=3857)
    parc_geom = unary_union(parc_3857.geometry)
    minx, miny, maxx, maxy = parc_geom.bounds
    pad = 80.0

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


def _format_pct_fr(pct: float, decimals: int = 1) -> str:
    return f"{pct:.{decimals}f}".replace(".", ",")


def _legend_single_panel(ax_leg, label: str, color: str) -> None:
    ax_leg.cla()
    ax_leg.axis("off")
    ax_leg.set_xlim(0, 1)
    ax_leg.set_ylim(0, 1)
    h = [
        mpatches.Patch(
            facecolor=color,
            edgecolor="white",
            linewidth=0.75,
        )
    ]
    ax_leg.legend(
        h,
        [label[:72]],
        loc="center",
        fontsize=9,
        framealpha=0.92,
        edgecolor="#cccccc",
        facecolor="#fafafa",
        title="Légende",
        title_fontsize=9,
    )


def render_preemption_combined_png(
    parcelle_gdf: gpd.GeoDataFrame,
    zones_gdf: gpd.GeoDataFrame,
    legend_label: str,
    map_color: str,
    out_path: str,
    dpi: int = 180,
    parcelles_cadastrales_gdf: Optional[gpd.GeoDataFrame] = None,
) -> str:
    side = float(PLU_MAP_SQUARE_SIDE_IN)
    right_ratio = float(PLU_MAP_RIGHT_PANEL_RATIO)
    fig = plt.figure(figsize=(side * (1.0 + right_ratio), side), facecolor="white")
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
    _populate_preemption_map_axis(
        ax_map,
        parcelle_gdf,
        zones_gdf,
        map_color,
        parcelles_cadastrales_gdf=parcelles_cadastrales_gdf,
    )
    _legend_single_panel(ax_leg, legend_label, map_color)
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


def _reglementation_to_flowables(md: str, inner_w: float, body_style: ParagraphStyle) -> List[Any]:
    try:
        from .zonage_markdown_pdf import laius_reglement_to_flowables

        flows = laius_reglement_to_flowables(md, inner_w)
        if flows:
            return flows
    except Exception:
        pass
    raw = (md or "").replace("\r\n", "\n").replace("\r", "\n")
    return [Paragraph(xml_escape(raw).replace("\n", "<br/>"), body_style)]


def build_preemption_section_flowables(
    map_png_path: str,
    *,
    table_width: float,
    reglementation_texts: List[str],
    section_title: str,
    section_kicker: str,
    legend_label: str,
    overlap_pct: Optional[float],
    c_kerelia_light: Any,
    c_border: Any,
    c_laius_bg: Any,
) -> List[Any]:
    pp = Path(map_png_path)
    if not pp.is_file():
        return []

    base = getSampleStyleSheet()
    ps_kicker = ParagraphStyle(
        "PreemKicker",
        parent=base["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#6b7f72"),
        fontName="Helvetica-Bold",
        spaceAfter=6,
        leading=10,
    )
    ps_title = ParagraphStyle(
        "PreemTitle",
        parent=base["Normal"],
        fontSize=17,
        textColor=colors.HexColor("#1e4d2f"),
        fontName="Helvetica-Bold",
        spaceAfter=8,
        leading=22,
    )
    ps_sub = ParagraphStyle(
        "PreemSub",
        parent=base["Normal"],
        fontSize=9.5,
        textColor=colors.HexColor("#4a5568"),
        fontName="Helvetica",
        spaceAfter=6,
        leading=13,
    )
    ps_reg_head = ParagraphStyle(
        "PreemRegHead",
        parent=base["Normal"],
        fontSize=11.5,
        textColor=colors.HexColor("#1e4d2f"),
        fontName="Helvetica-Bold",
        spaceAfter=8,
        leading=15,
    )
    ps_body = ParagraphStyle(
        "PreemBody",
        parent=base["Normal"],
        fontSize=8,
        textColor=colors.HexColor("#2d3748"),
        fontName="Helvetica",
        leading=11,
    )

    tw = max(float(table_width), 120.0)
    content_w = max(tw * 0.98, 1.0)
    img_w, img_h = _image_size_pt(pp, content_w)
    inner_w_md = max(tw - 24, 120.0)

    flow: List[Any] = []
    flow.append(Spacer(1, 0.35 * cm))
    flow.append(Paragraph(xml_escape(section_kicker), ps_kicker))
    flow.append(Paragraph(xml_escape(section_title), ps_title))
    if overlap_pct is not None:
        flow.append(
            Paragraph(
                xml_escape(
                    "Part de la surface d’étude recouverte par les zones de préemption "
                    f"(intersection surfacique) : {_format_pct_fr(overlap_pct)} %"
                ),
                ps_sub,
            )
        )
    flow.append(Spacer(1, 8))
    flow.append(HRFlowable(width="100%", thickness=1, color=c_kerelia_light))
    flow.append(Spacer(1, 10))
    flow.append(Image(str(pp), width=img_w, height=img_h))

    if reglementation_texts:
        flow.append(Spacer(1, 14))
        flow.append(HRFlowable(width="100%", thickness=1, color=c_kerelia_light))
        flow.append(Spacer(1, 10))
        flow.append(
            Paragraph(xml_escape("Réglementation (extrait)"), ps_reg_head),
        )
        flow.append(Spacer(1, 6))
        bh = c_laius_bg
        bc = c_border
        for i, raw_txt in enumerate(reglementation_texts):
            md_flows = _reglementation_to_flowables(raw_txt, inner_w_md, ps_body)
            inner_rows = [[f] for f in md_flows]
            inner_tbl = Table(inner_rows, colWidths=[inner_w_md])
            inner_tbl.setStyle(
                TableStyle(
                    [
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 0),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                        ("TOPPADDING", (0, 0), (-1, -1), 0),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                    ]
                )
            )
            title_cell = (
                Paragraph(
                    f'<font color="#1e4d2f"><b>Texte {i + 1}</b></font>',
                    ps_body,
                )
                if len(reglementation_texts) > 1
                else Paragraph(
                    f'<font color="#1e4d2f"><b>{xml_escape(legend_label)}</b></font>',
                    ps_body,
                )
            )
            tbl = Table([[title_cell], [inner_tbl]], colWidths=[tw])
            tbl.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), bh),
                        ("BACKGROUND", (0, 1), (-1, 1), colors.white),
                        ("BOX", (0, 0), (-1, -1), 0.7, bc),
                        ("TOPPADDING", (0, 0), (-1, -1), 8),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                        ("LEFTPADDING", (0, 0), (-1, -1), 10),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("ROUNDEDCORNERS", [3, 3, 3, 3]),
                    ]
                )
            )
            flow.append(tbl)
            flow.append(Spacer(1, 10))

    return flow


def generate_preemption_visuals_from_uf_geometry(
    geometry: dict[str, Any],
    out_dir: str,
    *,
    srid: Optional[int] = None,
    buffer_m: float = PREEMPTION_BUFFER_M,
    dpi: int = 180,
    insee: str = "",
    parcelles_cadastrales: Optional[list[dict[str, Any]]] = None,
) -> Optional[Tuple[str, List[str], float]]:
    """
    Retourne (chemin PNG, textes `laius_reglement` distincts, % surface UF couverte)
    si la part surfacique d’intersection ≥ ``PREEMPTION_UF_MIN_INTERSECTION_PCT`` ;
    sinon None (simple contact de limite exclu).
    """
    schema = _db_schema()
    table = PREEMPTION_TABLE
    if not re.match(r"^[a-z_][a-z0-9_]*$", table):
        return None

    conn = psycopg2.connect(**_db_params())
    try:
        geom_col = pick_preemption_geom_column(conn, schema, table)
    finally:
        conn.close()

    if not geom_col:
        logger.warning("Préemption : aucune colonne géométrique pour %s.%s", schema, table)
        return None

    parcelle_gdf = parcelle_gdf_from_geojson(geometry, srid)
    overlap_pct = compute_preemption_uf_overlap_pct(
        parcelle_gdf, geom_col, schema, table
    )
    if overlap_pct < PREEMPTION_UF_MIN_INTERSECTION_PCT:
        return None

    zones_gdf = fetch_preemption_geoms_buffer_3857(
        parcelle_gdf, buffer_m, geom_col, schema, table
    )
    if zones_gdf.empty:
        return None

    reg_texts = fetch_distinct_reglementation_uf(
        parcelle_gdf, geom_col, schema, table, PREEMPTION_LAIUS_REGLEMENT_COLUMN
    )

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    sub = out / "preemption_visuels_assets"
    sub.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256(
        json.dumps(geometry, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]
    map_path = str(sub / f"preemption_map_{h}.png")

    cat_name = catalogue_display_name(CATALOGUE_TABLE_KEY)
    legend_label = f"{cat_name} — {_format_pct_fr(overlap_pct)} %"
    parcelles_pc_gdf, _ = fetch_parcelles_latresne_uf(
        insee,
        parcelle_gdf,
        parcelles_cadastrales,
    )

    render_preemption_combined_png(
        parcelle_gdf,
        zones_gdf,
        legend_label,
        PREEMPTION_MAP_COLOR,
        map_path,
        dpi=dpi,
        parcelles_cadastrales_gdf=parcelles_pc_gdf if not parcelles_pc_gdf.empty else None,
    )
    return map_path, reg_texts, overlap_pct


def build_preemption_flowables_for_report(
    *,
    map_png_path: str,
    reglementation_texts: List[str],
    overlap_pct: float,
    table_width: float,
    c_kerelia_light: Any,
    c_border: Any,
    c_laius_header_bg: Any,
) -> List[Any]:
    title = catalogue_display_name(CATALOGUE_TABLE_KEY)
    legend_label = f"{title} — {_format_pct_fr(overlap_pct)} %"
    return build_preemption_section_flowables(
        map_png_path,
        table_width=table_width,
        reglementation_texts=reglementation_texts,
        section_title=title,
        section_kicker=PREEMPTION_SECTION_KICKER,
        legend_label=legend_label,
        overlap_pct=overlap_pct,
        c_kerelia_light=c_kerelia_light,
        c_border=c_border,
        c_laius_bg=c_laius_header_bg,
    )


def is_preemption_catalog_layer(table_key: Optional[str]) -> bool:
    return (table_key or "").strip() == CATALOGUE_TABLE_KEY
