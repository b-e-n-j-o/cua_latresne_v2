"""
Section PPRI du rapport PDF (carte satellite + zonage, légende %, laius Markdown).

Ne s'affiche que si l'unité foncière intersecte réellement `latresne.pm1_detaillee_gironde`
(surface d'intersection > 0). Sinon : pas d'image, pas de page.

Les textes réglementaires (`laius_reglement`, Markdown) sont lus dans la même table
`latresne.pm1_detaillee_gironde`, par `codezone`.

Logique d'absorption :
    Une petite zone à contrainte forte, entièrement couverte par le buffer ±2.5 m d'une zone
    moins restrictive (rang hiérarchique ≥), est « absorbée » : elle disparaît de la carte
    finale et est comptabilisée dans la table d'absorption du rapport.
    Seuil surface minimale : 1.0 m² (fragments ignorés).
    Buffer tolérance : ABSORPTION_TOLERANCE_M = 2.5 m (conforme doc PPRI).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import warnings
from dataclasses import dataclass, field
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
    fetch_parcelles_uf,
    parcelle_gdf_from_geojson,
)

matplotlib.use("Agg")
warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
PPRI_GEO_TABLE = "latresne.pm1_detaillee_gironde"
PPRI_REGLEMENT_URL = (
    "https://www.mairie-latresne.fr/wp-content/uploads/2022/07/03_PPR_Latresne_Reglement.pdf"
)
PPRI_ZONAGE_MIN_PCT = 1.0
PPRI_BUFFER_MAP_M = 300.0
PPRI_LAYER_TABLE_KEY = "pm1_detaillee_gironde"

# Paramètres d'absorption (doc PPRI Latresne)
ABSORPTION_TOLERANCE_M: float = 2.5   # buffer autour de chaque zone pour détecter les recouvrements
ABSORPTION_MIN_AREA_M2: float = 1.0   # fragments en dessous ignorés
ABSORPTION_FETCH_BUFFER_M: float = 50.0  # buffer SQL pour charger les zones voisines

# Hiérarchie de contrainte : rang bas = plus restrictif
# Une zone de rang R peut être absorbée par une zone de rang >= R
HIERARCHIE_CONTRAINTE: Dict[str, int] = {
    "Grenat": 1,
    "Rouge foncé": 2,
    "Rouge non urbanisé": 3,
    "Rouge": 4,
    "Rouge centre urbain": 5,
    "Rouge industrialo-portuaire": 6,
    "Rouge urbanisé": 7,
    "Rouge clair": 8,
    "Bleu": 9,
    "Bleu clair": 10,
    "Byzantin": 10,
    "Violette": 10,
    "Jaune": 10,
    "Orange": 10,
    "Marron": 10,
}

# Couleurs par `nom_code`
NOM_CODE_HEX: Dict[Optional[str], str] = {
    None: "#9CA3AF",
    "ROUGE_URBA": "#B91C1C",
    "ROUGE_NON_URBA": "#DC2626",
    "GRENAT": "#7F1D1D",
    "BLEU_CLAIR": "#38BDF8",
    "ROUGE_CENTRE": "#EF4444",
    "BLEUE": "#1D4ED8",
}


# ---------------------------------------------------------------------------
# Dataclass résultat absorption
# ---------------------------------------------------------------------------
@dataclass
class ZoneAbsorbee:
    codezone: str          # code de la zone absorbée
    nom_code: Optional[str]
    surface_m2: float      # surface totale absorbée pour ce codezone
    nb_entites: int        # nombre de fragments absorbés


@dataclass
class AbsorptionResult:
    zones_conservees: gpd.GeoDataFrame   # GDF à afficher sur la carte (EPSG:3857)
    zones_absorbees: List[ZoneAbsorbee] = field(default_factory=list)

    @property
    def has_absorption(self) -> bool:
        return bool(self.zones_absorbees)

    @property
    def nb_total_absorbe(self) -> int:
        return sum(z.nb_entites for z in self.zones_absorbees)


# ---------------------------------------------------------------------------
# Utilitaires internes
# ---------------------------------------------------------------------------
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


def _norm_nom_code(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, float) and str(v) == "nan":
        return None
    s = str(v).strip()
    return s if s and s.lower() not in ("nan", "none", "<na>") else None


def color_for_nom_code(nom_code: Any) -> str:
    return NOM_CODE_HEX.get(_norm_nom_code(nom_code), NOM_CODE_HEX[None])


def _uf_wkt_4326(parcelle_gdf: gpd.GeoDataFrame) -> str:
    return unary_union(parcelle_gdf.geometry).wkt


def _get_niveau_contrainte(codezone: str) -> int:
    """Rang hiérarchique d'une zone (1 = plus restrictif)."""
    if codezone in HIERARCHIE_CONTRAINTE:
        return HIERARCHIE_CONTRAINTE[codezone]
    cz_lower = codezone.lower()
    for k, v in HIERARCHIE_CONTRAINTE.items():
        if k.lower() in cz_lower:
            return v
    return 1  # inconnu → traité comme le plus restrictif (conservé)


def _peut_absorber(zone_absorbante: str, zone_absorbee: str) -> bool:
    """True si zone_absorbante a un rang >= zone_absorbee (moins ou aussi restrictive)."""
    return _get_niveau_contrainte(zone_absorbante) >= _get_niveau_contrainte(zone_absorbee)


# ---------------------------------------------------------------------------
# Chargement brut depuis la DB
# ---------------------------------------------------------------------------
def _fetch_ppri_raw(
    parcelle_gdf: gpd.GeoDataFrame,
    buffer_m: float,
    geo_table: str,
    crs_out: int,
) -> gpd.GeoDataFrame:
    """
    Charge les entités PPRI découpées à l'UF + buffer, dans le CRS demandé.
    Retourne un GDF avec colonnes : codezone, nom_code, geometry.
    """
    wkt = _uf_wkt_4326(parcelle_gdf)
    conn = psycopg2.connect(**_db_params())
    rows: List[dict] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH uf AS (
                    SELECT ST_Transform(ST_GeomFromText(%s, 4326), 2154) AS geom
                ),
                buf AS (
                    SELECT ST_Buffer(uf.geom, %s) AS geom FROM uf
                )
                SELECT
                    z.codezone,
                    z.nom_code,
                    ST_AsGeoJSON(
                        ST_Transform(
                            ST_Intersection(z.geom_2154, buf.geom),
                            {crs_out}
                        )
                    ) AS geom_json
                FROM {geo_table} z, buf
                WHERE ST_Intersects(z.geom_2154, buf.geom)
                ORDER BY z.codezone NULLS LAST;
                """,
                (wkt, buffer_m),
            )
            for row in cur.fetchall():
                cols = [d[0] for d in cur.description]
                d = dict(zip(cols, row))
                gj = d.pop("geom_json", None)
                if not gj:
                    continue
                geom = shape(json.loads(gj))
                if geom.is_empty:
                    continue
                d["geometry"] = geom
                rows.append(d)
    finally:
        conn.close()

    if not rows:
        return gpd.GeoDataFrame(columns=["codezone", "nom_code", "geometry"], crs=f"EPSG:{crs_out}")
    return gpd.GeoDataFrame(rows, crs=f"EPSG:{crs_out}")


# ---------------------------------------------------------------------------
# Logique d'absorption
# ---------------------------------------------------------------------------
def apply_ppri_absorption(
    ppri_raw_2154: gpd.GeoDataFrame,
    uf_geom_2154,
) -> AbsorptionResult:
    """
    Applique la règle d'absorption PPRI sur le GDF brut en EPSG:2154.

    Algorithme (fidèle à analyser_ppri_tolerance) :
      1. Découper chaque entité sur l'UF, ignorer les fragments < ABSORPTION_MIN_AREA_M2.
      2. Éclater les MultiPolygons en Polygons.
      3. Pour chaque entité, construire un buffer ABSORPTION_TOLERANCE_M.
      4. Si une entité est entièrement couverte par les buffers des autres ET qu'au moins
         un des voisins couvrants peut l'absorber (rang >= sien), elle est absorbée.
      5. Sinon, elle est conservée.

    Retourne un AbsorptionResult avec :
      - zones_conservees : GDF en EPSG:3857 (prêt pour la carto matplotlib)
      - zones_absorbees  : liste de ZoneAbsorbee agrégées par codezone
    """
    if ppri_raw_2154.empty:
        return AbsorptionResult(
            zones_conservees=gpd.GeoDataFrame(
                columns=["codezone", "nom_code", "geometry"], crs="EPSG:3857"
            )
        )

    # --- 1. Découper sur l'UF et filtrer les trop petits fragments ---
    ppri = ppri_raw_2154.copy()
    ppri["geometry"] = ppri.geometry.intersection(uf_geom_2154)
    ppri = ppri[ppri.geometry.area >= ABSORPTION_MIN_AREA_M2].copy().reset_index(drop=True)

    if ppri.empty:
        return AbsorptionResult(
            zones_conservees=gpd.GeoDataFrame(
                columns=["codezone", "nom_code", "geometry"], crs="EPSG:3857"
            )
        )

    # --- 2. Éclater les MultiPolygons ---
    exploded: List[dict] = []
    for _, row in ppri.iterrows():
        geom = row.geometry
        parts = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
        for part in parts:
            if part.area >= ABSORPTION_MIN_AREA_M2:
                exploded.append({
                    "codezone": row["codezone"],
                    "nom_code": row.get("nom_code"),
                    "geometry": part,
                })
    if not exploded:
        return AbsorptionResult(
            zones_conservees=gpd.GeoDataFrame(
                columns=["codezone", "nom_code", "geometry"], crs="EPSG:3857"
            )
        )
    ppri = gpd.GeoDataFrame(exploded, crs="EPSG:2154").reset_index(drop=True)

    # --- 3. Buffers de tolérance ---
    buffers = ppri.geometry.buffer(ABSORPTION_TOLERANCE_M).intersection(uf_geom_2154)

    # --- 4. Classification conservé / absorbé ---
    conservees: List[dict] = []
    absorbees_raw: List[dict] = []   # une entrée par fragment absorbé

    for i, row in ppri.iterrows():
        # union des buffers de tous les autres
        other_buffers = [buffers.iloc[j] for j in range(len(ppri)) if j != i]
        if not other_buffers:
            conservees.append(row.to_dict())
            continue

        union_others = unary_union(other_buffers)
        residue = row.geometry.difference(union_others)
        est_couverte = residue.area <= 0.01 or row.geometry.within(union_others)

        if not est_couverte:
            conservees.append(row.to_dict())
            continue

        # Vérifier si au moins un voisin couvrant peut absorber
        absorbeurs_ok = []
        for j, row_other in ppri.iterrows():
            if j == i:
                continue
            overlap = row.geometry.intersection(buffers.iloc[j])
            if overlap.is_empty:
                continue
            overlap_pct = overlap.area / row.geometry.area * 100 if row.geometry.area > 0 else 0
            if overlap_pct > 1.0 and _peut_absorber(row_other["codezone"], row["codezone"]):
                absorbeurs_ok.append(row_other["codezone"])

        if not absorbeurs_ok:
            # Couverte géométriquement mais aucun voisin n'a le rang requis → conservée
            conservees.append(row.to_dict())
        else:
            absorbees_raw.append({
                "codezone": row["codezone"],
                "nom_code": row.get("nom_code"),
                "surface_m2": row.geometry.area,
                "absorbeurs": absorbeurs_ok,
            })

    # --- 5. Construire le GDF conservé en EPSG:3857 ---
    if conservees:
        gdf_cons_2154 = gpd.GeoDataFrame(conservees, crs="EPSG:2154")
        gdf_cons_3857 = gdf_cons_2154.to_crs("EPSG:3857")
    else:
        gdf_cons_3857 = gpd.GeoDataFrame(
            columns=["codezone", "nom_code", "geometry"], crs="EPSG:3857"
        )

    # --- 6. Agréger les absorbées par codezone ---
    absorbed_by_cz: Dict[str, ZoneAbsorbee] = {}
    for entry in absorbees_raw:
        cz = str(entry["codezone"]).strip()
        if cz not in absorbed_by_cz:
            absorbed_by_cz[cz] = ZoneAbsorbee(
                codezone=cz,
                nom_code=_norm_nom_code(entry.get("nom_code")),
                surface_m2=0.0,
                nb_entites=0,
            )
        absorbed_by_cz[cz].surface_m2 += float(entry["surface_m2"])
        absorbed_by_cz[cz].nb_entites += 1

    # Trier du plus restrictif au moins restrictif
    zones_absorbees_sorted = sorted(
        absorbed_by_cz.values(),
        key=lambda z: _get_niveau_contrainte(z.codezone),
    )

    return AbsorptionResult(
        zones_conservees=gdf_cons_3857,
        zones_absorbees=zones_absorbees_sorted,
    )


# ---------------------------------------------------------------------------
# Statistiques % sur les zones conservées
# ---------------------------------------------------------------------------
def compute_ppri_pct_by_codezone(
    parcelle_gdf: gpd.GeoDataFrame,
    geo_table: str = PPRI_GEO_TABLE,
    absorption_result: Optional[AbsorptionResult] = None,
) -> Dict[str, float]:
    """
    % de surface UF par `codezone`.
    Si absorption_result est fourni, les pourcentages sont calculés uniquement
    sur les zones conservées (après absorption) — ce qui est affiché sur la carte.
    Sinon, calcul brut depuis la DB (comportement original).
    """
    uf_2154 = parcelle_gdf.to_crs(epsg=2154)
    uf_geom = unary_union(uf_2154.geometry)
    total_area = uf_geom.area
    if total_area <= 0:
        return {}

    # --- Cas avec absorption : calculer depuis le GDF déjà filtré ---
    if absorption_result is not None and not absorption_result.zones_conservees.empty:
        gdf = absorption_result.zones_conservees.to_crs(epsg=2154)
        stats: Dict[str, float] = {}
        for _, row in gdf.iterrows():
            cz = str(row.get("codezone", "—")).strip() or "—"
            area = row.geometry.intersection(uf_geom).area
            if area > 0:
                stats[cz] = stats.get(cz, 0.0) + area
        return {k: (v / total_area) * 100.0 for k, v in stats.items() if v > 0}

    # --- Cas sans absorption : requête DB directe (comportement original) ---
    wkt = uf_geom.wkt
    conn = psycopg2.connect(**_db_params())
    stats_db: Dict[str, float] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT codezone, SUM(
                    ST_Area(ST_Intersection(z.geom_2154, ST_GeomFromText(%s, 2154)))
                ) AS area_m2
                FROM {geo_table} z
                WHERE ST_Intersects(z.geom_2154, ST_GeomFromText(%s, 2154))
                GROUP BY codezone
                """,
                (wkt, wkt),
            )
            for cz, area in cur.fetchall():
                if area and float(area) > 0:
                    key = str(cz).strip() if cz is not None else "—"
                    stats_db[key] = stats_db.get(key, 0.0) + float(area)
    finally:
        conn.close()
    return {k: (v / total_area) * 100.0 for k, v in stats_db.items() if v > 0}


# ---------------------------------------------------------------------------
# Génération des visuels PPRI (point d'entrée principal)
# ---------------------------------------------------------------------------
def generate_ppri_visuals_from_uf_geometry(
    geometry: dict[str, Any],
    out_dir: str,
    *,
    srid: Optional[int] = None,
    buffer_m: float = PPRI_BUFFER_MAP_M,
    dpi: int = 180,
    insee: str = "",
    parcelles_cadastrales: Optional[list[dict[str, Any]]] = None,
    geo_table: str = PPRI_GEO_TABLE,
) -> Optional[Tuple[str, Dict[str, float], List[dict[str, Any]], AbsorptionResult]]:
    """
    Génère la carte PNG et calcule les stats PPRI avec absorption.

    Retourne (chemin PNG, stats % par codezone, détail parcelles, AbsorptionResult)
    si l'UF intersecte le PPRI ; sinon None.

    Changement vs version précédente :
      - La carte affiche uniquement les zones CONSERVÉES après absorption.
      - AbsorptionResult est retourné pour permettre la génération de la table PDF.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    sub = out / "ppri_visuels_assets"
    sub.mkdir(parents=True, exist_ok=True)

    h = hashlib.sha256(
        json.dumps(geometry, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]
    tag = f"uf_{h}"
    map_path = str(sub / f"ppri_map_{tag}.png")

    parcelle_gdf = parcelle_gdf_from_geojson(geometry, srid)

    # 1. Charger le brut en 2154 (buffer large pour l'affichage carto)
    ppri_raw_2154 = _fetch_ppri_raw(
        parcelle_gdf,
        buffer_m=ABSORPTION_FETCH_BUFFER_M,   # buffer réduit pour l'absorption
        geo_table=geo_table,
        crs_out=2154,
    )

    if ppri_raw_2154.empty:
        return None

    # 2. Appliquer l'absorption → zones conservées + stats absorbées
    uf_2154 = parcelle_gdf.to_crs(epsg=2154)
    uf_geom_2154 = unary_union(uf_2154.geometry)
    absorption = apply_ppri_absorption(ppri_raw_2154, uf_geom_2154)

    if absorption.zones_conservees.empty:
        return None

    # 3. Récupérer les zones pour l'affichage carto (buffer large, EPSG:3857)
    ppri_display_3857 = _fetch_ppri_raw(
        parcelle_gdf,
        buffer_m=buffer_m,
        geo_table=geo_table,
        crs_out=3857,
    )
    # Filtrer pour n'afficher que les codezones conservés
    conserved_codes = set(absorption.zones_conservees["codezone"].unique())
    if not ppri_display_3857.empty and "codezone" in ppri_display_3857.columns:
        ppri_display_3857 = ppri_display_3857[
            ppri_display_3857["codezone"].isin(conserved_codes)
        ].copy()

    # 4. Stats % sur les zones conservées uniquement
    pct_stats = compute_ppri_pct_by_codezone(
        parcelle_gdf, geo_table=geo_table, absorption_result=absorption
    )
    if not pct_stats:
        return None

    color_map = _color_map_for_codezones(ppri_display_3857, pct_stats)

    parcelles_pc_gdf, parcelles_detail = fetch_parcelles_uf(
        insee, parcelle_gdf, parcelles_cadastrales
    )

    render_ppri_combined_png(
        parcelle_gdf,
        ppri_display_3857,
        color_map,
        pct_stats,
        map_path,
        dpi=dpi,
        pct_min_affiche=PPRI_ZONAGE_MIN_PCT,
        parcelles_cadastrales_gdf=parcelles_pc_gdf if not parcelles_pc_gdf.empty else None,
    )

    return map_path, pct_stats, parcelles_detail, absorption


# ---------------------------------------------------------------------------
# Rendu matplotlib
# ---------------------------------------------------------------------------
def _color_map_for_codezones(
    ppri_gdf: gpd.GeoDataFrame, pct_stats: Dict[str, float]
) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for cz in pct_stats:
        ck = str(cz).strip()
        sub = ppri_gdf[ppri_gdf["codezone"].astype(str).str.strip() == ck]
        nc = sub.iloc[0].get("nom_code") if not sub.empty else None
        out[ck] = color_for_nom_code(nc)
    return out


def _populate_ppri_map_axis(
    ax,
    parcelle_gdf: gpd.GeoDataFrame,
    ppri_gdf: gpd.GeoDataFrame,
    color_map: Dict[str, str],
    *,
    parcelles_cadastrales_gdf: Optional[gpd.GeoDataFrame] = None,
) -> None:
    parc_3857 = parcelle_gdf.to_crs(epsg=3857)
    parc_geom = unary_union(parc_3857.geometry)
    minx, miny, maxx, maxy = parc_geom.bounds
    pad = 80.0

    if not ppri_gdf.empty and "codezone" in ppri_gdf.columns:
        for cz in ppri_gdf["codezone"].unique():
            subset = ppri_gdf[ppri_gdf["codezone"] == cz]
            key = str(cz).strip() if cz is not None else "—"
            color = color_map.get(key, "#888888")
            rgba = list(to_rgba(color))
            rgba[3] = 0.40
            try:
                subset.plot(ax=ax, facecolor=rgba, edgecolor=color, linewidth=1.1, zorder=2)
            except Exception:
                pass

    if parcelles_cadastrales_gdf is not None and not parcelles_cadastrales_gdf.empty:
        try:
            ppc = parcelles_cadastrales_gdf.to_crs(epsg=3857)
            if len(ppc) > 1:
                ppc.plot(ax=ax, facecolor="none", edgecolor="#FFE135", linewidth=0.65, zorder=3)
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
                    c.x, c.y, lbl,
                    fontsize=5.2, ha="center", va="center",
                    color="#1a1a1a", fontweight="bold",
                    path_effects=[pe.withStroke(linewidth=1.0, foreground="white")],
                    zorder=6, clip_on=True,
                )
        except Exception:
            pass

    parc_3857.plot(ax=ax, facecolor="none", edgecolor="#FFD600", linewidth=2.5, zorder=4)
    ax.set_xlim(minx - pad, maxx + pad)
    ax.set_ylim(miny - pad, maxy + pad)
    ax.set_aspect("equal", adjustable="box")

    crs_str = parc_3857.crs.to_string()
    try:
        ctx.add_basemap(ax, crs=crs_str, source=ctx.providers.Esri.WorldImagery,
                        zoom="auto", attribution=False, zorder=0)
    except Exception:
        try:
            ctx.add_basemap(ax, crs=crs_str, source=ctx.providers.OpenStreetMap.Mapnik,
                            zoom="auto", attribution=False, zorder=0)
        except Exception:
            pass
    ax.set_axis_off()


def _legend_ppri_panel(
    ax_leg,
    pct_stats: Dict[str, float],
    color_map: Dict[str, str],
    pct_min: float,
) -> None:
    ax_leg.cla()
    ax_leg.axis("off")
    ax_leg.set_xlim(0, 1)
    ax_leg.set_ylim(0, 1)
    filtre = {k: v for k, v in pct_stats.items() if v >= pct_min}
    if not filtre:
        ax_leg.text(0.5, 0.5, f"Aucune zone ≥ {pct_min:g} %",
                    ha="center", va="center", fontsize=8)
        return
    labels = list(filtre.keys())
    legend_labels = [f"{str(l).strip()}  {filtre[l]:.1f} %" for l in labels]
    handles = [
        mpatches.Patch(
            facecolor=color_map.get(str(l).strip(), "#888888"),
            edgecolor="white",
            linewidth=0.75,
        )
        for l in labels
    ]
    ax_leg.legend(
        handles, legend_labels,
        loc="center", fontsize=9, framealpha=0.9,
        edgecolor="#cccccc", facecolor="#fafafa",
        title="Zones PPRI (après absorption)",
        title_fontsize=9,
    )


def render_ppri_combined_png(
    parcelle_gdf: gpd.GeoDataFrame,
    ppri_gdf: gpd.GeoDataFrame,
    color_map: Dict[str, str],
    pct_stats: Dict[str, float],
    out_path: str,
    dpi: int = 180,
    *,
    pct_min_affiche: float = PPRI_ZONAGE_MIN_PCT,
    parcelles_cadastrales_gdf: Optional[gpd.GeoDataFrame] = None,
) -> str:
    side = float(PLU_MAP_SQUARE_SIDE_IN)
    right_ratio = float(PLU_MAP_RIGHT_PANEL_RATIO)
    fig_w = side * (1.0 + right_ratio)
    fig_h = side
    fig = plt.figure(figsize=(fig_w, fig_h), facecolor="white")
    gs = gridspec.GridSpec(
        1, 2, figure=fig,
        width_ratios=[1.0, right_ratio],
        wspace=0.06, left=0.04, right=0.98, bottom=0.07, top=0.97,
    )
    ax_map = fig.add_subplot(gs[0, 0])
    ax_leg = fig.add_subplot(gs[0, 1])
    _populate_ppri_map_axis(
        ax_map, parcelle_gdf, ppri_gdf, color_map,
        parcelles_cadastrales_gdf=parcelles_cadastrales_gdf,
    )
    _legend_ppri_panel(ax_leg, pct_stats, color_map, pct_min_affiche)
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight", facecolor="white", pad_inches=0.12)
    plt.close(fig)
    return out_path


# ---------------------------------------------------------------------------
# Table d'absorption ReportLab
# ---------------------------------------------------------------------------
def build_absorption_table_flowables(
    absorption: AbsorptionResult,
    table_width: float,
    ps: Dict[str, ParagraphStyle],
    c_border: Any,
) -> List[Any]:
    """
    Génère les flowables ReportLab pour la table d'absorption.
    Ne retourne rien si aucune zone n'a été absorbée.
    """
    if not absorption.has_absorption:
        return []

    from reportlab.lib import colors as rl_colors

    flow: List[Any] = []
    tw = float(table_width)

    flow.append(Spacer(1, 10))
    flow.append(HRFlowable(width="100%", thickness=0.5, color=rl_colors.HexColor("#d1d5db")))
    flow.append(Spacer(1, 8))

    nb = absorption.nb_total_absorbe
    tol_m_str = f"{ABSORPTION_TOLERANCE_M:g}".replace(".", ",")
    label_total = f"{nb} zone{'s' if nb > 1 else ''} absorbée{'s' if nb > 1 else ''} " \
                  f"(réglementation de tolérance ±{tol_m_str} m)"
    flow.append(Paragraph(label_total, ps["ppri_absorption_head"]))
    flow.append(Spacer(1, 6))

    # En-tête
    col_w = [tw * 0.42, tw * 0.30, tw * 0.28]
    header_row = [
        Paragraph("Zone PPRI absorbée", ps["ppri_absorption_col_head"]),
        Paragraph("Entités absorbées", ps["ppri_absorption_col_head"]),
        Paragraph("Surface absorbée", ps["ppri_absorption_col_head"]),
    ]

    data_rows = [header_row]
    row_colors = []

    for i, zone in enumerate(absorption.zones_absorbees):
        surface_str = (
            f"{zone.surface_m2:.0f} m²"
            if zone.surface_m2 >= 1.0
            else f"{zone.surface_m2:.2f} m²"
        )
        entites_str = (
            f"{zone.nb_entites} fragment{'s' if zone.nb_entites > 1 else ''}"
        )
        row = [
            Paragraph(xml_escape(zone.codezone), ps["ppri_absorption_cell"]),
            Paragraph(entites_str, ps["ppri_absorption_cell"]),
            Paragraph(surface_str, ps["ppri_absorption_cell"]),
        ]
        data_rows.append(row)
        row_colors.append(("#fef3c7" if i % 2 == 0 else "#fffbeb"))  # alternance légère

    tbl = Table(data_rows, colWidths=col_w)

    style_cmds = [
        # En-tête
        ("BACKGROUND", (0, 0), (-1, 0), rl_colors.HexColor("#1e4d2f")),
        ("TEXTCOLOR", (0, 0), (-1, 0), rl_colors.white),
        # Grille
        ("GRID", (0, 0), (-1, -1), 0.4, c_border),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
    ]
    # Alternance de couleurs sur les lignes de données
    for i, bg in enumerate(row_colors):
        style_cmds.append(("BACKGROUND", (0, i + 1), (-1, i + 1), rl_colors.HexColor(bg)))

    tbl.setStyle(TableStyle(style_cmds))
    flow.append(tbl)
    flow.append(Spacer(1, 4))
    flow.append(
        Paragraph(
            "Les zones absorbées ne sont pas représentées sur la carte. "
            "Seules les zones conservées après application de la règle de tolérance "
            f"(±{tol_m_str} m, seuil résiduel {ABSORPTION_MIN_AREA_M2:g} m²) "
            "sont affichées.",
            ps["ppri_absorption_note"],
        )
    )

    return flow


# ---------------------------------------------------------------------------
# Construction des flowables PDF section PPRI
# ---------------------------------------------------------------------------
def build_ppri_section_flowables(
    ppri_map_png_path: str,
    *,
    table_width: float,
    codezones_display: List[str],
    laius_by_zone: Optional[Dict[str, str]],
    c_kerelia_light: Any,
    c_border: Any,
    c_laius_header_bg: Any,
    absorption: Optional[AbsorptionResult] = None,   # ← nouveau paramètre
) -> List[Any]:
    """Contenu PDF : encart titre, lien règlement, image, table absorption, laius par zone."""
    pp = Path(ppri_map_png_path)
    if not pp.is_file():
        return []

    ps = _ppri_pdf_styles()
    tw = max(float(table_width), 120.0)
    content_w = max(tw * 0.98, 1.0)
    img_w, img_h = _ppri_image_size_pt(pp, content_w)

    flow: List[Any] = []
    flow.append(Spacer(1, 0.35 * cm))
    flow.append(Paragraph("RISQUES — PPRI", ps["ppri_kicker"]))
    flow.append(
        Paragraph("Plan de Prévention des Risques d'Inondation (PPRI)", ps["ppri_title"])
    )
    flow.append(Spacer(1, 6))

    zones_txt = ", ".join(xml_escape(str(z)) for z in codezones_display[:24])
    if len(codezones_display) > 24:
        zones_txt += "…"

    rows_intro = [
        [
            Paragraph(xml_escape("Zone"), ps["ppri_label"]),
            Paragraph(zones_txt or "—", ps["ppri_value"]),
        ],
        [
            Paragraph(xml_escape("Règlement complet"), ps["ppri_label"]),
            Paragraph(
                f'<a href="{xml_escape(PPRI_REGLEMENT_URL)}" color="#1d4ed8">'
                f"<u>Consulter le règlement PPRI (PDF)</u></a>",
                ps["ppri_link"],
            ),
        ],
    ]
    t0 = Table(rows_intro, colWidths=[tw * 0.34, tw * 0.66])
    t0.setStyle(
        TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.5, c_border),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F0F7F4")),
        ])
    )
    flow.append(t0)
    flow.append(Spacer(1, 12))
    flow.append(HRFlowable(width="100%", thickness=1, color=c_kerelia_light))
    flow.append(Spacer(1, 10))

    # Carte (zones conservées uniquement)
    flow.append(Image(str(pp), width=img_w, height=img_h))

    # Table d'absorption (si pertinente)
    if absorption is not None:
        flow.extend(build_absorption_table_flowables(absorption, tw, ps, c_border))

    # Laius par zone
    laius = laius_by_zone or {}
    non_vides = {
        str(k).strip(): str(v).strip()
        for k, v in laius.items()
        if k is not None and str(k).strip() and v is not None and str(v).strip()
    }
    if non_vides:
        flow.append(Spacer(1, 14))
        flow.append(HRFlowable(width="100%", thickness=1, color=c_kerelia_light))
        flow.append(Spacer(1, 10))
        flow.append(Paragraph("Réglementation par zone", ps["ppri_laius_head"]))
        flow.append(Spacer(1, 6))
        bh = c_laius_header_bg
        bc = c_border
        for z_key in sorted(non_vides.keys(), key=lambda x: str(x).lower()):
            inner_w_md = max(float(tw) - 24, 120.0)
            md_flows = _laius_to_flowables(non_vides[z_key], inner_w_md, ps["ppri_laius_body"])
            inner_rows = [[f] for f in md_flows]
            inner_tbl = Table(inner_rows, colWidths=[inner_w_md])
            inner_tbl.setStyle(TableStyle([
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ]))
            zt = xml_escape(str(z_key))
            tbl = Table(
                [
                    [Paragraph(f'<font color="#1e4d2f"><b>Zone {zt}</b> — PPRI</font>',
                               ps["ppri_zone_title"])],
                    [inner_tbl],
                ],
                colWidths=[tw],
            )
            tbl.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), bh),
                ("BACKGROUND", (0, 1), (-1, 1), colors.white),
                ("BOX", (0, 0), (-1, -1), 0.7, bc),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ROUNDEDCORNERS", [3, 3, 3, 3]),
            ]))
            flow.append(tbl)
            flow.append(Spacer(1, 10))

    return flow


# ---------------------------------------------------------------------------
# Point d'entrée haut niveau pour le rapport
# ---------------------------------------------------------------------------
def build_ppri_flowables_for_report(
    *,
    ppri_map_png_path: str,
    pct_stats: Dict[str, float],
    table_width: float,
    c_kerelia_light: Any,
    c_border: Any,
    c_laius_header_bg: Any,
    absorption: Optional[AbsorptionResult] = None,   # ← nouveau paramètre
) -> List[Any]:
    """Laius filtrés par le seuil PPRI_ZONAGE_MIN_PCT (comme PLU)."""
    allowed = {k for k, v in pct_stats.items() if v >= PPRI_ZONAGE_MIN_PCT}
    zones_sorted = sorted(allowed, key=lambda x: str(x).lower())
    laius_map = fetch_ppri_laius_par_codezones(list(allowed)) if allowed else {}
    return build_ppri_section_flowables(
        ppri_map_png_path,
        table_width=table_width,
        codezones_display=zones_sorted,
        laius_by_zone=laius_map or None,
        c_kerelia_light=c_kerelia_light,
        c_border=c_border,
        c_laius_header_bg=c_laius_header_bg,
        absorption=absorption,
    )


# ---------------------------------------------------------------------------
# Laius réglementaires
# ---------------------------------------------------------------------------
def fetch_ppri_laius_par_codezones(
    codezones: List[str],
    geo_table: str = PPRI_GEO_TABLE,
) -> Dict[str, str]:
    """Textes `laius_reglement` (Markdown) par `codezone`."""
    cleaned = sorted({str(z).strip() for z in codezones if z is not None and str(z).strip()})
    if not cleaned:
        return {}
    conn = None
    try:
        conn = psycopg2.connect(**_db_params())
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT ON (codezone)
                    codezone,
                    laius_reglement
                FROM {geo_table}
                WHERE codezone = ANY(%s)
                  AND laius_reglement IS NOT NULL
                  AND TRIM(COALESCE(laius_reglement::text, '')) <> ''
                ORDER BY codezone;
                """,
                (cleaned,),
            )
            rows = cur.fetchall()
    except Exception as exc:
        logger.warning("Laius PPRI (%s.laius_reglement) : %s", geo_table, exc)
        return {}
    finally:
        if conn is not None:
            conn.close()
    out: Dict[str, str] = {}
    for cz, laius in rows:
        if cz is None or laius is None:
            continue
        ks = str(cz).strip()
        txt = str(laius).strip()
        if txt:
            out[ks] = txt
    return out


# ---------------------------------------------------------------------------
# Utilitaires PDF
# ---------------------------------------------------------------------------
def _ppri_image_size_pt(png_path: Path, target_width_pt: float) -> Tuple[float, float]:
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


def _laius_to_flowables(md: str, inner_w: float, body_style: ParagraphStyle) -> List[Any]:
    try:
        from .zonage_markdown_pdf import laius_reglement_to_flowables
        flows = laius_reglement_to_flowables(md, inner_w)
        if flows:
            return flows
    except Exception:
        pass
    raw = (md or "").replace("\r\n", "\n").replace("\r", "\n")
    return [Paragraph(xml_escape(raw).replace("\n", "<br/>"), body_style)]


def _ppri_pdf_styles() -> Dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "ppri_kicker": ParagraphStyle(
            "PpriKicker", parent=base["Normal"],
            fontSize=8, textColor=colors.HexColor("#6b7f72"),
            fontName="Helvetica-Bold", spaceAfter=6, leading=10,
        ),
        "ppri_title": ParagraphStyle(
            "PpriTitle", parent=base["Normal"],
            fontSize=17, textColor=colors.HexColor("#1e4d2f"),
            fontName="Helvetica-Bold", spaceAfter=8, leading=22,
        ),
        "ppri_label": ParagraphStyle(
            "PpriLabel", parent=base["Normal"],
            fontSize=9, textColor=colors.HexColor("#5a5a5a"),
            fontName="Helvetica-Bold", leading=12,
        ),
        "ppri_value": ParagraphStyle(
            "PpriValue", parent=base["Normal"],
            fontSize=9.5, textColor=colors.HexColor("#1a1a1a"),
            fontName="Helvetica", leading=12,
        ),
        "ppri_link": ParagraphStyle(
            "PpriLink", parent=base["Normal"],
            fontSize=9.5, textColor=colors.HexColor("#1d4ed8"),
            fontName="Helvetica", leading=12,
        ),
        "ppri_laius_head": ParagraphStyle(
            "PpriLaiusHead", parent=base["Normal"],
            fontSize=11.5, textColor=colors.HexColor("#1e4d2f"),
            fontName="Helvetica-Bold", spaceAfter=10, leading=15,
        ),
        "ppri_zone_title": ParagraphStyle(
            "PpriZoneTitle", parent=base["Normal"],
            fontSize=9, textColor=colors.HexColor("#1a1a1a"),
            fontName="Helvetica-Bold", leading=12,
        ),
        "ppri_laius_body": ParagraphStyle(
            "PpriLaiusBody", parent=base["Normal"],
            fontSize=8, textColor=colors.HexColor("#2d3748"),
            fontName="Helvetica", leading=11,
        ),
        # Styles table d'absorption
        "ppri_absorption_head": ParagraphStyle(
            "PpriAbsorptionHead", parent=base["Normal"],
            fontSize=10, textColor=colors.HexColor("#92400e"),
            fontName="Helvetica-Bold", leading=14, spaceAfter=4,
        ),
        "ppri_absorption_col_head": ParagraphStyle(
            "PpriAbsorptionColHead", parent=base["Normal"],
            fontSize=8.5, textColor=colors.white,
            fontName="Helvetica-Bold", leading=11,
        ),
        "ppri_absorption_cell": ParagraphStyle(
            "PpriAbsorptionCell", parent=base["Normal"],
            fontSize=8.5, textColor=colors.HexColor("#1a1a1a"),
            fontName="Helvetica", leading=11,
        ),
        "ppri_absorption_note": ParagraphStyle(
            "PpriAbsorptionNote", parent=base["Normal"],
            fontSize=7.5, textColor=colors.HexColor("#6b7280"),
            fontName="Helvetica-Oblique", leading=10, spaceAfter=4,
        ),
    }


# ---------------------------------------------------------------------------
# Utilitaire catalogue
# ---------------------------------------------------------------------------
def uf_intersects_ppri_layer(intersections: List[Dict[str, Any]]) -> bool:
    """True si la couche catalogue pm1_detaillee_gironde a au moins un élément."""
    for ly in intersections:
        if not isinstance(ly, dict):
            continue
        if (ly.get("table") or "").strip() != PPRI_LAYER_TABLE_KEY:
            continue
        if ly.get("elements"):
            return True
    return False