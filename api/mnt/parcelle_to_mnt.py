#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
parcelle_topo_3d.py
-------------------
Génère une visualisation 3D Plotly de la topographie (MNT)
d'une parcelle cadastrale donnée par :
- code INSEE
- section
- numéro

Pipeline :
1) Géométrie parcellaire depuis Supabase (latresne.parcelles_latresne, Lambert 93)
2) Parcelles contiguës (ST_Touches) pour élargir l'emprise terrain
3) Sélection des dalles MNT dans Supabase (ST_Intersects sur l'union)
4) Téléchargement depuis Supabase Storage, merge + clip sur l'union
5) Export Plotly : surface contexte + contour 3D de la parcelle cible
"""

import os
import tempfile
import requests
import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask
from rasterio.transform import rowcol
from shapely.geometry import mapping, Polygon, MultiPolygon
from shapely.ops import unary_union
from shapely import wkt as shapely_wkt
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import plotly.graph_objects as go
import logging

# ============================================================
# CONFIGURATION ENV
# ============================================================

load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

DB_ENGINE = create_engine(
    f"postgresql+psycopg2://{os.getenv('SUPABASE_USER')}:"
    f"{os.getenv('SUPABASE_PASSWORD')}@"
    f"{os.getenv('SUPABASE_HOST')}:"
    f"{os.getenv('SUPABASE_PORT', '5432')}/"
    f"{os.getenv('SUPABASE_DB')}",
    connect_args={"sslmode": "require"},
    pool_pre_ping=True
)

SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# ============================================================
# FONCTIONS
# ============================================================

def _largest_polygon(geom):
    """Retourne le polygone principal (surface cadastre en général)."""
    if geom is None or geom.is_empty:
        raise ValueError("Géométrie vide")
    if geom.geom_type == "Polygon":
        return geom
    if geom.geom_type == "MultiPolygon":
        return max(geom.geoms, key=lambda p: p.area)
    raise ValueError(f"Type géométrique inattendu : {geom.geom_type}")


def fetch_parcelle_geometry(code_insee, section, numero):
    """
    Récupère la géométrie Lambert 93 de la parcelle cible depuis
    latresne.parcelles_latresne (Supabase).
    """
    section_n = (section or "").strip().upper()
    numero_n = (numero or "").strip()
    code_insee_n = (code_insee or "").strip()

    logger.info(
        "Récupération parcelle (Supabase latresne.parcelles_latresne) : "
        f"{code_insee_n} / {section_n} / {numero_n}"
    )

    sql = """
    SELECT ST_AsText(ST_MakeValid(geom_2154)) AS wkt, idu
    FROM latresne.parcelles_latresne
    WHERE code_insee = :code_insee
      AND upper(trim(section)) = :section
      AND trim(numero) = :numero
    LIMIT 1;
    """

    with DB_ENGINE.connect() as conn:
        row = conn.execute(
            text(sql),
            {
                "code_insee": code_insee_n,
                "section": section_n,
                "numero": numero_n,
            },
        ).mappings().first()

    if not row or not row["wkt"]:
        raise ValueError(
            "Parcelle introuvable en base (latresne.parcelles_latresne) pour cette référence"
        )

    geom = shapely_wkt.loads(row["wkt"])
    geom = _largest_polygon(geom)
    logger.info(
        "Géométrie cible : surface = %.2f m² (idu=%s)",
        geom.area,
        row.get("idu") or "—",
    )
    return geom


def fetch_parcelles_contigues(geom_cible, code_insee: str):
    """
    Parcelles qui partagent un côté avec la cible (ST_Touches), même commune INSEE.
    Retourne (liste de Polygons pour l'union MNT, nombre de parcelles cadastrales voisines).
    """
    code_insee_n = (code_insee or "").strip()
    wkt = geom_cible.wkt

    sql = """
    SELECT ST_AsText(ST_MakeValid(geom_2154)) AS wkt, section, numero, idu
    FROM latresne.parcelles_latresne
    WHERE code_insee = :code_insee
      AND ST_Touches(
        geom_2154,
        ST_SetSRID(ST_GeomFromText(:wkt_cible, 2154), 2154)
      );
    """

    with DB_ENGINE.connect() as conn:
        rows = conn.execute(
            text(sql),
            {"code_insee": code_insee_n, "wkt_cible": wkt},
        ).mappings().all()

    out = []
    labels = []
    for r in rows:
        if not r["wkt"]:
            continue
        labels.append(f"{(r.get('section') or '').strip()}{(r.get('numero') or '').strip()}")
        g = shapely_wkt.loads(r["wkt"])
        if g.geom_type == "Polygon":
            out.append(g)
        elif g.geom_type == "MultiPolygon":
            out.extend(list(g.geoms))
        else:
            logger.warning("Voisin ignoré (type %s, idu=%s)", g.geom_type, r.get("idu"))

    n_parcelles = len(rows)
    preview = ", ".join(labels[:12])
    if len(labels) > 12:
        preview += f", … (+{len(labels) - 12})"
    logger.info("%d parcelle(s) contiguë(s) : %s", n_parcelles, preview or "—")

    return out, n_parcelles


def build_emprise_mnt(geom_cible, voisins: list) -> object:
    """Union de la parcelle cible et des voisins pour le MNT (terrain environnant)."""
    parts = [geom_cible] + list(voisins)
    u = unary_union(parts)
    logger.info(
        "Emprise MNT : %d polygone(s) fusionnés, aire totale ≈ %.2f m²",
        len(parts),
        u.area,
    )
    return u


def fetch_mnt_from_geometry(geometry):
    """Récupère et clippe le MNT Supabase à partir d'une géométrie."""
    logger.info("Recherche des dalles MNT dans Supabase...")
    sql = """
    SELECT nom_fichier, storage_url
    FROM public.mnt_dalles
    WHERE ST_Intersects(
        emprise,
        ST_GeomFromText(:geom, 2154)
    )
    ORDER BY nom_fichier;
    """

    with DB_ENGINE.connect() as conn:
        rows = conn.execute(text(sql), {"geom": geometry.wkt})
        dalles = [dict(r._mapping) for r in rows]

    if not dalles:
        raise ValueError("Aucune dalle MNT ne couvre cette parcelle")

    logger.info(f"{len(dalles)} dalle(s) MNT trouvée(s) : {[d['nom_fichier'] for d in dalles]}")

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }

    with tempfile.TemporaryDirectory() as tmp:
        logger.info(f"Téléchargement des dalles MNT dans {tmp}...")
        paths = []
        for d in dalles:
            logger.info(f"Téléchargement : {d['nom_fichier']}")
            r = requests.get(d["storage_url"], headers=headers)
            r.raise_for_status()
            p = os.path.join(tmp, d["nom_fichier"])
            with open(p, "wb") as f:
                f.write(r.content)
            paths.append(p)
            logger.info(f"✓ {d['nom_fichier']} téléchargé ({len(r.content) / 1024 / 1024:.2f} Mo)")

        logger.info("Ouverture des rasters...")
        srcs = [rasterio.open(p) for p in paths]

        if len(srcs) > 1:
            logger.info(f"Fusion de {len(srcs)} rasters en mosaïque...")
            mosaic, transform = merge(srcs)
            logger.info(f"Mosaïque créée : shape = {mosaic.shape}")
            src = rasterio.io.MemoryFile().open(
                driver="GTiff",
                height=mosaic.shape[1],
                width=mosaic.shape[2],
                count=1,
                dtype=mosaic.dtype,
                crs=srcs[0].crs,
                transform=transform
            )
            src.write(mosaic)
        else:
            logger.info("Une seule dalle, pas de fusion nécessaire")
            src = srcs[0]

        logger.info("Clippage du MNT selon l'emprise (cible + voisins éventuels)...")
        out, transform = mask(
            src,
            [mapping(geometry)],
            crop=True,
            all_touched=True
        )

        data = out[0]
        if src.nodata is not None:
            data = np.where(data == src.nodata, np.nan, data)

        resolution = src.res[0]
        logger.info(f"MNT clippé : shape = {data.shape}, résolution = {resolution:.2f} m")
        logger.info(f"Altitude min = {np.nanmin(data):.2f} m, max = {np.nanmax(data):.2f} m")
        src.close()

    return data, transform, resolution


def _densify_ring_xy(poly: Polygon, step_m: float) -> list[tuple[float, float]]:
    """Points le long du pourtour extérieur, espacés d'environ step_m."""
    ring = poly.exterior
    length = ring.length
    if length <= 0:
        return []
    pts = []
    d = 0.0
    while d < length:
        p = ring.interpolate(d)
        pts.append((float(p.x), float(p.y)))
        d += step_m
    c0 = ring.coords[0]
    pts.append((float(c0[0]), float(c0[1])))
    return pts


def _sample_z_dem(
    dem: np.ndarray,
    transform,
    x: float,
    y: float,
) -> float | None:
    """Lit Z sur la grille MNT ; fenêtre 3×3 si nodata."""
    r, c = rowcol(transform, x, y)
    h, w = dem.shape
    if 0 <= r < h and 0 <= c < w:
        z = dem[r, c]
        if z == z and not np.isnan(z):
            return float(z)

    vals = []
    for dr in (-1, 0, 1):
        for dc in (-1, 0, 1):
            rr, cc = r + dr, c + dc
            if 0 <= rr < h and 0 <= cc < w:
                z = dem[rr, cc]
                if z == z and not np.isnan(z):
                    vals.append(float(z))
    if vals:
        return float(np.median(vals))
    return None


def _boundary_xyz(
    geometry_target,
    dem: np.ndarray,
    transform,
    resolution: float,
    exaggeration: float,
) -> tuple[list[float], list[float], list[float]] | None:
    """Polyline 3D suivant le bord de la parcelle cible, Z = MNT."""
    try:
        poly = _largest_polygon(geometry_target)
    except ValueError:
        return None
    if not isinstance(poly, Polygon):
        return None

    step_m = max(float(resolution) * 2.0, 2.0)
    xy = _densify_ring_xy(poly, step_m)
    if len(xy) < 2:
        return None

    xs, ys, zs = [], [], []
    for x, y in xy:
        z = _sample_z_dem(dem, transform, x, y)
        if z is None:
            continue
        xs.append(x)
        ys.append(y)
        zs.append(z * exaggeration)

    if len(xs) < 2:
        return None
    return xs, ys, zs


def export_plotly_3d(
    geometry_target,
    mnt,
    transform,
    resolution,
    code_insee,
    section,
    numero,
    output_dir="./out_3d",
    exaggeration=1.5,
    n_voisins: int = 0,
):

    logger.info("Génération de la visualisation 3D...")
    logger.info(f"Répertoire de sortie : {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    rows, cols = mnt.shape
    logger.info(f"Dimensions MNT : {rows} x {cols}")
    x = np.arange(cols) * resolution + transform[2]
    y = np.arange(rows) * transform[4] + transform[5]
    X, Y = np.meshgrid(x, y)
    Z = mnt * exaggeration
    logger.info(f"Exagération verticale : {exaggeration}x")

    step = max(1, min(rows, cols) // 200)
    logger.info(f"Échantillonnage pour visualisation : step = {step}")
    Xs, Ys, Zs = X[::step, ::step], Y[::step, ::step], Z[::step, ::step]
    logger.info(f"Points de surface : {Xs.shape}")

    titre = f"Topographie 3D – {section} {numero} (cible)"
    if n_voisins > 0:
        titre += f" + {n_voisins} parcelle(s) voisine(s)"

    logger.info("Création de la figure Plotly...")
    fig = go.Figure(
        go.Surface(
            x=Xs,
            y=Ys,
            z=Zs,
            colorscale="Earth",
            showscale=True,
            name="MNT",
        )
    )

    bxyz = _boundary_xyz(geometry_target, mnt, transform, resolution, exaggeration)
    if bxyz:
        bx, by, bz = bxyz
        fig.add_trace(
            go.Scatter3d(
                x=bx,
                y=by,
                z=bz,
                mode="lines",
                line=dict(color="#ffea00", width=10),
                name="Limite parcelle cible",
                showlegend=True,
            )
        )
        logger.info("Contour 3D parcelle cible : %d points", len(bx))
    else:
        logger.warning("Contour parcelle cible non tracé (échantillonnage vide ou hors grille)")

    z_title = "Altitude (m)"
    if exaggeration != 1.0:
        z_title += f" × {exaggeration}"

    fig.update_layout(
        title=titre,
        scene=dict(
            xaxis_title="X Lambert 93 (m)",
            yaxis_title="Y Lambert 93 (m)",
            zaxis_title=z_title,
            aspectmode="data",
        ),
        margin=dict(l=0, r=0, t=48, b=0),
    )

    filename = f"parcelle_3d_{code_insee}_{section}{numero}.html"
    path = os.path.join(output_dir, filename)
    logger.info(f"Export HTML : {path}")

    fig.write_html(path, include_plotlyjs=True, full_html=True)
    logger.info("✓ Fichier HTML généré avec succès")

    result = {
        "path": path,
        "surface_m2": float(geometry_target.area),
        "resolution_m": resolution,
        "n_voisins": int(n_voisins),
    }
    logger.info(
        "Résultat : surface cible = %.2f m², résolution = %.2f m, voisins = %d",
        result["surface_m2"],
        result["resolution_m"],
        result["n_voisins"],
    )
    return result

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    import json

    # Référence parcellaire en dur : Latresne Ac 0042
    CODE_INSEE = "33234"  # Latresne (Gironde)
    SECTION = "AE"
    NUMERO = "0364"
    OUTPUT_DIR = "./out_3d"
    EXAGGERATION = 1.5

    logger.info("=" * 60)
    logger.info("Génération visualisation 3D - Parcelle Latresne AC 0042")
    logger.info("=" * 60)
    logger.info(f"Code INSEE : {CODE_INSEE}")
    logger.info(f"Section : {SECTION}")
    logger.info(f"Numéro : {NUMERO}")
    logger.info(f"Répertoire de sortie : {OUTPUT_DIR}")
    logger.info(f"Exagération verticale : {EXAGGERATION}x")
    logger.info("")

    try:
        logger.info("Étape 1/4 : Parcelle cible + voisins (Supabase)")
        geom_cible = fetch_parcelle_geometry(CODE_INSEE, SECTION, NUMERO)
        voisins, n_vois = fetch_parcelles_contigues(geom_cible, CODE_INSEE)
        emprise = build_emprise_mnt(geom_cible, voisins)
        logger.info("✓ Emprise MNT OK (%d voisin(s))\n", n_vois)

        logger.info("Étape 2/4 : Récupération et traitement du MNT")
        mnt, transform, res = fetch_mnt_from_geometry(emprise)
        logger.info("✓ MNT récupéré et clippé\n")

        logger.info("Étape 3/4 : Génération de la visualisation 3D")
        result = export_plotly_3d(
            geom_cible,
            mnt,
            transform,
            res,
            CODE_INSEE,
            SECTION,
            NUMERO,
            OUTPUT_DIR,
            EXAGGERATION,
            n_voisins=n_vois,
        )
        logger.info("✓ Visualisation générée\n")

        logger.info("=" * 60)
        logger.info("SUCCÈS - Traitement terminé")
        logger.info("=" * 60)
        print(json.dumps(result, indent=2, ensure_ascii=False))

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"ERREUR : {str(e)}")
        logger.error("=" * 60)
        raise
