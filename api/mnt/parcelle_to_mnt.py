#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parcelle_topo_3d.py
-------------------
Génère une visualisation 3D Plotly de la topographie (MNT)
d'une parcelle cadastrale donnée par :
- code INSEE
- section
- numéro

Pipeline :
1) Récupération géométrie parcellaire via WFS IGN
2) Sélection des dalles MNT dans Supabase (ST_Intersects)
3) Téléchargement depuis Supabase Storage
4) Merge + clip raster
5) Export HTML Plotly (autonome, mobile-friendly)
"""

import os
import io
import tempfile
import requests
import numpy as np
import geopandas as gpd
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask
from shapely.geometry import mapping
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

IGN_WFS_ENDPOINT = "https://data.geopf.fr/wfs/ows"
IGN_LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
SRS = "EPSG:2154"

# ============================================================
# FONCTIONS
# ============================================================

def fetch_parcelle_geometry(code_insee, section, numero):
    """Récupère la géométrie officielle IGN d'une parcelle."""
    logger.info(f"Récupération de la géométrie parcellaire : {code_insee} - {section} - {numero}")
    cql = f"code_insee='{code_insee}' AND section='{section}' AND numero='{numero}'"
    logger.debug(f"Filtre CQL : {cql}")

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": IGN_LAYER,
        "srsName": SRS,
        "outputFormat": "application/json",
        "CQL_FILTER": cql
    }

    logger.info(f"Requête WFS IGN : {IGN_WFS_ENDPOINT}")
    r = requests.get(IGN_WFS_ENDPOINT, params=params)
    r.raise_for_status()
    logger.info("Réponse WFS reçue avec succès")

    gdf = gpd.read_file(io.BytesIO(r.content))
    logger.info(f"Géométrie chargée : {len(gdf)} feature(s)")

    if gdf.empty:
        raise ValueError("Parcelle introuvable via l'IGN")

    geom = gdf.geometry.iloc[0]
    logger.info(f"Géométrie récupérée : surface = {geom.area:.2f} m²")
    return geom


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

        logger.info("Clippage du MNT selon la géométrie de la parcelle...")
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


def export_plotly_3d(geometry, mnt, transform, resolution,
                     code_insee, section, numero,
                     output_dir="./out_3d",
                     exaggeration=1.5):

    logger.info(f"Génération de la visualisation 3D...")
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

    logger.info("Création de la figure Plotly...")
    fig = go.Figure(go.Surface(
        x=Xs,
        y=Ys,
        z=Zs,
        colorscale="Earth",
        showscale=True
    ))

    fig.update_layout(
        title=f"Topographie 3D – Parcelle {section}{numero}",
        scene=dict(
            xaxis_title="X (m)",
            yaxis_title="Y (m)",
            zaxis_title="Altitude (m)",
            aspectmode="data"
        ),
        margin=dict(l=0, r=0, t=40, b=0)
    )

    filename = f"parcelle_3d_{code_insee}_{section}{numero}.html"
    path = os.path.join(output_dir, filename)
    logger.info(f"Export HTML : {path}")

    fig.write_html(path, include_plotlyjs=True, full_html=True)
    logger.info("✓ Fichier HTML généré avec succès")

    result = {
        "path": path,
        "surface_m2": float(geometry.area),
        "resolution_m": resolution
    }
    logger.info(f"Résultat : surface = {result['surface_m2']:.2f} m², résolution = {result['resolution_m']:.2f} m")
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
        logger.info("Étape 1/3 : Récupération de la géométrie parcellaire")
        geom = fetch_parcelle_geometry(CODE_INSEE, SECTION, NUMERO)
        logger.info("✓ Géométrie récupérée\n")

        logger.info("Étape 2/3 : Récupération et traitement du MNT")
        mnt, transform, res = fetch_mnt_from_geometry(geom)
        logger.info("✓ MNT récupéré et clippé\n")

        logger.info("Étape 3/3 : Génération de la visualisation 3D")
        result = export_plotly_3d(
            geom,
            mnt,
            transform,
            res,
            CODE_INSEE,
            SECTION,
            NUMERO,
            OUTPUT_DIR,
            EXAGGERATION
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
