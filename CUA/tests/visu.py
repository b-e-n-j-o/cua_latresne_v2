#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
visualisation_2_5d_supabase_highres.py
--------------------------------------
Version "2.5D" (sans exagération verticale)
- Rendu bilinéaire (plus fluide)
- Hover affiche la vraie altitude NGF
- Caméra verrouillée pour ne pas passer sous le sol
"""

import os, tempfile, requests, numpy as np, rasterio, logging
from rasterio.mask import mask as rasterio_mask
from rasterio.merge import merge
from shapely import wkt
from shapely.geometry import mapping
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from matplotlib import cm
from matplotlib.colors import LightSource
import plotly.graph_objects as go
from scipy.ndimage import zoom

# ============================================================
# ⚙️ CONFIG
# ============================================================

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger("mnt_2_5d")

DB_HOST = os.getenv("SUPABASE_HOST")
DB_USER = os.getenv("SUPABASE_USER")
DB_PASS = os.getenv("SUPABASE_PASSWORD")
DB_NAME = os.getenv("SUPABASE_DB")
DB_PORT = os.getenv("SUPABASE_PORT", "5432")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600)

EXAGGERATION = 1.0   # ✅ Aucune exagération
UPSCALE_FACTOR = 2   # Interpolation bilinéaire pour rendu fluide

# ============================================================
# 🧩 EXTRACTION MNT
# ============================================================

def get_mnt_data_from_wkt(wkt_path):
    """Télécharge et clippe les dalles MNT Supabase correspondant à une géométrie WKT."""
    if not os.path.exists(wkt_path):
        raise FileNotFoundError(f"❌ Fichier WKT introuvable : {wkt_path}")

    geom = wkt.loads(open(wkt_path, "r", encoding="utf-8").read().strip())
    geom_wkt = geom.wkt
    geom_mapping = mapping(geom)

    sql_query = """
    SELECT nom_fichier, storage_url
    FROM public.mnt_dalles
    WHERE ST_Intersects(emprise, ST_GeomFromText(:geom_wkt, 2154))
    ORDER BY nom_fichier;
    """

    with DB_ENGINE.connect() as conn:
        result = conn.execute(text(sql_query), {"geom_wkt": geom_wkt})
        dalles = [dict(row._mapping) for row in result]

    if not dalles:
        raise ValueError("❌ Aucune dalle MNT ne couvre cette unité foncière")

    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    with tempfile.TemporaryDirectory() as tmpdir:
        local_paths = []
        for dalle in dalles:
            r = requests.get(dalle["storage_url"], headers=headers, timeout=60)
            r.raise_for_status()
            path = os.path.join(tmpdir, dalle["nom_fichier"])
            with open(path, "wb") as f:
                f.write(r.content)
            local_paths.append(path)

        src_files = [rasterio.open(p) for p in local_paths]
        if len(src_files) > 1:
            mosaic, out_transform = merge(src_files)
            src_to_clip = rasterio.io.MemoryFile().open(
                driver="GTiff",
                height=mosaic.shape[1],
                width=mosaic.shape[2],
                count=1,
                dtype=mosaic.dtype,
                crs=src_files[0].crs,
                transform=out_transform
            )
            src_to_clip.write(mosaic)
        else:
            src_to_clip = src_files[0]

        out_image, out_transform = rasterio_mask(src_to_clip, [geom_mapping], crop=True, all_touched=True)
        mnt = out_image[0]
        nodata = src_to_clip.nodata
        res = src_to_clip.res[0]
        src_to_clip.close()

        if nodata is not None:
            mnt = np.where(mnt == nodata, np.nan, mnt)
        mnt = np.where(np.isclose(mnt, 0.0, atol=1e-6), np.nan, mnt)

    return mnt, out_transform, res, geom


# ============================================================
# 🌄 VISUALISATION 2.5D (SANS EXAGÉRATION)
# ============================================================

def visualiser_2_5d(wkt_path, output_dir="./out_2_5d"):
    os.makedirs(output_dir, exist_ok=True)
    mnt, out_transform, resolution, geom = get_mnt_data_from_wkt(wkt_path)

    valid = mnt[~np.isnan(mnt)]
    if valid.size == 0:
        raise ValueError("❌ Aucune valeur MNT valide")

    zmin, zmax, zmean = np.min(valid), np.max(valid), np.mean(valid)
    log.info(f"📊 MNT — min={zmin:.2f} | max={zmax:.2f} | mean={zmean:.2f}")
    log.info(f"🧮 Résolution horizontale native : {resolution:.2f} m/pixel")

    if UPSCALE_FACTOR > 1:
        log.info(f"🔍 Interpolation bilinéaire (zoom x{UPSCALE_FACTOR}) pour rendu fluide…")
        mnt = zoom(mnt, UPSCALE_FACTOR, order=1)
        resolution = resolution / UPSCALE_FACTOR

    rows, cols = mnt.shape
    x = np.arange(cols) * resolution + out_transform[2]
    y = np.arange(rows) * out_transform[4] + out_transform[5]
    y = y[::-1]
    X, Y = np.meshgrid(x, y)

    # Pas d'exagération
    Z_display = mnt

    # Ombrage terrain
    ls = LightSource(azdeg=315, altdeg=45)
    hillshade = ls.shade(mnt, cmap=cm.get_cmap("terrain"), blend_mode="hsv")

    hover_text = np.vectorize(lambda v: f"{v:.2f}")(mnt)

    fig = go.Figure(data=[
        go.Surface(
            x=X, y=Y, z=Z_display,
            surfacecolor=hillshade[..., 1],
            colorscale="Viridis",
            showscale=False,
            text=hover_text,
            hovertemplate="<b>Altitude NGF réelle</b> : %{text} m<extra></extra>"
        )
    ])

    # 🔒 Verrouillage "2.5D"
    # empêche la caméra de passer sous la surface
    camera = dict(
        up=dict(x=0, y=0, z=1),  # le "haut" est fixe
        eye=dict(x=1.8, y=1.8, z=0.8)  # angle oblique mais au-dessus du sol
    )

    fig.update_layout(
        title="Relief 2.5D – Unité foncière (altitudes réelles)",
        scene=dict(
            xaxis_title="X (m, Lambert-93)",
            yaxis_title="Y (m, Lambert-93)",
            zaxis_title="Altitude NGF (m)",
            aspectmode="data",
            camera=camera,
            zaxis=dict(range=[zmin - 1, zmax + 1])  # bornage vertical
        ),
        width=1300, height=850,
        paper_bgcolor="white",
        margin=dict(l=0, r=0, t=60, b=0)
    )

    html_path = os.path.join(output_dir, "carte_2_5d.html")
    fig.write_html(html_path, include_plotlyjs=True, full_html=True)
    log.info(f"✅ Carte 2.5D générée : {html_path}")

    return html_path


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Carte 2.5D (altitudes réelles, MNT Supabase)")
    parser.add_argument("--geom-wkt", required=True, help="Chemin du fichier .wkt EPSG:2154")
    parser.add_argument("--output", default="./out_2_5d", help="Dossier de sortie")
    args = parser.parse_args()

    path = visualiser_2_5d(args.geom_wkt, args.output)
    print(f"\n✅ Fichier généré : {path}")
