# -*- coding: utf-8 -*-
"""
altimetrie_ngf_supabase.py
--------------------------------------
Analyse altimétrique d'une unité foncière (version Supabase)
- Utilise une géométrie WKT (EPSG:2154)
- Télécharge et fusionne les dalles MNT stockées sur Supabase (public.mnt_dalles)
- Calcule min / max / moyenne NGF directement sur les pixels du MNT
- Produit un paragraphe synthétique cohérent avec la carte 3D
"""

import os
import tempfile
import requests
import numpy as np
import rasterio
from rasterio.mask import mask as rasterio_mask
from rasterio.merge import merge
from shapely import wkt
from shapely.geometry import mapping
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ============================================================
# 🔧 CONFIGURATION SUPABASE
# ============================================================

load_dotenv()

DB_HOST = os.getenv("SUPABASE_HOST")
DB_USER = os.getenv("SUPABASE_USER")
DB_PASS = os.getenv("SUPABASE_PASSWORD")
DB_NAME = os.getenv("SUPABASE_DB")
DB_PORT = os.getenv("SUPABASE_PORT", "5432")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_ENGINE = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={"connect_timeout": 10, "sslmode": "require"}
)

# ============================================================
# 🧩 FONCTIONS UTILITAIRES
# ============================================================

def get_mnt_data_from_wkt(wkt_path):
    """
    Télécharge, fusionne et clippe les dalles MNT à partir d'une géométrie WKT stockée sur Supabase.
    Retourne : (mnt_data, out_transform, resolution, geom)
    """
    if not os.path.exists(wkt_path):
        raise FileNotFoundError(f"❌ Fichier WKT introuvable : {wkt_path}")

    geom = wkt.loads(open(wkt_path, "r", encoding="utf-8").read().strip())
    geom_wkt = geom.wkt
    geometry_parcelle = mapping(geom)

    # 🔍 Recherche des dalles intersectées dans la table public.mnt_dalles
    sql_query = """
    SELECT nom_fichier, storage_url
    FROM public.mnt_dalles
    WHERE ST_Intersects(emprise, ST_GeomFromText(:geom_wkt, 2154))
    ORDER BY nom_fichier;
    """
    with DB_ENGINE.connect() as conn:
        result = conn.execute(text(sql_query), {'geom_wkt': geom_wkt})
        dalles = [dict(row._mapping) for row in result]

    if not dalles:
        raise ValueError("❌ Aucune dalle MNT ne couvre cette unité foncière")

    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

    with tempfile.TemporaryDirectory() as temp_dir:
        dalles_paths = []
        for dalle in dalles:
            r = requests.get(dalle["storage_url"], headers=headers, timeout=60)
            r.raise_for_status()
            local_path = os.path.join(temp_dir, dalle["nom_fichier"])
            with open(local_path, "wb") as f:
                f.write(r.content)
            dalles_paths.append(local_path)

        src_files = [rasterio.open(p) for p in dalles_paths]

        # Fusion si plusieurs dalles
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

        out_image, out_transform = rasterio_mask(src_to_clip, [geometry_parcelle], crop=True, all_touched=True)
        mnt_data = out_image[0]
        nodata = src_to_clip.nodata
        resolution = src_to_clip.res[0]
        src_to_clip.close()

        if nodata is not None:
            mnt_data = np.where(mnt_data == nodata, np.nan, mnt_data)

    return mnt_data, out_transform, resolution, geom


# ============================================================
# 🧮 CALCUL ALTITUDES NGF
# ============================================================

def cote_ngf_parcelle_supabase(wkt_path):
    """
    Calcule les altitudes NGF (min / max / moyenne) à partir du MNT Supabase.
    Retourne un paragraphe et les statistiques associées.
    """
    mnt_data, _, _, geom = get_mnt_data_from_wkt(wkt_path)

    arr = mnt_data[~np.isnan(mnt_data)]
    arr = arr[~np.isclose(arr, 0.0, atol=1e-6)]
    if arr.size == 0:
        raise ValueError("❌ Aucune donnée MNT valide trouvée pour cette géométrie")

    zmin = round(float(np.min(arr)), 2)
    zmax = round(float(np.max(arr)), 2)
    zmean = round(float(np.mean(arr)), 2)
    surface_m2 = round(float(geom.area), 2)

    paragraphe = (
        f"L'unité foncière présente une altitude moyenne de {zmean} mètres NGF, "
        f"avec un point le plus bas à {zmin} m et un point le plus haut à {zmax} m. "
        f"Ces valeurs sont calculées à partir du modèle numérique de terrain RGE ALTI "
        f"pour une surface d'environ {surface_m2:.0f} m²."
    )

    stats = {
        "zmin": zmin,
        "zmax": zmax,
        "zmean": zmean,
        "surface_m2": surface_m2,
        "n_pixels": int(arr.size)
    }

    return paragraphe, stats


# ============================================================
# 🧪 TEST CLI
# ============================================================

if __name__ == "__main__":
    import argparse, json
    parser = argparse.ArgumentParser(description="Calcul altimétrique NGF à partir du MNT Supabase (WKT)")
    parser.add_argument("--geom-wkt", required=True, help="Chemin vers un fichier .wkt (EPSG:2154)")
    args = parser.parse_args()

    paragraphe, stats = cote_ngf_parcelle_supabase(args.geom_wkt)
    print("\nRésultats altimétriques NGF :")
    print(paragraphe)
    print(json.dumps(stats, indent=2, ensure_ascii=False))
