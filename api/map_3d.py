# -*- coding: utf-8 -*-
"""
map_3d.py
---------------------------
Génère une visualisation 3D d'une unité foncière ou d'une parcelle cadastrale avec Plotly.
Connexion via pooler Supabase (sécurisée, stable).
Compatibilité :
- Mode moderne : via fichier .wkt (unité foncière issue de verification_unite_fonciere)
- Mode legacy : via WFS IGN (section + numéro)
"""

import os
import io
import tempfile
import requests
import geopandas as gpd
import rasterio
from rasterio.mask import mask as rasterio_mask
from rasterio.merge import merge
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from shapely import wkt as wkt_loader
from shapely.geometry import mapping
import plotly.graph_objects as go

# ============================================================
# 🔧 CONFIGURATION - CONNEXION VIA POOLER SUPABASE
# ============================================================

load_dotenv()

DB_HOST = os.getenv("SUPABASE_HOST")
DB_USER = os.getenv("SUPABASE_USER")
DB_PASS = os.getenv("SUPABASE_PASSWORD")
DB_NAME = os.getenv("SUPABASE_DB")
DB_PORT = os.getenv("SUPABASE_PORT", "5432")

if "pooler.supabase.com" not in DB_HOST:
    print("⚠️  Attention : vous n'utilisez pas le pooler Supabase !")
    print("   Exemple attendu : aws-0-eu-west-3.pooler.supabase.com\n")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_ENGINE = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={"connect_timeout": 10, "sslmode": "require"}
)

SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# ============================================================
# 🧩 FONCTIONS PRINCIPALES
# ============================================================

def inject_mobile_compatibility(html_path):
    """
    Injecte les balises meta viewport et le CSS responsive dans un fichier HTML Plotly.
    
    Args:
        html_path (str): Chemin du fichier HTML à modifier
    
    Returns:
        None (modification en place)
    """
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    # ✅ Injecter un header responsive pour mobile avec JavaScript pour forcer le resize
    responsive_header = """
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  html, body {
    margin: 0;
    padding: 0;
    height: 100%;
    width: 100%;
    overflow: hidden;
  }
  .plotly-graph-div {
    width: 100% !important;
    height: 100% !important;
  }
</style>
<script>
  window.addEventListener('load', function() {
    setTimeout(function() {
      var plotDiv = document.querySelector('.plotly-graph-div');
      if (plotDiv && window.Plotly) {
        Plotly.Plots.resize(plotDiv);
      }
    }, 100);
    
    window.addEventListener('resize', function() {
      var plotDiv = document.querySelector('.plotly-graph-div');
      if (plotDiv && window.Plotly) {
        Plotly.Plots.resize(plotDiv);
      }
    });
  });
</script>
"""
    
    # Insérer juste après <head>
    if "<head>" in html:
        html = html.replace("<head>", "<head>" + responsive_header)
    else:
        # Fallback si la structure est différente
        html = responsive_header + html
    
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    
    print("   📱 Compatibilité mobile ajoutée (viewport + responsive CSS + auto-resize)")


def fetch_parcelle_wfs(code_insee, id_parcelle):
    """Récupère une parcelle cadastrale via le WFS de l’IGN (méthode legacy)."""
    ENDPOINT = "https://data.geopf.fr/wfs/ows"
    LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
    SRS = "EPSG:2154"

    parts = id_parcelle.strip().split()
    section = parts[0] if len(parts) == 2 else id_parcelle[:2]
    numero = parts[1] if len(parts) == 2 else id_parcelle[2:]

    FILTER = f"code_insee='{code_insee}' AND section='{section}' AND numero='{numero}'"
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": LAYER,
        "srsName": SRS,
        "outputFormat": "application/json",
        "CQL_FILTER": FILTER
    }

    r = requests.get(ENDPOINT, params=params)
    r.raise_for_status()
    gdf = gpd.read_file(io.BytesIO(r.content))

    if gdf.empty:
        raise ValueError(f"❌ Parcelle {id_parcelle} introuvable dans la commune {code_insee}")

    if gdf.crs is None or gdf.crs.to_string() != SRS:
        gdf = gdf.to_crs(SRS)

    return gdf


def get_mnt_data_from_wkt(wkt_path):
    """Télécharge, fusionne et clippe les dalles MNT à partir d'une géométrie WKT (unité foncière)."""
    if not os.path.exists(wkt_path):
        raise FileNotFoundError(f"❌ Fichier WKT introuvable : {wkt_path}")

    geom = wkt_loader.loads(open(wkt_path, "r", encoding="utf-8").read().strip())
    geom_wkt = geom.wkt
    geometry_parcelle = mapping(geom)

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
            response = requests.get(dalle['storage_url'], headers=headers)
            response.raise_for_status()
            local_path = os.path.join(temp_dir, dalle['nom_fichier'])
            with open(local_path, 'wb') as f:
                f.write(response.content)
            dalles_paths.append(local_path)

        src_files = [rasterio.open(p) for p in dalles_paths]

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


def get_mnt_data(code_insee, id_parcelle):
    """Méthode ancienne (compatibilité) : télécharge et clippe les dalles MNT via WFS."""
    parcelles = fetch_parcelle_wfs(code_insee, id_parcelle)
    geometry_parcelle = parcelles.geometry.iloc[0]
    geom_wkt = geometry_parcelle.wkt

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
        raise ValueError("❌ Aucune dalle MNT ne couvre cette parcelle")

    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

    with tempfile.TemporaryDirectory() as temp_dir:
        dalles_paths = []
        for dalle in dalles:
            response = requests.get(dalle['storage_url'], headers=headers)
            response.raise_for_status()
            local_path = os.path.join(temp_dir, dalle['nom_fichier'])
            with open(local_path, 'wb') as f:
                f.write(response.content)
            dalles_paths.append(local_path)

        src_files = [rasterio.open(p) for p in dalles_paths]

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

    return mnt_data, out_transform, resolution, parcelles


def exporter_visualisation_3d_plotly_from_wkt(wkt_path, output_dir="./out_3d", exaggeration=1.0):
    """Exporte une visualisation Plotly 3D à partir d'un fichier WKT (unité foncière)."""
    os.makedirs(output_dir, exist_ok=True)

    try:
        mnt_data, out_transform, resolution, geom = get_mnt_data_from_wkt(wkt_path)
        rows, cols = mnt_data.shape

        x = np.arange(cols) * resolution + out_transform[2]
        y = np.arange(rows) * out_transform[4] + out_transform[5]
        X, Y = np.meshgrid(x, y)
        Z = mnt_data * exaggeration

        step = max(1, min(rows, cols) // 200)
        Xs, Ys, Zs = X[::step, ::step], Y[::step, ::step], Z[::step, ::step]

        fig = go.Figure(data=[go.Surface(x=Xs, y=Ys, z=Zs, colorscale="Earth", showscale=True)])
        fig.update_traces(contours_z=dict(show=True, usecolormap=True, highlightcolor="limegreen", project_z=True))
        fig.update_layout(
            title="Topographie 3D – Unité foncière",
            scene=dict(
                xaxis_title="X (m)",
                yaxis_title="Y (m)",
                zaxis_title="Altitude (m)",
                aspectmode="data"
            ),
            autosize=True,
            margin=dict(l=0, r=0, t=40, b=0)
        )

        html_name = "carte_3d_unite_fonciere.html"
        html_path = os.path.join(output_dir, html_name)
        
        # ✅ Embarquer Plotly directement (pas de CDN) pour compatibilité mobile
        fig.write_html(html_path, include_plotlyjs=True, full_html=True)
        
        # ✅ Injecter les optimisations mobile (viewport + CSS responsive)
        inject_mobile_compatibility(html_path)

        metadata = {
            "resolution": resolution,
            "rows": rows,
            "cols": cols,
            "exaggeration": exaggeration,
            "surface_m2": float(geom.area)
        }

        print(f"✅ Visualisation 3D générée : {html_path}")
        return {"path": html_path, "filename": html_name, "metadata": metadata}

    except Exception as e:
        print(f"❌ Erreur génération 3D : {e}")
        return {"error": str(e), "path": None, "filename": None}


def exporter_visualisation_3d_plotly(code_insee, id_parcelle, output_dir, exaggeration=1.0):
    """Méthode ancienne (parcelle simple, via WFS IGN)."""
    os.makedirs(output_dir, exist_ok=True)
    parcelle_id = id_parcelle.replace(" ", "_")

    try:
        mnt_data, out_transform, resolution, parcelles = get_mnt_data(code_insee, id_parcelle)
        rows, cols = mnt_data.shape

        x = np.arange(cols) * resolution + out_transform[2]
        y = np.arange(rows) * out_transform[4] + out_transform[5]
        X, Y = np.meshgrid(x, y)
        Z = mnt_data * exaggeration

        step = max(1, min(rows, cols) // 200)
        Xs, Ys, Zs = X[::step, ::step], Y[::step, ::step], Z[::step, ::step]

        fig = go.Figure(data=[go.Surface(x=Xs, y=Ys, z=Zs, colorscale="Earth", showscale=True)])
        fig.update_layout(
            title=f"Topographie 3D – Parcelle {id_parcelle}",
            scene=dict(
                xaxis_title="X (m)",
                yaxis_title="Y (m)",
                zaxis_title="Altitude (m)",
                aspectmode="data"
            ),
            autosize=True,
            margin=dict(l=0, r=0, t=40, b=0)
        )

        html_name = f"carte_3d_{parcelle_id}.html"
        html_path = os.path.join(output_dir, html_name)
        
        # ✅ Embarquer Plotly directement (pas de CDN) pour compatibilité mobile
        fig.write_html(html_path, include_plotlyjs=True, full_html=True)
        
        # ✅ Injecter les optimisations mobile (viewport + CSS responsive)
        inject_mobile_compatibility(html_path)

        metadata = {
            "section": parcelles["section"].iloc[0],
            "numero": parcelles["numero"].iloc[0],
            "surface_m2": float(parcelles.geometry.area.sum()),
            "resolution": resolution,
            "rows": rows,
            "cols": cols,
            "exaggeration": exaggeration,
        }

        print(f"✅ Visualisation 3D générée : {html_path}")
        return {"path": html_path, "filename": html_name, "metadata": metadata}

    except Exception as e:
        print(f"❌ Erreur génération 3D : {e}")
        return {"error": str(e), "path": None, "filename": None}


# ============================================================
# 🧪 TEST CLI
# ============================================================

if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="Génère une carte 3D Plotly à partir du MNT Supabase.")
    parser.add_argument("--code_insee", help="Code INSEE de la commune (requis si mode parcelle)")
    parser.add_argument("--id_parcelle", help="Identifiant de la parcelle (ex: 'AC 0242')")
    parser.add_argument("--geom-wkt", help="Chemin vers un fichier WKT (mode unité foncière)")
    parser.add_argument("--output", default="./out_3d", help="Dossier de sortie")
    parser.add_argument("--exaggeration", type=float, default=1.0, help="Facteur d'exagération verticale")
    args = parser.parse_args()

    if args.geom_wkt:
        result = exporter_visualisation_3d_plotly_from_wkt(args.geom_wkt, args.output, exaggeration=args.exaggeration)
    elif args.code_insee and args.id_parcelle:
        result = exporter_visualisation_3d_plotly(args.code_insee, args.id_parcelle, args.output, exaggeration=args.exaggeration)
    else:
        parser.error("❌ Fournir soit --geom-wkt, soit (--code_insee + --id_parcelle)")
    
    print(json.dumps(result, indent=2, ensure_ascii=False))
