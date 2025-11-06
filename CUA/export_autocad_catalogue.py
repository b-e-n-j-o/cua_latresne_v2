#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_autocad_v3.py
------------------------------------------------------------
Export DXF enrichi + version Web-safe pour AutoCAD / QGIS
------------------------------------------------------------
🟢 Contenu :
- Export multi-calques DXF géoréférencé (EPSG:2154)
- Libellés textuels automatiques sur la carte
- Couleurs et calques par type du catalogue
- Métadonnées réglementaires (Extended Entity Data)
- Version DXF R2000 simplifiée compatible AutoCAD Web
------------------------------------------------------------
"""

import os
import json
import logging
import geopandas as gpd
import ezdxf
from ezdxf import recover
from shapely import wkt
from sqlalchemy import create_engine, text
from shapely.geometry import Polygon, MultiPolygon, LineString, MultiLineString, Point
from dotenv import load_dotenv
from pathlib import Path

# ============================================================
# CONFIG LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("export_autocad_v3")

# ============================================================
# CONFIGURATION GLOBALE
# ============================================================
load_dotenv()
SCHEMA = "latresne"
CATALOGUE_PATH = os.path.join(os.path.dirname(__file__), "catalogue_couches_map.json")

COULEURS_TYPE = {
    "Zonage PLU": 3,         # Vert
    "Servitudes": 1,         # Rouge
    "Prescriptions": 5,      # Violet
    "Informations": 4,       # Bleu
    "Parcelle": 2            # Jaune
}

# ============================================================
# CONNEXION BASE SUPABASE
# ============================================================
def get_db_engine():
    DB_HOST = os.getenv("SUPABASE_HOST")
    DB_USER = os.getenv("SUPABASE_USER")
    DB_PASS = os.getenv("SUPABASE_PASSWORD")
    DB_NAME = os.getenv("SUPABASE_DB")
    DB_PORT = os.getenv("SUPABASE_PORT", "5432")

    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={"connect_timeout": 10, "sslmode": "require"}
    )

# ============================================================
# OUTILS GEOMETRIQUES
# ============================================================
def add_geometry_with_label(msp, geom, layer_name, color, label_text=None, attrs=None):
    """Ajoute une géométrie shapely + texte + données attributaires dans le DXF."""
    if geom.is_empty:
        return

    def safe_xdata(entity, attrs):
        """Évite les erreurs XDATA pour AutoCAD Web (longueur max, encodage)"""
        for k, v in attrs.items():
            try:
                entity.set_xdata("KERELIA", [(1000, f"{k}:{str(v)[:240]}")])
            except Exception:
                pass

    # Polygones
    if isinstance(geom, (Polygon, MultiPolygon)):
        polys = geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
        for poly in polys:
            coords = [(x, y) for x, y in poly.exterior.coords]
            entity = msp.add_lwpolyline(coords, close=True, dxfattribs={"layer": layer_name, "color": color})
            if attrs:
                safe_xdata(entity, attrs)
            if label_text:
                centroid = poly.centroid
                msp.add_text(
                    label_text,
                    dxfattribs={
                        "layer": layer_name,
                        "height": 2.5,
                        "color": color,
                        "insert": (centroid.x, centroid.y, 0),
                        "halign": 2,
                        "valign": 2
                    }
                )

    # Lignes
    elif isinstance(geom, (LineString, MultiLineString)):
        lines = geom.geoms if geom.geom_type == "MultiLineString" else [geom]
        for line in lines:
            coords = [(x, y) for x, y in line.coords]
            entity = msp.add_lwpolyline(coords, dxfattribs={"layer": layer_name, "color": color})
            if attrs:
                safe_xdata(entity, attrs)
            if label_text:
                midpoint = line.interpolate(0.5, normalized=True)
                msp.add_text(
                    label_text,
                    dxfattribs={
                        "layer": layer_name,
                        "height": 2.5,
                        "color": color,
                        "insert": (midpoint.x, midpoint.y, 0),
                        "halign": 2,
                        "valign": 2
                    }
                )

    # Points
    elif isinstance(geom, Point):
        msp.add_point((geom.x, geom.y), dxfattribs={"layer": layer_name, "color": color})
        if label_text:
            msp.add_text(
                label_text,
                dxfattribs={
                    "layer": layer_name,
                    "height": 2.5,
                    "color": color,
                    "insert": (geom.x, geom.y + 2, 0),
                    "halign": 2,
                    "valign": 2
                }
            )

# ============================================================
# EXPORT PRINCIPAL
# ============================================================
def export_autocad_enriched(wkt_path, output_dir="./out_autocad_enriched"):
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"📄 Lecture du WKT : {wkt_path}")

    geom_wkt = Path(wkt_path).read_text(encoding="utf-8").strip()
    geom = wkt.loads(geom_wkt)

    engine = get_db_engine()
    with open(CATALOGUE_PATH, "r", encoding="utf-8") as f:
        catalogue = json.load(f)

    doc = ezdxf.new(setup=True)
    msp = doc.modelspace()

    # Parcelle principale
    doc.layers.add("PARCELLE", color=COULEURS_TYPE["Parcelle"])
    add_geometry_with_label(msp, geom, "PARCELLE", COULEURS_TYPE["Parcelle"], label_text="UNITÉ FONCIÈRE")

    # Boucle sur toutes les couches
    for table, cfg in catalogue.items():
        type_couche = cfg.get("type", "Autres")
        nom_couche = cfg.get("nom", table)
        color = COULEURS_TYPE.get(type_couche, 7)
        layer_name = nom_couche[:30]  # Limite DXF 31 caractères

        logger.info(f"➡️  Export {layer_name} ({type_couche})...")

        q = f"""
        SELECT ST_Intersection(ST_MakeValid(t.geom_2154), ST_GeomFromText(:geom, 2154)) AS geom,
               t.*
        FROM {SCHEMA}.{table} t
        WHERE t.geom_2154 IS NOT NULL
          AND ST_Intersects(t.geom_2154, ST_GeomFromText(:geom, 2154));
        """

        try:
            gdf = gpd.read_postgis(text(q), engine, geom_col="geom", params={"geom": geom_wkt})
        except Exception as e:
            logger.warning(f"⚠️  Erreur {table}: {e}")
            continue

        if gdf.empty:
            continue

        gdf = gdf.set_crs(2154, allow_override=True)

        doc.layers.add(layer_name, color=color)
        for idx, row in gdf.iterrows():
            geom_entity = gdf.loc[idx, gdf.geometry.name]
            attrs = {}
            for col in ["nom", "codezone", "typereg", "reglementation", "legende"]:
                if col in gdf.columns and row[col]:
                    attrs[col] = str(row[col])

            label = row.get("nom") or row.get("codezone") or row.get("legende") or nom_couche
            add_geometry_with_label(msp, geom_entity, layer_name, color, label_text=label, attrs=attrs)

        logger.info(f"   ✅ {len(gdf)} entités ajoutées à {layer_name}")

    # ===================== SAUVEGARDE =====================
    output_dxf = os.path.join(output_dir, "export_reglementaire_enriched.dxf")
    output_prj = os.path.join(output_dir, "export_reglementaire_enriched.prj")

    # Fichier principal enrichi
    doc.saveas(output_dxf)
    with open(output_prj, "w") as f:
        f.write("""PROJCS["RGF93 / Lambert-93",GEOGCS["RGF93",DATUM["Reseau_Geodesique_Francais_1993",SPHEROID["GRS 1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["standard_parallel_1",49],PARAMETER["standard_parallel_2",44],PARAMETER["latitude_of_origin",46.5],PARAMETER["central_meridian",3],PARAMETER["false_easting",700000],PARAMETER["false_northing",6600000],UNIT["metre",1]]""")

    logger.info(f"✅ Export enrichi terminé : {output_dxf}")

    # ===================== VERSION WEB-SAFE =====================
    simple_path = os.path.join(output_dir, "export_reglementaire_websafe.dxf")
    logger.info(f"🌐 Création de la version Web-safe : {simple_path}")

    try:
        doc2, auditor = recover.readfile(output_dxf)
        doc2.header["$ACADVER"] = "AC1015"  # DXF R2000
        for dict_name in list(doc2.rootdict.keys()):
            if dict_name.startswith("ACAD_"):
                del doc2.rootdict[dict_name]
        doc2.saveas(simple_path)
        logger.info("✅ Version Web-safe DXF R2000 prête pour AutoCAD Web")
    except Exception as e:
        logger.error(f"❌ Erreur génération version Web-safe : {e}")

    return output_dxf, simple_path, output_prj

# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export DXF enrichi + Web-safe Kerelia")
    parser.add_argument("--wkt", required=True, help="Chemin vers le WKT de l’unité foncière")
    parser.add_argument("--output", default="./out_autocad_enriched", help="Dossier de sortie")
    args = parser.parse_args()

    export_autocad_enriched(args.wkt, args.output)
