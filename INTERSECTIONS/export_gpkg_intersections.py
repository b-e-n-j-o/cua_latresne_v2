#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
export_gpkg_intersections.py — Génère un GeoPackage stylé depuis l'unité foncière (WKT)
-------------------------------------------------------------------------------
Contient :
- unité foncière (EPSG:2154)
- couches intersects dissolues ou brutes selon CATALOGUE
- styles QGIS copiés depuis un modèle
-------------------------------------------------------------------------------
"""

import os
import json
import logging
import sqlite3
from pathlib import Path
from dotenv import load_dotenv

import geopandas as gpd
from shapely import wkt
from sqlalchemy import create_engine, text

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("gpkg_export")

# -------------------------------------------------------------------
# ⚙️ Connexion DB PostGIS (Supabase)
# -------------------------------------------------------------------
SUPABASE_HOST = os.getenv('SUPABASE_HOST')
SUPABASE_DB = os.getenv('SUPABASE_DB')
SUPABASE_USER = os.getenv('SUPABASE_USER')
SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')
SUPABASE_PORT = os.getenv('SUPABASE_PORT')

DATABASE_URL = (
    f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}"
    f"@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
)
engine = create_engine(DATABASE_URL)

DEFAULT_SCHEMA = "latresne"  # adapter si d'autres communes

# -------------------------------------------------------------------
# 📚 Charger le catalogue
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CATALOGUE_PATH = PROJECT_ROOT / "catalogues" / "catalogue_intersections_tagged.json"

with open(CATALOGUE_PATH, "r", encoding="utf-8") as f:
    CATALOGUE = json.load(f)


# -------------------------------------------------------------------
# 🟦 Charger géométrie UF depuis WKT
# -------------------------------------------------------------------
def load_uf_geom_from_wkt(wkt_path: str):
    wkt_path = Path(wkt_path)
    if not wkt_path.exists():
        raise FileNotFoundError(f"Fichier WKT introuvable : {wkt_path}")

    wkt_str = wkt_path.read_text(encoding="utf-8").strip()
    return wkt.loads(wkt_str)


# -------------------------------------------------------------------
# 🟦 Intersection générique pour une couche PostGIS
# -------------------------------------------------------------------
def get_intersection_gdf(parcelle_geom, table_name, schema=DEFAULT_SCHEMA):
    """
    Retourne un GeoDataFrame contenant :
    - la géométrie intersectée
    - les attributs spécifiés dans le CATALOGUE
    - dissolve selon group_by si configuré
    """
    config = CATALOGUE.get(table_name)
    if not config:
        logger.warning(f"⚠️ {table_name} non trouvé dans le catalogue.")
        return None

    keep_cols = config.get("keep") or []
    group_by_cfg = config.get("group_by")

    # normalisation du group_by
    if not group_by_cfg:
        group_by = []
    elif isinstance(group_by_cfg, str):
        group_by = [group_by_cfg]
    else:
        group_by = list(group_by_cfg)

    with engine.connect() as conn:

        # -------------------------------------------------------------------
        # Cas 1 : sans group_by → entités individuelles
        # -------------------------------------------------------------------
        if not group_by:
            select_cols = ", ".join([f"t.{c}" for c in keep_cols]) or ""
            if select_cols:
                select_cols += ","

            sql = f"""
                WITH p AS (SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g)
                SELECT
                    {select_cols}
                    ST_AsText(
                        ST_Intersection(ST_MakeValid(t.geom_2154), p.g)
                    ) AS inter_wkt
                FROM {schema}.{table_name} t, p
                WHERE t.geom_2154 IS NOT NULL
                  AND ST_Intersects(t.geom_2154, p.g)
            """

        # -------------------------------------------------------------------
        # Cas 2 : avec group_by → dissolve / ST_Union
        # -------------------------------------------------------------------
        else:
            gb_sql = ", ".join([f"t.{c}" for c in group_by])
            non_group = [c for c in keep_cols if c not in group_by]

            agg_attrs = ", ".join([
                f"(array_agg(t.{c}) FILTER (WHERE t.{c} IS NOT NULL))[1] AS {c}"
                for c in non_group
            ])

            select_final = ", ".join(group_by + non_group)

            sql = f"""
                WITH p AS (SELECT ST_MakeValid(ST_GeomFromText(:wkt, 2154)) AS g),
                raw AS (
                    SELECT
                        {gb_sql},
                        ST_UnaryUnion(
                            ST_Collect(
                                ST_Intersection(t.geom_2154, p.g)
                            )
                        ) AS geom_union
                        {"," + agg_attrs if agg_attrs else ""}
                    FROM {schema}.{table_name} t, p
                    WHERE t.geom_2154 IS NOT NULL
                      AND ST_Intersects(t.geom_2154, p.g)
                    GROUP BY {gb_sql}
                )
                SELECT
                    {select_final},
                    ST_AsText(geom_union) AS inter_wkt
                FROM raw
            """

        rows = conn.execute(text(sql), {"wkt": parcelle_geom.wkt}).mappings().all()

    if not rows:
        return None

    # -------------------------------------------------------------------
    # Nettoyage & conversion
    # -------------------------------------------------------------------
    processed = []
    for row in rows:
        r = dict(row)

        # trouver la colonne contenant le WKT
        geom_wkt = r.pop("inter_wkt", None)
        if not geom_wkt:
            logger.error(f"❌ Pas de WKT retourné pour {table_name}")
            continue

        try:
            geom = wkt.loads(geom_wkt)
        except Exception as e:
            logger.error(f"❌ Impossible de charger WKT → {e}")
            continue

        r["geometry"] = geom
        processed.append(r)

    if not processed:
        return None

    return gpd.GeoDataFrame(processed, geometry="geometry", crs="EPSG:2154")


# -------------------------------------------------------------------
# 🎨 Copier styles depuis un modèle GPKG
# -------------------------------------------------------------------
def apply_styles_from_template(template_gpkg: str | Path, target_gpkg: str | Path):
    template_gpkg = Path(template_gpkg)
    target_gpkg = Path(target_gpkg)

    if not template_gpkg.exists():
        logger.warning("⚠️ Modèle GPKG introuvable → styles ignorés.")
        return

    src = sqlite3.connect(template_gpkg)
    dst = sqlite3.connect(target_gpkg)

    try:
        cur = src.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='layer_styles'
        """)
        if cur.fetchone() is None:
            logger.warning("⚠️ Pas de table layer_styles dans le modèle.")
            return

        # structure
        cols_info = src.execute("PRAGMA table_info(layer_styles)").fetchall()
        columns = [c[1] for c in cols_info]
        col_list = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)

        # valeurs du modèle
        styles = src.execute(f"SELECT {col_list} FROM layer_styles").fetchall()

        # recréer table
        dst.execute("DROP TABLE IF EXISTS layer_styles")

        create_stmt = src.execute("""
            SELECT sql FROM sqlite_master
            WHERE type='table' AND name='layer_styles'
        """).fetchone()[0]
        dst.execute(create_stmt)

        # insérer
        dst.executemany(
            f"INSERT INTO layer_styles ({col_list}) VALUES ({placeholders})", styles
        )
        dst.commit()

        logger.info("🎨 Styles QGIS appliqués.")

    except Exception as e:
        logger.error(f"❌ Erreur styles : {e}")

    finally:
        src.close()
        dst.close()


# -------------------------------------------------------------------
# 🟦 EXPORT PRINCIPAL : UF + intersections → GPKG
# -------------------------------------------------------------------
def export_gpkg_from_wkt(wkt_path: str, out_path: str):
    logger.info(f"📦 Export GPKG (UF + intersections) → {out_path}")

    # 1) Charger l'unité foncière
    parcelle_geom = load_uf_geom_from_wkt(wkt_path)

    # 2) Écrire la couche UF
    gdf_uf = gpd.GeoDataFrame(
        [{"id": "unite_fonciere"}],
        geometry=[parcelle_geom],
        crs="EPSG:2154"
    )
    gdf_uf.to_file(out_path, layer="unite_fonciere", driver="GPKG")
    logger.info("   ✔ couche unite_fonciere")

    # 3) Ajouter les couches intersectées
    for table_name in CATALOGUE.keys():
        logger.info(f"→ Intersection avec {table_name}")
        gdf = get_intersection_gdf(parcelle_geom, table_name)

        if gdf is None or gdf.empty:
            logger.info("   ❌ aucune intersection")
            continue

        layer_name = table_name.lower()
        gdf.to_file(out_path, layer=layer_name, driver="GPKG")
        logger.info(f"   ✔ {len(gdf)} entité(s)")

    # 4) Appliquer les styles QGIS
    template_gpkg = PROJECT_ROOT / "INTERSECTIONS" / "STYLES" / "modele_styles.gpkg"
    apply_styles_from_template(template_gpkg, out_path)

    logger.info("🎉 GPKG généré avec succès !")


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export intersections en GeoPackage depuis un WKT")
    parser.add_argument("--wkt", required=True, help="Chemin vers le WKT d'unité foncière")
    parser.add_argument("--out", required=False, default="intersections.gpkg")

    args = parser.parse_args()

    export_gpkg_from_wkt(args.wkt, args.out)
