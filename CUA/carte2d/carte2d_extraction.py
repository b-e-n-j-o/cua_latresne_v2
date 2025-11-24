#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
carte2d_extraction.py
----------------------------------------------------
Responsable de :
- la connexion à la base Supabase/PostGIS
- la lecture du catalogue
- la sélection des couches intersectant l'UF
- l'extraction SQL des entités (UF + buffer)
- la normalisation vers un format commun
"""

import os
import json
import logging
from pathlib import Path

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from CUA.map_utils import get_layers_on_parcel_with_buffer

logger = logging.getLogger("carte2d.extraction")
logger.setLevel(logging.INFO)

# ============================================================
# 🔌 Connexion DB
# ============================================================

load_dotenv()

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")

DATABASE_URL = (
    f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@"
    f"{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
)

ENGINE = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={
        "connect_timeout": 10,
        "sslmode": "require"
    }
)

SCHEMA_PAR_DEFAUT = "latresne"


# ============================================================
# 📚 Charger catalogue
# ============================================================

def charger_catalogue(catalogue_path: str | None = None):
    """
    Charge le catalogue même si 'catalogues/' est à côté du dossier CUA/.
    """
    # Si aucun chemin fourni : chercher automatiquement
    if not catalogue_path:

        # 📌 On remonte DEUX niveaux :
        # CUA/carte2d -> CUA -> racine projet (cua_latresne_v4)
        racine_projet = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

        default_path = os.path.join(
            racine_projet,
            "catalogues",
            "catalogue_couches_map.json"
        )

        catalogue_path = default_path

    catalogue_path = os.path.abspath(catalogue_path)

    if not os.path.exists(catalogue_path):
        raise FileNotFoundError(f"Catalogue introuvable : {catalogue_path}")

    with open(catalogue_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# 🧩 Sélection des couches intersectant l'UF
# ============================================================

def selectionner_couches_pour_parcelle(
    parcelle_wkt: str,
    schema: str = SCHEMA_PAR_DEFAUT,
    buffer_dist: int = 200,
    catalogue: dict | None = None,
    engine=ENGINE,
):
    if catalogue is None:
        catalogue = charger_catalogue()

    logger.info("🔍 Sélection des couches intersectant la parcelle...")
    layers = get_layers_on_parcel_with_buffer(
        engine, schema, catalogue, parcelle_wkt, buffer_dist
    )
    logger.info(f"   ✅ {len(layers)} couches retenues")
    return layers


# ============================================================
# 🧮 Extraction SQL d'une couche
# ============================================================

def extraire_entites_pour_couche(
    table: str,
    config: dict,
    parcelle_wkt: str,
    buffer_dist: int,
    schema: str = SCHEMA_PAR_DEFAUT,
    engine=ENGINE,
):
    keep = config.get("keep", [])
    attribut_map = config.get("attribut_map", None)
    attribut_split = config.get("attribut_split", None)

    mode_couche_entiere = (
        not attribut_map or str(attribut_map).lower() == "none"
    )

    if not mode_couche_entiere:
        select_cols_list = list(keep[:3]) if keep else []
        if attribut_map not in select_cols_list:
            select_cols_list.insert(0, attribut_map)
        if attribut_split and attribut_split not in select_cols_list:
            select_cols_list.append(attribut_split)
        select_cols = ", ".join(select_cols_list)
    else:
        select_cols = ", ".join(keep[:3]) if keep else "gml_id"

    sql = f"""
        WITH
          p AS (SELECT ST_GeomFromText(:wkt,2154) AS g),
          centroid AS (SELECT ST_Centroid(g) AS c FROM p),
          buffer AS (SELECT ST_Buffer(c,:buffer) AS b FROM centroid)
        SELECT
          ST_AsGeoJSON(ST_Transform(ST_Intersection(ST_MakeValid(t.geom_2154), buffer.b),4326)) AS geom,
          ROW_NUMBER() OVER() AS fid,
          {select_cols}
        FROM {schema}.{table} t, p, buffer
        WHERE t.geom_2154 IS NOT NULL
          AND ST_Intersects(ST_MakeValid(t.geom_2154), p.g)
        LIMIT 300;
    """

    logger.debug(f"SQL ({table}):\n{sql}")

    try:
        with engine.connect() as conn:
            rs = conn.execute(text(sql), {"wkt": parcelle_wkt, "buffer": buffer_dist})
            rows = rs.fetchall()
            keys = list(rs.keys())
    except Exception as e:
        logger.error(f"❌ ERREUR SQL ({table}): {e}")
        return [], []

    logger.info(f"   📊 {table}: {len(rows)} entités brutes")
    return rows, keys


# ============================================================
# 🎛 Normalisation d’une couche
# ============================================================

def normaliser_rows(rows, keys, table):
    features = []
    for row in rows:
        geom_json = row[0]
        if not geom_json:
            continue

        geom = json.loads(geom_json)
        props = {}

        for k, v in zip(keys[1:], row[1:]):  # skip geom
            props[k] = v

        features.append({"geom": geom, "props": props})

    return features


# ============================================================
# 🚀 Extraction Multicouche (API principale)
# ============================================================

def extract_all_layers(
    parcelle_wkt: str,
    buffer_dist: int = 200,
    schema: str = SCHEMA_PAR_DEFAUT,
    catalogue: dict | None = None,
    engine=ENGINE
):
    if catalogue is None:
        catalogue = charger_catalogue()

    layers_detected = selectionner_couches_pour_parcelle(
        parcelle_wkt=parcelle_wkt,
        schema=schema,
        buffer_dist=buffer_dist,
        catalogue=catalogue,
        engine=engine
    )

    result = {}

    for table, cfg in layers_detected.items():
        logger.info(f"📦 Extraction {table}...")

        rows, keys = extraire_entites_pour_couche(
            table=table,
            config=cfg,
            parcelle_wkt=parcelle_wkt,
            buffer_dist=buffer_dist,
            schema=schema,
            engine=engine
        )

        if not rows:
            continue

        features = normaliser_rows(rows, keys, table)
        result[table] = {
            "config": cfg,
            "features": features,
        }

    return result
