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

from CUA.map_utils import get_layers_on_buffer

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
    buffer_dist: int = 150,
    catalogue: dict | None = None,
    engine=ENGINE,
):
    if catalogue is None:
        catalogue = charger_catalogue()

    # 📚 Log complet du catalogue
    logger.info("📚 Catalogue — liste complète des couches disponibles :")
    for layer_name, cfg in catalogue.items():
        table = cfg.get("table", layer_name)
        layer_type = cfg.get("type", "inconnu")
        geom_col = cfg.get("geom", "geom_2154")
        logger.info(f"   • {table} (type={layer_type}, geom={geom_col})")

    logger.info("🔍 Sélection des couches intersectant la parcelle (buffer-centroïde)...")
    layers = get_layers_on_buffer(
        engine, schema, catalogue, parcelle_wkt, buffer_dist
    )

    logger.info("🧭 Couches RETENUES pour intersection :")
    for table in layers.keys():
        logger.info(f"   ✅ {schema}.{table}")

    rejected = set(catalogue.keys()) - set(layers.keys())
    if rejected:
        logger.warning("🚫 Couches IGNORÉES (non candidates à l’intersection) :")
        for layer in sorted(rejected):
            logger.warning(f"   ❌ {layer}")

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

    # 📏 Contexte spatial de l'intersection
    logger.info(
        f"🔎 [{table}] Intersection avec buffer {buffer_dist} m (centroïde UF)"
    )
    logger.debug(f"WKT UF (tronqué): {parcelle_wkt[:120]}...")

    # ============================================================
    # 🔎 DEBUG — comptage intersections buffer
    # ============================================================
    sql_debug = f"""
        WITH
          p AS (SELECT ST_GeomFromText(:wkt, 2154) AS g),
          centroid AS (SELECT ST_Centroid(g) AS c FROM p),
          buffer AS (SELECT ST_Buffer(c, :buffer) AS b FROM centroid)
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (
            WHERE ST_Intersects(ST_MakeValid(t.geom_2154), buffer.b)
          ) AS intersect_buffer
        FROM {schema}.{table} t, buffer;
    """

    try:
        with engine.connect() as conn:
            total, intersect_buffer = conn.execute(
                text(sql_debug),
                {"wkt": parcelle_wkt, "buffer": buffer_dist},
            ).fetchone()

        logger.info(
            f"🧪 [{table}] {total} entités totales — "
            f"{intersect_buffer} intersectent le buffer {buffer_dist} m"
        )

        if intersect_buffer == 0:
            logger.warning(
                f"⚠️ [{table}] AUCUNE intersection avec le buffer — couche ignorée"
            )
            return [], []
    except Exception as e:
        logger.warning(
            f"⚠️ [{table}] Impossible de compter les entités / intersections : {e}"
        )

    # (Optionnel) — inspection des SRID détectés
    try:
        sql_srid = f"SELECT DISTINCT ST_SRID(geom_2154) FROM {schema}.{table} LIMIT 3;"
        with engine.connect() as conn:
            srids = [r[0] for r in conn.execute(text(sql_srid)).fetchall()]
        logger.info(f"🧭 [{table}] SRID détecté(s): {srids}")
    except Exception as e:
        logger.warning(f"⚠️ [{table}] Impossible de lire les SRID : {e}")

    # ⚙️ Extraction buffer uniquement (l'UF est incluse car contenue dans le buffer)
    sql = f"""
        WITH
          p AS (
            SELECT ST_GeomFromText(:wkt, 2154) AS g
          ),
          centroid AS (
            SELECT ST_Centroid(g) AS c FROM p
          ),
          buffer AS (
            SELECT ST_Buffer(c, :buffer) AS b FROM centroid
          )
        SELECT
          ST_AsGeoJSON(
            ST_Transform(
              ST_Intersection(ST_MakeValid(t.geom_2154), buffer.b),
              4326
            )
          ) AS geom,
          ROW_NUMBER() OVER () AS fid,
          {select_cols}
        FROM {schema}.{table} t, buffer
        WHERE t.geom_2154 IS NOT NULL
          AND ST_Intersects(ST_MakeValid(t.geom_2154), buffer.b)
        LIMIT 300;

    """

    logger.debug(f"SQL ({table}):\n{sql}")

    try:
        with engine.connect() as conn:
            rs = conn.execute(
                text(sql),
                {"wkt": parcelle_wkt, "buffer": buffer_dist},
            )
            rows = rs.fetchall()
            keys = list(rs.keys())
    except Exception as e:
        logger.error(f"❌ ERREUR SQL ({table}): {e}")
        return [], []

    if not rows:
        logger.warning(
            f"⚠️ [{table}] 0 entité intersecte le buffer "
            f"(geom={schema}.{table}.geom_2154)"
        )
    else:
        logger.info(
            f"✅ [{table}] {len(rows)} entité(s) intersectent le buffer"
        )

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
