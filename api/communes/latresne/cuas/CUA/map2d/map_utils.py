#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
map_utils.py
----------------------------------------------------
Fonctions utilitaires pour la génération de cartes 2D.
"""

import random
from sqlalchemy import text
from shapely import wkt

# ============================================================
# FONCTIONS UTILITAIRES GÉNÉRALES
# ============================================================

def random_color():
    """Génère une couleur aléatoire pastel."""
    return f"#{random.randint(50,200):02x}{random.randint(50,200):02x}{random.randint(50,200):02x}"


def truncate_text(value, max_length=100):
    """Limite la longueur d’un texte dans les tooltips."""
    if isinstance(value, str) and len(value) > max_length:
        return value[:max_length] + "..."
    return value


def clean_properties(props, layer_name):
    """Nettoie les champs, supprime les IDs techniques et ajoute le nom de la couche."""
    ignore_patterns = ["id", "uuid", "gid", "fid", "globalid"]
    props_clean = {
        k: truncate_text(v)
        for k, v in props.items()
        if not any(pat in k.lower() for pat in ignore_patterns)
    }
    props_clean = {"__layer_name__": layer_name, **props_clean}
    return props_clean


def get_parcelle_geometry(engine, section, numero):
    """Récupère la géométrie WKT d’une parcelle via SQLAlchemy."""
    q = text("""
        SELECT ST_AsText(geom_2154)
        FROM latresne.parcelles_latresne
        WHERE section = :s AND numero = :n
    """)
    with engine.connect() as conn:
        row = conn.execute(q, {"s": section, "n": numero}).fetchone()
        if not row:
            raise ValueError(f"Parcelle {section} {numero} introuvable")
        return wkt.loads(row[0])


def get_layers_on_parcel_with_buffer(engine, schema, catalogue, parcelle_wkt, buffer_dist=200):
    """
    Sélectionne les couches qui intersectent STRICTEMENT l'unité foncière.

    ⚠️ Utilisé par d'autres scripts — NE PAS MODIFIER.
    """
    layers_on_parcel = {}

    with engine.connect() as conn:
        for table, cfg in catalogue.items():
            try:
                q_check = text(f"""
                    WITH parcelle AS (
                        SELECT ST_GeomFromText(:wkt, 2154) AS g
                    )
                    SELECT COUNT(*)
                    FROM {schema}.{table} t, parcelle
                    WHERE t.geom_2154 IS NOT NULL
                      AND ST_Intersects(ST_MakeValid(t.geom_2154), parcelle.g);
                """)
                count = conn.execute(q_check, {"wkt": parcelle_wkt}).scalar()

                if count and count > 0:
                    layers_on_parcel[table] = cfg

            except Exception:
                continue

    return layers_on_parcel


def get_layers_on_buffer(engine, schema, catalogue, parcelle_wkt, buffer_dist=200):
    """
    Sélectionne les couches qui intersectent le BUFFER du centroïde de l'UF.

    👉 Fonction dédiée carte 2D (contexte réglementaire élargi).
    """
    layers_on_buffer = {}

    with engine.connect() as conn:
        for table, cfg in catalogue.items():
            try:
                q_check = text(f"""
                    WITH
                      parcelle AS (
                        SELECT ST_GeomFromText(:wkt, 2154) AS g
                      ),
                      centroid AS (
                        SELECT ST_Centroid(g) AS c FROM parcelle
                      ),
                      buffer AS (
                        SELECT ST_Buffer(c, :buffer) AS b FROM centroid
                      )
                    SELECT COUNT(*)
                    FROM {schema}.{table} t, buffer
                    WHERE t.geom_2154 IS NOT NULL
                      AND ST_Intersects(ST_MakeValid(t.geom_2154), buffer.b);
                """)
                count = conn.execute(
                    q_check,
                    {"wkt": parcelle_wkt, "buffer": buffer_dist}
                ).scalar()

                if count and count > 0:
                    layers_on_buffer[table] = cfg

            except Exception:
                continue

    return layers_on_buffer
