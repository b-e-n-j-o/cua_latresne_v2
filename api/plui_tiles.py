#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
plui_tiles.py
-------------
Expose les tuiles vectorielles (MVT) du PLU/PLUi unifié.
Version optimisée (geom_3857 + index).
Table unifiée : plu.plu_zonage_all (PLU + PLUi)
"""

import time
import logging
import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from fastapi import APIRouter, Response

logger = logging.getLogger("plui.tiles")
logger.setLevel(logging.INFO)

router = APIRouter()

# Pool de connexions PostgreSQL pour éviter "max clients reached"
DB_POOL = SimpleConnectionPool(
    minconn=1,
    maxconn=5,  # ⚠️ important avec Supabase
    host=os.getenv("SUPABASE_HOST"),
    dbname=os.getenv("SUPABASE_DB"),
    user=os.getenv("SUPABASE_USER"),
    password=os.getenv("SUPABASE_PASSWORD"),
    port=5432,
)

SQL_MVT = """
SELECT ST_AsMVT(tile, 'plu', 4096, 'geom') AS mvt
FROM (
    SELECT
        insee,
        commune,
        source_type,
        libelle,
        typezone,
        ST_AsMVTGeom(
            geom_3857,
            ST_TileEnvelope(%s, %s, %s),
            4096,
            256,
            true
        ) AS geom
    FROM plu.plu_zonage_all
    WHERE geom_3857 && ST_TileEnvelope(%s, %s, %s)
) tile;
"""

@router.get("/tiles/plui/{z}/{x}/{y}.mvt")
def get_plui_tile(z: int, x: int, y: int):
    t0 = time.time()
    conn = None
    cur = None
    
    try:
        conn = DB_POOL.getconn()
        cur = conn.cursor()
        cur.execute(SQL_MVT, (z, x, y, z, x, y))
        row = cur.fetchone()

        duration_ms = int((time.time() - t0) * 1000)

        if not row or not row[0]:
            logger.info(
                f"[PLUI TILE] z={z} x={x} y={y} → empty ({duration_ms} ms)"
            )
            return Response(
                content=b"",
                media_type="application/x-protobuf"
            )

        tile_bytes = row[0]
        size_kb = len(tile_bytes) / 1024

        logger.info(
            f"[PLUI TILE] z={z} x={x} y={y} "
            f"size={size_kb:.1f} KB time={duration_ms} ms"
        )

        return Response(
            content=tile_bytes,
            media_type="application/x-protobuf"
        )
    finally:
        if cur:
            cur.close()
        if conn:
            DB_POOL.putconn(conn)
