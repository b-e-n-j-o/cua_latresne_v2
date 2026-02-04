"""
Expose les tuiles vectorielles (MVT) des couches carto.
"""

# api/tiles_generic.py
import os
import time
import logging
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from fastapi import APIRouter, HTTPException, Response

router = APIRouter()
logger = logging.getLogger("tiles.generic")
logger.setLevel(logging.INFO)

DB_POOL = SimpleConnectionPool(
    minconn=1,
    maxconn=5,
    host=os.getenv("SUPABASE_HOST"),
    dbname=os.getenv("SUPABASE_DB"),
    user=os.getenv("SUPABASE_USER"),
    password=os.getenv("SUPABASE_PASSWORD"),
    port=5432,
)

REGISTRY_SQL = """
SELECT table_schema, table_name, geom_column, minzoom, maxzoom
FROM carto.layer_registry
WHERE layer_id = %s AND is_active = true;
"""

MVT_SQL_TEMPLATE = """
SELECT ST_AsMVT(tile, %s, 4096, 'geom') AS mvt
FROM (
    SELECT *, ST_AsMVTGeom({geom_column}, ST_TileEnvelope(%s, %s, %s, 3857), 4096, 2048, true) AS geom
    FROM {table_schema}.{table_name}
    WHERE {geom_column} && ST_TileEnvelope(%s, %s, %s, 3857)
) tile;
"""

@router.get("/tiles/{layer}/{z}/{x}/{y}.mvt")
def get_tile(layer: str, z: int, x: int, y: int):
    t0 = time.time()
    conn = None
    cur = None

    try:
        conn = DB_POOL.getconn()
        cur = conn.cursor()

        cur.execute(REGISTRY_SQL, (layer,))
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Layer '{layer}' not found")

        table_schema, table_name, geom_column, minzoom, maxzoom = row

        if (minzoom and z < minzoom) or (maxzoom and z > maxzoom):
            return Response(content=b"", media_type="application/x-protobuf")

        sql = MVT_SQL_TEMPLATE.format(
            table_schema=table_schema,
            table_name=table_name,
            geom_column=geom_column
        )

        cur.execute(sql, (layer, z, x, y, z, x, y))
        tile = cur.fetchone()[0]
        duration = int((time.time() - t0) * 1000)

        if not tile:
            logger.info(f"[TILE EMPTY] layer={layer} z={z} ({duration} ms)")
            return Response(content=b"", media_type="application/x-protobuf")

        logger.info(f"[TILE OK] layer={layer} z={z} ({duration} ms)")
        return Response(
            content=tile,
            media_type="application/x-protobuf",
            headers={"Cache-Control": "public, max-age=3600"}
        )

    except Exception as e:
        logger.error(f"[TILE ERROR] layer={layer}: {e}")
        return Response(content=b"", media_type="application/x-protobuf")

    finally:
        if cur:
            cur.close()
        if conn:
            DB_POOL.putconn(conn)  # ← SEULE MODIFICATION