# api/tiles_generic.py

import os
import time
import logging
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from fastapi import APIRouter, HTTPException, Response
from cachetools import LRUCache
from collections import defaultdict

router = APIRouter()
logger = logging.getLogger("tiles.generic")
logger.setLevel(logging.INFO)

DB_POOL = ThreadedConnectionPool(
    minconn=5,
    maxconn=40,
    host=os.getenv("SUPABASE_HOST"),
    dbname=os.getenv("SUPABASE_DB"),
    user=os.getenv("SUPABASE_USER"),
    password=os.getenv("SUPABASE_PASSWORD"),
    port=5432,
    connect_timeout=10
)

TILE_CACHE = LRUCache(maxsize=2000)

# ✅ Stats en mémoire
TILE_STATS = defaultdict(lambda: {
    "requests": 0,
    "cache_hits": 0,
    "cache_misses": 0,
    "errors": 0,
    "total_time": 0.0,
    "empty_tiles": 0
})

REGISTRY_SQL = """
SELECT
  table_schema,
  table_name,
  geom_column,
  minzoom,
  maxzoom
FROM carto.layer_registry
WHERE layer_id = %s
  AND is_active = true;
"""

MVT_SQL_TEMPLATE = """
SELECT ST_AsMVT(tile, %s, 4096, 'geom') AS mvt
FROM (
    SELECT
        *,
        ST_AsMVTGeom(
            {geom_column},
            ST_TileEnvelope(%s, %s, %s),
            4096,
            256,
            true
        ) AS geom
    FROM {table_schema}.{table_name}
    WHERE {geom_column} && ST_TileEnvelope(%s, %s, %s)
) tile;
"""

@router.get("/tiles/{layer}/{z}/{x}/{y}.mvt")
def get_tile(layer: str, z: int, x: int, y: int):
    t0 = time.time()
    
    # Stats : incrémenter requêtes
    TILE_STATS[layer]["requests"] += 1
    
    # Vérifier le cache
    cache_key = f"{layer}:{z}:{x}:{y}"
    if cache_key in TILE_CACHE:
        TILE_STATS[layer]["cache_hits"] += 1
        elapsed = time.time() - t0
        TILE_STATS[layer]["total_time"] += elapsed
        
        logger.info(f"[TILE CACHE HIT] layer={layer} z={z} x={x} y={y}")
        return Response(
            content=TILE_CACHE[cache_key],
            media_type="application/x-protobuf",
            headers={"X-Cache": "HIT"}
        )
    
    TILE_STATS[layer]["cache_misses"] += 1
    
    conn = None
    cur = None

    try:
        conn = DB_POOL.getconn()
        cur = conn.cursor()

        cur.execute(REGISTRY_SQL, (layer,))
        row = cur.fetchone()

        if not row:
            TILE_STATS[layer]["errors"] += 1
            raise HTTPException(status_code=404, detail=f"Layer '{layer}' not found")

        table_schema, table_name, geom_column, minzoom, maxzoom = row

        if (minzoom and z < minzoom) or (maxzoom and z > maxzoom):
            TILE_STATS[layer]["empty_tiles"] += 1
            elapsed = time.time() - t0
            TILE_STATS[layer]["total_time"] += elapsed
            return Response(content=b"", media_type="application/x-protobuf")

        sql = MVT_SQL_TEMPLATE.format(
            table_schema=table_schema,
            table_name=table_name,
            geom_column=geom_column
        )

        cur.execute(sql, (layer, z, x, y, z, x, y))
        tile = cur.fetchone()[0]
        
        elapsed = time.time() - t0
        TILE_STATS[layer]["total_time"] += elapsed
        duration = int(elapsed * 1000)

        if not tile:
            TILE_STATS[layer]["empty_tiles"] += 1
            logger.info(f"[TILE EMPTY] layer={layer} z={z} x={x} y={y} ({duration} ms)")
            return Response(content=b"", media_type="application/x-protobuf")

        # Mettre en cache
        TILE_CACHE[cache_key] = tile

        logger.info(f"[TILE OK] layer={layer} z={z} x={x} y={y} ({duration} ms)")

        return Response(
            content=tile,
            media_type="application/x-protobuf",
            headers={"X-Cache": "MISS"}
        )

    except Exception as e:
        TILE_STATS[layer]["errors"] += 1
        elapsed = time.time() - t0
        TILE_STATS[layer]["total_time"] += elapsed
        duration = int(elapsed * 1000)
        
        logger.error(f"[TILE ERROR] layer={layer} z={z} x={x} y={y} ({duration} ms): {e}")
        return Response(content=b"", media_type="application/x-protobuf")

    finally:
        if cur:
            cur.close()
        if conn:
            DB_POOL.putconn(conn)


# ✅ Endpoint de monitoring
@router.get("/tiles/stats")
def get_tile_stats():
    """
    Retourne les statistiques d'utilisation des tuiles
    Exemple: GET /tiles/stats
    """
    stats = {}
    
    for layer, data in TILE_STATS.items():
        requests = data["requests"]
        total_time = data["total_time"]
        
        stats[layer] = {
            "requests": requests,
            "cache_hits": data["cache_hits"],
            "cache_misses": data["cache_misses"],
            "cache_hit_rate": round(data["cache_hits"] / requests * 100, 1) if requests > 0 else 0,
            "errors": data["errors"],
            "empty_tiles": data["empty_tiles"],
            "avg_time_ms": round(total_time / requests * 1000, 2) if requests > 0 else 0,
            "total_time_seconds": round(total_time, 2)
        }
    
    # Stats globales du cache
    cache_info = {
        "size": len(TILE_CACHE),
        "maxsize": TILE_CACHE.maxsize,
        "usage_pct": round(len(TILE_CACHE) / TILE_CACHE.maxsize * 100, 1)
    }
    
    return {
        "by_layer": stats,
        "cache": cache_info,
        "pool": {
            "description": "Connection pool unavailable in stats (use DB monitoring)"
        }
    }