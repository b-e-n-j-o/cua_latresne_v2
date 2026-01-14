"""
Expose les tuiles vectorielles (MVT) des couches latresne.  
"""

# api/tiles_latresne.py
import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from fastapi import APIRouter, Response, HTTPException
from functools import lru_cache

router = APIRouter(prefix="/latresne")

DB_POOL = SimpleConnectionPool(
    minconn=2,
    maxconn=20,  # Augmenté pour supporter ~20 tuiles en parallèle de MapLibre
    host=os.getenv("SUPABASE_HOST"),
    dbname=os.getenv("SUPABASE_DB"),
    user=os.getenv("SUPABASE_USER"),
    password=os.getenv("SUPABASE_PASSWORD"),
    port=5432,
)

MVT_SQL = """
SELECT ST_AsMVT(tile, %s, 4096, 'geom') AS mvt
FROM (
    SELECT *, ST_AsMVTGeom({geom_column}, ST_TileEnvelope(%s, %s, %s), 4096, 256, true) AS geom
    FROM latresne.{table_name}
    WHERE {geom_column} && ST_TileEnvelope(%s, %s, %s)
) tile;
"""

@lru_cache(maxsize=2000)
def get_tile_cached(layer: str, z: int, x: int, y: int) -> bytes | None:
    """
    Récupère une tuile MVT depuis la base de données avec cache LRU.
    Le cache évite de refrapper la base pour les mêmes tuiles.
    """
    conn = DB_POOL.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name, geom_column, minzoom, maxzoom FROM latresne.layer_registry WHERE layer_id = %s AND is_active = true",
            (layer,)
        )
        row = cur.fetchone()
        
        if not row:
            return None
        
        table_name, geom_column, minzoom, maxzoom = row
        
        # Retourner None si hors zoom
        if z < (minzoom or 0) or z > (maxzoom or 22):
            return None
        
        sql = MVT_SQL.format(table_name=table_name, geom_column=geom_column)
        cur.execute(sql, (layer, z, x, y, z, x, y))
        tile = cur.fetchone()[0]
        
        return tile if tile else None
    finally:
        DB_POOL.putconn(conn)

@router.get("/tiles/{layer}/{z}/{x}/{y}.mvt")
def get_tile(layer: str, z: int, x: int, y: int):
    """
    Endpoint pour récupérer une tuile MVT avec cache LRU.
    """
    tile = get_tile_cached(layer, z, x, y)
    
    if tile is None:
        return Response(
            content=b"",
            media_type="application/x-protobuf",
            headers={"Cache-Control": "public, max-age=3600"}
        )
    
    return Response(
        content=tile,
        media_type="application/x-protobuf",
        headers={"Cache-Control": "public, max-age=3600"}
    )

@router.get("/layers")
def get_layers():
    """Retourne la liste des couches avec leur config"""
    conn = DB_POOL.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT layer_id, nom, type, attribut_map, minzoom, maxzoom 
            FROM latresne.layer_registry 
            WHERE is_active = true 
            ORDER BY type, nom
        """)
        return [
            {
                "id": r[0],
                "nom": r[1],
                "type": r[2],
                "attribut_map": r[3],
                "minzoom": r[4] or 0,
                "maxzoom": r[5] or 22
            }
            for r in cur.fetchall()
        ]
    finally:
        DB_POOL.putconn(conn)