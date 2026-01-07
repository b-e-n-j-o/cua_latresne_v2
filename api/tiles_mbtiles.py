# api/tiles_mbtiles.py
import sqlite3
from fastapi import APIRouter, Response
from functools import lru_cache

router = APIRouter()

@lru_cache(maxsize=1)
def get_conn():
    return sqlite3.connect("/mnt/tiles-cache/plui_bordeaux.mbtiles", check_same_thread=False)

@router.get("/tiles/plui-bordeaux/{z}/{x}/{y}.mvt")
def get_tile(z: int, x: int, y: int):
    y_tms = (2**z - 1) - y
    cursor = get_conn().execute(
        "SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
        (z, x, y_tms)
    )
    row = cursor.fetchone()
    return Response(
        content=row[0] if row else b"",
        media_type="application/x-protobuf",
        headers={"Cache-Control": "public, max-age=2592000"}
    )