# api/tiles_mbtiles.py
import sqlite3
from fastapi import APIRouter, Response, HTTPException
from functools import lru_cache
from pathlib import Path

router = APIRouter()

MBTILES_DIR = Path(__file__).parent / "mbtiles"

@lru_cache(maxsize=5)
def get_conn(name: str) -> sqlite3.Connection:
    path = MBTILES_DIR / f"{name}.mbtiles"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"MBTiles '{name}' not found")
    return sqlite3.connect(path, check_same_thread=False)

@router.get("/tiles/{name}/{z}/{x}/{y}.mvt")
def get_tile(name: str, z: int, x: int, y: int):
    conn = get_conn(name)

    # MBTiles = schéma TMS
    y_tms = (2 ** z - 1) - y

    row = conn.execute(
        """
        SELECT tile_data
        FROM tiles
        WHERE zoom_level = ?
          AND tile_column = ?
          AND tile_row = ?
        """,
        (z, x, y_tms)
    ).fetchone()

    if not row or not row[0]:
        return Response(status_code=204)

    tile = row[0]

    headers = {
        "Content-Type": "application/x-protobuf",
        "Cache-Control": "public, max-age=31536000, immutable"
    }

    # gzip auto
    if tile[:2] == b"\x1f\x8b":
        headers["Content-Encoding"] = "gzip"

    return Response(content=tile, headers=headers)
