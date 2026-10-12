"""Tuiles MVT des subdivisions cadastrales (schémas latresne et argeles)."""
import json

from fastapi import APIRouter, HTTPException, Path, Response
from pydantic import BaseModel

from api.tiles_generic import DB_POOL

router = APIRouter(prefix="/communes/{commune}/subdivisions", tags=["subdivisions"])

SCHEMAS = {"latresne", "argeles"}
_SRID: dict[str, int] = {}

SQL = """
SELECT ST_AsMVT(q, 'subdivisions', 4096, 'geom')
FROM (
  SELECT
    parcelle,
    lettre,
    ST_AsMVTGeom(
      ST_Transform(
        CASE WHEN ST_SRID(geom) = 0 THEN ST_SetSRID(geom, %s) ELSE geom END,
        3857
      ),
      ST_TileEnvelope(%s, %s, %s),
      4096, 64, true
    ) AS geom
  FROM {schema}.subdivisions
  WHERE geom && ST_Transform(ST_TileEnvelope(%s, %s, %s), %s)
) q
WHERE geom IS NOT NULL
"""


def _srid(cur, schema: str) -> int:
    known = _SRID.get(schema)
    if known:
        return known
    cur.execute(
        f"SELECT ST_SRID(geom) FROM {schema}.subdivisions WHERE geom IS NOT NULL LIMIT 1"
    )
    row = cur.fetchone()
    srid = row[0] if row and row[0] else 2154
    if srid == 0:
        srid = 2154
    _SRID[schema] = srid
    return srid


@router.get("/{z}/{x}/{y}.mvt")
def tuile_subdivisions(
    commune: str,
    z: int = Path(..., ge=0, le=22),
    x: int = Path(...),
    y: int = Path(...),
):
    schema = commune.strip().lower()
    if schema not in SCHEMAS:
        raise HTTPException(404, "Commune sans subdivisions")
    if not (0 <= x < 2**z and 0 <= y < 2**z):
        raise HTTPException(422, "tuile hors grille")

    conn = DB_POOL.getconn()
    try:
        cur = conn.cursor()
        srid = _srid(cur, schema)
        cur.execute(SQL.format(schema=schema), (srid, z, x, y, z, x, y, srid))
        tile = cur.fetchone()[0]
    finally:
        DB_POOL.putconn(conn)

    if not tile:
        return Response(content=b"", media_type="application/vnd.mapbox-vector-tile")
    return Response(content=bytes(tile), media_type="application/vnd.mapbox-vector-tile")


class _GeomBody(BaseModel):
    geometry: dict


# Part de la surface de la subdivision qui doit recouvrir la parcelle.
SEUIL = 0.05

SQL_INTERSECTION = """
WITH parcelle AS (
  SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s) AS g
)
SELECT json_build_object(
  'type', 'FeatureCollection',
  'features', coalesce(json_agg(json_build_object(
    'type', 'Feature',
    'id', s.gid,
    'geometry', ST_AsGeoJSON(ST_Transform(
      CASE WHEN ST_SRID(s.geom) = 0 THEN ST_SetSRID(s.geom, %s) ELSE s.geom END,
      4326
    ))::json,
    'properties', json_build_object('parcelle', s.parcelle, 'lettre', s.lettre)
  )), '[]'::json)
)
FROM {schema}.subdivisions s, parcelle p
WHERE ST_Area(s.geom) > 0
  AND s.geom && p.g
  AND ST_Intersects(s.geom, p.g)
  AND ST_Area(ST_Intersection(
        CASE WHEN ST_SRID(s.geom) = 0 THEN ST_SetSRID(s.geom, %s) ELSE s.geom END,
        p.g
      )) >= %s * ST_Area(
        CASE WHEN ST_SRID(s.geom) = 0 THEN ST_SetSRID(s.geom, %s) ELSE s.geom END
      )
"""


@router.post("/intersection")
def subdivisions_de_la_parcelle(commune: str, body: _GeomBody):
    schema = commune.strip().lower()
    if schema not in SCHEMAS:
        raise HTTPException(404, "Commune sans subdivisions")
    if body.geometry.get("type") not in {
        "Polygon", "MultiPolygon", "GeometryCollection",
    }:
        raise HTTPException(422, "géométrie de parcelle attendue")

    conn = DB_POOL.getconn()
    try:
        cur = conn.cursor()
        srid = _srid(cur, schema)
        cur.execute(
            SQL_INTERSECTION.format(schema=schema),
            (json.dumps(body.geometry), srid, srid, srid, SEUIL, srid),
        )
        payload = cur.fetchone()[0]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        DB_POOL.putconn(conn)

    return payload if isinstance(payload, dict) else json.loads(payload)
