# -*- coding: utf-8 -*-
"""Calcul géométrique léger pour l'historique carte (centroïde WGS84)."""

from __future__ import annotations

import logging

logger = logging.getLogger("cua")


def compute_centroid_from_wkt_l93(wkt_str: str | None) -> dict | None:
    """Centroïde lon/lat (EPSG:4326) depuis une géométrie WKT Lambert-93 (EPSG:2154)."""
    if not wkt_str or not str(wkt_str).strip():
        return None
    try:
        from pyproj import Transformer
        from shapely import wkt as shapely_wkt

        geom = shapely_wkt.loads(str(wkt_str).strip())
        if geom.is_empty:
            return None
        c = geom.centroid
        transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
        lon, lat = transformer.transform(c.x, c.y)
        return {"lon": float(lon), "lat": float(lat)}
    except Exception as exc:
        logger.warning("Centroïde UF non calculé : %s", exc)
        return None
