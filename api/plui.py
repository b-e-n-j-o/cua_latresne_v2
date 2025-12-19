# backend/api/plui.py
from fastapi import APIRouter
import psycopg2
import json
import os

router = APIRouter()

conn = psycopg2.connect(
    host=os.getenv("SUPABASE_HOST"),
    dbname=os.getenv("SUPABASE_DB"),
    user=os.getenv("SUPABASE_USER"),
    password=os.getenv("SUPABASE_PASSWORD"),
    port=5432,
)

# ============================================================================
# ⚠️ OBSOLÈTE POUR LA CARTE — remplacé par /tiles/plui/{z}/{x}/{y}.mvt
# ============================================================================
# Cet endpoint est conservé pour :
#   - Export / analyse de données
#   - Debug / tests
#   - Fallback éventuel
# La carte utilise désormais les vector tiles (MVT) via plui_tiles.py
# ============================================================================

# @router.get("/plui/bdx")
# def get_plui_bdx(bbox: str | None = None):
#     cur = conn.cursor()
#     
#     if bbox:
#         # Parse bbox: "minLng,minLat,maxLng,maxLat"
#         xmin, ymin, xmax, ymax = map(float, bbox.split(","))
#         cur.execute("""
#             SELECT
#               lib_idzone,
#               libelle,
#               typezone,
#               ST_AsGeoJSON(ST_Transform(geometry, 4326))
#             FROM plui.plui_bdx_zone_urba
#             WHERE geometry IS NOT NULL
#               AND ST_Intersects(
#                 ST_Transform(geometry, 4326),
#                 ST_MakeEnvelope(%s, %s, %s, %s, 4326)
#               )
#         """, (xmin, ymin, xmax, ymax))
#     else:
#         # Sans bbox, retourner toutes les données (comportement par défaut)
#         cur.execute("""
#             SELECT
#               lib_idzone,
#               libelle,
#               typezone,
#               ST_AsGeoJSON(ST_Transform(geometry, 4326))
#             FROM plui.plui_bdx_zone_urba
#             WHERE geometry IS NOT NULL
#         """)
# 
#     features = []
#     for lib_idzone, libelle, typezone, geom in cur.fetchall():
#         features.append({
#             "type": "Feature",
#             "geometry": json.loads(geom),
#             "properties": {
#                 "lib_idzone": lib_idzone,
#                 "libelle": libelle,
#                 "typezone": typezone
#             }
#         })
# 
#     return {
#         "type": "FeatureCollection",
#         "features": features
#     }
